"""Ops API routes: worker state, audit log, broker status, console SSE."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from bifrost_api.ops.celery_redis_logs import celery_log_reader_single_stream
from bifrost_api.ops.models.schemas import (
    AuditEntry,
    BrokerControlRequest,
    ScaleRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ops"])

OPS_SHUTDOWN_EXIT_DELAY_SEC = 2.5


# ── helpers ───────────────────────────────────────────────────────────────────


def _worker_svc(request: Request):
    return request.app.state.worker_state_service


def _executor(request: Request):
    return request.app.state.executor


def _audit_log(request: Request) -> list:
    return getattr(request.app.state, "audit_log", [])


def _ops_auth(request: Request):
    from bifrost_api.ops.auth import OpsAuth
    return getattr(request.app.state, "ops_auth", OpsAuth.__new__(OpsAuth))


async def _wait_worker_unit_quiet(exc: Any, unit: str) -> str:
    """Poll ``systemctl_is_active`` / subprocess view until unit is no longer busy shutting down."""
    if not hasattr(exc, "systemctl_is_active"):
        return "unknown"
    last = "unknown"
    busy = frozenset({"active", "activating", "deactivating", "reloading"})
    for _ in range(24):
        last = await exc.systemctl_is_active(unit)
        if last == "unknown":
            return last
        if last not in busy:
            return last
        await asyncio.sleep(0.35)
    return last


def _identity(request: Request):
    return _ops_auth(request).resolve(request)


def _operator(request: Request) -> str:
    return _identity(request).name


def _role(request: Request) -> str:
    return _identity(request).role


def _audit(
    request: Request,
    action: str,
    target: str,
    outcome: str,
    command_id: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    ident = _identity(request)
    entry = AuditEntry(
        operator=ident.name,
        source_ip=request.client.host if request.client else None,
        action=action,
        target=target,
        command_id=command_id,
        outcome=outcome,
        detail=detail,
    )
    audit_store = getattr(request.app.state, "audit_store", None)
    if audit_store is not None:
        audit_store.append(entry)
    else:
        _audit_log(request).append(entry)
    logger.info(
        "AUDIT: %s %s -> %s by %s from %s",
        action,
        target,
        outcome,
        ident.name,
        entry.source_ip,
    )


def _require_role(request: Request, minimum: str) -> Optional[JSONResponse]:
    """Return a 403 JSONResponse if the caller lacks the required role, else None."""
    _, denied = _ops_auth(request).require_role(request, minimum)
    return denied


# ── Auth / capabilities ───────────────────────────────────────────────────────


@router.get("/ops/auth/capabilities")
def auth_capabilities(request: Request) -> Dict[str, Any]:
    """Return the caller's identity, role, and capabilities."""
    return _ops_auth(request).capabilities(request)


@router.post("/ops/shutdown")
def post_ops_shutdown(request: Request) -> Any:
    """Terminate the Ops API process (``run_server_ops.py`` / uvicorn). Requires operator role."""
    denied = _require_role(request, "operator")
    if denied:
        _audit(request, "ops_shutdown", "process", "denied", detail=f"role={_role(request)}")
        return denied
    _audit(request, "ops_shutdown", "process", "scheduled", detail="process exit")

    def _exit_after_send() -> None:
        time.sleep(OPS_SHUTDOWN_EXIT_DELAY_SEC)
        logger.info("Ops API shutdown: exiting process.")
        os._exit(0)

    threading.Thread(target=_exit_after_send, daemon=True).start()
    return {"ok": True}


# ── Worker status ─────────────────────────────────────────────────────────────


@router.get("/ops/workers")
def list_workers(
    request: Request,
    force_refresh: bool = Query(
        False,
        description="Re-scan Redis worker presence keys only (bifrost:ops:worker_presence:*); no Celery inspect.",
    ),
) -> Dict[str, Any]:
    """All workers with aggregated status + broker overview."""
    svc = _worker_svc(request)
    workers = svc.list_workers(force_refresh=force_refresh)
    broker = svc.broker_status()
    return {
        "ok": True,
        "workers": [w.model_dump() for w in workers],
        "broker": broker,
        "count": len(workers),
    }


# ── Scale / instances (static paths — must precede {worker_id:path}) ───────────


@router.get("/ops/workers/profiles")
def list_worker_profiles(request: Request) -> Dict[str, Any]:
    """Return available worker profiles for the Add Instance dropdown."""
    registry = getattr(request.app.state, "worker_profile_registry", None)
    if registry is None:
        return {"ok": True, "profiles": [], "count": 0}
    profiles = registry.list_profiles()
    return {"ok": True, "profiles": profiles, "count": len(profiles)}


@router.post("/ops/workers/scale")
async def scale_worker(
    request: Request, body: ScaleRequest = Body(...),
) -> Any:
    """Add or remove a systemd template-instance worker.

    **add**: requires ``worker_type`` (profile key); backend allocates instance_id.
    **remove**: requires ``instance_id``.
    """
    denied = _require_role(request, "operator")
    if denied:
        target = body.instance_id or body.worker_type or "?"
        _audit(request, f"scale_{body.action.value}", f"instance:{target}", "denied",
               detail=f"role={_role(request)}")
        return denied

    exc = _executor(request)

    if body.action.value == "add":
        # Typed scaling: allocate instance_id from profile
        if not body.worker_type:
            return JSONResponse(status_code=400,
                                content={"ok": False, "error": "worker_type is required for add."})

        registry = getattr(request.app.state, "worker_profile_registry", None)
        if registry is None or registry.get(body.worker_type) is None:
            return JSONResponse(status_code=400,
                                content={"ok": False,
                                         "error": f"Unknown worker_type {body.worker_type!r}."})

        from bifrost_api.ops.worker_profiles import allocate_instance_id

        broker_url = getattr(request.app.state, "broker_url", "")
        try:
            existing = await exc.list_instances()
            existing_units = [i.get("unit", "") for i in existing]
        except Exception:
            existing_units = []

        try:
            instance_id = allocate_instance_id(body.worker_type, broker_url, existing_units)
        except Exception as e:
            return JSONResponse(status_code=500,
                                content={"ok": False, "error": f"ID allocation failed: {e}"})

        try:
            unit = exc.instance_unit(instance_id)
        except ValueError as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

        try:
            exc._validate("start", unit)
            result = await exc._systemctl("start", unit)
        except PermissionError as e:
            _audit(request, "scale_add", unit, "rejected", detail=str(e))
            return JSONResponse(status_code=403, content={"ok": False, "error": str(e)})
        except Exception as e:
            _audit(request, "scale_add", unit, "failed", detail=str(e))
            return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

        profile = registry.get(body.worker_type)
        _audit(request, "scale_add", unit, "success",
               detail=f"worker_type={body.worker_type}, queues={profile.queues if profile else []}")
        # #region agent log
        logger.info(
            "debug-6a885c scale_add unit=%s instance_id=%s method=%s",
            unit,
            instance_id,
            (result or {}).get("method"),
        )
        # #endregion
        return {
            "ok": True, "action": "add", "unit": unit,
            "instance_id": instance_id, "worker_type": body.worker_type,
            "result": result,
        }

    # ── remove ────────────────────────────────────────────────────────────
    if not body.instance_id:
        return JSONResponse(status_code=400,
                            content={"ok": False, "error": "instance_id is required for remove."})

    try:
        unit = exc.instance_unit(body.instance_id)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

    try:
        exc._validate("stop", unit)
        result = await exc._systemctl("stop", unit)
    except PermissionError as e:
        _audit(request, "scale_remove", unit, "rejected", detail=str(e))
        return JSONResponse(status_code=403, content={"ok": False, "error": str(e)})
    except Exception as e:
        _audit(request, "scale_remove", unit, "failed", detail=str(e))
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

    after_state = await _wait_worker_unit_quiet(exc, unit)
    force_result: Optional[Dict[str, Any]] = None
    if after_state == "active" and body.force:
        if not hasattr(exc, "force_stop_worker_unit"):
            detail = "Force remove is not supported by this Ops executor."
            _audit(request, "scale_remove", unit, "failed", detail=detail)
            return JSONResponse(
                status_code=501,
                content={
                    "ok": False,
                    "error": detail,
                    "action": "remove",
                    "unit": unit,
                    "instance_id": body.instance_id,
                    "after_state": after_state,
                    "result": result,
                },
            )
        try:
            force_result = await exc.force_stop_worker_unit(unit)
        except Exception as e:
            _audit(request, "scale_remove", unit, "failed", detail=f"force_kill:{e}")
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": str(e), "action": "remove", "unit": unit},
            )
        after_state = await _wait_worker_unit_quiet(exc, unit)

    if after_state == "active":
        detail = (
            "Unit still reports active after stop. Another machine may be running a worker with the "
            "same instance id on this Redis broker, or the process was not started by this unit."
        )
        if body.force:
            detail += " A force kill was already attempted on this host."
        else:
            detail += " Retry remove with force=true to SIGKILL the local unit/process."
        _audit(request, "scale_remove", unit, "failed", detail=f"after_state={after_state}")
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "error": detail,
                "action": "remove",
                "unit": unit,
                "instance_id": body.instance_id,
                "after_state": after_state,
                "result": result,
                **({"force_result": force_result} if force_result is not None else {}),
            },
        )

    audit_detail = f"after_state={after_state}"
    if force_result is not None:
        audit_detail += ",force_kill=ok"
    _audit(request, "scale_remove", unit, "success", detail=audit_detail)
    out: Dict[str, Any] = {
        "ok": True,
        "action": "remove",
        "unit": unit,
        "instance_id": body.instance_id,
        "after_state": after_state,
        "result": result,
    }
    if force_result is not None:
        out["force_result"] = force_result
    return out


@router.get("/ops/workers/instances")
async def list_instances(request: Request) -> Dict[str, Any]:
    """List known systemd bifrost-celery-worker@* template instances."""
    exc = _executor(request)
    try:
        instances = await exc.list_instances()
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"Failed to list instances: {e}"},
        )
    # #region agent log
    logger.info(
        "debug-6a885c list_instances count=%s units=%s",
        len(instances),
        [i.get("unit") for i in instances],
    )
    # #endregion
    return {"ok": True, "instances": instances, "count": len(instances)}


# ── Single worker detail ──────────────────────────────────────────────────────


@router.get("/ops/workers/{worker_id:path}")
def get_worker(request: Request, worker_id: str) -> Any:
    """Single worker detail (queues, concurrency, active/reserved tasks, stats)."""
    svc = _worker_svc(request)
    detail = svc.get_worker(worker_id)
    if detail is None:
        return JSONResponse(
            status_code=404,
            content={
                "ok": False,
                "error": f"Worker {worker_id!r} not found or not responding.",
            },
        )
    return {"ok": True, "worker": detail.model_dump()}


# ── Queue summary (read-only) ────────────────────────────────────────────────


@router.get("/ops/queues/summary")
def queue_summary(request: Request) -> Dict[str, Any]:
    """Per-queue broker backlog, Celery running count, and PostgreSQL job totals."""
    svc = _worker_svc(request)
    data = svc.queue_summaries()
    return {"ok": True, **data}


# ── Broker ────────────────────────────────────────────────────────────────────


@router.get("/ops/broker/status")
async def broker_status_extended(request: Request) -> Dict[str, Any]:
    """Extended broker status including local-management detection."""
    import anyio
    svc = _worker_svc(request)
    # broker_status() makes blocking Redis/network calls; run in thread pool to avoid blocking the event loop.
    status = await anyio.to_thread.run_sync(svc.broker_status)
    exc = _executor(request)
    locally_managed = await exc.redis_is_local()
    status["locally_managed"] = locally_managed
    return {"ok": True, "broker": status}


@router.post("/ops/broker/control")
async def broker_control(
    request: Request, body: BrokerControlRequest = Body(...),
) -> Any:
    """Start / stop / restart the local Redis broker. Requires admin role."""
    denied = _require_role(request, "admin")
    if denied:
        _audit(
            request,
            f"broker_{body.action.value}",
            "redis",
            "denied",
            detail=f"role={_role(request)}",
        )
        return denied

    exc = _executor(request)
    if not await exc.redis_is_local():
        _audit(
            request,
            f"broker_{body.action.value}",
            "redis",
            "rejected",
            detail="Redis not locally managed via systemd",
        )
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "Redis is not locally managed; cannot control via systemd.",
            },
        )

    try:
        result = await exc.systemctl_redis(body.action.value)
    except Exception as e:
        _audit(
            request,
            f"broker_{body.action.value}",
            "redis",
            "failed",
            detail=str(e),
        )
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e)},
        )

    _audit(
        request,
        f"broker_{body.action.value}",
        "redis",
        "success",
    )
    return {"ok": True, "action": body.action.value, "result": result}


# ── Console log streaming (SSE) ───────────────────────────────────────


def _ops_broker_ping_ok(request: Request) -> bool:
    try:
        import redis

        r = redis.from_url(request.app.state.broker_url)
        r.ping()
        return True
    except Exception:
        return False


def _journalctl_available() -> bool:
    return shutil.which("journalctl") is not None


def _tail_available() -> bool:
    return shutil.which("tail") is not None


def _console_tail_file(unit: str, request: Request | None) -> Optional[Path]:
    """When ``journalctl`` is unavailable (e.g. macOS), tail broker log file if configured."""
    if unit == "redis" or unit == "redis.service":
        raw = (os.environ.get("BIFROST_BROKER_CONSOLE_LOG") or "").strip()
        return Path(raw).expanduser().resolve() if raw else None
    return None


def _console_fallback_notice(_unit: str, _tail_path: Optional[Path]) -> str:
    return (
        "Console: journalctl not found (typical on macOS). "
        "Set BIFROST_BROKER_CONSOLE_LOG to a Redis log file to stream via tail -F, "
        "or use the Ops API on Linux with systemd."
    )


def _journalctl_cmd(unit: str, lines: int) -> List[str]:
    """Build journalctl argv. Use ``BIFROST_JOURNAL_USE_SUDO=1`` when the Ops user
    cannot read system journals without sudo (see ``deploy/sudoers/bifrost-ops-journalctl``).
    """
    base = [
        "journalctl",
        "-u",
        unit,
        "-f",
        "--no-pager",
        "-o",
        "short-iso",
        "-n",
        str(lines),
    ]
    if os.environ.get("BIFROST_JOURNAL_USE_SUDO", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return ["sudo", "-n"] + base
    return base


def _tail_follow_cmd(path: str, lines: int) -> List[str]:
    return ["tail", "-n", str(lines), "-F", path]


async def _stream_command_sse(
    cmd: List[str],
    request: Request | None,
    *,
    stderr_label: str,
) -> AsyncGenerator[str, None]:
    logger.debug("Console SSE cmd: %s", cmd)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as e:
        yield f"data: Console: failed to spawn {cmd[0]!r}: {e}\n\n"
        return

    async def _drain_stderr(p: asyncio.subprocess.Process) -> None:
        if p.stderr is None:
            return
        while True:
            raw = await p.stderr.readline()
            if not raw:
                break
            text = raw.decode("utf-8", errors="replace").rstrip()
            logger.warning("%s stderr: %s", stderr_label, text)

    drain = asyncio.create_task(_drain_stderr(proc))
    try:
        assert proc.stdout is not None
        while True:
            if request is not None and await request.is_disconnected():
                break
            try:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=30)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            yield f"data: {text}\n\n"
    finally:
        drain.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await drain
        proc.kill()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            proc.kill()


async def _journal_sse(
    unit: str, lines: int = 200, request: Request | None = None,
) -> AsyncGenerator[str, None]:
    """Stream journald (Linux) or ``tail -F`` on a log file (macOS / dev)."""
    if _journalctl_available():
        async for chunk in _stream_command_sse(
            _journalctl_cmd(unit, lines),
            request,
            stderr_label="journalctl",
        ):
            yield chunk
        return

    tail_path = _console_tail_file(unit, request)
    if tail_path and _tail_available():
        async for chunk in _stream_command_sse(
            _tail_follow_cmd(str(tail_path), lines),
            request,
            stderr_label="tail",
        ):
            yield chunk
        return

    notice = _console_fallback_notice(unit, tail_path)
    yield f"data: {notice}\n\n"


@router.get("/ops/celery/supported-tasks")
async def get_ops_celery_supported_tasks(request: Request) -> Dict[str, Any]:
    """Celery task registry names (``src.*``) and default queue per ``task_routes`` — same app as workers.

    Must be ``async`` so the handler runs on the event-loop thread: importing ``src.bars.tasks``
    pulls ``ib_insync``/``eventkit``, which require a current event loop and fail in Starlette's
    sync-route threadpool workers.
    """
    from bifrost_api.ops.services.celery_supported_tasks import build_supported_tasks_payload

    svc = _worker_svc(request)
    celery_app = getattr(svc, "_celery", None)
    if celery_app is None:
        return {"ok": False, "error": "celery app unavailable", "tasks": [], "count": 0}
    try:
        return build_supported_tasks_payload(celery_app)
    except Exception as e:
        logger.exception("get_ops_celery_supported_tasks failed: %s", e)
        return {"ok": False, "error": str(e), "tasks": [], "count": 0}


@router.get("/ops/celery/capabilities")
async def get_ops_celery_capabilities(request: Request) -> Dict[str, Any]:
    """Celery self-description: task route default queues, canonical broker queue names, ``run_massive_job`` matrix."""
    from bifrost_api.ops.services.celery_capabilities import build_celery_capabilities_payload

    svc = _worker_svc(request)
    celery_app = getattr(svc, "_celery", None)
    if celery_app is None:
        return {
            "ok": False,
            "error": "celery app unavailable",
            "registered_tasks": [],
            "count": 0,
            "canonical_broker_queues": [],
            "run_massive_job_matrix": [],
            "beat_tasks": [],
            "broker_queue_labels": {},
        }
    try:
        return build_celery_capabilities_payload(celery_app)
    except Exception as e:
        logger.exception("get_ops_celery_capabilities failed: %s", e)
        return {
            "ok": False,
            "error": str(e),
            "registered_tasks": [],
            "count": 0,
            "canonical_broker_queues": [],
            "run_massive_job_matrix": [],
            "beat_tasks": [],
            "broker_queue_labels": {},
        }


@router.get("/ops/celery/logs")
def get_ops_celery_logs(
    request: Request,
    worker: str = Query(
        ...,
        min_length=1,
        description="Celery worker nodename (same as worker_id from /status or inspect)",
    ),
    tail: int = Query(1000, ge=1, le=5000, description="Latest N lines, oldest-first in response"),
) -> Dict[str, Any]:
    """Last N lines from this worker's Celery console Redis stream (same data as former bifrost-server GET /api/celery/logs)."""
    try:
        import redis
        from bifrost_worker.celery.celery_app import celery_console_stream_key

        r = redis.from_url(request.app.state.broker_url)
        key = celery_console_stream_key(worker)
        raw = r.xrevrange(key, count=tail)
        lines_out: List[str] = []
        for _eid, fields in reversed(raw):
            line = (fields.get(b"line") or fields.get("line") or b"").decode("utf-8", errors="replace")
            lines_out.append(line)
        return {"lines": lines_out}
    except Exception as e:
        logger.warning("get_ops_celery_logs failed: %s", e)
        return {"lines": [], "error": str(e)}


@router.delete("/ops/celery/logs")
def delete_ops_celery_logs(
    request: Request,
    worker: str = Query(
        ...,
        min_length=1,
        description="Celery worker nodename whose console stream to delete",
    ),
) -> Dict[str, Any]:
    try:
        import redis
        from bifrost_worker.celery.celery_app import celery_console_stream_key

        r = redis.from_url(request.app.state.broker_url)
        r.delete(celery_console_stream_key(worker))
        return {"ok": True}
    except Exception as e:
        logger.warning("delete_ops_celery_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.post("/ops/celery/logs/trim")
def trim_ops_celery_logs(
    request: Request,
    worker: str = Query(
        ...,
        min_length=1,
        description="Celery worker nodename whose console stream to trim",
    ),
    body: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
    try:
        max_lines = body.get("max_lines")
        if max_lines is None:
            return {"ok": False, "error": "max_lines required"}
        max_lines = int(max_lines)
        if max_lines < 1 or max_lines > 10000:
            return {"ok": False, "error": "max_lines must be between 1 and 10000"}
        import redis
        from bifrost_worker.celery.celery_app import celery_console_stream_key

        r = redis.from_url(request.app.state.broker_url)
        r.xtrim(celery_console_stream_key(worker), maxlen=max_lines, approximate=True)
        return {"ok": True}
    except Exception as e:
        logger.warning("trim_ops_celery_logs failed: %s", e)
        return {"ok": False, "error": str(e)}


@router.get("/ops/console/worker/{worker_id:path}", response_model=None)
async def worker_console(
    request: Request,
    worker_id: str,
    lines: int = Query(200, ge=10, le=2000),
):
    """SSE: live lines from this worker's Celery console Redis stream (same as former bifrost-server /api/celery/logs/stream).

    Query ``lines`` is accepted for API compatibility; the live stream does not use it.
    """
    from bifrost_worker.celery.celery_app import celery_console_stream_key

    if not _ops_broker_ping_ok(request):
        return JSONResponse(
            status_code=503,
            content={"detail": "Celery broker (Redis) not available"},
        )
    stream_key = celery_console_stream_key(worker_id)
    broker_url = request.app.state.broker_url
    app = request.app
    queue: asyncio.Queue = asyncio.Queue(maxsize=512)
    stop = threading.Event()
    with app.state.celery_log_lock:
        app.state.celery_log_queues.append(queue)
        if app.state._celery_log_loop is None:
            app.state._celery_log_loop = asyncio.get_running_loop()
    reader = threading.Thread(
        target=celery_log_reader_single_stream,
        args=(app, broker_url, stream_key, queue, stop),
        name="ops-celery-log-reader",
        daemon=True,
    )
    reader.start()

    async def event_gen() -> AsyncGenerator[str, None]:
        try:
            while True:
                try:
                    line = await asyncio.wait_for(queue.get(), timeout=25.0)
                    yield f"data: {json.dumps({'line': line})}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            stop.set()
            reader.join(timeout=5.0)
            with app.state.celery_log_lock:
                if queue in app.state.celery_log_queues:
                    app.state.celery_log_queues.remove(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/ops/console/broker")
async def broker_console(
    request: Request,
    lines: int = Query(200, ge=10, le=2000),
) -> StreamingResponse:
    """SSE stream of Redis logs (journald on Linux, else ``BIFROST_BROKER_CONSOLE_LOG`` tail -F)."""
    return StreamingResponse(
        _journal_sse("redis", lines=lines, request=request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )




# ── Audit log ─────────────────────────────────────────────────────────────────


@router.get("/ops/audit")
def list_audit(
    request: Request,
    limit: int = Query(100, ge=1, le=500),
) -> Dict[str, Any]:
    """Recent audit entries (newest first).  Requires admin role."""
    denied = _require_role(request, "admin")
    if denied:
        return denied
    audit_store = getattr(request.app.state, "audit_store", None)
    if audit_store is not None:
        entries = audit_store.list_recent(limit=limit)
    else:
        log = _audit_log(request)
        entries = sorted(log, key=lambda e: e.timestamp, reverse=True)[:limit]
    return {
        "ok": True,
        "entries": [e.model_dump() for e in entries],
        "count": len(entries),
    }
