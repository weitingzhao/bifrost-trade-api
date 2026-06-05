from __future__ import annotations

import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bifrost_api.research.sepa.crs_engine import compute_crs_scores
from bifrost_api.research.sepa.fundamentals_engine import (
    FUNDAMENTALS_RULE_VERSION,
    FundamentalsConfig,
    evaluate_fundamentals,
)
from bifrost_api.research.sepa.phase1_engine import Phase1Config, evaluate_phase1_batch
from bifrost_worker.data.massive.vendor.reader import (
    get_sepa_fundamentals_cache_snapshot,
    get_job_sepa_phase4,
    get_job_sepa_phase4_result,
    get_stock_day_close_series_for_crs,
    get_stock_day_series_for_sepa,
    insert_job_sepa_phase4,
    list_job_sepa_phase4,
    delete_job_sepa_phase4,
    update_job_sepa_phase4,
    upsert_sepa_fundamentals_cache,
)
from bifrost_worker.data.massive.vendor.reference_cache_keys import (
    CACHE_TTL_SEPA_FUNDAMENTALS_SEC,
    key_sepa_fundamentals,
    redis_client_from_status_config,
)

PHASE4_VERSION = "sepa_phase4_v1"
_RATE_LOCK = threading.Lock()
_LAST_CALL_TS = 0.0
_PROGRESS_LOCK = threading.Lock()
_LAST_PROGRESS_WRITE_TS: Dict[str, float] = {}


@dataclass
class Phase4JobConfig:
    source: str = "massive"
    lookback_days: int = 420
    volume_threshold: float = 100000.0
    strict_sma200_rising: bool = False
    min_crs: Optional[float] = 70.0
    max_workers: int = 4
    max_retries: int = 3
    rate_limit_rps: float = 4.0
    retry_base_sec: float = 0.6
    cache_ttl_sec: int = CACHE_TTL_SEPA_FUNDAMENTALS_SEC
    use_parallel: bool = True


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_job(job_id: str, symbols: List[str], payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    req: Dict[str, Any] = {"symbols": symbols}
    if isinstance(payload, dict):
        req.update(payload)
    return {
        "job_id": job_id,
        "status": "queued",
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "started_at": None,
        "finished_at": None,
        "progress": {"current": 0, "total": len(symbols), "stage": "queued", "pct": 0.0},
        "request": req,
        "summary": {},
        "result": None,
        "errors": [],
        "version": PHASE4_VERSION,
    }


def create_phase4_job(
    status_config: dict,
    symbols: List[str],
    payload: Optional[Dict[str, Any]] = None,
) -> str:
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    job_id = uuid.uuid4().hex
    row = _empty_job(job_id, syms, payload=payload)
    insert_job_sepa_phase4(
        status_config,
        job_id,
        request_payload=row["request"],
        version=PHASE4_VERSION,
    )
    return job_id


def get_phase4_job(status_config: dict, job_id: str) -> Optional[Dict[str, Any]]:
    row = get_job_sepa_phase4(status_config, job_id)
    if row is None:
        return None
    out = dict(row)
    for k in ("progress", "request", "summary"):
        if not isinstance(out.get(k), dict):
            out[k] = {}
    if not isinstance(out.get("errors"), list):
        out["errors"] = []
    return out


def get_phase4_job_result(
    status_config: dict,
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 200,
) -> Optional[Dict[str, Any]]:
    return get_job_sepa_phase4_result(status_config, job_id, offset=offset, limit=limit)


def list_phase4_jobs(
    status_config: dict,
    *,
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    created_from: Optional[str] = None,
    created_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    return list_job_sepa_phase4(
        status_config,
        limit=limit,
        offset=offset,
        status_filter=status_filter,
        created_from=created_from,
        created_to=created_to,
    )


def delete_phase4_job(status_config: dict, job_id: str) -> bool:
    return delete_job_sepa_phase4(status_config, job_id)


def _update_job(status_config: dict, job_id: str, **fields: Any) -> None:
    update_job_sepa_phase4(status_config, job_id, **fields)


def _update_progress(
    status_config: dict,
    job_id: str,
    *,
    stage: str,
    current: int,
    total: int,
    force: bool = False,
) -> None:
    pct = 0.0 if total <= 0 else round((float(current) / float(total)) * 100.0, 2)
    now = time.monotonic()
    if not force:
        with _PROGRESS_LOCK:
            last = _LAST_PROGRESS_WRITE_TS.get(job_id, 0.0)
            if now - last < 2.0:
                return
            _LAST_PROGRESS_WRITE_TS[job_id] = now
    _update_job(status_config, job_id, progress={"current": current, "total": total, "stage": stage, "pct": pct})


def _throttle(rate_limit_rps: float) -> None:
    global _LAST_CALL_TS
    if rate_limit_rps <= 0:
        return
    gap = 1.0 / float(rate_limit_rps)
    with _RATE_LOCK:
        now = time.monotonic()
        wait = gap - (now - _LAST_CALL_TS)
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _LAST_CALL_TS = now


def _fetch_income_with_retry(
    client: Any,
    symbol: str,
    *,
    timeframe: str,
    max_retries: int,
    retry_base_sec: float,
    rate_limit_rps: float,
) -> Dict[str, Any]:
    last_error: Optional[str] = None
    for attempt in range(max(1, max_retries)):
        _throttle(rate_limit_rps)
        try:
            res = client.fetch_stock_income_statements(
                symbol,
                timeframe=timeframe,
                limit=12 if timeframe == "quarterly" else 5,
                sort="filing_date.desc",
            )
            if not isinstance(res, dict):
                return {"results": [], "error": "invalid_response"}
            if res.get("error"):
                err = str(res.get("error"))
                last_error = err
                lowered = err.lower()
                if ("429" in lowered or "timeout" in lowered or "503" in lowered or "502" in lowered) and (
                    attempt + 1 < max_retries
                ):
                    backoff = (retry_base_sec * (2**attempt)) + random.uniform(0, retry_base_sec / 2.0)
                    time.sleep(backoff)
                    continue
            return res
        except Exception as exc:
            last_error = str(exc)
            if attempt + 1 < max_retries:
                backoff = (retry_base_sec * (2**attempt)) + random.uniform(0, retry_base_sec / 2.0)
                time.sleep(backoff)
                continue
    return {"results": [], "error": last_error or "unknown_error"}


def _fetch_eval_one(
    symbol: str,
    client: Any,
    status_config: dict,
    cfg: FundamentalsConfig,
    p4cfg: Phase4JobConfig,
    redis_client: Any,
) -> Dict[str, Any]:
    cache_key = key_sepa_fundamentals(symbol, FUNDAMENTALS_RULE_VERSION)
    if redis_client:
        try:
            raw = redis_client.get(cache_key)
            if raw:
                import json

                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    out = dict(parsed.get("evaluation") or {})
                    out["symbol"] = symbol
                    out["cache_hit"] = "redis"
                    return out
        except Exception:
            pass

    pg_hit = get_sepa_fundamentals_cache_snapshot(
        status_config,
        symbol,
        rule_version=FUNDAMENTALS_RULE_VERSION,
    )
    if pg_hit and isinstance(pg_hit.get("payload"), dict):
        payload = pg_hit["payload"]
        out = dict(payload.get("evaluation") or {})
        out["symbol"] = symbol
        out["cache_hit"] = "postgres"
        if redis_client:
            try:
                import json

                redis_client.setex(cache_key, int(p4cfg.cache_ttl_sec), json.dumps(payload))
            except Exception:
                pass
        return out

    from bifrost_api.research.sepa.financials_data import fetch_income_rows_for_sepa_from_pg

    pg_pair = fetch_income_rows_for_sepa_from_pg(status_config, symbol)
    if pg_pair:
        qrows, arows = pg_pair
        evaluated = evaluate_fundamentals(qrows, arows, cfg=cfg)
        evaluated["symbol"] = symbol
        evaluated["cache_hit"] = "postgres_income_table"
        eval_payload = {
            "evaluation": evaluated,
            "quarterly_rows": qrows,
            "annual_rows": arows,
            "error": None,
            "saved_at": _utc_now(),
        }
        upsert_sepa_fundamentals_cache(
            status_config,
            symbol,
            eval_payload,
            rule_version=FUNDAMENTALS_RULE_VERSION,
            ttl_sec=p4cfg.cache_ttl_sec,
        )
        if redis_client:
            try:
                import json

                redis_client.setex(cache_key, int(p4cfg.cache_ttl_sec), json.dumps(eval_payload))
            except Exception:
                pass
        return evaluated

    qres = _fetch_income_with_retry(
        client,
        symbol,
        timeframe="quarterly",
        max_retries=p4cfg.max_retries,
        retry_base_sec=p4cfg.retry_base_sec,
        rate_limit_rps=p4cfg.rate_limit_rps,
    )
    ares = _fetch_income_with_retry(
        client,
        symbol,
        timeframe="annual",
        max_retries=p4cfg.max_retries,
        retry_base_sec=p4cfg.retry_base_sec,
        rate_limit_rps=p4cfg.rate_limit_rps,
    )
    qrows = qres.get("results") if isinstance(qres, dict) else []
    arows = ares.get("results") if isinstance(ares, dict) else []
    if not isinstance(qrows, list):
        qrows = []
    if not isinstance(arows, list):
        arows = []
    evaluated = evaluate_fundamentals(qrows, arows, cfg=cfg)
    evaluated["symbol"] = symbol
    evaluated["cache_hit"] = None
    eval_payload = {
        "evaluation": evaluated,
        "quarterly_rows": qrows,
        "annual_rows": arows,
        "error": qres.get("error") or ares.get("error"),
        "saved_at": _utc_now(),
    }
    upsert_sepa_fundamentals_cache(
        status_config,
        symbol,
        eval_payload,
        rule_version=FUNDAMENTALS_RULE_VERSION,
        ttl_sec=p4cfg.cache_ttl_sec,
    )
    if redis_client:
        try:
            import json

            redis_client.setex(cache_key, int(p4cfg.cache_ttl_sec), json.dumps(eval_payload))
        except Exception:
            pass
    if qres.get("error") or ares.get("error"):
        evaluated.setdefault("issues", []).append("api_error")
    return evaluated


def run_sepa_phase4_job(
    job_id: str,
    *,
    symbols: List[str],
    status_config: dict,
    merged_config: dict,
    massive_client: Any,
    cfg: Optional[Phase4JobConfig] = None,
) -> None:
    p4cfg = cfg or Phase4JobConfig()
    syms = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    _update_job(status_config, job_id, status="running", started_at=_utc_now())
    _update_progress(status_config, job_id, stage="phase1", current=0, total=len(syms), force=True)
    started = time.monotonic()
    errors: List[str] = []

    try:
        rows_by_symbol = get_stock_day_series_for_sepa(
            status_config,
            syms,
            lookback_days=p4cfg.lookback_days,
            source=p4cfg.source,
        )
        p1 = evaluate_phase1_batch(
            rows_by_symbol,
            cfg=Phase1Config(
                volume_threshold=p4cfg.volume_threshold,
                strict_sma200_rising=p4cfg.strict_sma200_rising,
            ),
        )
        phase1_rows = p1.get("results", [])
        tech_pass = [r["symbol"] for r in phase1_rows if r.get("technical_pass")]
        _update_progress(status_config, job_id, stage="phase2_crs", current=len(phase1_rows), total=len(syms), force=True)

        crs_rows_by_symbol = get_stock_day_close_series_for_crs(
            status_config,
            tech_pass,
            lookback_days=max(420, p4cfg.lookback_days),
            source=p4cfg.source,
        )
        crs = compute_crs_scores(crs_rows_by_symbol, lookback=252, min_crs=p4cfg.min_crs)
        crs_map = {r["symbol"]: r for r in crs.get("results", [])}
        candidates = [s for s in tech_pass if crs_map.get(s, {}).get("pass")]
        _update_progress(status_config, job_id, stage="phase3_fundamentals", current=0, total=len(candidates), force=True)

        fund_cfg = FundamentalsConfig()
        redis_client = redis_client_from_status_config(status_config)
        fund_rows: List[Dict[str, Any]] = []
        retry_count = 0
        cache_hits = {"redis": 0, "postgres": 0}
        external_calls = 0

        if p4cfg.use_parallel and p4cfg.max_workers > 1 and candidates:
            with ThreadPoolExecutor(max_workers=max(1, int(p4cfg.max_workers))) as ex:
                futures = {
                    ex.submit(
                        _fetch_eval_one,
                        sym,
                        massive_client,
                        status_config,
                        fund_cfg,
                        p4cfg,
                        redis_client,
                    ): sym
                    for sym in candidates
                }
                done = 0
                for f in as_completed(futures):
                    sym = futures[f]
                    try:
                        row = f.result()
                    except Exception as exc:
                        row = {
                            "symbol": sym,
                            "fundamental_pass": False,
                            "insufficient_data": True,
                            "not_comparable": False,
                            "conditions": [],
                            "pass_count": 0,
                            "fail_count": 0,
                            "metrics": {},
                            "issues": ["evaluation_failed", str(exc)],
                            "cache_hit": None,
                        }
                        errors.append(f"{sym}: {exc}")
                    fund_rows.append(row)
                    done += 1
                    _update_progress(status_config, job_id, stage="phase3_fundamentals", current=done, total=len(candidates))
        else:
            for idx, sym in enumerate(candidates):
                row = _fetch_eval_one(sym, massive_client, status_config, fund_cfg, p4cfg, redis_client)
                fund_rows.append(row)
                _update_progress(status_config, job_id, stage="phase3_fundamentals", current=idx + 1, total=len(candidates))

        for r in fund_rows:
            ch = r.get("cache_hit")
            if ch == "redis":
                cache_hits["redis"] += 1
            elif ch == "postgres":
                cache_hits["postgres"] += 1
            else:
                external_calls += 1
            if "api_error" in (r.get("issues") or []):
                retry_count += 1

        fund_map = {r["symbol"]: r for r in fund_rows}
        merged_rows: List[Dict[str, Any]] = []
        for row in phase1_rows:
            sym = row.get("symbol")
            cr = crs_map.get(sym) or {}
            fr = fund_map.get(sym) or {}
            merged_rows.append(
                {
                    "symbol": sym,
                    "technical_pass": bool(row.get("technical_pass")),
                    "crs_score": cr.get("crs_score"),
                    "crs_pass": bool(cr.get("pass")),
                    "fundamental_pass": bool(fr.get("fundamental_pass")) if fr else False,
                    "phase1": row,
                    "crs": cr,
                    "fundamentals": fr,
                    "final_pass": bool(row.get("technical_pass"))
                    and bool(cr.get("pass"))
                    and bool(fr.get("fundamental_pass")),
                }
            )

        final_passed = sum(1 for r in merged_rows if r.get("final_pass"))
        duration_sec = round(time.monotonic() - started, 3)
        summary = {
            "total_symbols": len(syms),
            "phase1_passed": len(tech_pass),
            "crs_passed": len(candidates),
            "final_passed": final_passed,
            "fundamentals_evaluated": len(fund_rows),
            "cache_hit_redis": cache_hits["redis"],
            "cache_hit_postgres": cache_hits["postgres"],
            "fundamentals_external_calls": external_calls,
            "retry_count": retry_count,
            "failed_symbols": len(errors),
            "duration_sec": duration_sec,
            "version": PHASE4_VERSION,
        }
        _update_job(
            status_config,
            job_id,
            status="succeeded" if not errors else "partial",
            summary=summary,
            result={"rows": merged_rows, "phase1": p1, "crs": crs},
            finished_at=_utc_now(),
            errors=errors[:200],
            version=PHASE4_VERSION,
        )
        _update_progress(status_config, job_id, stage="done", current=len(syms), total=len(syms), force=True)
    except Exception as exc:
        _update_job(
            status_config,
            job_id,
            status="failed",
            finished_at=_utc_now(),
            errors=[str(exc)],
            version=PHASE4_VERSION,
        )
        _update_progress(status_config, job_id, stage="failed", current=0, total=len(syms), force=True)

