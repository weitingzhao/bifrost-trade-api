"""ops_feedback.* DDL — run by db-init, never on a request (TD-77, Owner 2026-10-03).

``scripts/run_db_refresh_schema.py`` (the db-init Job, api-monitor image) calls
``ensure_feedback_schema`` on its Golden Source connection after
``ensure_brokerage_schema``, every STG and PROD release. Until 0.6.8 the same DDL
ran lazily inside the first request of every process, reads included, as the
runtime role.

Rules for changing it:

- **Additive only.** ``ops_feedback`` is installation-keyed: DEV, STG and PROD
  share one copy, so the STG release's db-init changes the schema PROD is reading.
  Expand first (a new column ships here), read it in the following release.
- **Idempotent steps, in order, no version table** (Owner E3): ``IF NOT EXISTS``,
  or a catalog check before an ALTER that has no such form — the same style as
  core's ``ensure_brokerage_schema``. Append new steps at the end.
- **Owner ``bifrost``.** db-init connects as ``bifrost``; ``CREATE TABLE IF NOT
  EXISTS`` checks schema privileges before existence, so these objects must be
  owned by ``bifrost`` (a one-time ``ALTER … OWNER TO bifrost``, Owner E2/B1).
  The runtime role ``analytics_writer`` keeps reading and writing through its
  membership in ``bifrost``.
"""

from __future__ import annotations

from typing import Any, Callable, Tuple

FEEDBACK_SCHEMA_STEPS: Tuple[Tuple[str, str], ...] = (
    ("schema ops_feedback", "CREATE SCHEMA IF NOT EXISTS ops_feedback"),
    (
        "table ops_feedback.report",
        """
        CREATE TABLE IF NOT EXISTS ops_feedback.report (
            report_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            kind           text NOT NULL,
            title          text NOT NULL,
            body_md        text NOT NULL DEFAULT '',
            page_route     text NOT NULL DEFAULT '',
            page_label     text NOT NULL DEFAULT '',
            blocks_trading boolean NOT NULL DEFAULT false,
            context        jsonb NOT NULL DEFAULT '{}'::jsonb,
            status         text NOT NULL DEFAULT 'new',
            reply_md       text,
            replied_at     timestamptz,
            unread_reply   boolean NOT NULL DEFAULT false,
            created_at     timestamptz NOT NULL DEFAULT now(),
            updated_at     timestamptz NOT NULL DEFAULT now()
        )
        """,
    ),
    (
        "table ops_feedback.report_image",
        """
        CREATE TABLE IF NOT EXISTS ops_feedback.report_image (
            report_image_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            report_id       bigint NOT NULL REFERENCES ops_feedback.report(report_id) ON DELETE CASCADE,
            seq             smallint NOT NULL,
            mime            text NOT NULL,
            bytes           bytea NOT NULL,
            UNIQUE (report_id, seq)
        )
        """,
    ),
    (
        "index idx_feedback_report_status",
        "CREATE INDEX IF NOT EXISTS idx_feedback_report_status ON ops_feedback.report (status, created_at DESC)",
    ),
)


def ensure_feedback_schema(conn: Any, *, log: Callable[[str], None] = print) -> None:
    """Apply every step in order in one transaction; commit, or roll back and raise.

    ``lock_timeout`` keeps a step from queueing behind the live service's reads
    and writes for long: a release that cannot take its lock fails and says so.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '20s'")
            cur.execute("SET LOCAL statement_timeout = '120s'")
            for name, sql in FEEDBACK_SCHEMA_STEPS:
                cur.execute(sql)
                log(f"{name}: ok")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
