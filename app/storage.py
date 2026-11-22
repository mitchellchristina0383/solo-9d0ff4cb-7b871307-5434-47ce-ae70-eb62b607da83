"""SQLite persistence layer for the tail-sampling workbench.

All state needed to resume an unfinished window after a restart lives here:
window definitions, per-service quotas, received spans and export records.
The trace-level keep/drop decisions themselves are *derived* from spans and
quotas and therefore do not need a separate table.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS windows (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    start_ms        INTEGER NOT NULL,
    collect_ms      INTEGER NOT NULL,
    grace_ms        INTEGER NOT NULL,
    slow_ms         INTEGER NOT NULL,
    default_quota   INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('open','closed')),
    finalized_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS quotas (
    window_id    INTEGER NOT NULL,
    service      TEXT NOT NULL,
    quota        INTEGER NOT NULL,
    PRIMARY KEY (window_id, service),
    FOREIGN KEY (window_id) REFERENCES windows (id)
);

CREATE TABLE IF NOT EXISTS spans (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    span_id       TEXT NOT NULL UNIQUE,
    trace_id      TEXT NOT NULL,
    service       TEXT NOT NULL,
    operation     TEXT NOT NULL,
    start_ms      INTEGER NOT NULL,
    duration_ms   INTEGER NOT NULL,
    status        TEXT NOT NULL,
    received_ms   INTEGER NOT NULL,
    late          INTEGER NOT NULL DEFAULT 0,
    window_id     INTEGER NOT NULL,
    FOREIGN KEY (window_id) REFERENCES windows (id)
);
CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans (window_id, trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_window ON spans (window_id);

CREATE TABLE IF NOT EXISTS exports (
    window_id      INTEGER NOT NULL,
    trace_id       TEXT NOT NULL,
    exported_at_ms INTEGER NOT NULL,
    span_count     INTEGER NOT NULL,
    PRIMARY KEY (window_id, trace_id),
    FOREIGN KEY (window_id) REFERENCES windows (id)
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Snapshot of the tail-sampling decision taken when a window closed. It is
-- kept after the span details are purged so history can still answer
-- "kept or dropped, why, over which services, how many spans".
CREATE TABLE IF NOT EXISTS trace_records (
    window_id       INTEGER NOT NULL,
    trace_id        TEXT NOT NULL,
    decision        TEXT NOT NULL,
    reason          TEXT NOT NULL,
    tier            INTEGER NOT NULL,
    span_count      INTEGER NOT NULL,
    services        TEXT NOT NULL,
    latency_ms      INTEGER NOT NULL,
    start_ms        INTEGER NOT NULL,
    has_error       INTEGER NOT NULL DEFAULT 0,
    has_timeout     INTEGER NOT NULL DEFAULT 0,
    late_span_count INTEGER NOT NULL DEFAULT 0,
    blocked_service TEXT,
    exported        INTEGER NOT NULL DEFAULT 0,
    preserved_at_ms INTEGER,
    purged_at_ms    INTEGER,
    PRIMARY KEY (window_id, trace_id),
    FOREIGN KEY (window_id) REFERENCES windows (id)
);
CREATE INDEX IF NOT EXISTS idx_trace_records_window
    ON trace_records (window_id);

-- Per-service quota occupancy as of window close. Spans for purged traces
-- are gone, so occupancy can no longer be derived from them.
CREATE TABLE IF NOT EXISTS quota_usage (
    window_id INTEGER NOT NULL,
    service   TEXT NOT NULL,
    quota     INTEGER NOT NULL,
    used      INTEGER NOT NULL,
    PRIMARY KEY (window_id, service),
    FOREIGN KEY (window_id) REFERENCES windows (id)
);

-- Durable span-id dedupe registry. It is *not* purged with span details, so
-- re-reporting an already-seen span_id is still detected as a duplicate
-- even after the original rows were deleted. Only an explicit full reset
-- clears it.
CREATE TABLE IF NOT EXISTS span_registry (
    span_id       TEXT PRIMARY KEY,
    trace_id      TEXT NOT NULL,
    window_id     INTEGER NOT NULL,
    first_seen_ms INTEGER NOT NULL
);

-- Durable cleanup job. Each batch commits on its own; a running job that
-- loses its worker (restart / interruption) becomes 'interrupted' and can
-- be retried from the already-committed prefix.
CREATE TABLE IF NOT EXISTS purge_jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    window_id       INTEGER NOT NULL,
    status          TEXT NOT NULL
                    CHECK (status IN ('running','interrupted','completed','failed')),
    batch_size      INTEGER NOT NULL,
    pause_ms        INTEGER NOT NULL DEFAULT 0,
    total_traces    INTEGER NOT NULL,
    purged_traces   INTEGER NOT NULL DEFAULT 0,
    deleted_spans   INTEGER NOT NULL DEFAULT 0,
    started_ms      INTEGER NOT NULL,
    updated_ms      INTEGER NOT NULL,
    error           TEXT,
    FOREIGN KEY (window_id) REFERENCES windows (id)
);
CREATE INDEX IF NOT EXISTS idx_purge_jobs_window
    ON purge_jobs (window_id, id);
"""


class Storage:
    """Thin, thread-safe wrapper around a single SQLite database file."""

    def __init__(self, path: str):
        self.path = path
        # check_same_thread=False is safe because every call is guarded by
        # the re-entrant lock; the HTTP server is single threaded anyway.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    # ------------------------------------------------------------------ meta
    def get_meta(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    # --------------------------------------------------------------- windows
    def create_window(
        self,
        start_ms: int,
        collect_ms: int,
        grace_ms: int,
        slow_ms: int,
        default_quota: int,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO windows(start_ms, collect_ms, grace_ms, slow_ms, "
                "default_quota, status) VALUES(?,?,?,?,?,'open')",
                (start_ms, collect_ms, grace_ms, slow_ms, default_quota),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_active_window(self) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM windows WHERE status='open' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()

    def get_window(self, window_id: int) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM windows WHERE id=?", (window_id,)
            ).fetchone()

    def list_windows(self, limit: int = 50) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM windows ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            )

    def close_window(self, window_id: int, finalized_at_ms: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE windows SET status='closed', finalized_at_ms=? WHERE id=?",
                (finalized_at_ms, window_id),
            )
            self._conn.commit()

    # ---------------------------------------------------------------- quotas
    def upsert_quota(self, window_id: int, service: str, quota: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO quotas(window_id, service, quota) VALUES(?,?,?) "
                "ON CONFLICT(window_id, service) DO UPDATE SET quota=excluded.quota",
                (window_id, service, quota),
            )
            self._conn.commit()

    def get_quotas(self, window_id: int) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT service, quota FROM quotas WHERE window_id=?",
                (window_id,),
            ).fetchall()
            return {r["service"]: r["quota"] for r in rows}

    # ----------------------------------------------------------------- spans
    def insert_span(self, span: dict[str, Any], window_id: int, now_ms: int,
                    late: bool) -> bool:
        """Insert a span. Returns False if span_id was ever seen (idempotent).

        Dedupe consults the durable ``span_registry`` rather than the
        ``spans`` table, so a span_id from a trace whose details were purged
        is still rejected as a duplicate.
        """
        with self._lock:
            exists = self._conn.execute(
                "SELECT 1 FROM span_registry WHERE span_id=?", (span["span_id"],)
            ).fetchone()
            if exists:
                return False
            self._conn.execute(
                "INSERT INTO spans(span_id, trace_id, service, operation, "
                "start_ms, duration_ms, status, received_ms, late, window_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    span["span_id"], span["trace_id"], span["service"],
                    span["operation"], span["start_ms"], span["duration_ms"],
                    span["status"], now_ms, 1 if late else 0, window_id,
                ),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO span_registry("
                "span_id, trace_id, window_id, first_seen_ms) VALUES(?,?,?,?)",
                (span["span_id"], span["trace_id"], window_id, now_ms),
            )
            self._conn.commit()
            return True

    def get_spans(self, window_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM spans WHERE window_id=? ORDER BY start_ms, id",
                    (window_id,),
                ).fetchall()
            )

    def count_window_spans(self, window_id: int) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS c FROM spans WHERE window_id=?", (window_id,)
            ).fetchone()
            return int(row["c"])

    # --------------------------------------------------------------- exports
    def save_export(self, window_id: int, trace_id: str, exported_at_ms: int,
                    span_count: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO exports(window_id, trace_id, "
                "exported_at_ms, span_count) VALUES(?,?,?,?)",
                (window_id, trace_id, exported_at_ms, span_count),
            )
            self._conn.commit()

    def get_exported_trace_ids(self, window_id: int) -> set[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT trace_id FROM exports WHERE window_id=?", (window_id,)
            ).fetchall()
            return {r["trace_id"] for r in rows}

    def get_exports(self, window_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM exports WHERE window_id=? ORDER BY exported_at_ms",
                    (window_id,),
                ).fetchall()
            )

    # -------------------------------------------------------- trace records
    def save_trace_record(self, window_id: int, trace_id: str,
                          decision: dict[str, Any], exported: bool) -> None:
        """Persist the final decision for one trace (survives purging)."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO trace_records(window_id, trace_id, decision, "
                "reason, tier, span_count, services, latency_ms, start_ms, "
                "has_error, has_timeout, late_span_count, blocked_service, "
                "exported) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(window_id, trace_id) DO UPDATE SET "
                "decision=excluded.decision, reason=excluded.reason, "
                "tier=excluded.tier, span_count=excluded.span_count, "
                "services=excluded.services, latency_ms=excluded.latency_ms, "
                "start_ms=excluded.start_ms, has_error=excluded.has_error, "
                "has_timeout=excluded.has_timeout, "
                "late_span_count=excluded.late_span_count, "
                "blocked_service=excluded.blocked_service, "
                "exported=excluded.exported",
                (
                    window_id, trace_id, decision["decision"],
                    decision["reason"], decision["tier"],
                    decision["span_count"], json.dumps(decision["services"]),
                    decision["latency_ms"], decision["start_ms"],
                    1 if decision["has_error"] else 0,
                    1 if decision["has_timeout"] else 0,
                    decision["late_span_count"], decision.get("blocked_service"),
                    1 if exported else 0,
                ),
            )
            self._conn.commit()

    def get_trace_records(self, window_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM trace_records WHERE window_id=? "
                    "ORDER BY start_ms, trace_id",
                    (window_id,),
                ).fetchall()
            )

    def get_trace_record(self, window_id: int, trace_id: str):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM trace_records WHERE window_id=? AND trace_id=?",
                (window_id, trace_id),
            ).fetchone()

    def mark_trace_preserved(self, window_id: int, trace_id: str, ts: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE trace_records SET preserved_at_ms=? "
                "WHERE window_id=? AND trace_id=?",
                (ts, window_id, trace_id),
            )
            self._conn.commit()

    def mark_trace_purged(self, window_id: int, trace_id: str, ts: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE trace_records SET purged_at_ms=? "
                "WHERE window_id=? AND trace_id=? AND preserved_at_ms IS NULL",
                (ts, window_id, trace_id),
            )
            self._conn.commit()

    def trace_records_exist(self, window_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM trace_records WHERE window_id=? LIMIT 1",
                (window_id,),
            ).fetchone()
            return row is not None

    # --------------------------------------------------------- quota usage
    def save_quota_usage(self, window_id: int,
                         usage: list[dict[str, Any]]) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM quota_usage WHERE window_id=?", (window_id,)
            )
            for item in usage:
                self._conn.execute(
                    "INSERT INTO quota_usage(window_id, service, quota, used) "
                    "VALUES(?,?,?,?)",
                    (window_id, item["service"], item["quota"], item["used"]),
                )
            self._conn.commit()

    def get_quota_usage(self, window_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM quota_usage WHERE window_id=? "
                    "ORDER BY service",
                    (window_id,),
                ).fetchall()
            )

    # ----------------------------------------------------------- purge jobs
    def create_purge_job(self, window_id: int, batch_size: int, pause_ms: int,
                         total_traces: int, now_ms: int) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO purge_jobs(window_id, status, batch_size, "
                "pause_ms, total_traces, started_ms, updated_ms) "
                "VALUES(?, 'running', ?, ?, ?, ?, ?)",
                (window_id, batch_size, pause_ms, total_traces, now_ms, now_ms),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_purge_job(self, job_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM purge_jobs WHERE id=?", (job_id,)
            ).fetchone()

    def get_latest_purge_job(self, window_id: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM purge_jobs WHERE window_id=? "
                "ORDER BY id DESC LIMIT 1",
                (window_id,),
            ).fetchone()

    def update_purge_job(self, job_id: int, *, status: str | None = None,
                         purged_traces: int | None = None,
                         deleted_spans: int | None = None,
                         updated_ms: int | None = None,
                         error: str | None = None) -> None:
        sets, args = [], []
        for col, val in (("status", status), ("purged_traces", purged_traces),
                         ("deleted_spans", deleted_spans),
                         ("updated_ms", updated_ms), ("error", error)):
            if val is not None:
                sets.append(f"{col}=?")
                args.append(val)
        if not sets:
            return
        args.append(job_id)
        with self._lock:
            self._conn.execute(
                f"UPDATE purge_jobs SET {', '.join(sets)} WHERE id=?", args
            )
            self._conn.commit()

    def mark_running_jobs_interrupted(self, now_ms: int) -> int:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE purge_jobs SET status='interrupted', updated_ms=? "
                "WHERE status='running'",
                (now_ms,),
            )
            self._conn.commit()
            return int(cur.rowcount)

    def trace_ids_for_purge(self, window_id: int) -> list[str]:
        """Traces still holding span details in a purged/eligible window."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT s.trace_id FROM spans s "
                "LEFT JOIN trace_records r "
                "  ON r.window_id=s.window_id AND r.trace_id=s.trace_id "
                "WHERE s.window_id=? AND (r.preserved_at_ms IS NULL) "
                "ORDER BY s.trace_id",
                (window_id,),
            ).fetchall()
            return [r["trace_id"] for r in rows]

    def purge_trace_batch(self, window_id: int, trace_ids: list[str],
                          ts: int) -> tuple[int, int]:
        """Delete span rows of a batch of whole traces in one transaction.

        Returns ``(deleted_spans, purged_traces)``. Marks the corresponding
        trace records as purged at the same time, so a crash can never leave
        "details gone but not marked purged" behind. Preserved traces are
        skipped defensively. ``span_registry`` rows are intentionally left
        untouched so duplicate re-reports are still detected afterwards.
        """
        if not trace_ids:
            return 0, 0
        marks = ",".join("?" for _ in trace_ids)
        with self._lock:
            try:
                # Exclude preserved traces in SQL itself: a preserve request
                # could race between the worker selecting a batch and this
                # delete, and preservation must always win.
                present = [
                    r["trace_id"] for r in self._conn.execute(
                        f"SELECT DISTINCT trace_id FROM spans "
                        f"WHERE window_id=? AND trace_id IN ({marks}) "
                        f"AND trace_id NOT IN ("
                        f"  SELECT trace_id FROM trace_records"
                        f"  WHERE window_id=? AND preserved_at_ms IS NOT NULL)",
                        (window_id, *trace_ids, window_id),
                    ).fetchall()
                ]
                if not present:
                    return 0, 0
                cur = self._conn.execute(
                    f"DELETE FROM spans WHERE window_id=? "
                    f"AND trace_id IN ({','.join('?' for _ in present)})",
                    (window_id, *present),
                )
                deleted = int(cur.rowcount)
                self._conn.execute(
                    f"UPDATE trace_records SET purged_at_ms=? "
                    f"WHERE window_id=? AND trace_id IN ("
                    f"{','.join('?' for _ in present)}) "
                    f"AND preserved_at_ms IS NULL",
                    (ts, window_id, *present),
                )
                self._conn.commit()
                return deleted, len(present)
            except Exception:
                self._conn.rollback()
                raise

    def window_span_counts(self, window_id: int) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT trace_id, COUNT(*) AS c FROM spans WHERE window_id=? "
                "GROUP BY trace_id",
                (window_id,),
            ).fetchall()
            return {r["trace_id"]: int(r["c"]) for r in rows}

    # ----------------------------------------------------------------- reset
    def reset(self) -> None:
        with self._lock:
            for table in ("exports", "spans", "quotas", "windows", "meta",
                          "trace_records", "quota_usage", "span_registry",
                          "purge_jobs"):
                self._conn.execute(f"DELETE FROM {table}")
            self._conn.commit()

    def reset_span_registry(self) -> None:
        """Test/demo helper: forget all seen span ids (not tied to one window)."""
        with self._lock:
            self._conn.execute("DELETE FROM span_registry")
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def export_state(self) -> str:
        """Debug helper: dump the whole database as JSON."""
        with self._lock:
            payload: dict[str, Any] = {}
            for table in ("windows", "quotas", "spans", "exports", "meta",
                          "trace_records", "quota_usage", "span_registry",
                          "purge_jobs"):
                rows = self._conn.execute(f"SELECT * FROM {table}").fetchall()
                payload[table] = [dict(r) for r in rows]
            return json.dumps(payload, ensure_ascii=False, indent=2)
