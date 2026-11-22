"""HTTP server for the local tail-sampling workbench.

Zero third-party dependencies: built on http.server + sqlite3.

Endpoints
---------
GET  /                         single-page UI (static/index.html)
GET  /static/*                 UI assets
GET  /api/health               liveness probe
GET  /api/window/current       full snapshot of the active/last window
GET  /api/windows              window history
GET  /api/windows/<id>         one closed/active window with full details
POST /api/windows              create a window (collect/grace/slow/quota)
POST /api/windows/<id>/quotas  upsert per-service retention quota
POST /api/windows/<id>/finalize close the window immediately (run decision)
POST /api/spans                submit one span (timestamps supplied by client)
POST /api/demo/<name>          seed a scripted demo scenario
POST /api/simulate-restart     drop in-memory state and reload from SQLite
POST /api/reset                wipe all data (demo convenience)
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from . import sampler
from .storage import Storage

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

VALID_STATUSES = {"ok", "error", "timeout"}
MAX_BODY_BYTES = 1_000_000


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class AppState:
    """Holds the database and the pluggable clock (real time by default)."""

    def __init__(self, db_path: str):
        self.storage = Storage(db_path)
        self.db_path = db_path
        # now_ms is indirected so tests can drive deterministic scenarios;
        # production simply reads the wall clock.
        self.now_ms = lambda: int(time.time() * 1000)
        self.startup_count = int(self.storage.get_meta("startup_count") or 0) + 1
        self.storage.set_meta("startup_count", str(self.startup_count))
        # Purge jobs run on daemon threads so an HTTP response can return
        # immediately and the UI can watch batches commit. SQLite access is
        # already serialized by the storage lock.
        self._purge_threads: dict[int, threading.Thread] = {}
        self._purge_lock = threading.Lock()
        self._shutting_down = False
        # A job that was 'running' when the process died is now interrupted;
        # the committed batches stay in effect and the job can be retried.
        self.storage.mark_running_jobs_interrupted(self.now_ms())

    # ------------------------------------------------------------- windows
    def phase_of(self, w) -> str:
        now = self.now_ms()
        collect_end = w["start_ms"] + w["collect_ms"]
        grace_end = collect_end + w["grace_ms"]
        if w["status"] == "closed":
            return "closed"
        if now < collect_end:
            return "collecting"
        if now <= grace_end:
            # The boundary instant itself still belongs to the grace
            # period, so a span arriving exactly at grace_end is accepted
            # before the window is finalized (matters when grace_ms == 0).
            return "grace"
        return "due"  # open but strictly past grace end: finalize on touch

    def advance(self) -> None:
        """Background/tick work: finalize any open window past its grace."""
        with self.storage.lock:
            w = self.storage.get_active_window()
            if w is not None and self.phase_of(w) == "due":
                self._finalize_locked(w["id"])

    def create_window(self, payload: dict) -> dict:
        collect_ms = int(payload["collect_ms"])
        grace_ms = int(payload["grace_ms"])
        slow_ms = int(payload["slow_ms"])
        default_quota = int(payload["default_quota"])
        if min(collect_ms, grace_ms, slow_ms, default_quota) < 0:
            raise ApiError(400, "窗口时长、阈值和配额不能为负数")
        if collect_ms < 500:
            raise ApiError(400, "收集窗口至少 500ms")
        if default_quota < 0:
            raise ApiError(400, "默认配额不能为负数")
        with self.storage.lock:
            active = self.storage.get_active_window()
            if active is not None:
                raise ApiError(409, f"已有进行中的窗口 #{active['id']}，请先结束它")
            start_ms = (
                int(payload["start_ms"]) if payload.get("start_ms") is not None
                else self.now_ms()
            )
            wid = self.storage.create_window(
                start_ms, collect_ms, grace_ms, slow_ms, default_quota
            )
            for svc, q in (payload.get("quotas") or {}).items():
                q = int(q)
                if q < 0:
                    raise ApiError(400, f"服务 {svc} 配额不能为负数")
                self.storage.upsert_quota(wid, str(svc), q)
            return self._snapshot_locked(self.storage.get_window(wid))

    def set_quota(self, window_id: int, payload: dict) -> dict:
        service = str(payload.get("service") or "").strip()
        if not service:
            raise ApiError(400, "缺少 service")
        quota = int(payload.get("quota"))
        if quota < 0:
            raise ApiError(400, "配额不能为负数")
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            if w["status"] != "open":
                raise ApiError(409, "窗口已关闭，不能再修改配额")
            self.storage.upsert_quota(window_id, service, quota)
            return self._snapshot_locked(self.storage.get_window(window_id))

    def finalize(self, window_id: int) -> dict:
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            if w["status"] == "closed":
                return self._snapshot_locked(w)  # idempotent
            self._finalize_locked(window_id)
            return self._snapshot_locked(self.storage.get_window(window_id))

    def _finalize_locked(self, window_id: int) -> None:
        """Run the tail decision once and persist exported trace ids.

        The export table has a primary key on (window_id, trace_id), so even
        a duplicate finalize pass can never export a trace twice. The full
        decision set (reason, services, original span count) is snapshotted
        into ``trace_records`` so history survives span-detail purges.
        """
        w = self.storage.get_window(window_id)
        span_rows = self.storage.get_spans(window_id)
        traces = sampler.build_traces(span_rows)
        quotas = self.storage.get_quotas(window_id)
        already = self.storage.get_exported_trace_ids(window_id)
        result = sampler.evaluate_window(
            traces, quotas, w["default_quota"], w["slow_ms"], already
        )
        now = self.now_ms()
        for trace_id, d in result["decisions"].items():
            if d["decision"] == sampler.KEEP and trace_id not in already:
                count = len(traces[trace_id].spans)
                self.storage.save_export(window_id, trace_id, now, count)
        # Snapshot decisions *after* exports are settled so 'exported' is
        # accurate and the record is complete even if a purge follows soon.
        for trace_id, t in traces.items():
            d = result["decisions"][trace_id]
            rec = sampler.trace_record_fields(t)
            rec.update({
                "decision": d["decision"],
                "reason": d["reason"],
                "tier": d["tier"],
                "blocked_service": d.get("blocked_service"),
            })
            self.storage.save_trace_record(
                window_id, trace_id, rec,
                exported=(d["decision"] == sampler.KEEP),
            )
        quota_view = []
        for svc in sorted(set(quotas) | set(result["occupancy"])):
            quota_view.append({
                "service": svc,
                "quota": quotas.get(svc, w["default_quota"]),
                "used": result["occupancy"].get(svc, {"used": 0})["used"],
            })
        self.storage.save_quota_usage(window_id, quota_view)
        self.storage.close_window(window_id, now)

    # ---------------------------------------------------------------- spans
    def submit_span(self, payload: dict) -> dict:
        required = ("span_id", "trace_id", "service", "operation",
                    "start_ms", "duration_ms", "status")
        for key in required:
            if payload.get(key) is None:
                raise ApiError(400, f"缺少字段 {key}")
        span = {
            "span_id": str(payload["span_id"]).strip(),
            "trace_id": str(payload["trace_id"]).strip(),
            "service": str(payload["service"]).strip(),
            "operation": str(payload["operation"]).strip(),
            "start_ms": int(payload["start_ms"]),
            "duration_ms": int(payload["duration_ms"]),
            "status": str(payload["status"]).strip().lower(),
        }
        if not all(span[k] for k in ("span_id", "trace_id", "service", "operation")):
            raise ApiError(400, "span_id/trace_id/service/operation 不能为空")
        if span["duration_ms"] < 0:
            raise ApiError(400, "duration_ms 不能为负数")
        if span["status"] not in VALID_STATUSES:
            raise ApiError(400, f"status 必须是 {sorted(VALID_STATUSES)} 之一")
        now = self.now_ms()
        if span["start_ms"] > now + 2000:
            raise ApiError(400, "start_ms 不能是未来时间（容差 2s）")
        with self.storage.lock:
            self.advance()
            w = self.storage.get_active_window()
            if w is None:
                raise ApiError(409, "当前没有进行中的窗口，请先创建窗口")
            collect_end = w["start_ms"] + w["collect_ms"]
            grace_end = collect_end + w["grace_ms"]
            if span["start_ms"] < w["start_ms"]:
                raise ApiError(
                    400,
                    f"span 时间戳 {span['start_ms']} 早于窗口开始 {w['start_ms']}",
                )
            if span["start_ms"] > grace_end:
                raise ApiError(
                    400,
                    "span 时间戳已超过收集窗口+宽限期，该窗口已不可接收新 span",
                )
            late = span["start_ms"] >= collect_end or now >= collect_end
            inserted = self.storage.insert_span(span, w["id"], now, late)
            snap = self._snapshot_locked(self.storage.get_window(w["id"]))
            snap["span_accepted"] = inserted
            if not inserted:
                snap["duplicate"] = True
            return snap

    # ------------------------------------------------------ purge & preserve
    def preserve_trace(self, window_id: int, trace_id: str) -> dict:
        """Flag one trace as protected from cleanup.

        Only makes sense while its span details still exist; once purged the
        operator is told preservation is no longer possible.
        """
        trace_id = str(trace_id or "").strip()
        if not trace_id:
            raise ApiError(400, "缺少 trace_id")
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            if w["status"] != "closed":
                raise ApiError(409, "只能保全已关闭窗口中的链路")
            rec = self.storage.get_trace_record(window_id, trace_id)
            if rec is None:
                raise ApiError(404, "该链路不属于此窗口")
            if rec["purged_at_ms"] is not None:
                raise ApiError(409, "该链路的 span 明细已清理，无法再保全")
            if rec["preserved_at_ms"] is None:
                self.storage.mark_trace_preserved(
                    window_id, trace_id, self.now_ms()
                )
            return self._snapshot_locked(self.storage.get_window(window_id))

    def start_purge(self, window_id: int, payload: dict) -> dict:
        """Begin (or resume) deleting span details of a closed window.

        Non-preserved traces are deleted whole, one batch per transaction.
        Each batch commits before the next starts, so if the job is
        interrupted the committed batches remain purged and a retry resumes
        from what is left.
        """
        batch_size = int(payload.get("batch_size", 5))
        pause_ms = int(payload.get("pause_ms", 0))
        if batch_size < 1:
            raise ApiError(400, "batch_size 至少为 1")
        if pause_ms < 0 or pause_ms > 10_000:
            raise ApiError(400, "pause_ms 必须在 0 到 10000 之间")
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            if w["status"] != "closed":
                raise ApiError(409, "只能清理已关闭窗口的 span 明细")
            latest = self.storage.get_latest_purge_job(window_id)
            if latest is not None and latest["status"] == "running":
                raise ApiError(409, "该窗口已有正在进行的清理任务")
            remaining = self.storage.trace_ids_for_purge(window_id)
            if not remaining:
                raise ApiError(409, "该窗口没有可清理的 span 明细（已全部清理或已保全）")
            job_id = self.storage.create_purge_job(
                window_id, batch_size, pause_ms, len(remaining), self.now_ms()
            )
        thread = threading.Thread(
            target=self._run_purge_job, args=(job_id,),
            name=f"purge-{job_id}", daemon=True,
        )
        with self._purge_lock:
            self._purge_threads[job_id] = thread
        thread.start()
        return self.purge_status(window_id)

    def purge_status(self, window_id: int) -> dict:
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            job = self.storage.get_latest_purge_job(window_id)
            return {
                "window_id": window_id,
                "job": dict(job) if job else None,
                "remaining_traces": len(
                    self.storage.trace_ids_for_purge(window_id)
                ),
                "remaining_spans": self.storage.count_window_spans(window_id),
            }

    def _run_purge_job(self, job_id: int) -> None:
        """Delete non-preserved traces batch by batch, committing each batch."""
        while True:
            with self.storage.lock:
                job = self.storage.get_purge_job(job_id)
                if job is None or job["status"] != "running":
                    return
                pending = self.storage.trace_ids_for_purge(job["window_id"])
                if not pending:
                    self.storage.update_purge_job(
                        job_id, status="completed", updated_ms=self.now_ms()
                    )
                    return
                batch = pending[:job["batch_size"]]
            # Sleep *between* batches, never inside the lock, so the current
            # window can keep accepting spans while cleanup is in progress.
            if job["purged_traces"] and job["pause_ms"]:
                time.sleep(job["pause_ms"] / 1000.0)
            if self._shutting_down:
                return
            # Re-read the job after sleeping: it may have been interrupted
            # meanwhile, and then this worker must stop without deleting.
            with self.storage.lock:
                job = self.storage.get_purge_job(job_id)
                if job is None or job["status"] != "running":
                    return
            try:
                deleted, purged = self.storage.purge_trace_batch(
                    job["window_id"], batch, self.now_ms()
                )
            except Exception as exc:  # pragma: no cover - defensive
                with self.storage.lock:
                    self.storage.update_purge_job(
                        job_id, status="failed", updated_ms=self.now_ms(),
                        error=str(exc),
                    )
                return
            with self.storage.lock:
                job = self.storage.get_purge_job(job_id)
                self.storage.update_purge_job(
                    job_id,
                    purged_traces=int(job["purged_traces"]) + purged,
                    deleted_spans=int(job["deleted_spans"]) + deleted,
                    updated_ms=self.now_ms(),
                )
        # Thread bookkeeping is best-effort cleanup only.
        with self._purge_lock:
            self._purge_threads.pop(job_id, None)

    def interrupt_purge(self, window_id: int) -> dict:
        """Simulate the worker dying mid-job; committed batches stay purged."""
        with self.storage.lock:
            job = self.storage.get_latest_purge_job(window_id)
            if job is None or job["status"] != "running":
                raise ApiError(409, "该窗口没有正在进行的清理任务")
            self.storage.update_purge_job(
                job["id"], status="interrupted", updated_ms=self.now_ms()
            )
            return self.purge_status(window_id)

    # ------------------------------------------------------------ snapshots
    def _snapshot_locked(self, w) -> dict:
        """Build the full JSON representation of one window.

        Once a window's span details were purged the snapshot is rebuilt from
        the persisted decision snapshot instead of the (now missing) rows, so
        the sampling decision, reasons, original span counts, quota usage and
        export records all remain visible.
        """
        records = self.storage.get_trace_records(w["id"])
        span_rows = self.storage.get_spans(w["id"])
        traces = sampler.build_traces(span_rows)
        quotas = self.storage.get_quotas(w["id"])
        exported = self.storage.get_exported_trace_ids(w["id"])
        phase = self.phase_of(w)
        details_purged = bool(records) and len(span_rows) < sum(
            r["span_count"] for r in records
        )

        if details_purged:
            trace_list = self._records_to_payload(
                records, w["slow_ms"], traces
            )
            occupancy = {
                r["service"]: {"used": r["used"], "quota": r["quota"]}
                for r in self.storage.get_quota_usage(w["id"])
            }
            services_seen = sorted({s for r in records
                                    for s in json.loads(r["services"])})
            result = {"occupancy": occupancy}
        else:
            if w["status"] == "closed":
                result = sampler.evaluate_window(
                    traces, quotas, w["default_quota"], w["slow_ms"], exported
                )
            else:
                result = sampler.tentative_evaluation(
                    traces, quotas, w["default_quota"], w["slow_ms"]
                )
            trace_list = []
            for trace_id in sorted(traces, key=lambda tid: (
                    traces[tid].tier(w["slow_ms"]), traces[tid].start_ms, tid)):
                t = traces[trace_id]
                payload = sampler.trace_payload(
                    t, w["slow_ms"], result["decisions"][trace_id]
                )
                rec = self.storage.get_trace_record(w["id"], trace_id)
                payload["preserved"] = bool(
                    rec and rec["preserved_at_ms"] is not None
                )
                trace_list.append(payload)
            services_seen = sorted({s["service"] for s in span_rows})

        collect_end = w["start_ms"] + w["collect_ms"]
        grace_end = collect_end + w["grace_ms"]
        quota_view = []
        for svc in sorted(set(services_seen) | set(quotas)):
            q = quotas.get(svc, w["default_quota"])
            quota_view.append({
                "service": svc,
                "quota": q,
                "explicit": svc in quotas,
                "used": result["occupancy"].get(svc, {"used": 0})["used"],
            })

        kept = sum(1 for t in trace_list if t["decision"] == sampler.KEEP)
        dropped = sum(1 for t in trace_list
                      if t["decision"] == sampler.DROP_QUOTA)
        original_span_count = (sum(r["span_count"] for r in records)
                               if records else len(span_rows))
        job = self.storage.get_latest_purge_job(w["id"])

        return {
            "window": {
                "id": w["id"],
                "start_ms": w["start_ms"],
                "collect_ms": w["collect_ms"],
                "grace_ms": w["grace_ms"],
                "slow_ms": w["slow_ms"],
                "default_quota": w["default_quota"],
                "status": w["status"],
                "phase": phase,
                "collect_end_ms": collect_end,
                "grace_end_ms": grace_end,
                "finalized_at_ms": w["finalized_at_ms"],
                "server_now_ms": self.now_ms(),
                "details_purged": details_purged,
            },
            "quotas": quota_view,
            "traces": trace_list,
            "exports": [dict(r) for r in self.storage.get_exports(w["id"])],
            "span_count": len(span_rows),
            "original_span_count": original_span_count,
            "kept_trace_count": kept,
            "dropped_trace_count": dropped,
            "purge": {
                "job": dict(job) if job else None,
                "remaining_traces": len(
                    self.storage.trace_ids_for_purge(w["id"])
                ),
            },
        }

    @staticmethod
    def _records_to_payload(records, slow_ms: int, live_traces=None) -> list[dict]:
        """Rebuild a window's trace list from persisted decisions.

        For traces whose rows still exist (preserved, or a partially
        completed cleanup) the live spans are re-attached so the UI can show
        full detail; purged traces show the retained decision metadata only.
        """
        live_traces = live_traces or {}
        out = []
        for r in records:
            services = json.loads(r["services"])
            item = {
                "trace_id": r["trace_id"],
                "services": services,
                "span_count": r["span_count"],
                "latency_ms": r["latency_ms"],
                "has_error": bool(r["has_error"]),
                "has_timeout": bool(r["has_timeout"]),
                "is_slow": r["latency_ms"] >= slow_ms,
                "tier": r["tier"],
                "tier_label": sampler.TIER_LABELS[r["tier"]],
                "late_span_count": r["late_span_count"],
                "start_ms": r["start_ms"],
                "decision": r["decision"],
                "reason": r["reason"],
                "blocked_service": r["blocked_service"],
                "preserved": r["preserved_at_ms"] is not None,
                "details_purged": r["purged_at_ms"] is not None,
                "spans": [],
            }
            live = live_traces.get(r["trace_id"])
            if live is not None:
                item["spans"] = sampler.trace_payload(
                    live, slow_ms, {"decision": r["decision"],
                                    "reason": r["reason"]}
                )["spans"]
            out.append(item)
        out.sort(key=lambda t: (t["tier"], t["start_ms"], t["trace_id"]))
        return out

    def current_snapshot(self) -> dict:
        with self.storage.lock:
            self.advance()
            w = self.storage.get_active_window()
            if w is None:
                rows = self.storage.list_windows(limit=1)
                if not rows:
                    return {"window": None, "message": "尚未创建窗口"}
                w = rows[0]
            return self._snapshot_locked(w)

    def window_detail(self, window_id: int) -> dict:
        with self.storage.lock:
            w = self.storage.get_window(window_id)
            if w is None:
                raise ApiError(404, "窗口不存在")
            return self._snapshot_locked(w)

    def history(self) -> dict:
        with self.storage.lock:
            out = []
            for w in self.storage.list_windows():
                records = self.storage.get_trace_records(w["id"])
                remaining_spans = self.storage.count_window_spans(w["id"])
                original_spans = (sum(r["span_count"] for r in records)
                                  if records else remaining_spans)
                job = self.storage.get_latest_purge_job(w["id"])
                purged = bool(records) and remaining_spans < original_spans
                out.append({
                    "id": w["id"],
                    "start_ms": w["start_ms"],
                    "status": w["status"],
                    "collect_ms": w["collect_ms"],
                    "grace_ms": w["grace_ms"],
                    "slow_ms": w["slow_ms"],
                    "default_quota": w["default_quota"],
                    "span_count": remaining_spans,
                    "original_span_count": original_spans,
                    "export_count": len(self.storage.get_exports(w["id"])),
                    "kept_trace_count": sum(
                        1 for r in records if r["decision"] == sampler.KEEP
                    ),
                    "dropped_trace_count": sum(
                        1 for r in records
                        if r["decision"] == sampler.DROP_QUOTA
                    ),
                    "preserved_trace_count": sum(
                        1 for r in records if r["preserved_at_ms"] is not None
                    ),
                    "details_purged": purged,
                    "purge_status": job["status"] if job else None,
                })
            return {"windows": out, "startup_count": self.startup_count}

    def reset(self) -> dict:
        with self.storage.lock:
            self.storage.reset()
            self.startup_count = 1
            self.storage.set_meta("startup_count", "1")
        return {"ok": True, "message": "所有数据已清空"}

    def simulate_restart(self) -> dict:
        """Re-open the Storage from the same file to prove resumability."""
        with self.storage.lock:
            # Tell in-flight purge workers to stop before the connection goes
            # away; their jobs become 'interrupted' and keep committed work.
            self._shutting_down = True
            self.storage.close()
            self.storage = Storage(self.db_path)
            self._shutting_down = False
            self.startup_count = int(
                self.storage.get_meta("startup_count") or 0
            ) + 1
            self.storage.set_meta("startup_count", str(self.startup_count))
            # In-flight purge workers just lost their database handle: mark
            # their jobs interrupted. Batches already committed stay purged.
            interrupted = self.storage.mark_running_jobs_interrupted(
                self.now_ms()
            )
            return {"ok": True, "startup_count": self.startup_count,
                    "interrupted_purge_jobs": interrupted,
                    "message": "已从 SQLite 重新加载全部状态（模拟进程重启）"}


# --------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "TailSamplingWorkbench/1.0"

    def log_message(self, fmt, *args):  # quieter logs
        pass

    @property
    def state(self) -> AppState:
        return self.server.state  # type: ignore[attr-defined]

    def _send_json(self, status: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        if length > MAX_BODY_BYTES:
            raise ApiError(413, "请求体过大")
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ApiError(400, "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "请求体必须是 JSON 对象")
        return data

    def _send_static(self, rel: str) -> None:
        # Prevent path traversal: resolve inside STATIC_DIR.
        safe = os.path.normpath(os.path.join(STATIC_DIR, rel))
        if not safe.startswith(STATIC_DIR + os.sep) and safe != STATIC_DIR:
            self._send_json(403, {"error": "forbidden"})
            return
        if os.path.isdir(safe) or not os.path.isfile(safe):
            self._send_json(404, {"error": "not found"})
            return
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(os.path.splitext(safe)[1], "application/octet-stream")
        with open(safe, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------- routing
    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path == "/":
                self._send_static("index.html")
            elif path.startswith("/static/"):
                self._send_static(path[len("/static/"):])
            elif path == "/api/health":
                self._send_json(200, {"ok": True,
                                      "startup_count": self.state.startup_count})
            elif path == "/api/window/current":
                self._send_json(200, self.state.current_snapshot())
            elif path == "/api/windows":
                self._send_json(200, self.state.history())
            elif path.startswith("/api/windows/") and path.endswith("/purge/status"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.purge_status(wid))
            elif path.startswith("/api/windows/"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.window_detail(wid))
            else:
                self._send_json(404, {"error": "not found"})
        except ApiError as e:
            self._send_json(e.status, {"error": e.message})
        except ValueError:
            self._send_json(400, {"error": "非法的 id 参数"})
        except Exception as e:  # pragma: no cover - defensive
            self._send_json(500, {"error": f"服务器内部错误: {e}"})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path.rstrip("/")
            payload = self._read_json()
            if path == "/api/windows":
                self._send_json(201, self.state.create_window(payload))
            elif path.startswith("/api/windows/") and path.endswith("/quotas"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.set_quota(wid, payload))
            elif path.startswith("/api/windows/") and path.endswith("/finalize"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.finalize(wid))
            elif path.startswith("/api/windows/") and path.endswith("/preserve"):
                wid = int(path.split("/")[3])
                trace_id = payload.get("trace_id")
                self._send_json(200, self.state.preserve_trace(wid, trace_id))
            elif path.startswith("/api/windows/") and path.endswith("/purge/interrupt"):
                wid = int(path.split("/")[3])
                self._send_json(200, self.state.interrupt_purge(wid))
            elif path.startswith("/api/windows/") and path.endswith("/purge"):
                wid = int(path.split("/")[3])
                self._send_json(202, self.state.start_purge(wid, payload))
            elif path == "/api/spans":
                snap = self.state.submit_span(payload)
                self._send_json(200 if snap.get("span_accepted", True) else 200,
                                snap)
            elif path.startswith("/api/demo/"):
                from .demo import run_demo
                name = path[len("/api/demo/"):]
                self._send_json(200, run_demo(self.state, name))
            elif path == "/api/simulate-restart":
                self._send_json(200, self.state.simulate_restart())
            elif path == "/api/reset":
                self._send_json(200, self.state.reset())
            else:
                self._send_json(404, {"error": "not found"})
        except ApiError as e:
            self._send_json(e.status, {"error": e.message})
        except (TypeError, ValueError) as e:
            self._send_json(400, {"error": f"参数类型错误: {e}"})
        except Exception as e:  # pragma: no cover - defensive
            self._send_json(500, {"error": f"服务器内部错误: {e}"})


def create_server(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.state = AppState(db_path)  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    db_path = os.environ.get("DB_PATH", "/data/tracing.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    httpd = create_server(host, port, db_path)
    print(f"tail-sampling workbench listening on http://{host}:{port} "
          f"(db={db_path})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.state.storage.close()
        httpd.server_close()


if __name__ == "__main__":
    main()
