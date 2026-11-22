"""Manual HTTP smoke test against a running server (default :8091).

Mirrors what an operator does in the browser, using only the stdlib.
"""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8091"
PASS, FAIL = 0, 0


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


print("== static assets ==")
html = urllib.request.urlopen(BASE + "/", timeout=5).read().decode()
check("index.html served", "尾部采样" in html)
js = urllib.request.urlopen(BASE + "/static/app.js", timeout=5).read().decode()
check("app.js served", "renderWindow" in js)
css = urllib.request.urlopen(BASE + "/static/styles.css", timeout=5).status
check("styles.css 200", css == 200)

print("== reset & create window ==")
call("POST", "/api/reset", {})
st, w = call("POST", "/api/windows", {
    "collect_ms": 2000, "grace_ms": 2000, "slow_ms": 400,
    "default_quota": 1, "quotas": {"gateway": 2, "payment": 1, "inventory": 0}})
check("window created", st == 201, str(st))
wid = w["window"]["id"]
base = w["window"]["start_ms"]
check("phase collecting", w["window"]["phase"] == "collecting", w["window"]["phase"])

print("== out-of-order spans ==")
for s in [
    ("s3", "T1", "gateway", base + 300, 100, "ok"),
    ("s1", "T1", "gateway", base, 200, "ok"),
    ("s2", "T1", "payment", base + 100, 80, "ok"),
]:
    st, _ = call("POST", "/api/spans", dict(
        span_id=s[0], trace_id=s[1], service=s[2], operation="op",
        start_ms=s[3], duration_ms=s[4], status=s[5]))
    check(f"span {s[0]} accepted", st == 200)
st, dup = call("POST", "/api/spans", dict(
    span_id="s1", trace_id="T1", service="gateway", operation="op",
    start_ms=base, duration_ms=200, status="ok"))
check("duplicate span flagged", dup.get("duplicate") is True)

print("== multi-service trace blocked by inventory quota 0 ==")
for sid, svc, off in (("m1", "gateway", 0), ("m2", "orders", 20),
                      ("m3", "inventory", 40)):
    call("POST", "/api/spans", dict(span_id=sid, trace_id="T-multi",
         service=svc, operation="op", start_ms=base + off,
         duration_ms=50, status="ok"))

print("== normal competitor for payment quota ==")
call("POST", "/api/spans", dict(span_id="c1", trace_id="T-comp",
     service="payment", operation="op", start_ms=base + 50,
     duration_ms=30, status="ok"))

print("== enter grace period, late error arrives ==")
time.sleep(2.2)
st, snap = call("GET", "/api/window/current")
check("phase grace", snap["window"]["phase"] == "grace", snap["window"]["phase"])
st, late = call("POST", "/api/spans", dict(
    span_id="late-err", trace_id="T1", service="payment", operation="settle",
    start_ms=base + 1500, duration_ms=120, status="error"))
check("late span accepted in grace", st == 200, str(st))
check("late flag set", any(sp["late"] for sp in
      next(t for t in late["traces"] if t["trace_id"] == "T1")["spans"]))

print("== wait for auto finalize ==")
deadline = time.time() + 6
while time.time() < deadline:
    st, snap = call("GET", "/api/window/current")
    if snap["window"]["status"] == "closed":
        break
    time.sleep(0.5)
check("window auto-closed", snap["window"]["status"] == "closed")
dec = {t["trace_id"]: t for t in snap["traces"]}
check("T1 kept (late error priority)", dec["T1"]["decision"] == "keep",
      dec["T1"]["reason"])
check("T-comp dropped (payment quota full)",
      dec["T-comp"]["decision"] == "drop_quota", dec["T-comp"]["reason"])
check("T-multi dropped (inventory=0, whole trace)",
      dec["T-multi"]["decision"] == "drop_quota", dec["T-multi"]["reason"])
check("T-multi blocked_service=inventory",
      dec["T-multi"]["blocked_service"] == "inventory")
exports = {e["trace_id"] for e in snap["exports"]}
check("only T1 exported, no partial spans", exports == {"T1"}, str(exports))
pay = next(q for q in snap["quotas"] if q["service"] == "payment")
check("payment quota not exceeded", pay["used"] <= pay["quota"] == 1,
      str(pay))

print("== idempotent re-finalize ==")
st, again = call("POST", f"/api/windows/{wid}/finalize", {})
check("re-finalize 200", st == 200)
check("no duplicate export", len(again["exports"]) == 1)

print("== reject span after close ==")
st, err = call("POST", "/api/spans", dict(
    span_id="x", trace_id="Tx", service="gateway", operation="op",
    start_ms=base + 10, duration_ms=5, status="ok"))
check("post-close span rejected 409", st == 409, str(st))

print("== history ==")
st, hist = call("GET", "/api/windows")
check("history lists window", any(x["id"] == wid for x in hist["windows"]))

print("== preserve before cleanup ==")
# Preserve T1 (already exported) so its details must survive the purge.
st, pres = call("POST", f"/api/windows/{wid}/preserve", {"trace_id": "T1"})
check("preserve before purge 200", st == 200, str(st))
t1 = next(t for t in pres["traces"] if t["trace_id"] == "T1")
check("T1 flagged preserved", t1["preserved"] is True)

print("== interrupted cleanup, then retry ==")
st, job = call("POST", f"/api/windows/{wid}/purge",
               {"batch_size": 1, "pause_ms": 1000})
check("purge accepted 202", st == 202, str(st))
time.sleep(0.4)  # first batch committed, worker sleeping between batches
st, ip = call("POST", f"/api/windows/{wid}/purge/interrupt", {})
check("interrupt 200", st == 200, str(st))
st, before = call("GET", f"/api/windows/{wid}")
remaining_before = before["span_count"]
check("some details already purged before interruption",
      0 < remaining_before < before["original_span_count"],
      f"{remaining_before}/{before['original_span_count']}")
# Wait out the sleep of the still-alive worker thread before retrying, so
# the retry sees the job finished/interrupted rather than briefly 'running'.
time.sleep(1.1)
# A current window still accepts spans while cleanup is paused/interrupted.
st, w2 = call("POST", "/api/windows", {
    "collect_ms": 20000, "grace_ms": 5000, "slow_ms": 500,
    "default_quota": 3, "quotas": {}})
check("new window while cleanup interrupted", st == 201, str(st))
base2 = w2["window"]["start_ms"]
st, live = call("POST", "/api/spans", dict(
    span_id="live-1", trace_id="T-live", service="gateway", operation="op",
    start_ms=base2 + 5, duration_ms=10, status="ok"))
check("span accepted during cleanup", st == 200 and live.get("span_accepted"))
st, retry = call("POST", f"/api/windows/{wid}/purge",
                 {"batch_size": 1, "pause_ms": 0})
check("retry purge 202", st == 202, str(st))
deadline = time.time() + 8
while time.time() < deadline:
    st, ps = call("GET", f"/api/windows/{wid}/purge/status")
    if ps["job"] and ps["job"]["status"] == "completed":
        break
    time.sleep(0.2)
check("cleanup completed after retry",
      ps["job"]["status"] == "completed", str(ps["job"]))
check("only preserved trace's spans remain", ps["remaining_spans"] == 4,
      str(ps["remaining_spans"]))

print("== purged window still answers history questions ==")
st, detail = call("GET", f"/api/windows/{wid}")
check("details_purged flag", detail["window"]["details_purged"] is True)
check("original span count retained",
      detail["original_span_count"] == before["original_span_count"])
keep_t = next(t for t in detail["traces"] if t["trace_id"] == "T1")
drop_t = next(t for t in detail["traces"] if t["trace_id"] == "T-multi")
check("preserved trace keeps details", len(keep_t["spans"]) == 4,
      str(len(keep_t["spans"])))
check("purged trace keeps decision", drop_t["decision"] == "drop_quota")
check("purged trace keeps reason", "配额" in drop_t["reason"])
check("purged trace keeps original span count", drop_t["span_count"] == 3)
check("purged trace has no span rows", drop_t["spans"] == [])
check("quota usage retained after purge",
      any(q["used"] == 1 and q["quota"] == 1
          for q in detail["quotas"] if q["service"] == "payment"))
check("exports retained after purge", len(detail["exports"]) == 1)
st, err = call("POST", f"/api/windows/{wid}/preserve", {"trace_id": "T-multi"})
check("preserve after purge rejected 409", st == 409, str(st))

print("== duplicate span_id still detected after purge ==")
st, dup2 = call("POST", "/api/spans", dict(
    span_id="s1", trace_id="T-new", service="gateway", operation="op",
    start_ms=base2 + 10, duration_ms=10, status="ok"))
check("duplicate after purge flagged", dup2.get("duplicate") is True)
st, cur2 = call("GET", "/api/window/current")
check("duplicate after purge not stored", cur2["span_count"] == 1,
      str(cur2["span_count"]))

print("== history marks cleaned windows ==")
st, hist2 = call("GET", "/api/windows")
row = next(x for x in hist2["windows"] if x["id"] == wid)
check("history marks details_purged", row["details_purged"] is True)
check("history keeps original span count",
      row["original_span_count"] == detail["original_span_count"])

print("== simulate restart (reload from SQLite) ==")
st, r = call("POST", "/api/simulate-restart", {})
check("restart endpoint ok", st == 200 and r["startup_count"] == 2, str(r))
st, snap2 = call("GET", f"/api/windows/{wid}")
check("closed window restored after restart",
      snap2["window"]["status"] == "closed" and len(snap2["exports"]) == 1)

print(f"\nSMOKE RESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
