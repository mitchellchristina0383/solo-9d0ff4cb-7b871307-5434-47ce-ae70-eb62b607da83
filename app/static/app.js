/* Tail-sampling workbench frontend — vanilla JS, no build step. */

const $ = (id) => document.getElementById(id);
const api = async (path, opts = {}) => {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
};

let toastTimer = null;
function toast(msg, kind = "") {
  const el = $("toast");
  el.textContent = msg;
  el.className = `toast ${kind}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add("hidden"), 4200);
}

function fmtTime(ms) {
  if (ms === null || ms === undefined) return "—";
  const d = new Date(ms);
  return d.toLocaleTimeString("zh-CN", { hour12: false }) +
    "." + String(d.getMilliseconds()).padStart(3, "0");
}

const PHASE_LABEL = {
  collecting: "收集中",
  grace: "宽限期（迟到 span 仍可改变决策）",
  due: "等待导出",
  closed: "已关闭并导出",
};
const DECISION_LABEL = {
  keep: "保留",
  waiting: "等待",
  drop_quota: "丢弃·配额",
  drop_priority: "丢弃·优先级",
};
const PURGE_STATUS_LABEL = {
  running: "清理中",
  interrupted: "已中断·可重试",
  completed: "已清理",
  failed: "清理失败",
};

// ---------------------------------------------------------------- window UI
// When the operator clicks a history row, the panel is "pinned" to that
// window (often a closed one) so preservation / cleanup actions target the
// window actually being viewed rather than whichever window is active.
let pinnedWindowId = null;
let lastSnap = null;

function unpin() {
  pinnedWindowId = null;
  $("pinned-banner").classList.add("hidden");
  refresh();
}

function renderWindow(snap) {
  const panel = $("window-panel");
  lastSnap = snap;
  if (!snap || !snap.window) {
    panel.innerHTML = '<p class="hint">尚未创建窗口。</p>';
    $("btn-finalize").disabled = true;
    $("btn-purge").disabled = true;
    $("btn-interrupt").disabled = true;
    $("purge-panel").innerHTML = '<p class="hint">窗口关闭后可清理。</p>';
    $("quota-panel").innerHTML = '<p class="hint">窗口创建后展示。</p>';
    $("trace-panel").innerHTML = '<p class="hint">窗口创建后展示。</p>';
    $("export-panel").innerHTML = '<p class="hint">窗口关闭后展示。</p>';
    return;
  }
  const w = snap.window;
  const phase = w.phase;
  const purge = snap.purge || {};
  const job = purge.job;
  const purged = !!w.details_purged;
  const jobRunning = job && job.status === "running";
  const isCurrent = pinnedWindowId === null;
  // Cleanup only ever applies to a closed window. When a history window is
  // pinned we act on it; otherwise on the current (possibly just-closed) one.
  const canPurge = w.status === "closed" && !jobRunning
    && purge.remaining_traces > 0;
  $("btn-finalize").disabled = w.status === "closed" || !isCurrent;
  $("btn-purge").disabled = !canPurge;
  $("btn-purge").textContent = job && job.status === "interrupted"
    ? "重试清理（从未清理处继续）" : "清理 span 明细（按链路分批）";
  $("btn-interrupt").disabled = !jobRunning;
  $("batch-size").disabled = jobRunning;
  $("purge-panel").innerHTML = job
    ? `<div class="purge-status">
         <span class="badge ${job.status === "completed" ? "closed"
              : job.status === "running" ? "grace" : "due"}">
           ${PURGE_STATUS_LABEL[job.status] || job.status} · 任务 #${job.id}
         </span>
         <span class="metric">已清理 ${job.purged_traces}/${job.total_traces} 条链路</span>
         <span class="metric">删除 span ${job.deleted_spans}</span>
         <span class="metric">每批 ${job.batch_size} 条</span>
         <span class="metric">剩余 ${purge.remaining_traces} 条待清 / ${snap.span_count} 个 span</span>
         ${job.error ? `<span class="late-flag">${job.error}</span>` : ""}
       </div>
       ${job.status === "interrupted" ? '<p class="hint">清理被中断，已提交的批次仍然生效；点击上方按钮可从未清理的链路继续。</p>' : ""}
       ${job.status === "running" ? '<p class="hint">批次之间当前窗口仍可接收 span；可点击「模拟中断」验证断点续清。</p>' : ""}`
    : `<p class="hint">共 ${snap.original_span_count} 个原始 span，${purge.remaining_traces || 0} 条链路可清理。保全的链路会跳过清理。</p>`;

  const now = w.server_now_ms;
  const total = w.collect_ms + w.grace_ms;
  const elapsed = Math.min(Math.max(now - w.start_ms, 0), total);
  const collectPct = Math.min(elapsed / total, w.collect_ms / total) * 100;
  const gracePct = Math.max(0, (elapsed - w.collect_ms) / total) * 100;

  panel.innerHTML = `
    <div class="phase-row">
      <span class="badge ${phase}">#${w.id} · ${PHASE_LABEL[phase] || phase}</span>
      <span class="metric">开始 ${fmtTime(w.start_ms)}</span>
      <span class="metric">收集截止 ${fmtTime(w.collect_end_ms)}</span>
      <span class="metric">宽限截止 ${fmtTime(w.grace_end_ms)}</span>
      <span class="metric">慢请求阈值 ${w.slow_ms}ms</span>
    </div>
    <div class="kv">
      <div><b>收集窗口</b>${w.collect_ms} ms</div>
      <div><b>宽限期</b>${w.grace_ms} ms</div>
      <div><b>默认配额</b>${w.default_quota} 条链路/服务</div>
      <div><b>现存 span</b>${snap.span_count}${purged ? ` / 原始 ${snap.original_span_count}` : ""}</div>
      <div><b>保留链路</b>${snap.kept_trace_count}</div>
      <div><b>丢弃链路</b>${snap.dropped_trace_count}</div>
      ${purged ? '<div><b>明细状态</b><span class="late-flag">已清理（保留决策与配额）</span></div>' : ""}
    </div>
    <div class="progress" title="蓝=收集，橙=宽限">
      <span style="width:${collectPct}%; display:inline-block; height:100%; background:var(--accent); float:left;"></span
      ><span class="grace-part" style="width:${gracePct}%; display:inline-block; height:100%; float:left;"></span>
    </div>`;

  // quotas
  $("quota-panel").innerHTML = snap.quotas.length
    ? snap.quotas.map((q) => {
        const pct = q.quota === 0 ? (q.used > 0 ? 100 : 0)
          : Math.min(100, (q.used / q.quota) * 100);
        const cls = q.used >= q.quota ? "full" : pct >= 70 ? "warn" : "";
        return `<div class="quota-item">
          <div class="quota-head">
            <span><b>${q.service}</b> ${q.explicit ? "" : '<span class="tier-tag">默认配额</span>'}</span>
            <span>${q.used} / ${q.quota} 条链路${q.used >= q.quota ? " · 已耗尽" : ""}</span>
          </div>
          <div class="bar"><span class="${cls}" style="width:${pct}%"></span></div>
        </div>`;
      }).join("")
    : '<p class="hint">当前窗口还没有任何服务的 span。</p>';

  // traces
  $("trace-panel").innerHTML = snap.traces.length
    ? snap.traces.map((t, i) => `
      <div class="trace" data-idx="${i}">
        <div class="trace-head">
          <span class="trace-title">${t.trace_id}</span>
          <span class="decision ${t.decision}">${DECISION_LABEL[t.decision] || t.decision}</span>
          ${t.preserved ? '<span class="preserve-tag">已保全</span>' : ""}
          ${t.details_purged ? '<span class="purged-tag">明细已清理</span>' : ""}
          <span class="tier-tag t${t.tier}">${t.tier_label}${t.has_timeout ? "·超时" : t.has_error ? "·错误" : ""}</span>
          <span class="reason">${t.reason}${t.late_span_count ? ` <span class="late-flag">（含 ${t.late_span_count} 个迟到 span）</span>` : ""}</span>
          <span class="metric">原始 ${t.span_count} span · ${t.services.map(s => `<span class="svc-tag">${s}</span>`).join(" ")} · ${t.latency_ms}ms</span>
          ${w.status === "closed" && !t.details_purged && !t.preserved
            ? `<button class="mini preserve" data-preserve="${t.trace_id}">保全</button>` : ""}
          ${t.details_purged
            ? '<span class="metric lost-hint">明细已清理，无法保全</span>' : ""}
        </div>
        <div class="trace-body">
          ${t.details_purged || !t.spans.length
            ? '<p class="hint">该链路的 span 明细已清理，仅保留清理前的采样决定、原因、原始 span 数与配额用量。</p>'
            : `<table>
            <thead><tr><th>span_id</th><th>服务</th><th>操作</th><th>开始时间</th><th>耗时</th><th>状态</th><th>到达</th></tr></thead>
            <tbody>${t.spans.map(s => `
              <tr>
                <td><code>${s.span_id}</code>${s.late ? ' <span class="late-flag">迟到</span>' : ""}</td>
                <td>${s.service}</td><td>${s.operation}</td>
                <td>${fmtTime(s.start_ms)}</td><td>${s.duration_ms}ms</td>
                <td class="status-${s.status}">${s.status}</td>
                <td>${fmtTime(s.received_ms)}</td>
              </tr>`).join("")}
            </tbody></table>`}
        </div>
      </div>`).join("")
    : '<p class="hint">还没有 span。在上方手工提交，或点击演示场景。</p>';

  document.querySelectorAll(".trace-head").forEach((head) => {
    head.addEventListener("click", () => head.parentElement.classList.toggle("open"));
  });
  document.querySelectorAll("button[data-preserve]").forEach((btn) => {
    btn.addEventListener("click", async (ev) => {
      ev.stopPropagation();
      try {
        await api(`/api/windows/${w.id}/preserve`, {
          method: "POST", body: { trace_id: btn.dataset.preserve },
        });
        toast(`已保全链路 ${btn.dataset.preserve}，清理将跳过它`, "success");
        refresh();
      } catch (e) { toast(e.message, "error"); }
    });
  });

  // exports
  $("export-panel").innerHTML = snap.exports.length
    ? `<table><thead><tr><th>trace_id</th><th>导出时间</th><th>span 数</th></tr></thead>
       <tbody>${snap.exports.map(e => `
         <tr><td><code>${e.trace_id}</code></td><td>${fmtTime(e.exported_at_ms)}</td><td>${e.span_count}</td></tr>`).join("")}
       </tbody></table>
       <p class="hint">导出记录持久化在 SQLite，重复结束窗口不会产生重复导出。</p>`
    : (w.status === "closed"
        ? '<p class="hint">本窗口没有任何链路满足保留条件。</p>'
        : '<p class="hint">窗口关闭后在此展示导出记录。</p>');
}

// ------------------------------------------------------------------ polling
async function refresh() {
  try {
    const [snap, hist] = await Promise.all([
      pinnedWindowId === null
        ? api("/api/window/current")
        : api(`/api/windows/${pinnedWindowId}`),
      api("/api/windows"),
    ]);
    $("health-dot").className = "dot ok";
    $("health-text").textContent = "服务在线";
    $("startup-count").textContent = `启动次数 ${hist.startup_count}`;
    renderWindow(snap);
    $("history-panel").innerHTML = hist.windows.length
      ? `<table class="history"><thead><tr>
          <th>#</th><th>状态</th><th>开始</th><th>收集(ms)</th><th>宽限(ms)</th>
          <th>慢阈值</th><th>span 数</th><th>导出链路数</th><th>明细</th></tr></thead><tbody>
        ${hist.windows.map(w => `
          <tr style="cursor:pointer" data-wid="${w.id}" title="点击查看详情">
            <td>${w.id}</td>
            <td><span class="badge ${w.status === "closed" ? "closed" : "collecting"}">${w.status === "closed" ? "已关闭" : "进行中"}</span></td>
            <td>${fmtTime(w.start_ms)}</td><td>${w.collect_ms}</td><td>${w.grace_ms}</td>
            <td>${w.slow_ms}</td>
            <td>${w.details_purged ? `<span class="purged-tag">已清理</span> ${w.span_count}/${w.original_span_count}` : w.span_count}</td>
            <td>${w.export_count}</td>
            <td>${w.details_purged ? `<span class="purged-tag">明细已清理</span>`
                  : w.purge_status === "running" ? '<span class="late-flag">清理中</span>'
                  : w.purge_status === "interrupted" ? '<span class="late-flag">清理已中断</span>'
                  : w.preserved_trace_count ? `已保全 ${w.preserved_trace_count} 条` : "完整"}</td>          </tr>`).join("")}
        </tbody></table>`
      : '<p class="hint">暂无历史窗口。</p>';
    document.querySelectorAll("tr[data-wid]").forEach((tr) => {
      tr.addEventListener("click", async () => {
        pinnedWindowId = Number(tr.dataset.wid);
        const detail = await api(`/api/windows/${pinnedWindowId}`);
        renderWindow(detail);
        $("pinned-banner").classList.remove("hidden");
        $("pinned-label").textContent = `正在查看历史窗口 #${pinnedWindowId}`;
      });
    });
  } catch (e) {
    $("health-dot").className = "dot bad";
    $("health-text").textContent = "服务不可用: " + e.message;
  }
}

// ------------------------------------------------------------------ events
$("btn-create-window").addEventListener("click", async () => {
  let quotas = {};
  try {
    quotas = JSON.parse($("quotas_json").value || "{}");
  } catch {
    return toast("每服务配额不是合法 JSON", "error");
  }
  try {
    const body = {
      collect_ms: Number($("collect_ms").value),
      grace_ms: Number($("grace_ms").value),
      slow_ms: Number($("slow_ms").value),
      default_quota: Number($("default_quota").value),
      quotas,
    };
    await api("/api/windows", { method: "POST", body });
    toast("窗口已创建", "success");
    refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("btn-finalize").addEventListener("click", async () => {
  try {
    const snap = await api("/api/window/current");
    if (!snap.window) return toast("没有进行中的窗口", "error");
    await api(`/api/windows/${snap.window.id}/finalize`, { method: "POST", body: {} });
    toast("窗口已结束，尾部采样决策已落库", "success");
    refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("btn-submit-span").addEventListener("click", async () => {
  const startVal = $("start_ms").value;
  const body = {
    span_id: $("span_id").value.trim(),
    trace_id: $("trace_id").value.trim(),
    service: $("service").value.trim(),
    operation: $("operation").value.trim(),
    start_ms: startVal === "" ? Date.now() : Number(startVal),
    duration_ms: Number($("duration_ms").value),
    status: $("status").value,
  };
  try {
    const snap = await api("/api/spans", { method: "POST", body });
    if (snap.duplicate) toast("span_id 已存在，幂等忽略（未重复入库）", "error");
    else toast(`span 已接收${snap.window && snap.window.phase === "grace" ? "（处于宽限期，标记为迟到）" : ""}`, "success");
    refresh();
  } catch (e) { toast(e.message, "error"); }
});

document.querySelectorAll("button[data-demo]").forEach((btn) => {
  btn.addEventListener("click", async () => {
    try {
      const r = await api(`/api/demo/${btn.dataset.demo}`, { method: "POST", body: {} });
      $("demo-msg").textContent = r.message;
      toast("演示场景已装载", "success");
      refresh();
    } catch (e) { toast(e.message, "error"); }
  });
});

$("btn-purge").addEventListener("click", async () => {
  try {
    const snap = lastSnap;
    if (!snap || !snap.window) return toast("没有可清理的窗口", "error");
    if (!confirm(`将按整条链路分批清理窗口 #${snap.window.id} 的 span 明细（已保全的链路跳过），确定继续？`)) return;
    const r = await api(`/api/windows/${snap.window.id}/purge`, {
      method: "POST",
      body: {
        batch_size: Number($("batch-size").value) || 5,
        pause_ms: Number($("batch-pause").value) || 0,
      },
    });
    toast(`清理任务 #${r.job.id} 已启动`, "success");
    refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("btn-interrupt").addEventListener("click", async () => {
  try {
    const snap = lastSnap;
    if (!snap || !snap.window) return;
    await api(`/api/windows/${snap.window.id}/purge/interrupt`, { method: "POST", body: {} });
    toast("已模拟清理中断；已提交的批次仍然生效，可点击重试继续", "error");
    refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("btn-restart").addEventListener("click", async () => {
  try {
    const r = await api("/api/simulate-restart", { method: "POST", body: {} });
    toast(r.message, "success");
    refresh();
  } catch (e) { toast(e.message, "error"); }
});

$("btn-reset").addEventListener("click", async () => {
  if (!confirm("确定清空所有窗口、span 和导出记录？")) return;
  try {
    await api("/api/reset", { method: "POST", body: {} });
    $("demo-msg").textContent = "";
    toast("数据已清空", "success");
    refresh();
  } catch (e) { toast(e.message, "error"); }
});

refresh();
setInterval(refresh, 1000);
