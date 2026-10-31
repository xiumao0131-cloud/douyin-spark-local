const $ = (selector) => document.querySelector(selector);

const STATUS_LABEL = {
  confirmed: "发送已确认",
  unconfirmed: "未确认",
  needs_confirmation: "待核实",
  rejected: "被拒收",
  failed: "失败",
  skipped: "跳过",
  pending: "待发送",
  in_flight: "发送中",
  login_blocked: "登录失效",
};
const STATUS_CLASS = {
  confirmed: "strong",
  unconfirmed: "dim",
  needs_confirmation: "outline",
  rejected: "outline",
  failed: "outline",
  skipped: "",
  pending: "",
  in_flight: "strong",
  login_blocked: "outline",
};
const WHEN_LABEL = {
  any: "通用",
  morning: "晨间",
  forenoon: "上午",
  noon: "午间",
  afternoon: "下午",
  evening: "晚间",
};
const RUN_STATUS_LABEL = {
  pending: "待执行",
  done: "已完成",
  missed: "已错过",
  failed: "失败",
  empty: "空闲",
};

function toast(text) {
  const el = $("#toast");
  el.textContent = text;
  el.classList.remove("hidden");
  setTimeout(() => el.classList.add("hidden"), 3200);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = await response.json();
      detail = body.detail || detail;
    } catch (err) { /* ignore */ }
    throw new Error(detail);
  }
  return response.json();
}

async function loadOverview() {
  const data = await api("/api/overview");
  const login = data.login;
  const loginPill = $("#status-pill");
  if (login.ok === true) {
    loginPill.className = "pill pill-strong";
    loginPill.textContent = "登录正常";
  } else if (login.ok === false) {
    loginPill.className = "pill pill-outline";
    loginPill.textContent = "登录失效";
  } else {
    loginPill.className = "pill pill-gray";
    loginPill.textContent = "未检测";
  }
  $("#ov-progress").textContent = `${data.confirmedToday} / ${data.friendsTotal}`;
  $("#ov-progress-note").textContent =
    `发送已确认 ${data.stats.confirmed} · 未确认 ${data.stats.unconfirmed} · 待核实 ${data.stats.needs_confirmation || 0} · 拒收 ${data.stats.rejected} · 失败 ${data.stats.failed}`;
  $("#ov-login").textContent = login.ok === true ? "正常" : login.ok === false ? "失效" : "–";
  $("#ov-login-note").textContent = login.note || (login.checkedAt ? `上次检查 ${login.checkedAt.slice(11, 16)}` : "尚未检查过");
  $("#ov-window").textContent = data.window;
  $("#ov-next").textContent = `下次计划：${data.nextRunAt}`;
  $("#ov-pool").textContent = `${data.poolSize} 条`;
  $("#ov-paused").textContent = data.paused ? "已暂停（人工确认后恢复）" : "正常运转";
  $("#ov-day").textContent = data.day;
  $("#ov-run").textContent = data.lastRun.status
    ? `${data.lastRun.trigger} · ${data.lastRun.status} · ${(data.lastRun.startedAt || "").slice(11, 19)}`
    : "今天还没有运行记录";
  $("#btn-pause").textContent = data.paused ? "恢复发送" : "暂停";
  $("#btn-pause").className = data.paused ? "btn btn-primary" : "btn btn-warn";
  $("#ov-paused").className = data.paused ? "card-note" : "card-note";
  return data;
}

async function loadSummary() {
  const data = await api("/api/summary");
  $("#ov-summary").textContent = data.text;
}

async function loadSlots() {
  const data = await api("/api/slots");
  const box = $("#ov-today");
  box.innerHTML = "";
  if (!data.slots.length) {
    box.innerHTML = '<span class="muted">今天还没有分组计划</span>';
    return;
  }
  for (const slot of data.slots) {
    const row = document.createElement("div");
    row.className = "slot-row";
    const head = document.createElement("div");
    head.className = "slot-head";
    head.textContent = `${slot.label} ${slot.start}–${slot.end}`;
    const status = document.createElement("span");
    let statusLabel = RUN_STATUS_LABEL[slot.runStatus] || slot.runStatus;
    let statusClass = slot.runStatus === "done" ? "chip strong" : "chip";
    if (["missed", "failed", "pending"].includes(slot.runStatus) && slot.allSent) {
      statusLabel = "成员已发送";
    } else if (slot.runStatus === "missed" || slot.runStatus === "failed") {
      statusClass = "chip outline";
    }
    status.className = statusClass;
    status.textContent = statusLabel;
    head.appendChild(status);
    const members = document.createElement("div");
    members.className = "slot-members";
    for (const member of slot.members) {
      const chip = document.createElement("span");
      chip.className = `chip ${STATUS_CLASS[member.status] || ""}${member.enabled ? "" : " dim"}`;
      chip.textContent = `${member.name} · ${STATUS_LABEL[member.status] || member.status}`;
      chip.title = member.message || "";
      members.appendChild(chip);
    }
    if (!slot.members.length) {
      const empty = document.createElement("span");
      empty.className = "muted";
      empty.textContent = "无分配好友";
      members.appendChild(empty);
    }
    row.append(head, members);
    box.appendChild(row);
  }
}

async function loadFriends() {
  const [data, slotsData] = await Promise.all([api("/api/friends"), api("/api/slots")]);
  const slotOf = {};
  for (const slot of slotsData.slots) {
    for (const member of slot.members) {
      slotOf[member.name] = slot.label;
    }
  }
  const box = $("#friend-list");
  box.innerHTML = "";
  for (const friend of data.friends) {
    const row = document.createElement("div");
    row.className = "friend-row" + (friend.enabled ? "" : " friend-off");
    const name = document.createElement("span");
    name.className = "friend-name";
    name.textContent = friend.name;
    const msg = document.createElement("span");
    msg.className = "friend-msg";
    const slotLabel = slotOf[friend.name] ? `【${slotOf[friend.name]}】` : "";
    msg.textContent = friend.today.message
      ? `${slotLabel}今天：${friend.today.message}（${STATUS_LABEL[friend.today.status] || friend.today.status}）`
      : `${slotLabel}最近：${friend.lastMessage || "无"}`;
    const toggle = document.createElement("label");
    toggle.className = "switch";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.checked = friend.enabled;
    input.addEventListener("change", async () => {
      await api(`/api/friends/${encodeURIComponent(friend.name)}/toggle`, { method: "POST" });
      loadFriends();
    });
    const slider = document.createElement("span");
    slider.className = "slider";
    toggle.append(input, slider);
    const remove = document.createElement("button");
    remove.className = "icon-btn";
    remove.textContent = "删除";
    remove.addEventListener("click", async () => {
      if (!confirm(`从列表删除「${friend.name}」？`)) return;
      await api(`/api/friends/${encodeURIComponent(friend.name)}`, { method: "DELETE" });
      loadFriends();
    });
    row.append(name, msg);
    if (["unconfirmed", "rejected", "failed", "needs_confirmation"].includes(friend.today.status)) {
      const confirmBtn = document.createElement("button");
      confirmBtn.className = "icon-btn confirm-btn";
      confirmBtn.textContent = "确认已发";
      confirmBtn.title = "我已核实这条真的送达了，手动记为已确认（今天不再发送）";
      confirmBtn.addEventListener("click", async () => {
        if (!confirm(`确认「${friend.name}」今天的消息已送达？（会记为已确认）`)) return;
        try {
          await api(`/api/friends/${encodeURIComponent(friend.name)}/confirm`, {
            method: "POST",
          });
          toast(`已手动确认：${friend.name}`);
        } catch (err) {
          toast(`确认失败：${err.message}`);
        }
        loadFriends();
        loadOverview();
      });
      row.append(confirmBtn);
    }
    row.append(toggle, remove);
    box.appendChild(row);
  }
  if (!data.friends.length) {
    box.innerHTML = '<span class="muted">还没有好友，先添加几位。</span>';
  }
}

async function loadMessages() {
  const data = await api("/api/messages");
  $("#message-count").textContent = `共 ${data.items.length} 条`;
  const box = $("#message-list");
  box.innerHTML = "";
  for (const item of data.items) {
    const row = document.createElement("div");
    row.className = "message-row";
    const span = document.createElement("span");
    span.className = "text";
    span.textContent = item.text;
    if (item.risky) {
      const badge = document.createElement("span");
      badge.className = "chip outline";
      badge.textContent = "风险词";
      row.append(badge);
    }
    const select = document.createElement("select");
    select.className = "theme-select";
    for (const [value, label] of Object.entries(WHEN_LABEL)) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      select.appendChild(option);
    }
    select.value = item.when;
    select.addEventListener("change", async () => {
      try {
        await api("/api/messages/update", {
          method: "POST",
          body: JSON.stringify({ text: item.text, when: select.value }),
        });
        toast("标签已更新");
      } catch (err) {
        toast(`更新失败：${err.message}`);
        select.value = item.when;
      }
    });
    const remove = document.createElement("button");
    remove.className = "icon-btn";
    remove.textContent = "删除";
    remove.addEventListener("click", async () => {
      await api("/api/messages/delete", { method: "POST", body: JSON.stringify({ text: item.text }) });
      loadMessages();
    });
    row.append(span, select, remove);
    box.appendChild(row);
  }
  loadPending();
}

function buildWhenSelect(current, onChange) {
  const select = document.createElement("select");
  select.className = "theme-select";
  for (const [value, label] of Object.entries(WHEN_LABEL)) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    select.appendChild(option);
  }
  select.value = current;
  if (onChange) select.addEventListener("change", () => onChange(select));
  return select;
}

async function loadPending() {
  const box = $("#pending-list");
  let data;
  try {
    data = await api("/api/messages/pending");
  } catch (err) {
    box.innerHTML = `<span class="muted">加载失败：${err.message}（刷新页面重试）</span>`;
    return;
  }
  $("#pending-count").textContent = `共 ${data.items.length} 条候选`;
  $("#btn-pending").textContent = `待筛选 ${data.items.length}`;
  box.innerHTML = "";
  if (!data.items.length) {
    box.innerHTML = '<span class="muted">暂无候选文案</span>';
    return;
  }
  for (const item of data.items) {
    const row = document.createElement("div");
    row.className = "message-row";
    const span = document.createElement("span");
    span.className = "text";
    span.textContent = item.text;
    if (item.risky) {
      const badge = document.createElement("span");
      badge.className = "chip outline";
      badge.textContent = "风险词";
      row.append(badge);
    }
    const select = buildWhenSelect(item.when);
    const accept = document.createElement("button");
    accept.className = "btn";
    accept.textContent = "采纳";
    accept.addEventListener("click", async () => {
      await api("/api/messages/pending/accept", {
        method: "POST",
        body: JSON.stringify({ text: item.text, when: select.value }),
      });
      toast("已采纳");
      loadMessages();
    });
    const remove = document.createElement("button");
    remove.className = "icon-btn";
    remove.textContent = "删除";
    remove.addEventListener("click", async () => {
      await api("/api/messages/pending/delete", {
        method: "POST",
        body: JSON.stringify({ text: item.text }),
      });
      loadPending();
    });
    row.append(span, select, accept, remove);
    box.appendChild(row);
  }
}

async function refreshPendingBadge() {
  try {
    const data = await api("/api/messages/pending");
    $("#btn-pending").textContent =
      data.items.length > 0 ? `待筛选 ${data.items.length}` : "待筛选";
  } catch (err) {
    /* 保持默认文案 */
  }
}

async function loadSettings() {
  const cfg = await api("/api/settings");
  const form = $("#settings-form");
  form.elements.windowStart.value = cfg.schedule.windowStart;
  form.elements.windowEnd.value = cfg.schedule.windowEnd;
  form.elements.minIntervalSec.value = cfg.send.minIntervalSec;
  form.elements.maxIntervalSec.value = cfg.send.maxIntervalSec;
  form.elements.retryPerFriendPerDay.value = cfg.send.retryPerFriendPerDay;
  form.elements.autoPauseOnConsecutiveBadDays.value = cfg.risk.autoPauseOnConsecutiveBadDays;
  form.elements.badDayRatioThreshold.value = cfg.risk.badDayRatioThreshold;
}

async function loadLogs() {
  const logs = await api("/api/logs");
  $("#log-day").textContent = logs.day;
  const body = $("#log-body");
  body.innerHTML = "";
  for (const row of logs.records) {
    const tr = document.createElement("tr");
    const status = document.createElement("td");
    status.textContent = STATUS_LABEL[row.status] || row.status;
    const cells = [row.friend, status.textContent, row.message || "", row.attempts, row.detail || ""];
    for (const cell of cells) {
      const td = document.createElement("td");
      if (cell === row.message) td.className = "msg";
      td.textContent = String(cell);
      tr.appendChild(td);
    }
    body.appendChild(tr);
  }
  if (!logs.records.length) {
    body.innerHTML = '<tr><td colspan="5" class="muted">今天还没有记录</td></tr>';
  }
  const history = await api("/api/history");
  const historyBody = $("#history-body");
  historyBody.innerHTML = "";
  for (const row of history.history) {
    const tr = document.createElement("tr");
    for (const key of ["day", "confirmed", "unconfirmed", "rejected", "failed"]) {
      const td = document.createElement("td");
      td.textContent = row[key];
      tr.appendChild(td);
    }
    historyBody.appendChild(tr);
  }
}

function setupTabs() {
  const tabs = document.querySelectorAll(".tab");
  const panels = document.querySelectorAll(".tab-panel");
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      tabs.forEach((item) => item.classList.toggle("active", item === tab));
      panels.forEach((panel) =>
        panel.classList.toggle("hidden", panel.id !== `tab-${tab.dataset.tab}`)
      );
      if (tab.dataset.tab === "friends") loadFriends();
      if (tab.dataset.tab === "messages") loadMessages();
      if (tab.dataset.tab === "settings") {
        loadSettings();
        loadLogs();
      }
    });
  });
}

function setupActions() {
  $("#btn-pending").addEventListener("click", () => {
    document.querySelector('.tab[data-tab="messages"]').click();
  });
  $("#btn-run").addEventListener("click", async () => {
    if (!confirm("现在就给所有启用的好友发送一轮？")) return;
    $("#btn-run").disabled = true;
    try {
      const result = await api("/api/actions/send-now", { method: "POST" });
      toast(result.started ? "已开始发送，可在总览/日志里看进度" : "有一轮正在运行中");
    } catch (err) {
      toast(`失败：${err.message}`);
    }
    setTimeout(() => { $("#btn-run").disabled = false; }, 3000);
  });
  $("#btn-catchup").addEventListener("click", async () => {
    const result = await api("/api/actions/catch-up", { method: "POST" });
    toast(result.started ? "窗口内补跑已开始" : `不满足补跑条件（${result.reason}）`);
  });
  $("#btn-login").addEventListener("click", async () => {
    if (!confirm("会在本机弹出浏览器窗口，请用手机抖音扫码。继续？")) return;
    const result = await api("/api/actions/login", { method: "POST" });
    if (!result.started) {
      toast("已有登录流程在进行中");
      return;
    }
    toast("浏览器已打开，请扫码……");
    const timer = setInterval(async () => {
      const status = await api("/api/actions/login-status");
      if (status.state === "done") {
        clearInterval(timer);
        toast(`登录成功：${status.result.username || status.result.unique_id}`);
        loadOverview();
      } else if (status.state === "failed") {
        clearInterval(timer);
        toast(`登录失败：${status.error}`);
      }
    }, 3000);
  });
  $("#btn-check").addEventListener("click", async () => {
    $("#btn-check").disabled = true;
    toast("正在检查登录状态，约 10–20 秒…");
    try {
      const result = await api("/api/actions/preflight", { method: "POST" });
      toast(result.ok ? "登录正常" : `登录已失效：${result.note}`);
      await loadOverview();
    } catch (err) {
      toast(`检查失败：${err.message}`);
    }
    $("#btn-check").disabled = false;
  });
  $("#btn-cookie").addEventListener("click", () => {
    $("#cookie-dialog").showModal();
  });
  $("#cookie-cancel").addEventListener("click", () => {
    $("#cookie-dialog").close();
  });
  $("#cookie-save").addEventListener("click", async () => {
    const raw = $("#cookie-input").value.trim();
    if (!raw) {
      toast("请先粘贴 Cookie 内容");
      return;
    }
    $("#cookie-save").disabled = true;
    try {
      const result = await api("/api/actions/login-cookies", {
        method: "POST",
        body: JSON.stringify({ raw }),
      });
      toast(result.ok ? result.note : `未保存：${result.note}`);
      if (result.ok) {
        $("#cookie-input").value = "";
        $("#cookie-dialog").close();
        await loadOverview();
      }
    } catch (err) {
      toast(`校验失败：${err.message}`);
    }
    $("#cookie-save").disabled = false;
  });
  $("#btn-pause").addEventListener("click", async () => {
    const isPaused = (await api("/api/overview")).paused;
    if (isPaused) {
      if (!confirm("确认恢复自动发送？")) return;
      await api("/api/actions/resume", { method: "POST" });
      toast("已恢复");
    } else {
      if (!confirm("暂停后今天不会再自动发送，确认？")) return;
      await api("/api/actions/pause", { method: "POST" });
      toast("已暂停");
    }
    loadOverview();
  });
  $("#pending-accept-all").addEventListener("click", async () => {
    if (!confirm("把全部候选文案采纳进发送池？")) return;
    const result = await api("/api/messages/pending/accept-all", { method: "POST" });
    toast(`已采纳 ${result.added} 条，池子共 ${result.pool} 条`);
    loadMessages();
  });
}

function setupForms() {
  $("#friend-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = $("#friend-input");
    await api("/api/friends", { method: "POST", body: JSON.stringify({ name: input.value }) });
    input.value = "";
    loadFriends();
  });
  $("#message-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const input = $("#message-input");
    try {
      await api("/api/messages", {
        method: "POST",
        body: JSON.stringify({ text: input.value, when: $("#message-when").value }),
      });
      input.value = "";
      loadMessages();
    } catch (err) {
      toast(`添加失败：${err.message}`);
    }
  });
  $("#settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.target;
    const cfg = await api("/api/settings");
    cfg.schedule.windowStart = form.elements.windowStart.value;
    cfg.schedule.windowEnd = form.elements.windowEnd.value;
    cfg.send.minIntervalSec = Number(form.elements.minIntervalSec.value);
    cfg.send.maxIntervalSec = Number(form.elements.maxIntervalSec.value);
    cfg.send.retryPerFriendPerDay = Number(form.elements.retryPerFriendPerDay.value);
    cfg.risk.autoPauseOnConsecutiveBadDays = Number(form.elements.autoPauseOnConsecutiveBadDays.value);
    cfg.risk.badDayRatioThreshold = Number(form.elements.badDayRatioThreshold.value);
    await api("/api/settings", { method: "POST", body: JSON.stringify({ config: cfg }) });
    toast("设置已保存");
  });
}

async function init() {
  setupTabs();
  setupActions();
  setupForms();
  try {
    const cfg = await api("/api/settings");
    const theme = cfg.ui?.theme || "system";
    $("#theme-select").value = theme;
    applyTheme(theme);
  } catch (err) { /* 默认跟随系统 */ }
  await loadOverview();
  await loadSlots();
  await loadSummary();
  await refreshPendingBadge();
  setInterval(() => {
    loadOverview().catch(() => {});
    loadSlots().catch(() => {});
  }, 30000);
}

init().catch((err) => toast(`加载失败：${err.message}`));

function applyTheme(mode) {
  if (mode === "system") {
    document.documentElement.removeAttribute("data-theme");
  } else {
    document.documentElement.setAttribute("data-theme", mode);
  }
}

$("#theme-select").addEventListener("change", async (event) => {
  applyTheme(event.target.value);
  try {
    const cfg = await api("/api/settings");
    cfg.ui = cfg.ui || {};
    cfg.ui.theme = event.target.value;
    await api("/api/settings", {
      method: "POST",
      body: JSON.stringify({ config: cfg }),
    });
    toast("主题已保存");
  } catch (err) {
    toast(`主题保存失败：${err.message}`);
  }
});
