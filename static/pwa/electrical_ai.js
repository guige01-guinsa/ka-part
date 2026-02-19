(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const state = {
    user: null,
    siteCode: "",
    siteName: "",
    settings: null,
    incidents: [],
    notifications: [],
  };

  function msg(text, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = String(text || "");
    el.style.color = isErr ? "#b92020" : "#224f9f";
  }

  function resultLine(text, isErr = false) {
    const el = $("#incidentResult");
    if (!el) return;
    el.textContent = String(text || "");
    el.style.color = isErr ? "#b92020" : "#0e7c36";
  }

  function getToken() {
    return window.KAAuth ? window.KAAuth.getToken() : "";
  }

  function siteCodeFromQuery() {
    const qs = new URLSearchParams(window.location.search);
    return String(qs.get("site_code") || "").trim().toUpperCase();
  }

  function siteIdFromQuery() {
    const qs = new URLSearchParams(window.location.search);
    const raw = String(qs.get("site_id") || "").trim();
    const n = Number(raw);
    return Number.isFinite(n) && n > 0 ? Math.trunc(n) : 0;
  }

  function queryWithSite(basePath) {
    const code = state.siteCode || siteCodeFromQuery();
    const siteId = siteIdFromQuery();
    const params = new URLSearchParams();
    if (code) params.set("site_code", code);
    if (siteId > 0) params.set("site_id", String(siteId));
    const raw = params.toString();
    if (!raw) return basePath;
    const sep = basePath.includes("?") ? "&" : "?";
    return `${basePath}${sep}${raw}`;
  }

  function isManager() {
    const u = state.user || {};
    return !!u.is_admin || !!u.is_site_admin;
  }

  async function apiGet(path) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    return await window.KAAuth.requestJson(path);
  }

  async function apiPost(path, payload = {}) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    return await window.KAAuth.requestJson(path, { method: "POST", body: JSON.stringify(payload || {}) });
  }

  async function apiPut(path, payload = {}) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    return await window.KAAuth.requestJson(path, { method: "PUT", body: JSON.stringify(payload || {}) });
  }

  function escapeHtml(s) {
    return String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function badge(level) {
    const k = String(level || "").trim().toLowerCase();
    const cls = k === "danger" ? "danger" : (k === "prealert" ? "prealert" : (k === "caution" ? "caution" : "ok"));
    return `<span class="badge ${cls}">${escapeHtml(k || "-")}</span>`;
  }

  function renderIncidents(items) {
    const list = $("#incidentList");
    if (!list) return;
    const rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      list.innerHTML = `<div class="muted" style="padding:10px;">데이터가 없습니다.</div>`;
      return;
    }
    const trs = rows.map((r) => {
      const id = Number(r.id || 0);
      const risk = badge(r.risk_level);
      const event = badge(r.event_level);
      const trend = badge(r.trend_state === "worsening" ? "prealert" : "ok");
      const m = `절연 ${Number(r.insulation_mohm || 0).toFixed(3)} / 접지 ${Number(r.ground_ohm || 0).toFixed(2)} / 누설 ${Number(r.leakage_ma || 0).toFixed(2)}`;
      return `
        <tr>
          <td>${id}</td>
          <td>${escapeHtml(r.created_at || "-")}</td>
          <td>${escapeHtml(r.location || "-")}</td>
          <td>${escapeHtml(r.title || "-")}</td>
          <td>${risk}</td>
          <td>${event}</td>
          <td>${trend}</td>
          <td>${escapeHtml(m)}</td>
          <td><button class="btn" type="button" data-report-id="${id}">PDF</button></td>
        </tr>
      `;
    }).join("");
    list.innerHTML = `
      <table class="table">
        <thead>
          <tr>
            <th>ID</th>
            <th>시각</th>
            <th>위치</th>
            <th>제목</th>
            <th>Risk</th>
            <th>Event</th>
            <th>Trend</th>
            <th>수치</th>
            <th>보고서</th>
          </tr>
        </thead>
        <tbody>${trs}</tbody>
      </table>
    `;
    for (const btn of list.querySelectorAll("button[data-report-id]")) {
      btn.addEventListener("click", () => {
        const id = Number(btn.getAttribute("data-report-id") || 0);
        if (id > 0) downloadReport(id).catch((err) => alert("PDF 오류: " + err.message));
      });
    }
  }

  function renderNotifications(items) {
    const list = $("#notifyList");
    if (!list) return;
    const rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      list.innerHTML = `<div class="muted" style="padding:10px;">대기 중인 알림이 없습니다.</div>`;
      return;
    }
    const trs = rows.map((r) => {
      const id = Number(r.id || 0);
      const event = badge(r.event_level);
      const due = escapeHtml(r.ack_due_at || "-");
      const route = escapeHtml(r.route_recipient_key || "-");
      const target = escapeHtml(r.recipient_key || "-");
      const token = escapeHtml(r.ack_token || "");
      return `
        <tr>
          <td>${id}</td>
          <td>${event}</td>
          <td>${route} → ${target}</td>
          <td>${due}</td>
          <td><button class="btn" type="button" data-ack-id="${id}" data-ack-token="${token}">ACK</button></td>
        </tr>
      `;
    }).join("");
    list.innerHTML = `
      <table class="table">
        <thead>
          <tr><th>ID</th><th>Event</th><th>수신대상</th><th>ACK 기한</th><th>작업</th></tr>
        </thead>
        <tbody>${trs}</tbody>
      </table>
    `;
    for (const btn of list.querySelectorAll("button[data-ack-id]")) {
      btn.addEventListener("click", () => {
        const id = Number(btn.getAttribute("data-ack-id") || 0);
        const token = String(btn.getAttribute("data-ack-token") || "").trim();
        if (id > 0) ackNotification(id, token).catch((err) => alert("ACK 오류: " + err.message));
      });
    }
  }

  function fillSettings(settings) {
    const rules = (settings && settings.rules) || {};
    const duty = Array.isArray(settings && settings.duty_schedule) ? settings.duty_schedule : [];
    const day = duty.find((x) => String(x.shift_code || "").toUpperCase() === "DAY") || {};
    const night = duty.find((x) => String(x.shift_code || "").toUpperCase() === "NIGHT") || {};

    $("#ruleCautionLeakage").value = String(rules.caution_leakage_ma ?? "");
    $("#ruleDangerLeakage").value = String(rules.danger_leakage_ma ?? "");
    $("#ruleCautionInsulation").value = String(rules.caution_insulation_mohm ?? "");
    $("#ruleDangerInsulation").value = String(rules.danger_insulation_mohm ?? "");
    $("#ruleCautionGround").value = String(rules.caution_ground_ohm ?? "");
    $("#ruleDangerGround").value = String(rules.danger_ground_ohm ?? "");
    $("#ruleAckTimeout").value = String(rules.ack_timeout_minutes ?? "");
    $("#ruleTrendLookback").value = String(rules.trend_lookback_count ?? "");
    $("#ruleTrendPrealert").checked = Number(rules.trend_prealert_enabled || 0) === 1;

    $("#dutyDayKey").value = String(day.user_key || "");
    $("#dutyDayStart").value = String(day.start_hhmm || "06:00");
    $("#dutyDayEnd").value = String(day.end_hhmm || "18:00");
    $("#dutyNightKey").value = String(night.user_key || "");
    $("#dutyNightStart").value = String(night.start_hhmm || "18:00");
    $("#dutyNightEnd").value = String(night.end_hhmm || "06:00");
  }

  function applyManagerUiPolicy() {
    const manager = isManager();
    const card = $("#settingsCard");
    const hint = $("#settingsHint");
    const runEsc = $("#btnRunEscalation");
    if (!card || !hint || !runEsc) return;
    if (!manager) {
      for (const input of card.querySelectorAll("input,button")) {
        if (input.id === "btnReload") continue;
        input.disabled = true;
      }
      runEsc.disabled = true;
      hint.textContent = "관리자 권한에서만 설정/에스컬레이션 실행이 가능합니다.";
      return;
    }
    hint.textContent = "규칙/당직 변경은 즉시 적용됩니다.";
  }

  async function loadBootstrap() {
    const data = await apiGet(queryWithSite("/api/elec/bootstrap"));
    state.siteCode = String(data.site_code || "").trim().toUpperCase();
    state.siteName = String(data.site_name || "").trim();
    state.user = data.user || null;
    state.settings = data.settings || {};
    state.incidents = Array.isArray(data.incidents) ? data.incidents : [];
    state.notifications = Array.isArray(data.pending_notifications) ? data.pending_notifications : [];

    $("#userLine").textContent = `사용자: ${state.user?.name || "-"} (${state.user?.role || "-"})`;
    $("#siteLine").textContent = `단지: ${state.siteCode || "-"} ${state.siteName ? "· " + state.siteName : ""} · 활성당직: ${data.active_duty_user_key || "-"}`;
    fillSettings(state.settings);
    applyManagerUiPolicy();
    renderIncidents(state.incidents);
    renderNotifications(state.notifications);
    msg("전기AI 모듈 준비 완료");
  }

  async function reloadLists() {
    const risk = String($("#qRiskLevel")?.value || "").trim();
    const event = String($("#qEventLevel")?.value || "").trim();
    const p = new URLSearchParams();
    if (state.siteCode) p.set("site_code", state.siteCode);
    if (risk) p.set("risk_level", risk);
    if (event) p.set("event_level", event);
    p.set("limit", "100");
    const out = await apiGet(`/api/elec/incidents?${p.toString()}`);
    state.incidents = Array.isArray(out.items) ? out.items : [];
    renderIncidents(state.incidents);

    const b = await apiGet(queryWithSite("/api/elec/bootstrap?limit=100"));
    state.notifications = Array.isArray(b.pending_notifications) ? b.pending_notifications : [];
    renderNotifications(state.notifications);
  }

  async function createIncident() {
    const payload = {
      site_code: state.siteCode || "",
      site_name: state.siteName || "",
      location: String($("#incidentLocation")?.value || "").trim(),
      title: String($("#incidentTitle")?.value || "").trim() || "누설전류 점검",
      insulation_mohm: Number($("#insulationMohm")?.value || 0),
      ground_ohm: Number($("#groundOhm")?.value || 0),
      leakage_ma: Number($("#leakageMa")?.value || 0),
      note: String($("#incidentNote")?.value || "").trim(),
    };
    if (!Number.isFinite(payload.insulation_mohm) || !Number.isFinite(payload.ground_ohm) || !Number.isFinite(payload.leakage_ma)) {
      throw new Error("수치 입력값을 확인해 주세요.");
    }
    const out = await apiPost("/api/elec/incidents", payload);
    const incident = out.incident || {};
    resultLine(`등록 완료: risk=${incident.risk_level || "-"}, event=${incident.event_level || "-"}, trend=${incident.trend_state || "-"}`);
    await reloadLists();
  }

  async function ackNotification(id, token) {
    const p = new URLSearchParams();
    if (state.siteCode) p.set("site_code", state.siteCode);
    if (token) p.set("token", token);
    await apiPost(`/api/elec/notifications/${Number(id)}/ack?${p.toString()}`, {});
    msg(`ACK 완료 (#${Number(id)})`);
    await reloadLists();
  }

  async function runEscalation() {
    const out = await apiPost("/api/elec/escalations/run", { site_code: state.siteCode || "", limit: 300 });
    msg(`에스컬레이션 완료: checked=${out.checked || 0}, escalated=${out.escalated || 0}`);
    state.notifications = Array.isArray(out.pending_notifications) ? out.pending_notifications : [];
    renderNotifications(state.notifications);
    await reloadLists();
  }

  async function saveRules() {
    const payload = {
      caution_leakage_ma: Number($("#ruleCautionLeakage")?.value || 0),
      danger_leakage_ma: Number($("#ruleDangerLeakage")?.value || 0),
      caution_insulation_mohm: Number($("#ruleCautionInsulation")?.value || 0),
      danger_insulation_mohm: Number($("#ruleDangerInsulation")?.value || 0),
      caution_ground_ohm: Number($("#ruleCautionGround")?.value || 0),
      danger_ground_ohm: Number($("#ruleDangerGround")?.value || 0),
      ack_timeout_minutes: Number($("#ruleAckTimeout")?.value || 30),
      trend_lookback_count: Number($("#ruleTrendLookback")?.value || 3),
      trend_prealert_enabled: !!$("#ruleTrendPrealert")?.checked,
    };
    const out = await apiPut(queryWithSite("/api/elec/settings/rules"), payload);
    state.settings = state.settings || {};
    state.settings.rules = out.rules || {};
    msg("규칙 저장 완료");
  }

  async function saveDuty() {
    const payload = {
      day_user_key: String($("#dutyDayKey")?.value || "").trim(),
      day_start_hhmm: String($("#dutyDayStart")?.value || "").trim(),
      day_end_hhmm: String($("#dutyDayEnd")?.value || "").trim(),
      night_user_key: String($("#dutyNightKey")?.value || "").trim(),
      night_start_hhmm: String($("#dutyNightStart")?.value || "").trim(),
      night_end_hhmm: String($("#dutyNightEnd")?.value || "").trim(),
    };
    if (!payload.day_user_key || !payload.night_user_key) throw new Error("주/야간 user_key를 입력해 주세요.");
    await apiPut(queryWithSite("/api/elec/settings/duty"), payload);
    msg("당직 스케줄 저장 완료");
    await loadBootstrap();
  }

  async function downloadReport(id) {
    const token = getToken();
    const p = new URLSearchParams();
    if (state.siteCode) p.set("site_code", state.siteCode);
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const res = await fetch(`/api/elec/incidents/${Number(id)}/report.pdf?${p.toString()}`, { headers });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(t || `HTTP ${res.status}`);
    }
    const blob = await res.blob();
    const a = document.createElement("a");
    const url = URL.createObjectURL(blob);
    a.href = url;
    a.download = `elec_incident_${Number(id)}.pdf`;
    document.body.appendChild(a);
    a.click();
    setTimeout(() => {
      URL.revokeObjectURL(url);
      a.remove();
    }, 150);
  }

  function wire() {
    $("#btnReload")?.addEventListener("click", () => loadBootstrap().catch((err) => alert("재조회 오류: " + err.message)));
    $("#btnLoadIncidents")?.addEventListener("click", () => reloadLists().catch((err) => alert("조회 오류: " + err.message)));
    $("#btnCreateIncident")?.addEventListener("click", () => createIncident().catch((err) => resultLine("등록 오류: " + err.message, true)));
    $("#btnRunEscalation")?.addEventListener("click", () => runEscalation().catch((err) => alert("에스컬레이션 오류: " + err.message)));
    $("#btnSaveRules")?.addEventListener("click", () => saveRules().catch((err) => alert("규칙 저장 오류: " + err.message)));
    $("#btnSaveDuty")?.addEventListener("click", () => saveDuty().catch((err) => alert("당직 저장 오류: " + err.message)));
  }

  async function init() {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    await window.KAAuth.requireAuth();
    wire();
    await loadBootstrap();
  }

  init().catch((err) => {
    const m = err && err.message ? err.message : String(err);
    alert("전기AI 초기화 오류: " + m);
  });
})();
