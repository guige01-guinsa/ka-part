(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  const MAX_ATTACHMENTS = 10;
  const MAX_URL_LENGTH = 500;
  const STATUS_LABELS = {
    RECEIVED: "접수",
    TRIAGED: "분류완료",
    GUIDANCE_SENT: "안내완료",
    ASSIGNED: "배정",
    IN_PROGRESS: "처리중",
    COMPLETED: "완료",
    CLOSED: "종결",
  };
  const SCOPE_LABELS = {
    COMMON: "공용시설",
    PRIVATE: "세대내부",
    EMERGENCY: "긴급",
  };

  let me = null;
  let categories = [];
  let selectedComplaintId = null;
  let selectedAdminComplaintId = null;

  function isAdmin(user) {
    return !!(user && (user.is_admin || user.is_site_admin));
  }

  function escapeHtml(v) {
    return String(v || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function setMsg(msg, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
  }

  function toStatusLabel(status) {
    const key = String(status || "").trim().toUpperCase();
    return STATUS_LABELS[key] || key || "-";
  }

  function toScopeLabel(scope) {
    const key = String(scope || "").trim().toUpperCase();
    return SCOPE_LABELS[key] || key || "-";
  }

  function formatDateTime(value) {
    const raw = String(value || "").trim();
    if (!raw) return "-";
    const normalized = raw.replace(" ", "T");
    const dt = new Date(normalized);
    if (Number.isNaN(dt.getTime())) return raw;
    return dt.toLocaleString("ko-KR", { hour12: false });
  }

  function normalizeText(value, maxLen, field, required = false) {
    const txt = String(value || "").replaceAll("\u0000", "").trim();
    if (required && !txt) {
      throw new Error(`${field}을(를) 입력하세요.`);
    }
    if (txt.length > maxLen) {
      throw new Error(`${field} 길이는 ${maxLen}자 이하여야 합니다.`);
    }
    return txt;
  }

  function parseSafeUrl(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (raw.length > MAX_URL_LENGTH) {
      throw new Error(`첨부 URL은 ${MAX_URL_LENGTH}자 이하여야 합니다.`);
    }
    let parsed = null;
    try {
      parsed = new URL(raw);
    } catch (_e) {
      throw new Error(`첨부 URL 형식이 올바르지 않습니다: ${raw}`);
    }
    const protocol = String(parsed.protocol || "").toLowerCase();
    if (protocol !== "http:" && protocol !== "https:") {
      throw new Error(`첨부 URL은 http/https만 허용됩니다: ${raw}`);
    }
    return parsed.toString();
  }

  function parseQuery() {
    try {
      const u = new URL(window.location.href);
      return {
        site_name: (u.searchParams.get("site_name") || "").trim(),
        site_code: (u.searchParams.get("site_code") || "").trim().toUpperCase(),
      };
    } catch (_e) {
      return { site_name: "", site_code: "" };
    }
  }

  function normalizeSiteContext() {
    const q = parseQuery();
    const userSiteName = String(me && me.site_name ? me.site_name : "").trim();
    const userSiteCode = String(me && me.site_code ? me.site_code : "").trim().toUpperCase();
    const storedSiteName = String(localStorage.getItem(SITE_NAME_KEY) || "").trim();
    const storedSiteCode = String(localStorage.getItem(SITE_CODE_KEY) || "").trim().toUpperCase();

    let siteName = q.site_name || storedSiteName || userSiteName || "";
    let siteCode = q.site_code || storedSiteCode || userSiteCode || "";

    if (!isAdmin(me)) {
      siteName = userSiteName || siteName;
      siteCode = userSiteCode || siteCode;
    }

    $("#siteName").value = siteName;
    $("#siteCode").value = siteCode;
    if (!isAdmin(me)) {
      $("#siteName").readOnly = true;
      $("#siteCode").readOnly = true;
    }
    localStorage.setItem(SITE_NAME_KEY, siteName);
    localStorage.setItem(SITE_CODE_KEY, siteCode);
  }

  function updateMetaLine() {
    const el = $("#metaLine");
    if (!el || !me) return;
    const level = me.is_admin ? "관리자" : (me.is_site_admin ? "단지관리자" : "일반");
    const siteCode = String(me.site_code || "").trim().toUpperCase();
    const siteName = String(me.site_name || "").trim();
    const site = siteCode ? `${siteCode}${siteName ? ` / ${siteName}` : ""}` : (siteName || "-");
    el.textContent = `${me.name || me.login_id} (${level}) · 소속: ${site}`;
  }

  function getSelectedScope() {
    return String($("#scopeSelect").value || "COMMON").trim().toUpperCase();
  }

  function fillCategories() {
    const scope = getSelectedScope();
    const sel = $("#categorySelect");
    if (!sel) return;
    const rows = categories.filter((x) => String(x.scope || "").toUpperCase() === scope);
    const opts = (rows.length ? rows : categories).map((x) => {
      return `<option value="${Number(x.id)}">${escapeHtml(x.name)} (${escapeHtml(toScopeLabel(x.scope))})</option>`;
    });
    sel.innerHTML = opts.join("");
  }

  async function loadCategories() {
    const data = await jfetch("/api/v1/codes/complaint-categories");
    categories = Array.isArray(data.items) ? data.items : [];
    fillCategories();
  }

  function parseAttachmentUrls() {
    const raw = String($("#attachInput").value || "");
    const rows = raw
      .split(/\r?\n/g)
      .map((x) => x.trim())
      .filter(Boolean);
    if (rows.length > MAX_ATTACHMENTS) {
      throw new Error(`첨부 URL은 최대 ${MAX_ATTACHMENTS}개까지 등록할 수 있습니다.`);
    }
    return rows.map((x) => parseSafeUrl(x));
  }

  function payloadForSubmit(forceEmergency) {
    const scope = getSelectedScope();
    const site_name = normalizeText($("#siteName").value, 80, "단지명", false);
    const site_code = normalizeText($("#siteCode").value, 32, "단지코드", false).toUpperCase();
    const unit_label = normalizeText($("#unitLabel").value, 80, "동/호", false);
    const title = normalizeText($("#titleInput").value, 140, "제목", true);
    const description = normalizeText($("#descInput").value, 8000, "내용", true);
    const location_detail = normalizeText($("#locInput").value, 200, "위치", false);
    const category_id = Number($("#categorySelect").value || 0);
    if (!category_id) {
      throw new Error("카테고리를 선택하세요.");
    }
    return {
      category_id,
      scope: forceEmergency ? "EMERGENCY" : scope,
      title,
      description,
      location_detail,
      priority: forceEmergency ? "URGENT" : "NORMAL",
      site_code,
      site_name,
      unit_label,
      attachments: parseAttachmentUrls(),
    };
  }

  async function submitComplaint(forceEmergency = false) {
    const payload = payloadForSubmit(forceEmergency);
    const endpoint = forceEmergency || payload.scope === "EMERGENCY" ? "/api/v1/emergencies" : "/api/v1/complaints";
    const data = await jfetch(endpoint, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const item = data && data.item ? data.item : null;
    setMsg(data && data.message ? String(data.message) : "접수되었습니다.");
    $("#titleInput").value = "";
    $("#descInput").value = "";
    $("#locInput").value = "";
    $("#attachInput").value = "";
    await loadMyComplaints();
    if (item && item.id) {
      selectedComplaintId = Number(item.id);
      await loadComplaintDetail(selectedComplaintId, false);
    }
  }

  function complaintCardHtml(row, activeId) {
    const id = Number(row.id || 0);
    const active = id === Number(activeId);
    return `
      <button class="item ${active ? "active" : ""}" data-id="${id}" type="button">
        <div class="head">
          <span>${escapeHtml(row.ticket_no || `#${id}`)}</span>
          <span>${escapeHtml(toStatusLabel(row.status))}</span>
        </div>
        <div class="sub">${escapeHtml(toScopeLabel(row.scope))} · ${escapeHtml(row.priority || "-")} · ${escapeHtml(row.category_name || "-")}</div>
        <div class="sub">${escapeHtml(row.title || "-")}</div>
        <div class="sub">${escapeHtml(formatDateTime(row.created_at || ""))}</div>
      </button>
    `;
  }

  async function loadMyComplaints() {
    const status = String($("#myStatusFilter").value || "").trim().toUpperCase();
    const qs = new URLSearchParams();
    if (status) qs.set("status", status);
    qs.set("limit", "80");
    const path = `/api/v1/complaints?${qs.toString()}`;
    const data = await jfetch(path);
    const rows = Array.isArray(data.items) ? data.items : [];
    const wrap = $("#myList");
    if (!wrap) return;
    if (!rows.length) {
      wrap.innerHTML = '<div class="detail muted">민원이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = rows.map((row) => complaintCardHtml(row, selectedComplaintId)).join("");
  }

  function renderSummary(item) {
    const lines = [];
    lines.push(`접수번호: ${item.ticket_no || "-"}`);
    lines.push(`상태: ${toStatusLabel(item.status)} / 구분: ${toScopeLabel(item.scope)}`);
    lines.push(`우선순위: ${item.priority || "-"}`);
    lines.push(`카테고리: ${item.category_name || "-"}`);
    lines.push(`제목: ${item.title || "-"}`);
    lines.push(`내용: ${item.description || "-"}`);
    lines.push(`위치: ${item.location_detail || "-"}`);
    lines.push(`접수일시: ${formatDateTime(item.created_at)}`);
    if (item.resolution_type) lines.push(`처리방식: ${item.resolution_type}`);
    if (item.assignee_name || item.assigned_to_user_id) {
      lines.push(`담당자: ${item.assignee_name || item.assigned_to_user_id}`);
    }
    if (item.closed_at) lines.push(`종결일시: ${formatDateTime(item.closed_at)}`);
    $("#detailSummary").textContent = lines.join("\n");
  }

  function renderTimeline(item) {
    const wrap = $("#timelineList");
    if (!wrap) return;
    const history = Array.isArray(item.history) ? item.history : [];
    if (!history.length) {
      wrap.innerHTML = '<div class="detail muted">타임라인이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = history
      .map((h) => {
        const fromStatus = h.from_status ? toStatusLabel(h.from_status) : "초기";
        const toStatus = toStatusLabel(h.to_status);
        const actor = h.changed_by_name || h.changed_by_user_id || "-";
        const note = String(h.note || "").trim();
        return `
          <div class="timeline-item">
            <div class="line1">
              <span class="status">${escapeHtml(fromStatus)} -> ${escapeHtml(toStatus)}</span>
              <span class="time">${escapeHtml(formatDateTime(h.created_at))}</span>
            </div>
            <div class="line2">처리자: ${escapeHtml(actor)}</div>
            ${note ? `<div class="line3">메모: ${escapeHtml(note)}</div>` : ""}
          </div>
        `;
      })
      .join("");
  }

  function renderComments(item) {
    const wrap = $("#commentList");
    if (!wrap) return;
    const comments = Array.isArray(item.comments) ? item.comments : [];
    if (!comments.length) {
      wrap.innerHTML = '<div class="detail muted">댓글이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = comments
      .map((c) => {
        return `
          <div class="timeline-item">
            <div class="line1">
              <span class="status">${escapeHtml(c.user_name || c.user_id || "-")}</span>
              <span class="time">${escapeHtml(formatDateTime(c.created_at))}</span>
            </div>
            <div class="line2">${escapeHtml(c.comment || "")}</div>
          </div>
        `;
      })
      .join("");
  }

  function renderMeta(item) {
    const wrap = $("#metaList");
    if (!wrap) return;
    const segments = [];
    const atts = Array.isArray(item.attachments) ? item.attachments : [];
    const wos = Array.isArray(item.work_orders) ? item.work_orders : [];
    const visits = Array.isArray(item.visits) ? item.visits : [];
    if (atts.length) {
      segments.push(`<div class="line2">첨부: ${atts.map((a) => escapeHtml(a.file_url || "-")).join(" / ")}</div>`);
    }
    if (wos.length) {
      segments.push(
        `<div class="line2">작업지시: ${wos
          .map((w) => `#${escapeHtml(w.id)} ${escapeHtml(w.status)} (${escapeHtml(w.assignee_name || w.assignee_user_id || "-")})`)
          .join(" / ")}</div>`
      );
    }
    if (visits.length) {
      segments.push(
        `<div class="line2">방문기록: ${visits
          .map((v) => `${escapeHtml(v.visit_reason)} ${escapeHtml(formatDateTime(v.check_in_at))}`)
          .join(" / ")}</div>`
      );
    }
    if (!segments.length) {
      wrap.innerHTML = '<div class="detail muted">상세 데이터가 없습니다.</div>';
      return;
    }
    wrap.innerHTML = `<div class="timeline-item">${segments.join("")}</div>`;
  }

  function renderDetail(item) {
    renderSummary(item);
    renderTimeline(item);
    renderComments(item);
    renderMeta(item);
  }

  async function loadComplaintDetail(id, adminMode) {
    if (!id) return;
    const endpoint = adminMode ? `/api/v1/admin/complaints/${Number(id)}` : `/api/v1/complaints/${Number(id)}`;
    const data = await jfetch(endpoint);
    const item = data && data.item ? data.item : null;
    if (!item) return;
    if (adminMode) {
      selectedAdminComplaintId = Number(item.id);
      $("#triageScope").value = String(item.scope || "COMMON").toUpperCase();
      $("#triagePriority").value = String(item.priority || "NORMAL").toUpperCase();
      $("#triageResolution").value = String(item.resolution_type || "REPAIR").toUpperCase();
      const selfId = Number(me && me.id ? me.id : 0);
      if (!$("#assignUserId").value && selfId > 0) {
        $("#assignUserId").value = String(selfId);
      }
      const wos = Array.isArray(item.work_orders) ? item.work_orders : [];
      if (wos.length) {
        $("#workOrderId").value = String(wos[0].id || "");
        $("#workOrderStatus").value = String(wos[0].status || "OPEN").toUpperCase();
      }
      await loadAdminComplaints();
    } else {
      selectedComplaintId = Number(item.id);
      await loadMyComplaints();
    }
    renderDetail(item);
  }

  async function addCommentToSelected() {
    if (!selectedComplaintId && !selectedAdminComplaintId) {
      throw new Error("먼저 민원을 선택하세요.");
    }
    const id = selectedAdminComplaintId || selectedComplaintId;
    const comment = normalizeText($("#commentInput").value, 8000, "댓글 내용", true);
    await jfetch(`/api/v1/complaints/${id}/comments`, {
      method: "POST",
      body: JSON.stringify({ comment }),
    });
    $("#commentInput").value = "";
    setMsg("댓글이 등록되었습니다.");
    if (selectedAdminComplaintId) {
      await loadComplaintDetail(selectedAdminComplaintId, true);
    } else {
      await loadComplaintDetail(selectedComplaintId, false);
    }
  }

  async function loadAdminComplaints() {
    if (!isAdmin(me)) return;
    const scope = String($("#adminScopeFilter").value || "").trim().toUpperCase();
    const status = String($("#adminStatusFilter").value || "").trim().toUpperCase();
    const qs = new URLSearchParams();
    if (scope) qs.set("scope", scope);
    if (status) qs.set("status", status);
    qs.set("limit", "120");
    const data = await jfetch(`/api/v1/admin/complaints?${qs.toString()}`);
    const rows = Array.isArray(data.items) ? data.items : [];
    const wrap = $("#adminList");
    if (!wrap) return;
    if (!rows.length) {
      wrap.innerHTML = '<div class="detail muted">조회 결과가 없습니다.</div>';
      return;
    }
    wrap.innerHTML = rows.map((row) => complaintCardHtml(row, selectedAdminComplaintId)).join("");
  }

  async function runTriage() {
    if (!selectedAdminComplaintId) throw new Error("관리자 목록에서 민원을 먼저 선택하세요.");
    const payload = {
      scope: String($("#triageScope").value || "COMMON").toUpperCase(),
      priority: String($("#triagePriority").value || "NORMAL").toUpperCase(),
      resolution_type: String($("#triageResolution").value || "REPAIR").toUpperCase(),
      note: normalizeText($("#triageNote").value, 2000, "분류 메모", false),
    };
    await jfetch(`/api/v1/admin/complaints/${selectedAdminComplaintId}/triage`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    setMsg("분류가 반영되었습니다.");
    await loadComplaintDetail(selectedAdminComplaintId, true);
  }

  async function runAssign() {
    if (!selectedAdminComplaintId) throw new Error("관리자 목록에서 민원을 먼저 선택하세요.");
    const uid = Number($("#assignUserId").value || 0);
    if (!uid) throw new Error("배정할 사용자 ID를 입력하세요.");
    const payload = {
      assignee_user_id: uid,
      note: normalizeText($("#assignNote").value, 2000, "배정 메모", false),
    };
    await jfetch(`/api/v1/admin/complaints/${selectedAdminComplaintId}/assign`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setMsg("담당자 배정이 반영되었습니다.");
    await loadComplaintDetail(selectedAdminComplaintId, true);
  }

  async function runPatchWorkOrder() {
    const workOrderId = Number($("#workOrderId").value || 0);
    if (!workOrderId) throw new Error("작업지시 ID를 입력하세요.");
    const payload = {
      status: String($("#workOrderStatus").value || "OPEN").toUpperCase(),
      result_note: normalizeText($("#workOrderNote").value, 4000, "작업 결과 메모", false),
    };
    await jfetch(`/api/v1/admin/work-orders/${workOrderId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    setMsg("작업 상태가 반영되었습니다.");
    if (selectedAdminComplaintId) {
      await loadComplaintDetail(selectedAdminComplaintId, true);
    }
  }

  async function loadStats() {
    if (!isAdmin(me)) return;
    const data = await jfetch("/api/v1/admin/stats/complaints");
    const item = data && data.item ? data.item : {};
    const byStatus = Array.isArray(item.by_status) ? item.by_status.map((x) => `${x.status}:${x.count}`).join(", ") : "-";
    const byScope = Array.isArray(item.by_scope) ? item.by_scope.map((x) => `${x.scope}:${x.count}`).join(", ") : "-";
    $("#statsBox").textContent =
      `전체:${item.total_count || 0} / 긴급:${item.emergency_count || 0} / 지연:${item.delayed_count || 0}\n` +
      `평균해결시간(시간): ${item.avg_resolution_hours == null ? "-" : Number(item.avg_resolution_hours).toFixed(2)}\n` +
      `상태별: ${byStatus}\n범위별: ${byScope}`;
  }

  function wire() {
    $("#scopeSelect")?.addEventListener("change", () => fillCategories());
    $("#btnReload")?.addEventListener("click", () => {
      init().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnSubmitComplaint")?.addEventListener("click", () => {
      submitComplaint(false).catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnSubmitEmergency")?.addEventListener("click", () => {
      submitComplaint(true).catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnLoadMine")?.addEventListener("click", () => {
      loadMyComplaints().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#myStatusFilter")?.addEventListener("change", () => {
      loadMyComplaints().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#myList")?.addEventListener("click", (e) => {
      const btn = e.target.closest(".item[data-id]");
      if (!btn) return;
      const id = Number(btn.dataset.id || 0);
      if (!id) return;
      loadComplaintDetail(id, false).catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnAddComment")?.addEventListener("click", () => {
      addCommentToSelected().catch((err) => setMsg(err.message || String(err), true));
    });

    $("#adminList")?.addEventListener("click", (e) => {
      const btn = e.target.closest(".item[data-id]");
      if (!btn) return;
      const id = Number(btn.dataset.id || 0);
      if (!id) return;
      loadComplaintDetail(id, true).catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnLoadAdmin")?.addEventListener("click", () => {
      loadAdminComplaints().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#adminScopeFilter")?.addEventListener("change", () => {
      loadAdminComplaints().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#adminStatusFilter")?.addEventListener("change", () => {
      loadAdminComplaints().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnTriage")?.addEventListener("click", () => {
      runTriage().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnAssign")?.addEventListener("click", () => {
      runAssign().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnPatchWorkOrder")?.addEventListener("click", () => {
      runPatchWorkOrder().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnLoadStats")?.addEventListener("click", () => {
      loadStats().catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnLogout")?.addEventListener("click", () => {
      const run = async () => {
        try {
          await jfetch("/api/auth/logout", { method: "POST" });
        } catch (_e) {}
        KAAuth.clearSession();
        KAAuth.redirectLogin("/pwa/complaints.html");
      };
      run().catch(() => {});
    });
  }

  async function init() {
    me = await KAAuth.requireAuth();
    updateMetaLine();
    normalizeSiteContext();
    await loadCategories();
    await loadMyComplaints();
    if (isAdmin(me)) {
      $("#adminSection").classList.remove("hidden");
      await loadAdminComplaints();
      await loadStats();
    } else {
      $("#adminSection").classList.add("hidden");
    }
    setMsg("민원 모듈 준비 완료");
  }

  wire();
  init().catch((err) => {
    const msg = err && err.message ? err.message : String(err);
    if (msg.includes("로그인이 필요")) return;
    setMsg(msg, true);
  });
})();
