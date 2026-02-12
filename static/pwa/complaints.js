(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";

  let me = null;
  let categories = [];
  let selectedComplaintId = null;
  let selectedAdminComplaintId = null;
  let selectedAdminDetail = null;

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
      return `<option value="${Number(x.id)}">${escapeHtml(x.name)} (${escapeHtml(x.scope)})</option>`;
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
    return raw
      .split(/\r?\n/g)
      .map((x) => x.trim())
      .filter(Boolean);
  }

  function payloadForSubmit(forceEmergency) {
    const scope = getSelectedScope();
    const site_name = String($("#siteName").value || "").trim();
    const site_code = String($("#siteCode").value || "").trim().toUpperCase();
    const unit_label = String($("#unitLabel").value || "").trim();
    const title = String($("#titleInput").value || "").trim();
    const description = String($("#descInput").value || "").trim();
    const location_detail = String($("#locInput").value || "").trim();
    const category_id = Number($("#categorySelect").value || 0);
    if (!title || !description || !category_id) {
      throw new Error("제목/내용/카테고리를 입력하세요.");
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
      await loadComplaintDetail(selectedComplaintId);
    }
  }

  function complaintCardHtml(row, activeId) {
    const id = Number(row.id || 0);
    const active = id === Number(activeId);
    return `
      <button class="item ${active ? "active" : ""}" data-id="${id}" type="button">
        <div class="head">
          <span>${escapeHtml(row.ticket_no || `#${id}`)}</span>
          <span>${escapeHtml(row.status || "-")}</span>
        </div>
        <div class="sub">${escapeHtml(row.scope || "-")} · ${escapeHtml(row.priority || "-")} · ${escapeHtml(row.category_name || "-")}</div>
        <div class="sub">${escapeHtml(row.title || "-")}</div>
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

  function formatDetail(item) {
    const lines = [];
    lines.push(`접수번호: ${item.ticket_no || "-"}`);
    lines.push(`상태: ${item.status || "-"} / 구분: ${item.scope || "-"}`);
    lines.push(`우선순위: ${item.priority || "-"}`);
    lines.push(`제목: ${item.title || "-"}`);
    lines.push(`내용: ${item.description || "-"}`);
    lines.push(`위치: ${item.location_detail || "-"}`);
    lines.push(`카테고리: ${item.category_name || "-"}`);
    if (item.resolution_type) lines.push(`처리방식: ${item.resolution_type}`);
    if (item.assignee_name || item.assigned_to_user_id) {
      lines.push(`담당자: ${item.assignee_name || item.assigned_to_user_id}`);
    }
    const comments = Array.isArray(item.comments) ? item.comments : [];
    if (comments.length) {
      lines.push("");
      lines.push("[댓글]");
      comments.slice(-8).forEach((c) => {
        lines.push(`- ${c.user_name || c.user_id}: ${c.comment || ""}`);
      });
    }
    const wos = Array.isArray(item.work_orders) ? item.work_orders : [];
    if (wos.length) {
      lines.push("");
      lines.push("[작업지시]");
      wos.slice(0, 3).forEach((w) => {
        lines.push(`- #${w.id} ${w.status} (${w.assignee_name || w.assignee_user_id})`);
      });
    }
    return lines.join("\n");
  }

  async function loadComplaintDetail(id) {
    if (!id) return;
    const data = await jfetch(`/api/v1/complaints/${Number(id)}`);
    const item = data && data.item ? data.item : null;
    if (!item) return;
    selectedComplaintId = Number(item.id);
    $("#detailBox").textContent = formatDetail(item);
    await loadMyComplaints();
  }

  async function addCommentToSelected() {
    if (!selectedComplaintId) {
      throw new Error("먼저 민원을 선택하세요.");
    }
    const comment = String($("#commentInput").value || "").trim();
    if (!comment) throw new Error("댓글 내용을 입력하세요.");
    await jfetch(`/api/v1/complaints/${selectedComplaintId}/comments`, {
      method: "POST",
      body: JSON.stringify({ comment }),
    });
    $("#commentInput").value = "";
    setMsg("댓글이 등록되었습니다.");
    await loadComplaintDetail(selectedComplaintId);
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

  async function loadAdminComplaintDetail(id) {
    if (!isAdmin(me) || !id) return;
    const data = await jfetch(`/api/v1/admin/complaints/${Number(id)}`);
    const item = data && data.item ? data.item : null;
    if (!item) return;
    selectedAdminComplaintId = Number(item.id);
    selectedAdminDetail = item;
    $("#detailBox").textContent = formatDetail(item);
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
  }

  async function runTriage() {
    if (!selectedAdminComplaintId) throw new Error("관리자 목록에서 민원을 먼저 선택하세요.");
    const payload = {
      scope: String($("#triageScope").value || "COMMON").toUpperCase(),
      priority: String($("#triagePriority").value || "NORMAL").toUpperCase(),
      resolution_type: String($("#triageResolution").value || "REPAIR").toUpperCase(),
      note: String($("#triageNote").value || "").trim(),
    };
    await jfetch(`/api/v1/admin/complaints/${selectedAdminComplaintId}/triage`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    setMsg("분류가 반영되었습니다.");
    await loadAdminComplaintDetail(selectedAdminComplaintId);
  }

  async function runAssign() {
    if (!selectedAdminComplaintId) throw new Error("관리자 목록에서 민원을 먼저 선택하세요.");
    const uid = Number($("#assignUserId").value || 0);
    if (!uid) throw new Error("배정할 사용자 ID를 입력하세요.");
    const payload = {
      assignee_user_id: uid,
      note: String($("#assignNote").value || "").trim(),
    };
    await jfetch(`/api/v1/admin/complaints/${selectedAdminComplaintId}/assign`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setMsg("담당자 배정이 반영되었습니다.");
    await loadAdminComplaintDetail(selectedAdminComplaintId);
  }

  async function runPatchWorkOrder() {
    const workOrderId = Number($("#workOrderId").value || 0);
    if (!workOrderId) throw new Error("작업지시 ID를 입력하세요.");
    const payload = {
      status: String($("#workOrderStatus").value || "OPEN").toUpperCase(),
      result_note: String($("#workOrderNote").value || "").trim(),
    };
    await jfetch(`/api/v1/admin/work-orders/${workOrderId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    setMsg("작업 상태가 반영되었습니다.");
    if (selectedAdminComplaintId) {
      await loadAdminComplaintDetail(selectedAdminComplaintId);
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
      loadComplaintDetail(id).catch((err) => setMsg(err.message || String(err), true));
    });
    $("#btnAddComment")?.addEventListener("click", () => {
      addCommentToSelected().catch((err) => setMsg(err.message || String(err), true));
    });

    $("#adminList")?.addEventListener("click", (e) => {
      const btn = e.target.closest(".item[data-id]");
      if (!btn) return;
      const id = Number(btn.dataset.id || 0);
      if (!id) return;
      loadAdminComplaintDetail(id).catch((err) => setMsg(err.message || String(err), true));
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
