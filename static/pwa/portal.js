(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const STATUS_VALUES = ["접수", "처리중", "완료", "이월"];
  const NOTICE_STATUS_LABELS = {
    draft: "임시저장",
    published: "게시중",
    archived: "보관",
  };
  const USER_ROLE_OPTIONS = [
    { value: "desk", label: "민원접수" },
    { value: "manager", label: "운영담당" },
    { value: "staff", label: "일반직원" },
    { value: "vendor", label: "외주업체" },
    { value: "reader", label: "읽기전용" },
    { value: "integration", label: "연동계정" },
  ];

  let me = null;
  let tenants = [];
  let selectedComplaintId = 0;
  let selectedComplaint = null;
  let lastAiResult = null;
  let users = [];
  let selectedUserId = 0;
  let selectedUser = null;
  let opsNotices = [];
  let opsDocuments = [];
  let opsSchedules = [];
  let opsVendors = [];
  let selectedNoticeId = 0;
  let selectedDocumentId = 0;
  let selectedScheduleId = 0;
  let selectedVendorId = 0;

  function setMessage(selector, message, isError = false) {
    const el = $(selector);
    if (!el) return;
    el.textContent = String(message || "");
    el.classList.toggle("error", !!isError);
  }

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function formatDateTime(value) {
    const raw = String(value || "").trim();
    if (!raw) return "-";
    const dt = new Date(raw.replace(" ", "T"));
    if (Number.isNaN(dt.getTime())) return raw;
    return dt.toLocaleString("ko-KR", { hour12: false });
  }

  function formatDate(value) {
    const raw = String(value || "").trim();
    if (!raw) return "-";
    const dt = new Date(`${raw}T00:00:00`);
    if (Number.isNaN(dt.getTime())) return raw;
    return dt.toLocaleDateString("ko-KR");
  }

  function isAdmin() {
    return !!(me && me.user && me.user.is_admin);
  }

  function canManageUsers() {
    return !!(me && me.user && (me.user.is_admin || me.user.is_site_admin));
  }

  function canEditOps() {
    if (!me || !me.user) return false;
    if (me.user.is_admin || me.user.is_site_admin) return true;
    return ["manager", "desk", "staff"].includes(String(me.user.role || ""));
  }

  function currentTenantId() {
    if (isAdmin()) {
      return String($("#tenantSelect")?.value || "").trim();
    }
    return String((me && (me.tenant?.id || me.user?.tenant_id)) || "").trim();
  }

  function currentTenantLabel() {
    const tenantId = currentTenantId();
    if (isAdmin()) {
      const tenant = tenants.find((item) => String(item.id) === tenantId);
      return tenant ? `${tenant.name} (${tenant.id})` : (tenantId || "전체");
    }
    return String(me?.tenant?.name || me?.user?.tenant_id || "-");
  }

  async function api(url, opts = {}) {
    return window.KAAuth.requestJson(url, opts);
  }

  async function authFetchJson(url, opts = {}) {
    const headers = { ...(opts.headers || {}) };
    const token = window.KAAuth.getToken();
    if (token && !headers.Authorization) {
      headers.Authorization = `Bearer ${token}`;
    }
    const response = await fetch(url, {
      ...opts,
      headers,
      credentials: "same-origin",
    });
    const contentType = response.headers.get("content-type") || "";
    const body = contentType.includes("application/json") ? await response.json() : await response.text();
    if (response.status === 401) {
      window.KAAuth.clearSession({ includeSensitive: true, broadcast: true });
      window.KAAuth.redirectLogin();
      throw new Error(typeof body === "string" ? body : String(body.detail || "401"));
    }
    if (!response.ok) {
      throw new Error(typeof body === "string" ? body : String(body.detail || body.message || response.status));
    }
    return body;
  }

  function complaintPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      building: String($("#buildingInput").value || "").trim(),
      unit: String($("#unitInput").value || "").trim(),
      complainant_phone: String($("#phoneInput").value || "").trim(),
      channel: String($("#channelInput").value || "기타").trim() || "기타",
      content: String($("#contentInput").value || "").trim(),
      manager: String($("#managerInput").value || "").trim(),
      auto_classify: true,
      summary: String((lastAiResult || {}).summary || "").trim(),
      type: String((lastAiResult || {}).type || "").trim(),
      urgency: String((lastAiResult || {}).urgency || "").trim(),
    };
  }

  function selectedFiles(inputSelector) {
    const files = $(inputSelector)?.files;
    return files ? Array.from(files) : [];
  }

  function updatePhotoHint(inputSelector, targetSelector, limit = 6) {
    const files = selectedFiles(inputSelector);
    const el = $(targetSelector);
    if (!el) return;
    if (!files.length) {
      el.textContent = "선택된 사진이 없습니다.";
      return;
    }
    el.textContent = `선택 ${files.length}장 / 최대 ${limit}장: ${files.map((file) => file.name).join(", ")}`;
  }

  function renderRoleOptions(selector, selected = "") {
    const el = $(selector);
    if (!el) return;
    el.innerHTML = USER_ROLE_OPTIONS
      .map((item) => `<option value="${escapeHtml(item.value)}"${item.value === selected ? " selected" : ""}>${escapeHtml(item.label)}</option>`)
      .join("");
  }

  function roleLabel(user) {
    if (!user) return "-";
    if (user.is_admin) return "최고관리자";
    const matched = USER_ROLE_OPTIONS.find((item) => item.value === String(user.role || ""));
    const base = matched ? matched.label : String(user.role || "일반직원");
    if (user.is_site_admin) return `현장관리자 / ${base}`;
    return base;
  }

  function isPendingApproval(user) {
    return !!user && !user.is_active && String(user.note || "").includes("[self-register]");
  }

  function userStatusLabel(user) {
    if (isPendingApproval(user)) return "승인대기";
    return user && user.is_active ? "활성" : "비활성";
  }

  function renderTenantBadge() {
    const wrap = $("#tenantBadge");
    if (!wrap || !me) return;
    const chips = [];
    chips.push(`<span class="badge">사용자: ${escapeHtml(me.user.name || me.user.login_id)}</span>`);
    if (me.tenant?.name) {
      chips.push(`<span class="badge">현재 테넌트: ${escapeHtml(me.tenant.name)}</span>`);
    } else if (isAdmin()) {
      chips.push('<span class="badge">최고관리자</span>');
    }
    if (canManageUsers() && !isAdmin()) {
      chips.push('<span class="badge">현장관리자 권한</span>');
    }
    wrap.innerHTML = chips.join("");
  }

  function applyHero() {
    renderTenantBadge();
    const role = isAdmin() ? "최고관리자" : (me?.user?.is_site_admin ? "현장관리자" : (me?.user?.role || "staff"));
    const tenantLabel = me?.tenant?.name || me?.user?.tenant_id || "선택 필요";
    $("#heroLine").textContent = `${role} 계정으로 접속 중입니다. 현재 작업 테넌트는 ${tenantLabel}입니다. 민원 접수와 함께 공지, 문서, 일정, 업체 관리까지 한 화면에서 운영할 수 있습니다.`;
  }

  function renderAiSuggestion(result) {
    const box = $("#aiSuggestion");
    if (!box) return;
    if (!result) {
      box.textContent = "민원내용을 입력하고 AI 자동분류를 실행하세요.";
      return;
    }
    box.innerHTML = [
      `<strong>유형:</strong> ${escapeHtml(result.type)}`,
      `<strong>긴급도:</strong> ${escapeHtml(result.urgency)}`,
      `<strong>요약:</strong> ${escapeHtml(result.summary)}`,
      `<strong>모델:</strong> ${escapeHtml(result.model || "-")}`,
    ].join("<br>");
  }

  function syncUserTenantDisplay() {
    const el = $("#userTenantDisplay");
    if (el) {
      el.value = currentTenantLabel();
    }
  }

  async function loadTenants() {
    if (!isAdmin()) return [];
    const data = await api("/api/admin/tenants");
    tenants = Array.isArray(data.items) ? data.items : [];
    const select = $("#tenantSelect");
    if (select) {
      select.innerHTML = tenants
        .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)} (${escapeHtml(item.id)})</option>`)
        .join("");
      if (tenants.length && !select.value) {
        select.value = String(tenants[0].id);
      }
    }
    syncUserTenantDisplay();
    renderTenantsTable();
    return tenants;
  }

  function renderTenantsTable() {
    const body = $("#tenantsTableBody");
    if (!body) return;
    body.innerHTML = tenants.map((item) => `
      <tr>
        <td class="mono">${escapeHtml(item.id)}</td>
        <td>${escapeHtml(item.name)}</td>
        <td>${escapeHtml(item.site_code || "")}</td>
        <td>${escapeHtml(item.status)}</td>
        <td>${escapeHtml(formatDateTime(item.last_used_at))}</td>
        <td><button class="ghost-btn tenant-rotate" data-tenant="${escapeHtml(item.id)}" type="button">키 재발급</button></td>
      </tr>
    `).join("");
    body.querySelectorAll(".tenant-rotate").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const tenantId = String(btn.getAttribute("data-tenant") || "").trim();
        const res = await api(`/api/admin/tenants/${encodeURIComponent(tenantId)}/rotate_key`, { method: "POST" });
        $("#apiKeyBox").textContent = `새 API Key (${tenantId}): ${res.item.api_key}`;
        setMessage("#adminMsg", "API Key를 재발급했습니다.");
        await loadTenants();
      });
    });
  }

  function syncOpsWriteState() {
    const editable = canEditOps();
    document.querySelectorAll("[data-ops-write='1']").forEach((el) => {
      el.disabled = !editable;
    });
    const hint = $("#opsReadOnlyHint");
    if (!hint) return;
    hint.textContent = editable
      ? "행정업무 편집 권한이 있습니다. 공지, 문서, 일정, 업체를 여기서 직접 관리할 수 있습니다."
      : "현재 계정은 행정업무 조회만 가능합니다. 수정이 필요하면 현장관리자 이상 권한으로 로그인하세요.";
  }

  function renderOpsVendorOptions(selected = "") {
    const el = $("#scheduleVendorId");
    if (!el) return;
    const current = String(selected || "");
    el.innerHTML = [
      '<option value="">업체 미지정</option>',
      ...opsVendors.map((item) => `<option value="${Number(item.id || 0)}"${String(item.id) === current ? " selected" : ""}>${escapeHtml(item.company_name || "-")} / ${escapeHtml(item.service_type || "-")}</option>`),
    ].join("");
  }

  function clearNoticeForm() {
    selectedNoticeId = 0;
    $("#noticeTitle").value = "";
    $("#noticeCategory").value = "공지";
    $("#noticeStatus").value = "published";
    $("#noticePinned").checked = false;
    $("#noticeBody").value = "";
    $("#opsNoticeDetail").textContent = "공지를 선택하거나 새로 등록하세요.";
  }

  function clearDocumentForm() {
    selectedDocumentId = 0;
    $("#documentTitle").value = "";
    $("#documentCategory").value = "계약";
    $("#documentStatus").value = "작성중";
    $("#documentOwner").value = "";
    $("#documentDueDate").value = "";
    $("#documentRefNo").value = "";
    $("#documentSummary").value = "";
    $("#opsDocumentDetail").textContent = "문서를 선택하거나 새로 등록하세요.";
  }

  function clearScheduleForm() {
    selectedScheduleId = 0;
    $("#scheduleTitle").value = "";
    $("#scheduleType").value = "행정";
    $("#scheduleStatus").value = "예정";
    $("#scheduleOwner").value = "";
    $("#scheduleDueDate").value = "";
    $("#scheduleNote").value = "";
    renderOpsVendorOptions("");
    $("#opsScheduleDetail").textContent = "일정을 선택하거나 새로 등록하세요.";
  }

  function clearVendorForm() {
    selectedVendorId = 0;
    $("#vendorCompanyName").value = "";
    $("#vendorServiceType").value = "";
    $("#vendorContactName").value = "";
    $("#vendorPhone").value = "";
    $("#vendorEmail").value = "";
    $("#vendorStatus").value = "활성";
    $("#vendorNote").value = "";
    $("#opsVendorDetail").textContent = "협력업체를 선택하거나 새로 등록하세요.";
  }

  function renderNoticeDetail(item) {
    selectedNoticeId = Number(item.id || 0);
    $("#noticeTitle").value = String(item.title || "");
    $("#noticeCategory").value = String(item.category || "공지");
    $("#noticeStatus").value = String(item.status || "published");
    $("#noticePinned").checked = !!item.pinned;
    $("#noticeBody").value = String(item.body || "");
    $("#opsNoticeDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `분류: ${escapeHtml(item.category || "-")}`,
      `상태: ${escapeHtml(NOTICE_STATUS_LABELS[item.status] || item.status || "-")}`,
      `고정: ${item.pinned ? "예" : "아니오"}`,
      `수정일: ${escapeHtml(formatDateTime(item.updated_at))}`,
      `작성자: ${escapeHtml(item.created_by_label || "-")}`,
    ].join("<br>");
  }

  function renderDocumentDetail(item) {
    selectedDocumentId = Number(item.id || 0);
    $("#documentTitle").value = String(item.title || "");
    $("#documentCategory").value = String(item.category || "기타");
    $("#documentStatus").value = String(item.status || "작성중");
    $("#documentOwner").value = String(item.owner || "");
    $("#documentDueDate").value = String(item.due_date || "");
    $("#documentRefNo").value = String(item.reference_no || "");
    $("#documentSummary").value = String(item.summary || "");
    $("#opsDocumentDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `분류: ${escapeHtml(item.category || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `담당: ${escapeHtml(item.owner || "-")}`,
      `기한: ${escapeHtml(formatDate(item.due_date))}`,
      `문서번호: ${escapeHtml(item.reference_no || "-")}`,
    ].join("<br>");
  }

  function renderScheduleDetail(item) {
    selectedScheduleId = Number(item.id || 0);
    $("#scheduleTitle").value = String(item.title || "");
    $("#scheduleType").value = String(item.schedule_type || "행정");
    $("#scheduleStatus").value = String(item.status || "예정");
    $("#scheduleOwner").value = String(item.owner || "");
    $("#scheduleDueDate").value = String(item.due_date || "");
    $("#scheduleNote").value = String(item.note || "");
    renderOpsVendorOptions(String(item.vendor_id || ""));
    $("#opsScheduleDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `분류: ${escapeHtml(item.schedule_type || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `예정일: ${escapeHtml(formatDate(item.due_date))}`,
      `담당: ${escapeHtml(item.owner || "-")}`,
      `업체: ${escapeHtml(item.vendor_name || "-")}`,
    ].join("<br>");
  }

  function renderVendorDetail(item) {
    selectedVendorId = Number(item.id || 0);
    $("#vendorCompanyName").value = String(item.company_name || "");
    $("#vendorServiceType").value = String(item.service_type || "");
    $("#vendorContactName").value = String(item.contact_name || "");
    $("#vendorPhone").value = String(item.phone || "");
    $("#vendorEmail").value = String(item.email || "");
    $("#vendorStatus").value = String(item.status || "활성");
    $("#vendorNote").value = String(item.note || "");
    $("#opsVendorDetail").innerHTML = [
      `<strong>${escapeHtml(item.company_name || "-")}</strong>`,
      `분야: ${escapeHtml(item.service_type || "-")}`,
      `담당자: ${escapeHtml(item.contact_name || "-")}`,
      `전화: ${escapeHtml(item.phone || "-")}`,
      `이메일: ${escapeHtml(item.email || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
    ].join("<br>");
  }

  async function loadOpsDashboard() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/ops/dashboard?tenant_id=${encodeURIComponent(tenantId)}`);
    const item = data.item || {};
    $("#opsMetricNotices").textContent = String(item.published_notices || 0);
    $("#opsMetricDocuments").textContent = String(item.open_documents || 0);
    $("#opsMetricSchedules").textContent = String(item.open_schedules || 0);
    $("#opsMetricVendors").textContent = String(item.active_vendors || 0);
    $("#opsRecentNotices").innerHTML = (item.recent_notices || []).length
      ? item.recent_notices.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.category || "-")} / ${escapeHtml(NOTICE_STATUS_LABELS[row.status] || row.status || "-")} / ${escapeHtml(formatDateTime(row.updated_at))}</p></article>`).join("")
      : '<div class="empty-state">등록된 공지가 없습니다.</div>';
    $("#opsOverdueDocuments").innerHTML = (item.overdue_documents || []).length
      ? item.overdue_documents.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.category || "-")} / ${escapeHtml(row.status || "-")} / 기한 ${escapeHtml(formatDate(row.due_date))}</p></article>`).join("")
      : '<div class="empty-state">기한 지연 문서가 없습니다.</div>';
    $("#opsUpcomingSchedules").innerHTML = (item.upcoming_schedules || []).length
      ? item.upcoming_schedules.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.schedule_type || "-")} / ${escapeHtml(row.status || "-")} / ${escapeHtml(formatDate(row.due_date))} / ${escapeHtml(row.vendor_name || "내부")}</p></article>`).join("")
      : '<div class="empty-state">등록된 일정이 없습니다.</div>';
  }

  async function loadOpsNotices() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/ops/notices?tenant_id=${encodeURIComponent(tenantId)}`);
    opsNotices = Array.isArray(data.items) ? data.items : [];
    const body = $("#opsNoticesTableBody");
    body.innerHTML = opsNotices.length
      ? opsNotices.map((item) => `
        <tr class="ops-notice-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.title || "-")}</td>
          <td>${escapeHtml(item.category || "-")}</td>
          <td>${escapeHtml(NOTICE_STATUS_LABELS[item.status] || item.status || "-")}</td>
          <td>${item.pinned ? "예" : "-"}</td>
          <td>${escapeHtml(formatDateTime(item.updated_at))}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 공지가 없습니다.</td></tr>';
    body.querySelectorAll(".ops-notice-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsNotices.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderNoticeDetail(item);
      });
    });
    if (selectedNoticeId) {
      const found = opsNotices.find((item) => Number(item.id || 0) === selectedNoticeId);
      if (found) renderNoticeDetail(found); else clearNoticeForm();
    }
    return opsNotices;
  }

  async function loadOpsDocuments() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/ops/documents?tenant_id=${encodeURIComponent(tenantId)}`);
    opsDocuments = Array.isArray(data.items) ? data.items : [];
    const body = $("#opsDocumentsTableBody");
    body.innerHTML = opsDocuments.length
      ? opsDocuments.map((item) => `
        <tr class="ops-document-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.title || "-")}</td>
          <td>${escapeHtml(item.category || "-")}</td>
          <td>${escapeHtml(item.status || "-")}</td>
          <td>${escapeHtml(item.owner || "-")}</td>
          <td>${escapeHtml(formatDate(item.due_date))}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 문서가 없습니다.</td></tr>';
    body.querySelectorAll(".ops-document-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsDocuments.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderDocumentDetail(item);
      });
    });
    if (selectedDocumentId) {
      const found = opsDocuments.find((item) => Number(item.id || 0) === selectedDocumentId);
      if (found) renderDocumentDetail(found); else clearDocumentForm();
    }
    return opsDocuments;
  }

  async function loadOpsVendors() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/ops/vendors?tenant_id=${encodeURIComponent(tenantId)}`);
    opsVendors = Array.isArray(data.items) ? data.items : [];
    renderOpsVendorOptions($("#scheduleVendorId")?.value || "");
    const body = $("#opsVendorsTableBody");
    body.innerHTML = opsVendors.length
      ? opsVendors.map((item) => `
        <tr class="ops-vendor-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.company_name || "-")}</td>
          <td>${escapeHtml(item.service_type || "-")}</td>
          <td>${escapeHtml(item.contact_name || "-")}</td>
          <td>${escapeHtml(item.phone || "-")}</td>
          <td>${escapeHtml(item.status || "-")}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 업체가 없습니다.</td></tr>';
    body.querySelectorAll(".ops-vendor-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsVendors.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderVendorDetail(item);
      });
    });
    if (selectedVendorId) {
      const found = opsVendors.find((item) => Number(item.id || 0) === selectedVendorId);
      if (found) renderVendorDetail(found); else clearVendorForm();
    }
    return opsVendors;
  }

  async function loadOpsSchedules() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/ops/schedules?tenant_id=${encodeURIComponent(tenantId)}`);
    opsSchedules = Array.isArray(data.items) ? data.items : [];
    const body = $("#opsSchedulesTableBody");
    body.innerHTML = opsSchedules.length
      ? opsSchedules.map((item) => `
        <tr class="ops-schedule-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.title || "-")}</td>
          <td>${escapeHtml(item.schedule_type || "-")}</td>
          <td>${escapeHtml(item.status || "-")}</td>
          <td>${escapeHtml(formatDate(item.due_date))}</td>
          <td>${escapeHtml(item.vendor_name || "-")}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 일정이 없습니다.</td></tr>';
    body.querySelectorAll(".ops-schedule-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsSchedules.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderScheduleDetail(item);
      });
    });
    if (selectedScheduleId) {
      const found = opsSchedules.find((item) => Number(item.id || 0) === selectedScheduleId);
      if (found) renderScheduleDetail(found); else clearScheduleForm();
    }
    return opsSchedules;
  }

  function noticePayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      title: String($("#noticeTitle").value || "").trim(),
      body: String($("#noticeBody").value || "").trim(),
      category: String($("#noticeCategory").value || "공지").trim(),
      status: String($("#noticeStatus").value || "published").trim(),
      pinned: !!$("#noticePinned").checked,
    };
  }

  function documentPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      title: String($("#documentTitle").value || "").trim(),
      summary: String($("#documentSummary").value || "").trim(),
      category: String($("#documentCategory").value || "기타").trim(),
      status: String($("#documentStatus").value || "작성중").trim(),
      owner: String($("#documentOwner").value || "").trim(),
      due_date: String($("#documentDueDate").value || "").trim(),
      reference_no: String($("#documentRefNo").value || "").trim(),
    };
  }

  function schedulePayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      title: String($("#scheduleTitle").value || "").trim(),
      schedule_type: String($("#scheduleType").value || "행정").trim(),
      status: String($("#scheduleStatus").value || "예정").trim(),
      due_date: String($("#scheduleDueDate").value || "").trim(),
      owner: String($("#scheduleOwner").value || "").trim(),
      note: String($("#scheduleNote").value || "").trim(),
      vendor_id: String($("#scheduleVendorId").value || "").trim(),
    };
  }

  function vendorPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      company_name: String($("#vendorCompanyName").value || "").trim(),
      service_type: String($("#vendorServiceType").value || "").trim(),
      contact_name: String($("#vendorContactName").value || "").trim(),
      phone: String($("#vendorPhone").value || "").trim(),
      email: String($("#vendorEmail").value || "").trim(),
      status: String($("#vendorStatus").value || "활성").trim(),
      note: String($("#vendorNote").value || "").trim(),
    };
  }

  async function createNotice() {
    const data = await api("/api/ops/notices", { method: "POST", body: JSON.stringify(noticePayloadFromForm()) });
    renderNoticeDetail(data.item || {});
    setMessage("#opsNoticeMsg", "공지를 등록했습니다.");
    await loadOpsNotices();
    await loadOpsDashboard();
  }

  async function updateNotice() {
    if (!selectedNoticeId) throw new Error("수정할 공지를 선택하세요.");
    const data = await api(`/api/ops/notices/${selectedNoticeId}`, { method: "PATCH", body: JSON.stringify(noticePayloadFromForm()) });
    renderNoticeDetail(data.item || {});
    setMessage("#opsNoticeMsg", "공지를 수정했습니다.");
    await loadOpsNotices();
    await loadOpsDashboard();
  }

  async function deleteNotice() {
    if (!selectedNoticeId) throw new Error("삭제할 공지를 선택하세요.");
    await api(`/api/ops/notices/${selectedNoticeId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearNoticeForm();
    setMessage("#opsNoticeMsg", "공지를 삭제했습니다.");
    await loadOpsNotices();
    await loadOpsDashboard();
  }

  async function createDocument() {
    const data = await api("/api/ops/documents", { method: "POST", body: JSON.stringify(documentPayloadFromForm()) });
    renderDocumentDetail(data.item || {});
    setMessage("#opsDocumentMsg", "문서를 등록했습니다.");
    await loadOpsDocuments();
    await loadOpsDashboard();
  }

  async function updateDocument() {
    if (!selectedDocumentId) throw new Error("수정할 문서를 선택하세요.");
    const data = await api(`/api/ops/documents/${selectedDocumentId}`, { method: "PATCH", body: JSON.stringify(documentPayloadFromForm()) });
    renderDocumentDetail(data.item || {});
    setMessage("#opsDocumentMsg", "문서를 수정했습니다.");
    await loadOpsDocuments();
    await loadOpsDashboard();
  }

  async function deleteDocument() {
    if (!selectedDocumentId) throw new Error("삭제할 문서를 선택하세요.");
    await api(`/api/ops/documents/${selectedDocumentId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearDocumentForm();
    setMessage("#opsDocumentMsg", "문서를 삭제했습니다.");
    await loadOpsDocuments();
    await loadOpsDashboard();
  }

  async function createVendor() {
    const data = await api("/api/ops/vendors", { method: "POST", body: JSON.stringify(vendorPayloadFromForm()) });
    renderVendorDetail(data.item || {});
    setMessage("#opsVendorMsg", "업체를 등록했습니다.");
    await loadOpsVendors();
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function updateVendor() {
    if (!selectedVendorId) throw new Error("수정할 업체를 선택하세요.");
    const data = await api(`/api/ops/vendors/${selectedVendorId}`, { method: "PATCH", body: JSON.stringify(vendorPayloadFromForm()) });
    renderVendorDetail(data.item || {});
    setMessage("#opsVendorMsg", "업체 정보를 수정했습니다.");
    await loadOpsVendors();
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function deleteVendor() {
    if (!selectedVendorId) throw new Error("삭제할 업체를 선택하세요.");
    await api(`/api/ops/vendors/${selectedVendorId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearVendorForm();
    setMessage("#opsVendorMsg", "업체를 삭제했습니다.");
    await loadOpsVendors();
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function createSchedule() {
    const data = await api("/api/ops/schedules", { method: "POST", body: JSON.stringify(schedulePayloadFromForm()) });
    renderScheduleDetail(data.item || {});
    setMessage("#opsScheduleMsg", "일정을 등록했습니다.");
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function updateSchedule() {
    if (!selectedScheduleId) throw new Error("수정할 일정을 선택하세요.");
    const data = await api(`/api/ops/schedules/${selectedScheduleId}`, { method: "PATCH", body: JSON.stringify(schedulePayloadFromForm()) });
    renderScheduleDetail(data.item || {});
    setMessage("#opsScheduleMsg", "일정을 수정했습니다.");
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function deleteSchedule() {
    if (!selectedScheduleId) throw new Error("삭제할 일정을 선택하세요.");
    await api(`/api/ops/schedules/${selectedScheduleId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearScheduleForm();
    setMessage("#opsScheduleMsg", "일정을 삭제했습니다.");
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function classifyCurrentText() {
    const payload = complaintPayloadFromForm();
    if (!payload.content) {
      throw new Error("민원내용을 입력하세요.");
    }
    const text = [payload.building ? `${payload.building}동` : "", payload.unit ? `${payload.unit}호` : "", payload.content].filter(Boolean).join(" ");
    const data = await api("/api/ai/classify", {
      method: "POST",
      body: JSON.stringify({ tenant_id: payload.tenant_id, text }),
    });
    lastAiResult = data.item || null;
    renderAiSuggestion(lastAiResult);
    return lastAiResult;
  }

  async function createComplaint() {
    const payload = complaintPayloadFromForm();
    const files = selectedFiles("#photoInput");
    if (!payload.tenant_id) throw new Error("테넌트를 선택하세요.");
    if (!payload.content) throw new Error("민원내용을 입력하세요.");
    if (files.length > 6) throw new Error("사진은 최대 6장까지 업로드할 수 있습니다.");
    if (!lastAiResult) {
      await classifyCurrentText();
      payload.summary = String((lastAiResult || {}).summary || "");
      payload.type = String((lastAiResult || {}).type || "");
      payload.urgency = String((lastAiResult || {}).urgency || "");
    }
    const data = await api("/api/complaints", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const item = data.item;
    for (const file of files) {
      if (!item?.id) break;
      const fd = new FormData();
      fd.append("file", file, file.name || "photo");
      await authFetchJson(`/api/complaints/${item.id}/attachments?tenant_id=${encodeURIComponent(payload.tenant_id)}`, {
        method: "POST",
        body: fd,
      });
    }
    setMessage("#intakeMsg", "민원을 저장했습니다.");
    $("#contentInput").value = "";
    $("#phoneInput").value = "";
    $("#photoInput").value = "";
    updatePhotoHint("#photoInput", "#photoHint");
    lastAiResult = null;
    renderAiSuggestion(null);
    await reloadAll();
  }

  async function loadDashboard() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/dashboard/summary?tenant_id=${encodeURIComponent(tenantId)}`);
    const item = data.item || {};
    $("#metricTodayTotal").textContent = String(item.today_total || 0);
    $("#metricTodayDone").textContent = String(item.today_done || 0);
    $("#metricPending").textContent = String(item.pending_total || 0);
    $("#metricCarry").textContent = String(item.carry_total || 0);

    $("#urgentList").innerHTML = (item.urgent_items || []).length
      ? item.urgent_items.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.summary || row.type)}</strong><p>${escapeHtml((row.building || "-") + "동 / " + (row.status || "-"))}</p></article>`).join("")
      : '<div class="empty-state">긴급 민원이 없습니다.</div>';

    $("#repeatList").innerHTML = (item.repeat_items || []).length
      ? item.repeat_items.map((row) => `<article class="timeline-item"><strong>${escapeHtml((row.building || "-") + "동 " + (row.unit || ""))}</strong><p>${escapeHtml(`${row.type} / ${row.count}회`)}</p></article>`).join("")
      : '<div class="empty-state">반복 민원이 없습니다.</div>';

    $("#typeSummary").innerHTML = (item.type_counts || []).length
      ? item.type_counts.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.type)}</strong><p>${escapeHtml(String(row.count))}건</p></article>`).join("")
      : '<div class="empty-state">유형별 데이터가 없습니다.</div>';

    $("#managerSummary").innerHTML = (item.manager_load || []).length
      ? item.manager_load.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.manager)}</strong><p>${escapeHtml(String(row.count))}건</p></article>`).join("")
      : '<div class="empty-state">담당자 부하 데이터가 없습니다.</div>';
  }

  function complaintRowHtml(row) {
    const location = [row.building ? `${row.building}동` : "", row.unit ? `${row.unit}호` : ""].filter(Boolean).join(" ");
    return `
      <tr class="complaint-row" data-id="${Number(row.id || 0)}">
        <td>${escapeHtml(formatDateTime(row.created_at))}</td>
        <td>${escapeHtml(location || "-")}</td>
        <td>${escapeHtml(row.type || "-")}</td>
        <td>${escapeHtml(row.urgency || "-")}</td>
        <td>${escapeHtml(row.status || "-")}</td>
        <td>${escapeHtml(row.manager || "-")}</td>
        <td>${escapeHtml(row.summary || row.content || "-")}</td>
      </tr>
    `;
  }

  async function loadComplaints() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const status = String($("#filterStatus").value || "").trim();
    const building = String($("#filterBuilding").value || "").trim();
    const params = new URLSearchParams({ tenant_id: tenantId });
    if (status) params.set("status", status);
    if (building) params.set("building", building);
    const data = await api(`/api/complaints?${params.toString()}`);
    const rows = Array.isArray(data.items) ? data.items : [];
    const body = $("#complaintsTableBody");
    body.innerHTML = rows.length ? rows.map(complaintRowHtml).join("") : '<tr><td colspan="7" class="empty-state">조회된 민원이 없습니다.</td></tr>';
    body.querySelectorAll(".complaint-row").forEach((rowEl) => {
      rowEl.addEventListener("click", async () => {
        selectedComplaintId = Number(rowEl.getAttribute("data-id") || 0);
        await loadComplaintDetail();
      });
    });
  }

  async function loadComplaintDetail() {
    if (!selectedComplaintId) return;
    const tenantId = currentTenantId();
    const data = await api(`/api/complaints/${selectedComplaintId}?tenant_id=${encodeURIComponent(tenantId)}`);
    selectedComplaint = data.item || null;
    if (!selectedComplaint) return;
    $("#complaintDetail").innerHTML = [
      `<strong>${escapeHtml(selectedComplaint.summary || "-")}</strong>`,
      `유형: ${escapeHtml(selectedComplaint.type || "-")}`,
      `긴급도: ${escapeHtml(selectedComplaint.urgency || "-")}`,
      `상태: ${escapeHtml(selectedComplaint.status || "-")}`,
      `채널: ${escapeHtml(selectedComplaint.channel || "-")}`,
      `연락처: ${escapeHtml(selectedComplaint.complainant_phone || "-")}`,
      `내용: ${escapeHtml(selectedComplaint.content || "-")}`,
      `반복접수: ${escapeHtml(String(selectedComplaint.repeat_count || 0))}회`,
    ].join("<br>");
    $("#detailStatus").value = String(selectedComplaint.status || "접수");
    $("#detailManager").value = String(selectedComplaint.manager || "");
    $("#attachmentSelectAll").checked = false;
    $("#detailAttachments").innerHTML = (selectedComplaint.attachments || []).length
      ? selectedComplaint.attachments.map((row) => `
        <article class="attachment-card">
          <label class="attachment-top">
            <input class="attachment-check" type="checkbox" value="${Number(row.id || 0)}" />
            <span>${escapeHtml(row.file_url || "")}</span>
          </label>
          <img src="${escapeHtml(row.file_url || "")}" alt="민원 첨부 이미지" loading="lazy" />
          <div class="attachment-meta">${escapeHtml(formatDateTime(row.created_at))}</div>
        </article>
      `).join("")
      : '<div class="empty-state">첨부 사진이 없습니다.</div>';
    $("#detailHistory").innerHTML = (selectedComplaint.history || []).length
      ? selectedComplaint.history.map((row) => `<article class="timeline-item"><strong>${escapeHtml((row.from_status || "초기") + " → " + row.to_status)}</strong><p>${escapeHtml(formatDateTime(row.created_at))} / ${escapeHtml(row.actor_label || "-")}</p>${row.note ? `<p>${escapeHtml(row.note)}</p>` : ""}</article>`).join("")
      : '<div class="empty-state">이력이 없습니다.</div>';
  }

  function selectedAttachmentIds() {
    return Array.from(document.querySelectorAll(".attachment-check:checked")).map((el) => Number(el.value || 0)).filter(Boolean);
  }

  async function uploadDetailAttachments() {
    if (!selectedComplaintId || !selectedComplaint) throw new Error("목록에서 민원을 먼저 선택하세요.");
    const tenantId = currentTenantId();
    const currentCount = Array.isArray(selectedComplaint.attachments) ? selectedComplaint.attachments.length : 0;
    const files = selectedFiles("#detailPhotoInput");
    if (!files.length) throw new Error("추가할 사진을 선택하세요.");
    if (currentCount + files.length > 6) throw new Error("첨부 사진은 민원당 최대 6장까지 가능합니다.");
    for (const file of files) {
      const fd = new FormData();
      fd.append("file", file, file.name || "photo");
      await authFetchJson(`/api/complaints/${selectedComplaintId}/attachments?tenant_id=${encodeURIComponent(tenantId)}`, {
        method: "POST",
        body: fd,
      });
    }
    $("#detailPhotoInput").value = "";
    await loadComplaintDetail();
    await loadComplaints();
  }

  async function deleteAttachments(deleteAll = false) {
    if (!selectedComplaintId) throw new Error("목록에서 민원을 먼저 선택하세요.");
    const tenantId = currentTenantId();
    const attachmentIds = deleteAll ? [] : selectedAttachmentIds();
    if (!deleteAll && !attachmentIds.length) throw new Error("삭제할 첨부를 선택하세요.");
    await api(`/api/complaints/${selectedComplaintId}/attachments`, {
      method: "DELETE",
      body: JSON.stringify({ tenant_id: tenantId, delete_all: deleteAll, attachment_ids: attachmentIds }),
    });
    await loadComplaintDetail();
    await loadComplaints();
  }

  async function updateSelectedComplaint() {
    if (!selectedComplaintId) throw new Error("목록에서 민원을 먼저 선택하세요.");
    const tenantId = currentTenantId();
    const payload = {
      tenant_id: tenantId,
      status: String($("#detailStatus").value || "접수"),
      manager: String($("#detailManager").value || "").trim(),
      note: String($("#detailNote").value || "").trim(),
    };
    await api(`/api/complaints/${selectedComplaintId}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    $("#detailNote").value = "";
    await loadComplaints();
    await loadComplaintDetail();
    await loadDashboard();
  }

  async function generateReport() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/report/daily?tenant_id=${encodeURIComponent(tenantId)}`);
    $("#reportBox").textContent = String(data.item?.report_text || "");
  }

  async function digestChat() {
    const tenantId = currentTenantId();
    const text = String($("#chatInput").value || "").trim();
    const files = selectedFiles("#chatImageInput");
    if (!text && !files.length) throw new Error("카톡 대화 또는 이미지를 입력하세요.");
    if (files.length > 6) throw new Error("카톡 이미지는 최대 6장까지 업로드할 수 있습니다.");

    let data;
    if (files.length) {
      const fd = new FormData();
      fd.append("tenant_id", tenantId);
      fd.append("text", text);
      for (const file of files) {
        fd.append("files", file, file.name || "chat-image");
      }
      data = await authFetchJson("/api/ai/kakao_digest/images", {
        method: "POST",
        body: fd,
      });
    } else {
      data = await api("/api/ai/kakao_digest", {
        method: "POST",
        body: JSON.stringify({ tenant_id: tenantId, text }),
      });
    }
    $("#chatDigestBox").textContent = String(data.item?.report_text || "");
  }

  async function createTenant() {
    const payload = {
      tenant_id: String($("#newTenantId").value || "").trim(),
      name: String($("#newTenantName").value || "").trim(),
      site_code: String($("#newTenantSiteCode").value || "").trim(),
      site_name: String($("#newTenantSiteName").value || "").trim(),
    };
    const data = await api("/api/admin/tenants", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    $("#apiKeyBox").textContent = `신규 API Key (${data.item.id}): ${data.item.api_key}`;
    setMessage("#adminMsg", "테넌트를 생성했습니다.");
    ["#newTenantId", "#newTenantName", "#newTenantSiteCode", "#newTenantSiteName"].forEach((sel) => {
      const el = $(sel);
      if (el) el.value = "";
    });
    await loadTenants();
  }

  function clearUserCreateForm() {
    ["#newUserLoginId", "#newUserName", "#newUserPhone", "#newUserPassword", "#newUserNote"].forEach((sel) => {
      const el = $(sel);
      if (el) el.value = "";
    });
    $("#newUserIsSiteAdmin").checked = false;
    renderRoleOptions("#newUserRole", "desk");
  }

  function clearSelectedUserEditor() {
    selectedUserId = 0;
    selectedUser = null;
    $("#userDetail").textContent = "사용자를 선택하세요.";
    ["#editUserName", "#editUserPhone", "#editUserNote", "#resetUserPassword"].forEach((sel) => {
      const el = $(sel);
      if (el) el.value = "";
    });
    $("#editUserActive").checked = true;
    $("#editUserIsSiteAdmin").checked = false;
    renderRoleOptions("#editUserRole", "desk");
  }

  function renderUserDetail(user) {
    selectedUserId = Number(user.id || 0);
    selectedUser = user;
    $("#userDetail").innerHTML = [
      `<strong>${escapeHtml(user.name || user.login_id || "-")}</strong>`,
      `아이디: ${escapeHtml(user.login_id || "-")}`,
      `권한: ${escapeHtml(roleLabel(user))}`,
      `연락처: ${escapeHtml(user.phone || "-")}`,
      `상태: ${escapeHtml(userStatusLabel(user))}`,
      `최근 로그인: ${escapeHtml(formatDateTime(user.last_login_at))}`,
      `메모: ${escapeHtml(user.note || "-")}`,
    ].join("<br>");
    $("#editUserName").value = String(user.name || "");
    $("#editUserPhone").value = String(user.phone || "");
    $("#editUserNote").value = String(user.note || "");
    $("#editUserActive").checked = !!user.is_active;
    $("#editUserIsSiteAdmin").checked = !!user.is_site_admin;
    renderRoleOptions("#editUserRole", String(user.role || "desk"));
    $("#btnApproveUser")?.toggleAttribute("disabled", !isPendingApproval(user));
  }

  function renderUsersTable() {
    const body = $("#usersTableBody");
    if (!body) return;
    const pendingUsers = users.filter((user) => isPendingApproval(user));
    const hint = $("#pendingUsersHint");
    if (hint) {
      hint.textContent = pendingUsers.length
        ? `승인대기 회원 ${pendingUsers.length}건이 있습니다. 목록에서 바로 승인하거나 선택 후 승인할 수 있습니다.`
        : "승인대기 회원이 없습니다.";
    }
    body.innerHTML = users.length
      ? users.map((user) => `
        <tr class="user-row" data-id="${Number(user.id || 0)}">
          <td class="mono">${escapeHtml(user.login_id || "")}</td>
          <td>${escapeHtml(user.name || "")}</td>
          <td>${escapeHtml(roleLabel(user))}</td>
          <td>${escapeHtml(user.phone || "-")}</td>
          <td>${escapeHtml(userStatusLabel(user))}</td>
          <td>${escapeHtml(formatDateTime(user.last_login_at))}</td>
          <td class="table-actions">
            <button class="ghost-btn user-select" type="button" data-id="${Number(user.id || 0)}">선택</button>
            ${isPendingApproval(user) ? `<button class="action-btn action-secondary user-approve" type="button" data-id="${Number(user.id || 0)}">승인</button>` : ""}
          </td>
        </tr>
      `).join("")
      : '<tr><td colspan="7" class="empty-state">조회된 사용자가 없습니다.</td></tr>';
    body.querySelectorAll(".user-select").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const userId = Number(btn.getAttribute("data-id") || 0);
        await loadUser(userId);
      });
    });
    body.querySelectorAll(".user-approve").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const userId = Number(btn.getAttribute("data-id") || 0);
        await approveUser(userId);
      });
    });
  }

  async function loadUsers() {
    if (!canManageUsers()) return [];
    syncUserTenantDisplay();
    const tenantId = currentTenantId();
    const params = new URLSearchParams();
    params.set("active_only", "false");
    if (tenantId) params.set("tenant_id", tenantId);
    const data = await api(`/api/users?${params.toString()}`);
    users = Array.isArray(data.items) ? data.items : [];
    renderUsersTable();
    if (selectedUserId) {
      const found = users.find((item) => Number(item.id || 0) === selectedUserId);
      if (found) {
        renderUserDetail(found);
      } else {
        clearSelectedUserEditor();
      }
    }
    return users;
  }

  async function loadUser(userId) {
    if (!userId) throw new Error("사용자를 먼저 선택하세요.");
    const data = await api(`/api/users/${userId}`);
    renderUserDetail(data.item || {});
  }

  async function createUser() {
    const tenantId = currentTenantId();
    if (!tenantId && !isAdmin()) throw new Error("작업할 테넌트가 없습니다.");
    const payload = {
      tenant_id: tenantId,
      login_id: String($("#newUserLoginId").value || "").trim().toLowerCase(),
      name: String($("#newUserName").value || "").trim(),
      role: String($("#newUserRole").value || "desk").trim(),
      phone: String($("#newUserPhone").value || "").trim(),
      password: String($("#newUserPassword").value || ""),
      note: String($("#newUserNote").value || "").trim(),
    };
    if (isAdmin()) {
      payload.is_site_admin = !!$("#newUserIsSiteAdmin").checked;
    }
    const data = await api("/api/users", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    clearUserCreateForm();
    setMessage("#usersMsg", `${data.item?.login_id || "새 사용자"} 계정을 등록했습니다.`);
    await loadUsers();
  }

  async function updateUser() {
    if (!selectedUserId) throw new Error("수정할 사용자를 선택하세요.");
    const payload = {
      name: String($("#editUserName").value || "").trim(),
      role: String($("#editUserRole").value || "desk").trim(),
      phone: String($("#editUserPhone").value || "").trim(),
      note: String($("#editUserNote").value || "").trim(),
      is_active: !!$("#editUserActive").checked,
    };
    if (isAdmin()) {
      payload.is_site_admin = !!$("#editUserIsSiteAdmin").checked;
    }
    const data = await api(`/api/users/${selectedUserId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    setMessage("#usersMsg", `${data.item?.login_id || "사용자"} 정보를 저장했습니다.`);
    await loadUsers();
    await loadUser(selectedUserId);
  }

  async function approveUser(userId = selectedUserId) {
    if (!userId) throw new Error("승인할 사용자를 선택하세요.");
    const data = await api(`/api/users/${userId}/approve`, {
      method: "POST",
    });
    setMessage("#usersMsg", `${data.item?.login_id || "사용자"} 승인을 완료했습니다.`);
    await loadUsers();
    await loadUser(userId);
  }

  async function resetSelectedUserPassword() {
    if (!selectedUserId) throw new Error("사용자를 먼저 선택하세요.");
    const password = String($("#resetUserPassword").value || "");
    if (!password) throw new Error("초기화할 비밀번호를 입력하세요.");
    await api(`/api/users/${selectedUserId}/reset_password`, {
      method: "POST",
      body: JSON.stringify({ password }),
    });
    $("#resetUserPassword").value = "";
    setMessage("#usersMsg", "비밀번호를 초기화했습니다.");
  }

  async function deleteSelectedUser() {
    if (!selectedUserId || !selectedUser) throw new Error("삭제할 사용자를 선택하세요.");
    if (!window.confirm(`${selectedUser.login_id} 계정을 삭제하시겠습니까?`)) return;
    await api(`/api/users/${selectedUserId}`, {
      method: "DELETE",
    });
    setMessage("#usersMsg", `${selectedUser.login_id} 계정을 삭제했습니다.`);
    clearSelectedUserEditor();
    await loadUsers();
  }

  async function reloadAll() {
    await loadDashboard();
    await loadComplaints();
    await generateReport();
    await loadOpsDashboard();
    await loadOpsNotices();
    await loadOpsDocuments();
    await loadOpsVendors();
    await loadOpsSchedules();
    if (canManageUsers()) {
      await loadUsers();
    }
  }

  function wire() {
    $("#btnLogout")?.addEventListener("click", () => window.KAAuth.logout());
    $("#btnReloadAll")?.addEventListener("click", () => reloadAll().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnClassify")?.addEventListener("click", () => {
      setMessage("#intakeMsg", "");
      classifyCurrentText().catch((error) => setMessage("#intakeMsg", error.message || String(error), true));
    });
    $("#btnCreateComplaint")?.addEventListener("click", () => {
      setMessage("#intakeMsg", "");
      createComplaint().catch((error) => setMessage("#intakeMsg", error.message || String(error), true));
    });
    $("#btnRefreshDashboard")?.addEventListener("click", () => loadDashboard().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnLoadComplaints")?.addEventListener("click", () => loadComplaints().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnUpdateComplaint")?.addEventListener("click", () => updateSelectedComplaint().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnAddAttachments")?.addEventListener("click", () => uploadDetailAttachments().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDeleteSelectedAttachments")?.addEventListener("click", () => deleteAttachments(false).catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDeleteAllAttachments")?.addEventListener("click", () => deleteAttachments(true).catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnGenerateReport")?.addEventListener("click", () => generateReport().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDigestChat")?.addEventListener("click", () => digestChat().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnLoadOpsDashboard")?.addEventListener("click", () => loadOpsDashboard().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnCreateNotice")?.addEventListener("click", () => createNotice().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnUpdateNotice")?.addEventListener("click", () => updateNotice().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnDeleteNotice")?.addEventListener("click", () => deleteNotice().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnClearNotice")?.addEventListener("click", () => clearNoticeForm());
    $("#btnCreateDocument")?.addEventListener("click", () => createDocument().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnUpdateDocument")?.addEventListener("click", () => updateDocument().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnDeleteDocument")?.addEventListener("click", () => deleteDocument().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnClearDocument")?.addEventListener("click", () => clearDocumentForm());
    $("#btnCreateVendor")?.addEventListener("click", () => createVendor().catch((error) => setMessage("#opsVendorMsg", error.message || String(error), true)));
    $("#btnUpdateVendor")?.addEventListener("click", () => updateVendor().catch((error) => setMessage("#opsVendorMsg", error.message || String(error), true)));
    $("#btnDeleteVendor")?.addEventListener("click", () => deleteVendor().catch((error) => setMessage("#opsVendorMsg", error.message || String(error), true)));
    $("#btnClearVendor")?.addEventListener("click", () => clearVendorForm());
    $("#btnCreateSchedule")?.addEventListener("click", () => createSchedule().catch((error) => setMessage("#opsScheduleMsg", error.message || String(error), true)));
    $("#btnUpdateSchedule")?.addEventListener("click", () => updateSchedule().catch((error) => setMessage("#opsScheduleMsg", error.message || String(error), true)));
    $("#btnDeleteSchedule")?.addEventListener("click", () => deleteSchedule().catch((error) => setMessage("#opsScheduleMsg", error.message || String(error), true)));
    $("#btnClearSchedule")?.addEventListener("click", () => clearScheduleForm());
    $("#btnCreateTenant")?.addEventListener("click", () => createTenant().catch((error) => setMessage("#adminMsg", error.message || String(error), true)));
    $("#btnLoadTenants")?.addEventListener("click", () => loadTenants().catch((error) => setMessage("#adminMsg", error.message || String(error), true)));
    $("#btnLoadUsers")?.addEventListener("click", () => loadUsers().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnCreateUser")?.addEventListener("click", () => createUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnClearUserForm")?.addEventListener("click", () => clearUserCreateForm());
    $("#btnApproveUser")?.addEventListener("click", () => approveUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnUpdateUser")?.addEventListener("click", () => updateUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnResetUserPassword")?.addEventListener("click", () => resetSelectedUserPassword().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnDeleteUser")?.addEventListener("click", () => deleteSelectedUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#tenantSelect")?.addEventListener("change", () => reloadAll().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#photoInput")?.addEventListener("change", () => updatePhotoHint("#photoInput", "#photoHint"));
    $("#chatImageInput")?.addEventListener("change", () => updatePhotoHint("#chatImageInput", "#chatImageHint"));
    $("#attachmentSelectAll")?.addEventListener("change", (event) => {
      const checked = !!event.target.checked;
      document.querySelectorAll(".attachment-check").forEach((el) => {
        el.checked = checked;
      });
    });
  }

  async function init() {
    me = await api("/api/auth/me");
    renderRoleOptions("#newUserRole", "desk");
    renderRoleOptions("#editUserRole", "desk");
    $("#btnApproveUser")?.toggleAttribute("disabled", true);
    applyHero();
    syncUserTenantDisplay();
    syncOpsWriteState();
    clearNoticeForm();
    clearDocumentForm();
    clearScheduleForm();
    clearVendorForm();
    if (isAdmin()) {
      $("#tenantSelectWrap")?.classList.remove("hidden");
      $("#adminPanel")?.classList.remove("hidden");
      $("#newUserSiteAdminWrap")?.classList.remove("hidden");
      $("#editUserSiteAdminWrap")?.classList.remove("hidden");
      await loadTenants();
    }
    if (canManageUsers()) {
      $("#userPanel")?.classList.remove("hidden");
    }
    await reloadAll();
  }

  wire();
  init().catch((error) => {
    setMessage("#intakeMsg", error.message || String(error), true);
  });
})();
