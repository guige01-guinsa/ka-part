(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const CHANNEL_VALUES = ["전화", "카톡", "방문", "앱", "기타"];
  const STATUS_VALUES = ["접수", "처리중", "완료", "이월"];

  let me = null;
  let tenants = [];
  let selectedComplaintId = 0;
  let selectedComplaint = null;
  let lastAiResult = null;

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

  function isAdmin() {
    return !!(me && me.user && me.user.is_admin);
  }

  function currentTenantId() {
    if (isAdmin()) {
      return String($("#tenantSelect")?.value || "").trim();
    }
    return String((me && (me.tenant?.id || me.user?.tenant_id)) || "").trim();
  }

  async function api(url, opts = {}) {
    return window.KAAuth.requestJson(url, opts);
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
    wrap.innerHTML = chips.join("");
  }

  function applyHero() {
    renderTenantBadge();
    const role = isAdmin() ? "최고관리자" : (me?.user?.role || "staff");
    const tenantLabel = me?.tenant?.name || me?.user?.tenant_id || "선택 필요";
    $("#heroLine").textContent = `${role} 계정으로 접속 중입니다. 현재 작업 테넌트는 ${tenantLabel}입니다. 전화, 카톡, 방문 민원을 접수하고 자동 분류할 수 있습니다.`;
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
      await fetch(`/api/complaints/${item.id}/attachments?tenant_id=${encodeURIComponent(payload.tenant_id)}`, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
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
      await fetch(`/api/complaints/${selectedComplaintId}/attachments?tenant_id=${encodeURIComponent(tenantId)}`, {
        method: "POST",
        body: fd,
        credentials: "same-origin",
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
    const data = await api(`/api/report/daily?tenant_id=${encodeURIComponent(tenantId)}`);
    $("#reportBox").textContent = String(data.item?.report_text || "");
  }

  async function digestChat() {
    const tenantId = currentTenantId();
    const text = String($("#chatInput").value || "").trim();
    if (!text) throw new Error("카톡 대화를 붙여 넣으세요.");
    const data = await api("/api/ai/kakao_digest", {
      method: "POST",
      body: JSON.stringify({ tenant_id: tenantId, text }),
    });
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

  async function reloadAll() {
    await loadDashboard();
    await loadComplaints();
    await generateReport();
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
    $("#btnCreateTenant")?.addEventListener("click", () => createTenant().catch((error) => setMessage("#adminMsg", error.message || String(error), true)));
    $("#btnLoadTenants")?.addEventListener("click", () => loadTenants().catch((error) => setMessage("#adminMsg", error.message || String(error), true)));
    $("#tenantSelect")?.addEventListener("change", () => reloadAll().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#photoInput")?.addEventListener("change", () => updatePhotoHint("#photoInput", "#photoHint"));
    $("#attachmentSelectAll")?.addEventListener("change", (event) => {
      const checked = !!event.target.checked;
      document.querySelectorAll(".attachment-check").forEach((el) => {
        el.checked = checked;
      });
    });
  }

  async function init() {
    me = await api("/api/auth/me");
    applyHero();
    if (isAdmin()) {
      $("#tenantSelectWrap")?.classList.remove("hidden");
      $("#adminPanel")?.classList.remove("hidden");
      await loadTenants();
    }
    await reloadAll();
  }

  wire();
  init().catch((error) => {
    setMessage("#intakeMsg", error.message || String(error), true);
  });
})();
