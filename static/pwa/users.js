(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  let users = [];
  let roles = [];
  let permissionLevels = [
    { key: "admin", label: "최고/운영관리자" },
    { key: "site_admin", label: "단지관리자" },
    { key: "user", label: "사용자" },
    { key: "security_guard", label: "보안/경비" },
    { key: "resident", label: "입주민" },
    { key: "board_member", label: "입대의" },
  ];
  let adminScopes = [
    { key: "super_admin", label: "최고관리자" },
    { key: "ops_admin", label: "운영관리자" },
  ];
  const ROLE_BY_PERMISSION = {
    admin: "최고/운영관리자",
    site_admin: "단지관리자",
    user: "사용자",
    security_guard: "보안/경비",
    resident: "입주민",
    board_member: "입대의",
  };
  let editingId = null;
  let recommendedCount = 9;
  let me = null;
  let selfProfile = null;
  let isAdminView = false;
  let isSuperAdminView = false;
  let availableSites = [];
  let availableRegions = [];
  let siteRegistryRequests = [];

  const filterState = {
    active_only: false,
    site_code: "",
    site_name: "",
    region: "",
    keyword: "",
  };

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
  }

  function escapeHtml(v) {
    return String(v)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function setMsg(msg, isErr = false) {
    const el = $("#formMsg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function setSiteReqMsg(msg, isErr = false) {
    const el = $("#siteReqMsg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function permissionLabel(user) {
    const key = String(user.permission_level || (user.is_admin ? "admin" : user.is_site_admin ? "site_admin" : "user"));
    if (key === "admin") {
      const scopeLabel = String(user.admin_scope_label || "").trim();
      if (scopeLabel) return scopeLabel;
    }
    const found = permissionLevels.find((x) => x.key === key);
    return found ? found.label : key;
  }

  function isSuperAdmin(user) {
    if (!user || !user.is_admin) return false;
    return String(user.admin_scope || "").trim().toLowerCase() === "super_admin";
  }

  function isResidentRole(role) {
    const txt = String(role || "").trim();
    return txt === "입주민" || txt === "주민" || txt === "세대주민";
  }

  function permissionFromRole(role) {
    const txt = String(role || "").trim();
    if (!txt) return "user";
    if (txt === "최고/운영관리자" || txt === "최고관리자" || txt === "운영관리자") return "admin";
    if (txt === "단지관리자") return "site_admin";
    if (txt === "보안/경비" || txt.includes("보안") || txt.includes("경비")) return "security_guard";
    if (isResidentRole(txt)) return "resident";
    if (txt === "입대의" || txt === "입주자대표" || txt === "입주자대표회의") return "board_member";
    return "user";
  }

  function roleFromPermission(permissionLevel) {
    const key = String(permissionLevel || "").trim();
    return ROLE_BY_PERMISSION[key] || ROLE_BY_PERMISSION.user;
  }

  function syncRoleByPermission() {
    const perm = String($("#userPermission")?.value || "user").trim();
    const role = roleFromPermission(perm);
    if ($("#userRole")) $("#userRole").value = role;
  }

  function syncPermissionByRole() {
    const role = String($("#userRole")?.value || "").trim();
    const perm = permissionFromRole(role);
    if ($("#userPermission")) $("#userPermission").value = perm;
  }

  function applyMode() {
    isAdminView = !!(me && me.is_admin);
    isSuperAdminView = isSuperAdmin(me);
    $("#userListCard").hidden = !isAdminView;
    $("#siteRegistryReqCard").hidden = !isSuperAdminView;
    $("#selfHint").hidden = isAdminView;
    $("#permissionWrap").hidden = !isAdminView;
    $("#adminScopeWrap").hidden = !(isAdminView && isSuperAdminView);
    $("#activeWrap").hidden = !isAdminView;

    $("#userPermission").disabled = !isAdminView;
    $("#userRole").disabled = !isAdminView;
    $("#isActive").disabled = !isAdminView;
    $("#loginId").readOnly = !isAdminView;
    $("#userSiteCode").readOnly = !isAdminView;
    $("#userSiteName").readOnly = !isAdminView;

    $("#btnReload").textContent = isAdminView ? "새로고침" : "내정보 새로고침";
    $("#btnSaveUser").textContent = isAdminView ? "저장" : "내 정보 저장";
    $("#btnReset").textContent = isAdminView ? "초기화" : "다시 불러오기";
    if (!isAdminView) {
      $("#formTitle").textContent = "내 정보";
    }
    syncAdminScopeVisibility();
  }

  function syncAdminScopeVisibility() {
    const wrap = $("#adminScopeWrap");
    if (!wrap) return;
    const perm = String($("#userPermission")?.value || "user").trim();
    const visible = !!(isAdminView && isSuperAdminView && perm === "admin");
    wrap.hidden = !visible;
  }

  function clearForm() {
    editingId = null;
    $("#formTitle").textContent = isAdminView ? "사용자 등록" : "내 정보";
    $("#loginId").value = "";
    $("#userName").value = "";
    $("#userPhone").value = "";
    $("#userUnitLabel").value = "";
    $("#userSiteCode").value = "";
    $("#userSiteName").value = "";
    $("#userAddress").value = "";
    $("#userOfficePhone").value = "";
    $("#userOfficeFax").value = "";
    $("#userNote").value = "";
    $("#userPassword").value = "";
    $("#userPermission").value = "user";
    syncRoleByPermission();
    $("#adminScope").value = "ops_admin";
    $("#isActive").checked = true;
    syncAdminScopeVisibility();
    setMsg("");
  }

  function fillForm(user) {
    editingId = user.id;
    $("#formTitle").textContent = `사용자 수정 #${user.id}`;
    $("#loginId").value = user.login_id || "";
    $("#userName").value = user.name || "";
    $("#userPhone").value = user.phone || "";
    $("#userUnitLabel").value = user.unit_label || "";
    $("#userSiteCode").value = user.site_code || "";
    $("#userSiteName").value = user.site_name || "";
    $("#userAddress").value = user.address || "";
    $("#userOfficePhone").value = user.office_phone || "";
    $("#userOfficeFax").value = user.office_fax || "";
    $("#userNote").value = user.note || "";
    $("#userPassword").value = "";
    const perm = String(user.permission_level || (user.is_admin ? "admin" : user.is_site_admin ? "site_admin" : "user"));
    $("#userPermission").value = perm;
    $("#adminScope").value = String(user.admin_scope || "ops_admin");
    $("#isActive").checked = !!user.is_active;
    $("#userRole").value = roleFromPermission(perm);
    syncAdminScopeVisibility();
    setMsg("");
  }

  function payloadFromForm() {
    const pw = ($("#userPassword").value || "").trim();
    const permissionLevel = ($("#userPermission").value || "user").trim();
    const payload = {
      login_id: ($("#loginId").value || "").trim(),
      name: ($("#userName").value || "").trim(),
      role: roleFromPermission(permissionLevel),
      unit_label: ($("#userUnitLabel").value || "").trim(),
      phone: ($("#userPhone").value || "").trim(),
      site_code: ($("#userSiteCode").value || "").trim().toUpperCase(),
      site_name: ($("#userSiteName").value || "").trim(),
      address: ($("#userAddress").value || "").trim(),
      office_phone: ($("#userOfficePhone").value || "").trim(),
      office_fax: ($("#userOfficeFax").value || "").trim(),
      note: ($("#userNote").value || "").trim(),
      permission_level: permissionLevel,
      is_active: !!$("#isActive").checked,
    };
    if (payload.permission_level === "admin" && isSuperAdminView) {
      payload.admin_scope = ($("#adminScope").value || "ops_admin").trim();
    }
    if (pw) payload.password = pw;
    return payload;
  }

  function updateMeta() {
    const el = $("#metaLine");
    if (!el) return;

    if (!isAdminView) {
      const user = selfProfile || me || {};
      const name = String(user.name || user.login_id || "사용자");
      const level = permissionLabel(user);
      const siteCode = String(user.site_code || "-");
      const siteName = String(user.site_name || "-");
      el.textContent = `${name} (${level}) · ${siteCode} / ${siteName}`;
      el.classList.remove("warn");
      return;
    }

    const count = users.length;
    const active = users.filter((u) => u.is_active).length;
    const adminCount = users.filter((u) => u.is_admin && u.is_active).length;
    const siteAdminCount = users.filter((u) => !u.is_admin && u.is_site_admin && u.is_active).length;
    const userCount = users.filter((u) => !u.is_admin && !u.is_site_admin && u.is_active).length;
    el.textContent = `조회 ${count}명 (활성 ${active}명 / 관리자 ${adminCount}명 / 단지관리자 ${siteAdminCount}명 / 사용자 ${userCount}명 / 권장 ${recommendedCount}명)`;
    el.classList.toggle("warn", active > recommendedCount);
  }

  function renderFilterSummary() {
    const el = $("#filterSummary");
    if (!el) return;
    const parts = [];
    if (filterState.site_code) parts.push(`단지코드=${filterState.site_code}`);
    if (filterState.site_name) parts.push(`단지명=${filterState.site_name}`);
    if (filterState.region) parts.push(`지역=${filterState.region}`);
    if (filterState.keyword) parts.push(`키워드=${filterState.keyword}`);
    if (filterState.active_only) parts.push("활성만");
    el.textContent = parts.length ? `조회조건: ${parts.join(" / ")}` : "조회조건: 전체";
  }

  function rowHtml(u, idx) {
    const activeText = u.is_active ? "활성" : "비활성";
    const region = String(u.region || "").trim() || "-";
    const canEditRow = isSuperAdminView || !u.is_admin;
    return `
      <tr class="${u.is_active ? "" : "inactive"}" data-id="${u.id}">
        <td class="cell-center">${idx + 1}</td>
        <td>${escapeHtml(permissionLabel(u))}</td>
        <td>${escapeHtml(u.login_id || "")}</td>
        <td>${escapeHtml(u.name || "")}</td>
        <td>${escapeHtml(u.role || "")}</td>
        <td>${escapeHtml(u.phone || "-")}</td>
        <td>${escapeHtml(u.site_code || "-")}</td>
        <td>${escapeHtml(u.site_name || "-")}</td>
        <td>${escapeHtml(u.unit_label || "-")}</td>
        <td>${escapeHtml(region)}</td>
        <td>${escapeHtml(u.office_phone || "-")}</td>
        <td>${escapeHtml(u.office_fax || "-")}</td>
        <td>${escapeHtml(u.address || "-")}</td>
        <td>${activeText}</td>
        <td>
          <div class="cell-actions">
            <button class="btn" data-action="edit" data-id="${u.id}" type="button" ${canEditRow ? "" : "disabled"}>수정</button>
            <button class="btn danger" data-action="delete" data-id="${u.id}" type="button" ${canEditRow ? "" : "disabled"}>삭제</button>
          </div>
        </td>
      </tr>
    `;
  }

  function renderUsersSheet() {
    const body = $("#userSheetBody");
    if (!body) return;
    if (!users.length) {
      body.innerHTML = '<tr><td colspan="15" class="cell-center">조회된 사용자가 없습니다.</td></tr>';
      return;
    }
    body.innerHTML = users.map((u, idx) => rowHtml(u, idx)).join("");
  }

  function updateSiteReqMeta(pendingCount) {
    const el = $("#siteReqMeta");
    if (!el) return;
    const pending = Number(pendingCount || 0);
    const total = Array.isArray(siteRegistryRequests) ? siteRegistryRequests.length : 0;
    el.textContent = `조회 ${total}건 / 대기 ${pending}건`;
  }

  function siteReqRowHtml(item) {
    const status = String(item.status_label || item.status || "-");
    const requester = String(item.requester_name || "-");
    const phone = String(item.requester_phone || "-");
    const role = String(item.requester_role || "-");
    const unit = String(item.requester_unit_label || "-");
    const resultCode = String(item.resolved_site_code || "-");
    const resultName = String(item.resolved_site_name || "");
    const resultText = resultName && resultName !== "-" ? `${resultName} (${resultCode})` : resultCode;
    const canExecute = String(item.status || "").trim().toLowerCase() !== "executed";
    return `
      <tr data-id="${Number(item.id || 0)}">
        <td class="cell-center">${Number(item.id || 0)}</td>
        <td>${escapeHtml(status)}</td>
        <td>${escapeHtml(item.created_at || "-")}</td>
        <td>${escapeHtml(item.site_name || "-")}</td>
        <td>${escapeHtml(item.requested_site_code || "-")}</td>
        <td>${escapeHtml(requester)}</td>
        <td>${escapeHtml(phone)}</td>
        <td>${escapeHtml(`${role} / ${unit}`)}</td>
        <td>${escapeHtml(resultText)}</td>
        <td>
          <div class="cell-actions">
            <button class="btn primary" data-action="execute-site-req" data-id="${Number(item.id || 0)}" type="button" ${canExecute ? "" : "disabled"}>등록 처리</button>
          </div>
        </td>
      </tr>
    `;
  }

  function renderSiteReqSheet() {
    const body = $("#siteReqBody");
    if (!body) return;
    if (!Array.isArray(siteRegistryRequests) || !siteRegistryRequests.length) {
      body.innerHTML = '<tr><td colspan="10" class="cell-center">요청이 없습니다.</td></tr>';
      return;
    }
    body.innerHTML = siteRegistryRequests.map((item) => siteReqRowHtml(item)).join("");
  }

  async function loadSiteRegistryRequests() {
    if (!isSuperAdminView) return;
    const status = String($("#siteReqStatus")?.value || "pending").trim().toLowerCase();
    const qs = new URLSearchParams();
    qs.set("status", status || "pending");
    qs.set("limit", "200");
    const data = await jfetch(`/api/site_registry/requests?${qs.toString()}`);
    siteRegistryRequests = Array.isArray(data.items) ? data.items : [];
    updateSiteReqMeta(data.pending_count || 0);
    renderSiteReqSheet();
  }

  async function executeSiteRegistryRequest(id) {
    const req = siteRegistryRequests.find((x) => Number(x.id) === Number(id));
    if (!req) return;
    const siteName = String(req.site_name || "-");
    const requestedCode = String(req.requested_site_code || "").trim().toUpperCase();
    const ok = confirm(
      requestedCode
        ? `요청 #${id}를 처리하여 '${siteName}' 단지코드 '${requestedCode}'를 등록할까요?`
        : `요청 #${id}를 처리하여 '${siteName}' 단지코드를 등록할까요?`
    );
    if (!ok) return;
    const payload = {};
    if (requestedCode) payload.site_code = requestedCode;
    const data = await jfetch(`/api/site_registry/requests/${Number(id)}/execute`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const resolvedCode = String((data && data.site_code) || "").trim().toUpperCase();
    const lines = [];
    lines.push(resolvedCode ? `요청 #${id} 처리 완료: ${siteName} (${resolvedCode})` : `요청 #${id} 처리 완료`);
    const ready = data && data.signup_ready ? data.signup_ready : null;
    if (ready && ready.notified) {
      const delivery = String(ready.delivery || "sms").trim() || "sms";
      lines.push(`신규가입 안내 발송: ${delivery}`);
      if (ready.setup_url) lines.push(`설정링크 발급됨`);
      if (ready.debug_code) lines.push(`개발용 인증번호: ${String(ready.debug_code)}`);
    } else if (ready && ready.reason) {
      lines.push(`신규가입 안내 미발송: ${String(ready.reason)}`);
    }
    setSiteReqMsg(lines.join(" / "), false);
    await loadSiteRegistryRequests();
  }

  function uniqueSiteNames(sites) {
    const seen = new Set();
    const out = [];
    for (const s of sites) {
      const name = String(s.site_name || "").trim();
      if (!name || seen.has(name)) continue;
      seen.add(name);
      out.push(name);
    }
    return out.sort((a, b) => a.localeCompare(b));
  }

  function setSelectOptions(selectEl, items, currentValue, valueKey, labelBuilder) {
    if (!selectEl) return;
    const options = ['<option value="">전체</option>'];
    for (const item of items) {
      const value = String(item[valueKey] || "").trim();
      if (!value) continue;
      options.push(`<option value="${escapeHtml(value)}">${escapeHtml(labelBuilder(item))}</option>`);
    }
    selectEl.innerHTML = options.join("");
    if (currentValue) selectEl.value = currentValue;
  }

  function syncSiteFilterPairByCode() {
    const code = ($("#filterSiteCode")?.value || "").trim().toUpperCase();
    if (!code) return;
    const hit = availableSites.find((x) => String(x.site_code || "").trim().toUpperCase() === code);
    if (!hit) return;
    const name = String(hit.site_name || "").trim();
    if (name) $("#filterSiteName").value = name;
  }

  function syncSiteFilterPairByName() {
    const name = ($("#filterSiteName")?.value || "").trim();
    if (!name) return;
    const hits = availableSites.filter((x) => String(x.site_name || "").trim() === name);
    if (hits.length !== 1) return;
    const code = String(hits[0].site_code || "").trim().toUpperCase();
    if (code) $("#filterSiteCode").value = code;
  }

  function updateFilterControls() {
    const codeSel = $("#filterSiteCode");
    const nameSel = $("#filterSiteName");
    const regionSel = $("#filterRegion");
    if (!codeSel || !nameSel || !regionSel) return;

    setSelectOptions(
      codeSel,
      availableSites,
      filterState.site_code,
      "site_code",
      (x) => `${x.site_code || ""}${x.site_name ? ` (${x.site_name})` : ""} · ${Number(x.count || 0)}명`
    );

    const siteNames = uniqueSiteNames(availableSites).map((name) => ({ site_name: name }));
    setSelectOptions(nameSel, siteNames, filterState.site_name, "site_name", (x) => String(x.site_name || ""));

    const regions = availableRegions.map((x) => ({ region: String(x.region || "").trim(), count: Number(x.count || 0) }));
    setSelectOptions(regionSel, regions, filterState.region, "region", (x) => `${x.region} · ${x.count}명`);

    $("#filterKeyword").value = filterState.keyword || "";
    $("#filterActiveOnly").checked = !!filterState.active_only;
  }

  function collectFilterStateFromUI() {
    filterState.site_code = ($("#filterSiteCode")?.value || "").trim().toUpperCase();
    filterState.site_name = ($("#filterSiteName")?.value || "").trim();
    filterState.region = ($("#filterRegion")?.value || "").trim();
    filterState.keyword = ($("#filterKeyword")?.value || "").trim();
    filterState.active_only = !!$("#filterActiveOnly")?.checked;
  }

  function resetFilterState() {
    filterState.site_code = "";
    filterState.site_name = "";
    filterState.region = "";
    filterState.keyword = "";
    filterState.active_only = false;
  }

  function buildUserQuery() {
    const qs = new URLSearchParams();
    if (filterState.active_only) qs.set("active_only", "1");
    if (filterState.site_code) qs.set("site_code", filterState.site_code);
    if (filterState.site_name) qs.set("site_name", filterState.site_name);
    if (filterState.region) qs.set("region", filterState.region);
    if (filterState.keyword) qs.set("keyword", filterState.keyword);
    return qs.toString();
  }

  async function loadRoles() {
    const data = await jfetch("/api/user_roles");
    roles = Array.isArray(data.roles) ? data.roles : [];
    permissionLevels = Array.isArray(data.permission_levels) && data.permission_levels.length ? data.permission_levels : permissionLevels;
    adminScopes = Array.isArray(data.admin_scopes) && data.admin_scopes.length ? data.admin_scopes : adminScopes;
    recommendedCount = Number(data.recommended_staff_count || 9);

    const permSel = $("#userPermission");
    permSel.innerHTML = "";
    const allowAdminPermission = isSuperAdmin(me);
    const levelItems = allowAdminPermission ? permissionLevels : permissionLevels.filter((p) => String(p.key || "") !== "admin");
    for (const p of levelItems) {
      const o = document.createElement("option");
      o.value = String(p.key || "");
      o.textContent = String(p.label || p.key || "");
      permSel.appendChild(o);
    }
    if (!allowAdminPermission && permSel.value === "admin") {
      permSel.value = "site_admin";
    }

    const roleSel = $("#userRole");
    roleSel.innerHTML = "";
    const roleItems = Array.from(new Set(levelItems.map((x) => roleFromPermission(String(x.key || "")))));
    const sourceRoles = roleItems.length ? roleItems : roles;
    for (const role of sourceRoles) {
      const o = document.createElement("option");
      o.value = role;
      o.textContent = role;
      roleSel.appendChild(o);
    }
    if (!roleSel.value && sourceRoles.length) roleSel.value = sourceRoles[0];

    const scopeSel = $("#adminScope");
    if (scopeSel) {
      scopeSel.innerHTML = "";
      for (const s of adminScopes) {
        const o = document.createElement("option");
        o.value = String(s.key || "");
        o.textContent = String(s.label || s.key || "");
        scopeSel.appendChild(o);
      }
      if (!scopeSel.value) scopeSel.value = "ops_admin";
    }
    syncRoleByPermission();
    syncAdminScopeVisibility();
  }

  async function loadUsers() {
    if (!isAdminView) return;
    const query = buildUserQuery();
    const data = await jfetch(query ? `/api/users?${query}` : "/api/users");
    users = Array.isArray(data.users) ? data.users : [];
    recommendedCount = Number(data.recommended_staff_count || recommendedCount);

    const f = data && typeof data.filters === "object" ? data.filters : {};
    availableSites = Array.isArray(f.sites) ? f.sites : [];
    availableRegions = Array.isArray(f.regions) ? f.regions : [];

    updateFilterControls();
    updateMeta();
    renderUsersSheet();
    renderFilterSummary();
  }

  async function loadSelfProfile() {
    const data = await jfetch("/api/users/me");
    const user = data && data.user ? data.user : null;
    if (!user) {
      throw new Error("내 정보를 불러오지 못했습니다.");
    }
    selfProfile = user;
    fillForm(user);
    $("#formTitle").textContent = "내 정보";
    updateMeta();
  }

  function payloadFromSelfForm() {
    const pw = ($("#userPassword").value || "").trim();
    const role = roleFromPermission(String((selfProfile && selfProfile.permission_level) || permissionFromRole((selfProfile && selfProfile.role) || "")));
    const payload = {
      name: ($("#userName").value || "").trim(),
      role,
      unit_label: ($("#userUnitLabel").value || "").trim(),
      phone: ($("#userPhone").value || "").trim(),
      address: ($("#userAddress").value || "").trim(),
      office_phone: ($("#userOfficePhone").value || "").trim(),
      office_fax: ($("#userOfficeFax").value || "").trim(),
      note: ($("#userNote").value || "").trim(),
    };
    if (pw) payload.password = pw;
    return payload;
  }

  async function saveAdminUser() {
    const body = payloadFromForm();
    if (!isSuperAdminView && String(body.permission_level || "") === "admin") {
      setMsg("운영관리자는 관리자 권한을 부여할 수 없습니다.", true);
      return;
    }
    if (editingId == null && !body.password) {
      setMsg("신규 사용자는 비밀번호를 입력해야 합니다.", true);
      return;
    }
    if ((String(body.permission_level || "") === "resident" || isResidentRole(body.role)) && !body.unit_label) {
      setMsg("입주민 역할은 동/호를 입력해야 합니다.", true);
      return;
    }
    if (body.password && body.password.length < 8) {
      setMsg("비밀번호는 8자 이상이어야 합니다.", true);
      return;
    }

    if (editingId == null) {
      await jfetch("/api/users", { method: "POST", body: JSON.stringify(body) });
      setMsg("등록했습니다.");
    } else {
      await jfetch(`/api/users/${editingId}`, { method: "PATCH", body: JSON.stringify(body) });
      setMsg("수정했습니다.");
    }

    await loadUsers();
    clearForm();
  }

  async function saveSelfUser() {
    const body = payloadFromSelfForm();
    if (isResidentRole(body.role) && !body.unit_label) {
      setMsg("입주민 역할은 동/호를 입력해야 합니다.", true);
      return;
    }
    if (body.password && body.password.length < 8) {
      setMsg("비밀번호는 8자 이상이어야 합니다.", true);
      return;
    }
    const willRotateSession = !!body.password;
    const data = await jfetch("/api/users/me", { method: "PATCH", body: JSON.stringify(body) });
    selfProfile = data && data.user ? data.user : selfProfile;
    if (selfProfile) fillForm(selfProfile);
    setMsg("내 정보를 수정했습니다.");
    updateMeta();

    if (willRotateSession || (data && data.password_changed)) {
      alert("비밀번호가 변경되어 다시 로그인합니다.");
      await KAAuth.logout("/pwa/users.html");
    }
  }

  async function removeUser(id) {
    const u = users.find((x) => Number(x.id) === Number(id));
    if (!u) return;
    if (me && Number(me.id) === Number(id)) {
      setMsg("현재 로그인한 계정은 삭제할 수 없습니다.", true);
      return;
    }
    const ok = confirm(`사용자 '${u.name}'를 삭제할까요?`);
    if (!ok) return;
    await jfetch(`/api/users/${id}`, { method: "DELETE" });
    if (editingId === Number(id)) clearForm();
    setMsg("삭제했습니다.");
    await loadUsers();
  }

  function wire() {
    $("#btnReload").addEventListener("click", () => {
      if (!isAdminView) {
        loadSelfProfile().catch((e) => setMsg(e.message, true));
        return;
      }
      const jobs = [loadUsers()];
      if (isSuperAdminView) jobs.push(loadSiteRegistryRequests());
      Promise.all(jobs).catch((e) => setMsg(e.message, true));
    });
    $("#btnReset").addEventListener("click", () => {
      if (isAdminView) {
        clearForm();
      } else {
        loadSelfProfile().catch((e) => setMsg(e.message, true));
      }
    });
    $("#btnSaveUser").addEventListener("click", () => {
      const run = isAdminView ? saveAdminUser() : saveSelfUser();
      run.catch((e) => setMsg(e.message, true));
    });
    $("#userPermission")?.addEventListener("change", () => {
      syncRoleByPermission();
      syncAdminScopeVisibility();
    });
    $("#userRole")?.addEventListener("change", () => {
      syncPermissionByRole();
      syncAdminScopeVisibility();
    });

    if (isAdminView) {
      $("#btnApplyFilter").addEventListener("click", () => {
        collectFilterStateFromUI();
        loadUsers().catch((e) => setMsg(e.message, true));
      });

      $("#btnClearFilter").addEventListener("click", () => {
        resetFilterState();
        updateFilterControls();
        loadUsers().catch((e) => setMsg(e.message, true));
      });

      $("#filterSiteCode").addEventListener("change", () => {
        syncSiteFilterPairByCode();
      });

      $("#filterSiteName").addEventListener("change", () => {
        syncSiteFilterPairByName();
      });

      $("#filterKeyword").addEventListener("keydown", (e) => {
        if (e.key !== "Enter") return;
        e.preventDefault();
        collectFilterStateFromUI();
        loadUsers().catch((err) => setMsg(err.message, true));
      });

      if (isSuperAdminView) {
        $("#btnLoadSiteReq")?.addEventListener("click", () => {
          loadSiteRegistryRequests().catch((e) => setSiteReqMsg(e.message || String(e), true));
        });
        $("#siteReqStatus")?.addEventListener("change", () => {
          loadSiteRegistryRequests().catch((e) => setSiteReqMsg(e.message || String(e), true));
        });
      }
    }

    $("#btnLogout").addEventListener("click", () => {
      const run = async () => {
        await KAAuth.logout("/pwa/users.html");
      };
      run().catch(() => {});
    });

    if (isAdminView) {
      $("#userSheetBody").addEventListener("click", (e) => {
        const btn = e.target.closest("button[data-action]");
        if (!btn) return;
        const id = Number(btn.dataset.id);
        if (btn.dataset.action === "edit") {
          const u = users.find((x) => Number(x.id) === id);
          if (u) fillForm(u);
        } else if (btn.dataset.action === "delete") {
          removeUser(id).catch((err) => setMsg(err.message, true));
        }
      });

      if (isSuperAdminView) {
        $("#siteReqBody")?.addEventListener("click", (e) => {
          const btn = e.target.closest("button[data-action='execute-site-req']");
          if (!btn) return;
          const reqId = Number(btn.dataset.id);
          executeSiteRegistryRequest(reqId).catch((err) => setSiteReqMsg(err.message || String(err), true));
        });
      }
    }
  }

  async function init() {
    me = await KAAuth.requireAuth();
    await loadRoles();
    applyMode();
    if (isAdminView) {
      clearForm();
      updateFilterControls();
      await loadUsers();
      if (isSuperAdminView) {
        await loadSiteRegistryRequests();
      }
    } else {
      await loadSelfProfile();
    }
    wire();
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();
