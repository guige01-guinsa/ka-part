(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const PREFERRED_TAB_ORDER = [
    "home",
    "tr1",
    "tr2",
    "tr3",
    "tr4",
    "tr5",
    "tr6",
    "main_vcb",
    "dc_panel",
    "temperature",
    "meter",
    "facility",
    "facility_check",
    "facility_fire",
    "facility_mechanical",
    "facility_telecom",
  ];
  const FALLBACK_ROWS = {
    tr1: [
      ["lv1_L1_V", "lv1_L1_A", "lv1_L1_KW"],
      ["lv1_L2_V", "lv1_L2_A", "lv1_L2_KW"],
      ["lv1_L3_V", "lv1_L3_A", "lv1_L3_KW"],
      ["lv1_temp"],
    ],
    tr2: [
      ["lv2_L1_V", "lv2_L1_A", "lv2_L1_KW"],
      ["lv2_L2_V", "lv2_L2_A", "lv2_L2_KW"],
      ["lv2_L3_V", "lv2_L3_A", "lv2_L3_KW"],
      ["lv2_temp"],
    ],
    tr3: [
      ["lv3_L1_V", "lv3_L1_A", "lv3_L1_KW"],
      ["lv3_L2_V", "lv3_L2_A", "lv3_L2_KW"],
      ["lv3_L3_V", "lv3_L3_A", "lv3_L3_KW"],
      ["lv3_temp"],
    ],
    tr4: [
      ["lv4_L1_V", "lv4_L1_A", "lv4_L1_KW"],
      ["lv4_L2_V", "lv4_L2_A", "lv4_L2_KW"],
      ["lv4_L3_V", "lv4_L3_A", "lv4_L3_KW"],
      ["lv4_temp"],
    ],
    tr5: [
      ["lv5_L1_V", "lv5_L1_A", "lv5_L1_KW"],
      ["lv5_L2_V", "lv5_L2_A", "lv5_L2_KW"],
      ["lv5_L3_V", "lv5_L3_A", "lv5_L3_KW"],
      ["lv5_temp"],
    ],
    tr6: [
      ["lv6_L1_V", "lv6_L1_A", "lv6_L1_KW"],
      ["lv6_L2_V", "lv6_L2_A", "lv6_L2_KW"],
      ["lv6_L3_V", "lv6_L3_A", "lv6_L3_KW"],
      ["lv6_temp"],
    ],
    main_vcb: [
      ["main_vcb_kv", "main_vcb_l1_a", "main_vcb_l2_a", "main_vcb_l3_a"],
    ],
    dc_panel: [
      ["dc_panel_v", "dc_panel_a"],
    ],
    temperature: [
      ["temperature_tr1", "temperature_tr2", "temperature_tr3", "temperature_tr4", "temperature_indoor"],
    ],
    meter: [
      ["AISS_L1_A", "AISS_L2_A", "AISS_L3_A"],
      ["main_kwh", "industry_kwh", "street_kwh"],
    ],
    facility_check: [
      ["tank_level_1", "tank_level_2"],
      ["hydrant_pressure", "sp_pump_pressure"],
      ["high_pressure", "low_pressure"],
      ["office_pressure", "shop_pressure"],
    ],
  };
  const COMPACT_TABS = new Set(["tr1", "tr2", "tr3", "tr4", "tr5", "tr6", "main_vcb", "dc_panel", "temperature", "meter", "facility_check"]);
  const HOME_DRAFT_KEY = "ka_home_draft_v2";
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  const SITE_ID_KEY = "ka_current_site_id_v1";
  const DEFAULT_SITE_NAME = "미지정단지";
  const DEFAULT_WORK_TYPE = "일일";
  const WORK_TYPE_ALIAS_MAP = {
    정기: "일상",
    기타일상: "일상",
    기타: "일상",
  };
  const PUBLIC_ACCESS_ALLOWED_MENU_BUTTON_IDS = new Set(["btnParking", "btnComplaints", "btnLogout"]);
  const PUBLIC_ACCESS_SIGNUP_MESSAGE = "신규가입 후 사용해 주세요.";

  let TABS = [];
  let rangeDates = [];
  let rangeIndex = -1;
  let rangeQueryFrom = "";
  let rangeQueryTo = "";
  let authUser = null;
  let maintenancePollTimer = null;
  let reloginByConflictInProgress = false;
  let siteIdentityRecoverInProgress = false;
  let menuOpen = false;

  function hasAdminPermission(user) {
    return !!(user && user.is_admin);
  }

  function isSuperAdmin(user) {
    if (!user || !user.is_admin) return false;
    return String(user.admin_scope || "").trim().toLowerCase() === "super_admin";
  }

  function canViewSiteIdentity(user) {
    return isSuperAdmin(user);
  }

  function setWrapHiddenByInputSelector(inputSelector, hidden) {
    const el = typeof inputSelector === "string" ? document.querySelector(inputSelector) : null;
    const wrap = el ? (el.closest(".field") || el.closest(".input")) : null;
    if (wrap) wrap.classList.toggle("hidden", !!hidden);
  }

  function applySiteIdentityVisibility() {
    const show = canViewSiteIdentity(authUser);
    setWrapHiddenByInputSelector("#siteName", !show);
    setWrapHiddenByInputSelector("#siteCode", !show);
    // Home tab site identity fields (if present)
    setWrapHiddenByInputSelector("#f-home-complex_name", !show);
    setWrapHiddenByInputSelector("#f-home-complex_code", !show);
  }

  function hasSiteAdminPermission(user) {
    return !!(user && (user.is_admin || user.is_site_admin));
  }

  function permissionLabel(user) {
    const accountType = String((user && user.account_type) || "").trim();
    if (accountType) return accountType;
    if (hasAdminPermission(user)) return isSuperAdmin(user) ? "최고관리자" : "운영관리자";
    if (hasSiteAdminPermission(user)) return "단지대표자";
    return "사용자";
  }

  function roleText(user) {
    return String((user && user.role) || "").trim();
  }

  function permissionLevelText(user) {
    return String((user && user.permission_level) || "").trim().toLowerCase();
  }

  function isResidentRole(user) {
    const level = permissionLevelText(user);
    if (level === "resident") return true;
    const role = roleText(user);
    return role === "입주민" || role === "주민" || role === "세대주민";
  }

  function isBoardRole(user) {
    const level = permissionLevelText(user);
    if (level === "board_member") return true;
    const role = roleText(user);
    return role === "입대의" || role === "입주자대표" || role === "입주자대표회의";
  }

  function isComplaintsOnlyRole(user) {
    return isResidentRole(user) || isBoardRole(user);
  }

  function isSecurityRole(user) {
    const level = permissionLevelText(user);
    if (level === "security_guard") return true;
    const role = roleText(user);
    if (!role) return false;
    const compact = role.replaceAll(" ", "");
    if (compact === "보안/경비") return true;
    return role.includes("보안") || role.includes("경비");
  }

  function isPublicAccessUser(user) {
    if (!user || typeof user !== "object") return false;
    if (user.is_public_access === true) return true;
    const loginId = String(user.login_id || "").trim().toLowerCase();
    return loginId === "public_guest";
  }

  function showPublicAccessSignupNotice() {
    alert(`로그인 없이 사용자는 이 기능을 사용할 수 없습니다.\n${PUBLIC_ACCESS_SIGNUP_MESSAGE}`);
    toast(PUBLIC_ACCESS_SIGNUP_MESSAGE);
  }

  function isPublicAccessMenuActionAllowed(buttonId) {
    const id = String(buttonId || "").trim();
    if (!isPublicAccessUser(authUser)) return true;
    return PUBLIC_ACCESS_ALLOWED_MENU_BUTTON_IDS.has(id);
  }

  function menuDrawerFocusSelector() {
    if (isPublicAccessUser(authUser)) return "#btnParking";
    return canViewSiteIdentity(authUser) ? "#siteName" : "#dateStart";
  }

  function applyPublicAccessMenuPolicy() {
    if (!isPublicAccessUser(authUser)) return;
    const drawer = $("#menuDrawer");
    if (!drawer) return;

    for (const btn of drawer.querySelectorAll(".menu-grid button.btn")) {
      const allow = PUBLIC_ACCESS_ALLOWED_MENU_BUTTON_IDS.has(btn.id || "");
      btn.disabled = !allow;
      btn.style.display = allow ? "" : "none";
      if (allow) btn.removeAttribute("aria-hidden");
      else btn.setAttribute("aria-hidden", "true");
    }

    for (const section of drawer.querySelectorAll(".menu-section")) {
      let visible = true;
      if (section.querySelector("#filtersMountDrawer")) {
        visible = false;
      } else {
        const gridButtons = [...section.querySelectorAll(".menu-grid button.btn")];
        if (gridButtons.length) visible = gridButtons.some((btn) => btn.style.display !== "none");
      }
      section.style.display = visible ? "" : "none";
      if (visible) section.removeAttribute("aria-hidden");
      else section.setAttribute("aria-hidden", "true");
    }

    const saveBtn = $("#btnSave");
    if (saveBtn) {
      saveBtn.title = `로그인 없이 사용자는 저장할 수 없습니다. ${PUBLIC_ACCESS_SIGNUP_MESSAGE}`;
      saveBtn.setAttribute("aria-disabled", "true");
    }
  }

  function toast(msg) {
    const el = $("#toast");
    if (!el) return;
    el.textContent = msg;
    el.classList.add("show");
    clearTimeout(el._t);
    el._t = setTimeout(() => el.classList.remove("show"), 2200);
  }

  function isSiteIdentityConflictMessage(message) {
    const msg = String(message || "").trim();
    if (!msg) return false;
    return (
      msg.includes("site_code is immutable for existing site_name") ||
      msg.includes("site_code already mapped to another site_name") ||
      msg.includes("입력한 site_code가 다른 site_name에 연결되어 있습니다.") ||
      msg.includes("입력한 site_name이 다른 site_code에 연결되어 있습니다.") ||
      msg.includes("입력한 site_code에 매핑된 site_name이 없습니다.")
    );
  }

  async function reloginBySiteIdentityConflict(message) {
    if (reloginByConflictInProgress) return;
    reloginByConflictInProgress = true;
    const detail = String(message || "단지코드/단지명 매핑 충돌").trim();
    alert(`단지코드/단지명 충돌이 발생했습니다.\n${detail}\n보안을 위해 다시 로그인합니다.`);
    try {
      await window.KAAuth.logout("/pwa/");
    } catch (_e) {
      window.KAAuth.clearSession({ includeSensitive: true, broadcast: true });
      window.KAAuth.redirectLogin("/pwa/");
    }
  }

  function rewriteRequestUrlWithCurrentSiteIdentity(rawUrl) {
    const source = String(rawUrl || "").trim();
    if (!source) return source;
    if (source.startsWith("/api/auth/")) return source;
    const isAbsolute = /^https?:\/\//i.test(source);
    let u;
    try {
      u = new URL(source, window.location.origin);
    } catch (_e) {
      return source;
    }
    const hasSiteParams =
      u.searchParams.has("site_id") || u.searchParams.has("site_name") || u.searchParams.has("site_code");
    if (!hasSiteParams) return source;
    const siteName = getSiteNameRaw() || getSiteName();
    const siteCode = getSiteCodeRaw() || getSiteCode();
    const siteId = getSiteId();
    if (siteId > 0) u.searchParams.set("site_id", String(siteId));
    else u.searchParams.delete("site_id");
    if (siteName) u.searchParams.set("site_name", siteName);
    else u.searchParams.delete("site_name");
    if (siteCode) u.searchParams.set("site_code", siteCode);
    else u.searchParams.delete("site_code");
    if (isAbsolute) return u.toString();
    return `${u.pathname}${u.search ? `?${u.searchParams.toString()}` : ""}`;
  }

  async function tryRecoverSiteIdentityConflict() {
    if (siteIdentityRecoverInProgress) return false;
    siteIdentityRecoverInProgress = true;
    try {
      const data = await window.KAAuth.requestJson("/api/site_identity");
      if (!data || !data.ok) return false;
      if (Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
      if (Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(String(data.site_name || "").trim());
      if (Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
      enforceSiteIdentityPolicy();
      enforceHomeSiteIdentityPolicy();
      updateAddressBarSiteQuery();
      return true;
    } catch (_e) {
      return false;
    } finally {
      siteIdentityRecoverInProgress = false;
    }
  }

  function setTabRunStatus(tabTitle = "", isRunning = false) {
    const el = $("#tabRunStatus");
    if (!el) return;
    if (isRunning && tabTitle) {
      el.textContent = `실행여부: ${tabTitle} 실행중`;
      el.classList.add("running");
      el.classList.remove("idle");
      return;
    }
    el.textContent = "실행여부: 대기";
    el.classList.remove("running");
    el.classList.add("idle");
  }

  function isNarrowViewport() {
    return window.matchMedia("(max-width: 760px)").matches;
  }

  function updateContextLine() {
    const el = $("#contextLine");
    if (!el) return;
    const showSite = canViewSiteIdentity(authUser);
    const siteName = showSite ? String(getSiteNameRaw() || getSiteName() || "").trim() : "";
    const siteCode = showSite ? String(getSiteCodeRaw() || getSiteCode() || "").trim().toUpperCase() : "";
    const siteText = showSite ? ([siteCode, siteName].filter(Boolean).join(" / ") || "-") : "(숨김)";
    const displayDate = getDateStart() || "-";
    const from = rangeQueryFrom || getDateStart();
    const to = rangeQueryTo || getDateEnd();
    const rangeText = from && to ? (from === to ? from : `${from}~${to}`) : (from || to || "-");
    const workType = getSelectedWorkType() || "-";
    el.textContent = `단지: ${siteText} · 업무구분: ${workType} · 표시: ${displayDate} · 범위: ${rangeText}`;
  }

  function relocateFiltersBlock() {
    const block = $("#filtersBlock");
    const homeMount = $("#filtersMountHome");
    const drawerMount = $("#filtersMountDrawer");
    if (!block || !homeMount || !drawerMount) return;
    const target = isNarrowViewport() ? drawerMount : homeMount;
    if (block.parentElement !== target) {
      target.appendChild(block);
      syncStickyOffset();
    }
  }

  function applyMenuState({ focusSelector = "" } = {}) {
    const btn = $("#btnMenu");
    const overlay = $("#menuOverlay");
    const drawer = $("#menuDrawer");
    if (!btn || !overlay || !drawer) return;

    if (menuOpen) {
      overlay.classList.remove("hidden");
      overlay.setAttribute("aria-hidden", "false");
      drawer.hidden = false;
      btn.setAttribute("aria-expanded", "true");
      document.body.classList.add("menu-open");
      relocateFiltersBlock();

      try {
        let focusEl = null;
        if (focusSelector) {
          const desired = document.querySelector(focusSelector);
          if (desired && drawer.contains(desired)) focusEl = desired;
        }
        if (!focusEl) {
          focusEl = drawer.querySelector("input, select, textarea, button, a[href]");
        }
        if (focusEl && typeof focusEl.focus === "function") focusEl.focus();
      } catch (_e) {}
    } else {
      overlay.classList.add("hidden");
      overlay.setAttribute("aria-hidden", "true");
      drawer.hidden = true;
      btn.setAttribute("aria-expanded", "false");
      document.body.classList.remove("menu-open");
    }
    syncStickyOffset();
  }

  function openMenu(opts = {}) {
    menuOpen = true;
    applyMenuState(opts);
  }

  function closeMenu() {
    menuOpen = false;
    applyMenuState();
  }

  function wireMenuDrawer() {
    const btn = $("#btnMenu");
    const overlay = $("#menuOverlay");
    const drawer = $("#menuDrawer");
    const closeBtn = $("#btnCloseMenu");
    if (!btn || !overlay || !drawer) return;

    btn.addEventListener("click", () => {
      if (menuOpen) closeMenu();
      else openMenu({ focusSelector: menuDrawerFocusSelector() });
    });
    closeBtn?.addEventListener("click", () => closeMenu());
    overlay.addEventListener("click", () => closeMenu());

    drawer.addEventListener(
      "click",
      (e) => {
        const actionBtn = e.target.closest(".menu-grid button.btn");
        if (!actionBtn) return;
        if (isPublicAccessMenuActionAllowed(actionBtn.id)) return;
        e.preventDefault();
        e.stopPropagation();
        if (typeof e.stopImmediatePropagation === "function") e.stopImmediatePropagation();
        showPublicAccessSignupNotice();
      },
      true
    );

    drawer.addEventListener("click", (e) => {
      const actionBtn = e.target.closest(".menu-grid button.btn");
      if (!actionBtn) return;
      requestAnimationFrame(() => closeMenu());
    });

    document.addEventListener("keydown", (e) => {
      if (!menuOpen) return;
      if (e.key === "Escape") {
        e.preventDefault();
        closeMenu();
      }
    });

    $("#contextLine")?.addEventListener("click", () => openMenu({ focusSelector: menuDrawerFocusSelector() }));

    relocateFiltersBlock();
    updateContextLine();
  }

  function ymdToday() {
    const d = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  }

  async function jfetch(url, opts = {}) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    try {
      return await window.KAAuth.requestJson(url, opts);
    } catch (err) {
      const msg = err && err.message ? String(err.message) : String(err || "");
      if (msg.includes("invalid or expired session") || msg === "401") {
        try {
          window.KAAuth.clearSession();
        } catch (_e) {}
        window.KAAuth.redirectLogin("/pwa/");
        throw new Error("로그인이 필요합니다.");
      }
      if (isSiteIdentityConflictMessage(msg)) {
        const recovered = await tryRecoverSiteIdentityConflict();
        if (recovered) {
          const retryUrl = rewriteRequestUrlWithCurrentSiteIdentity(url);
          try {
            return await window.KAAuth.requestJson(retryUrl, opts);
          } catch (retryErr) {
            const retryMsg = retryErr && retryErr.message ? String(retryErr.message) : String(retryErr || "");
            if (!isSiteIdentityConflictMessage(retryMsg)) {
              throw retryErr;
            }
          }
        }
        await reloginBySiteIdentityConflict(msg);
        throw new Error("단지코드/단지명 충돌로 로그아웃되었습니다.");
      }
      throw err;
    }
  }

  function setCurrentUserChip(user) {
    const chip = $("#currentUser");
    if (!chip) return;
    if (!user) {
      chip.textContent = "미로그인";
      return;
    }
    const role = permissionLabel(user);
    chip.textContent = `${user.name || user.login_id} (${role})`;
  }

  function normalizeSiteId(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    const id = Math.trunc(n);
    return id > 0 ? id : 0;
  }

  function assignedSiteIdForUser() {
    if (!authUser || hasAdminPermission(authUser)) return 0;
    return normalizeSiteId(authUser.site_id);
  }

  function assignedSiteNameForUser() {
    if (!authUser || hasAdminPermission(authUser)) return "";
    return String(authUser.site_name || "").trim();
  }

  function assignedSiteCodeForUser() {
    if (!authUser || hasAdminPermission(authUser)) return "";
    return String(authUser.site_code || "").trim().toUpperCase();
  }

  function enforceSiteIdentityPolicy() {
    const nameEl = $("#siteName");
    const codeEl = $("#siteCode");
    if ((!nameEl && !codeEl) || !authUser) return;
    if (hasAdminPermission(authUser)) {
      if (nameEl) {
        nameEl.readOnly = false;
        nameEl.removeAttribute("aria-readonly");
        nameEl.title = "";
      }
      if (codeEl) {
        if (isSuperAdmin(authUser)) {
          codeEl.readOnly = false;
          codeEl.removeAttribute("aria-readonly");
          codeEl.title = "";
        } else {
          codeEl.readOnly = true;
          codeEl.setAttribute("aria-readonly", "true");
          codeEl.title = "단지코드 생성/변경은 최고관리자만 가능합니다.";
        }
      }
      return;
    }
    const assignedName = assignedSiteNameForUser();
    if (!assignedName) {
      throw new Error("계정에 소속 단지가 지정되지 않았습니다. 관리자에게 문의하세요.");
    }
    const assignedCode = assignedSiteCodeForUser();
    const assignedId = assignedSiteIdForUser();
    setSiteId(assignedId);
    setSiteName(assignedName);
    setSiteCode(assignedCode);
    if (nameEl) {
      nameEl.readOnly = true;
      nameEl.setAttribute("aria-readonly", "true");
      nameEl.title = "단지명 입력/수정은 관리자만 가능합니다.";
    }
    if (codeEl) {
      codeEl.readOnly = true;
      codeEl.setAttribute("aria-readonly", "true");
      codeEl.title = "단지코드 입력/수정은 관리자만 가능합니다.";
    }
    applySiteIdentityVisibility();
  }

  async function ensureAuth() {
    authUser = await window.KAAuth.requireAuth();
    if (isComplaintsOnlyRole(authUser)) {
      window.location.href = "/pwa/complaints.html";
      throw new Error("모듈 전환 중");
    }
    if (isSecurityRole(authUser)) {
      window.location.href = "/parking/admin2";
      throw new Error("모듈 전환 중");
    }
    setCurrentUserChip(authUser);
    const btnUsers = $("#btnUsers");
    if (btnUsers && !hasAdminPermission(authUser)) btnUsers.style.display = "none";
    const btnSpec = $("#btnSpecEnv");
    if (btnSpec && !hasSiteAdminPermission(authUser)) btnSpec.style.display = "none";
    const btnApt = $("#btnApartmentInfo");
    if (btnApt && !hasSiteAdminPermission(authUser)) btnApt.style.display = "none";
    const btnBackup = $("#btnBackup");
    if (btnBackup && !hasSiteAdminPermission(authUser)) btnBackup.style.display = "none";
    applyPublicAccessMenuPolicy();
    setSiteId(normalizeSiteId(authUser && authUser.site_id));
    enforceSiteIdentityPolicy();
    const assignedCode = assignedSiteCodeForUser();
    if (assignedCode) setSiteCode(assignedCode);
    applySiteIdentityVisibility();
  }

  function parseFilename(contentDisposition, fallback) {
    const value = String(contentDisposition || "");
    const utf8 = /filename\*=UTF-8''([^;]+)/i.exec(value);
    if (utf8 && utf8[1]) {
      try {
        return decodeURIComponent(utf8[1]);
      } catch (_e) {}
    }
    const plain = /filename=\"?([^\";]+)\"?/i.exec(value);
    if (plain && plain[1]) return plain[1];
    return fallback;
  }

  async function downloadWithAuth(url, fallbackName) {
    const token = window.KAAuth.getToken();
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const res = await fetch(url, {
      headers,
    });
    if (res.status === 401) {
      window.KAAuth.clearSession();
      window.KAAuth.redirectLogin();
      return;
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText} ${txt}`.trim());
    }
    const blob = await res.blob();
    const filename = parseFilename(res.headers.get("content-disposition"), fallbackName);
    const href = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = href;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(href);
  }

  function getSiteName() {
    const el = $("#siteName");
    return (el && el.value ? el.value : "").trim() || DEFAULT_SITE_NAME;
  }

  function getSiteNameRaw() {
    const el = $("#siteName");
    return (el && el.value ? el.value : "").trim();
  }

  function getSiteCodeRaw() {
    const el = $("#siteCode");
    return (el && el.value ? el.value : "").trim().toUpperCase();
  }

  function getSiteCode() {
    const inputCode = getSiteCodeRaw();
    if (inputCode) return inputCode;
    return (localStorage.getItem(SITE_CODE_KEY) || "").trim().toUpperCase();
  }

  function getSiteIdRaw() {
    return normalizeSiteId(localStorage.getItem(SITE_ID_KEY) || "");
  }

  function getSiteId() {
    const assigned = assignedSiteIdForUser();
    if (assigned > 0) return assigned;
    return getSiteIdRaw();
  }

  function syncHomeSiteIdentityDisplay(name = null, code = null) {
    const nameEl = document.getElementById("f-home-complex_name");
    const codeEl = document.getElementById("f-home-complex_code");
    const nextName = name === null ? getSiteNameRaw() || getSiteName() : String(name || "").trim();
    const nextCode = code === null ? getSiteCode() : String(code || "").trim().toUpperCase();
    if (nameEl) nameEl.value = nextName;
    if (codeEl) codeEl.value = nextCode;
  }

  function resolveSiteNameForSave() {
    return getSiteNameRaw();
  }

  function setSiteName(name) {
    const clean = (name || "").trim() || DEFAULT_SITE_NAME;
    const el = $("#siteName");
    if (el) el.value = clean;
    localStorage.setItem(SITE_NAME_KEY, clean);
    updateContextLine();
    return clean;
  }

  function setSiteCode(code) {
    const clean = (code || "").trim().toUpperCase();
    const el = $("#siteCode");
    if (el) el.value = clean;
    if (clean) localStorage.setItem(SITE_CODE_KEY, clean);
    else localStorage.removeItem(SITE_CODE_KEY);
    syncHomeSiteIdentityDisplay(null, clean);
    updateContextLine();
    return clean;
  }

  function setSiteId(siteId) {
    const clean = normalizeSiteId(siteId);
    if (clean > 0) localStorage.setItem(SITE_ID_KEY, String(clean));
    else localStorage.removeItem(SITE_ID_KEY);
    return clean;
  }

  function buildSiteQuery(siteName, siteCode, siteId = null) {
    const qs = new URLSearchParams();
    const s = String(siteName || "").trim();
    const c = String(siteCode || "").trim().toUpperCase();
    const i = normalizeSiteId(siteId === null ? getSiteId() : siteId);
    if (i > 0) qs.set("site_id", String(i));
    if (s) qs.set("site_name", s);
    if (c) qs.set("site_code", c);
    return qs.toString();
  }

  function resolveSiteName() {
    const assigned = assignedSiteNameForUser();
    if (assigned) return setSiteName(assigned);
    const u = new URL(window.location.href);
    const q = (u.searchParams.get("site_name") || u.searchParams.get("site") || "").trim();
    if (q) return setSiteName(q);
    const stored = (localStorage.getItem(SITE_NAME_KEY) || "").trim();
    if (stored) return setSiteName(stored);
    return setSiteName(DEFAULT_SITE_NAME);
  }

  function resolveSiteCode() {
    const assigned = assignedSiteCodeForUser();
    if (assigned) return setSiteCode(assigned);
    const u = new URL(window.location.href);
    const q = (u.searchParams.get("site_code") || "").trim().toUpperCase();
    if (q) return setSiteCode(q);
    const stored = (localStorage.getItem(SITE_CODE_KEY) || "").trim().toUpperCase();
    if (stored) return setSiteCode(stored);
    return setSiteCode("");
  }

  function resolveSiteId() {
    const assigned = assignedSiteIdForUser();
    if (assigned > 0) return setSiteId(assigned);
    const u = new URL(window.location.href);
    const q = normalizeSiteId(u.searchParams.get("site_id") || "");
    if (q > 0) return setSiteId(q);
    const stored = normalizeSiteId(localStorage.getItem(SITE_ID_KEY) || "");
    if (stored > 0) return setSiteId(stored);
    return setSiteId(0);
  }

  function updateAddressBarSiteQuery() {
    const u = new URL(window.location.href);
    const showSite = canViewSiteIdentity(authUser);
    const siteName = getSiteNameRaw();
    const siteCode = getSiteCodeRaw();
    const siteId = getSiteId();
    if (siteId > 0) u.searchParams.set("site_id", String(siteId));
    else u.searchParams.delete("site_id");
    if (showSite && siteName) u.searchParams.set("site_name", siteName);
    else u.searchParams.delete("site_name");
    if (showSite && siteCode) u.searchParams.set("site_code", siteCode);
    else u.searchParams.delete("site_code");
    const next = `${u.pathname}${u.search ? `?${u.searchParams.toString()}` : ""}`;
    window.history.replaceState({}, "", next);
  }

  function enforceHomeSiteIdentityPolicy() {
    const homeName = document.getElementById("f-home-complex_name");
    const homeCode = document.getElementById("f-home-complex_code");
    if (!homeName && !homeCode) return;
    const canEdit = !!(authUser && hasAdminPermission(authUser));
    if (homeName) {
      homeName.readOnly = true;
      homeName.setAttribute("aria-readonly", "true");
      homeName.title = canEdit ? "단지명은 상단 입력창에서 변경됩니다." : "단지명 입력/수정은 관리자만 가능합니다.";
    }
    if (homeCode) {
      homeCode.readOnly = true;
      homeCode.setAttribute("aria-readonly", "true");
      homeCode.title = canEdit ? "단지코드는 상단 입력창에서 변경됩니다." : "단지코드 입력/수정은 관리자만 가능합니다.";
    }
    syncHomeSiteIdentityDisplay();
    applySiteIdentityVisibility();
  }

  async function syncSiteIdentity({ requireInput = true } = {}) {
    const currentSite = getSiteNameRaw();
    const currentCode = getSiteCodeRaw();
    const currentId = getSiteId();
    if (requireInput && !currentSite && !currentCode && currentId <= 0) {
      throw new Error("단지명 또는 단지코드를 입력하세요.");
    }
    const qs = buildSiteQuery(currentSite, currentCode, currentId);
    const data = await jfetch(`/api/site_identity?${qs}`);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(String(data.site_name || "").trim());
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    enforceSiteIdentityPolicy();
    enforceHomeSiteIdentityPolicy();
    updateAddressBarSiteQuery();
    updateContextLine();
    return data || {};
  }

  function getDateStart() {
    const el = $("#dateStart");
    return el && el.value ? el.value : ymdToday();
  }

  function getDateEnd() {
    const el = $("#dateEnd");
    return el && el.value ? el.value : getDateStart();
  }

  function getPickedDate() {
    return getDateStart();
  }

  function normalizeWorkTypeValue(value, fallback = DEFAULT_WORK_TYPE) {
    const raw = String(value || "").trim();
    const mapped = WORK_TYPE_ALIAS_MAP[raw] || raw;
    const fallbackRaw = String(fallback || "").trim();
    const fallbackMapped = WORK_TYPE_ALIAS_MAP[fallbackRaw] || fallbackRaw;
    const select = document.getElementById("f-home-work_type");
    if (!select) return mapped || fallbackMapped || DEFAULT_WORK_TYPE;

    const allowed = new Set(
      Array.from(select.options || [])
        .map((opt) => String(opt.value || "").trim())
        .filter(Boolean)
    );
    if (mapped && allowed.has(mapped)) return mapped;
    if (fallbackMapped && allowed.has(fallbackMapped)) return fallbackMapped;
    const first = Array.from(allowed)[0];
    return first || DEFAULT_WORK_TYPE;
  }

  function getSelectedWorkType() {
    const el = document.getElementById("f-home-work_type");
    const raw = el && typeof el.value === "string" ? el.value : "";
    return normalizeWorkTypeValue(raw, DEFAULT_WORK_TYPE);
  }

  function appendWorkTypeQuery(urlOrPath) {
    const source = String(urlOrPath || "").trim();
    if (!source) return source;
    const wt = getSelectedWorkType();
    if (!wt) return source;
    try {
      const u = new URL(source, window.location.origin);
      u.searchParams.set("work_type", wt);
      return /^https?:\/\//i.test(source) ? u.toString() : `${u.pathname}${u.search ? `?${u.searchParams.toString()}` : ""}`;
    } catch (_e) {
      return source;
    }
  }

  function sortSchemaKeys(keys) {
    return [...keys].sort((a, b) => {
      const ia = PREFERRED_TAB_ORDER.indexOf(a);
      const ib = PREFERRED_TAB_ORDER.indexOf(b);
      const va = ia >= 0 ? ia : PREFERRED_TAB_ORDER.length + 100;
      const vb = ib >= 0 ? ib : PREFERRED_TAB_ORDER.length + 100;
      return va - vb || a.localeCompare(b);
    });
  }

  function normalizeTabsFromSchema(schema) {
    if (!schema || typeof schema !== "object") return [];
    const keys = sortSchemaKeys(Object.keys(schema));
    const tabs = [];
    for (const key of keys) {
      const def = schema[key];
      if (!def || !Array.isArray(def.fields)) continue;
      const fields = def.fields.filter((f) => f && String(f.k || "").trim());
      if (!fields.length) continue;
      tabs.push({
        key,
        title: def.title || key,
        fields,
        rows: Array.isArray(def.rows) ? def.rows : null,
      });
    }
    return tabs;
  }

  function chunk(arr, n) {
    const out = [];
    for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
    return out;
  }

  function inferRows(tab) {
    if (Array.isArray(tab.rows) && tab.rows.length) return tab.rows;
    const fallback = FALLBACK_ROWS[tab.key];
    if (fallback) return fallback;
    const keys = tab.fields.map((f) => f.k);
    const allNumeric = tab.fields.every((f) => f.type === "number");
    if (COMPACT_TABS.has(tab.key) || (allNumeric && keys.length >= 6)) return chunk(keys, 3);
    if (keys.length >= 4) return chunk(keys, 2);
    return keys.map((k) => [k]);
  }

  function shortLabel(tabKey, field) {
    if (["tr1", "tr2", "tr3", "tr4", "tr5", "tr6"].includes(tabKey)) {
      const custom = String(field.label || "").trim();
      const normalized = custom.toLowerCase();
      const defaultLike = new Set(["v", "a", "kw", "온도", "온도(℃)", "온도(°c)"]);
      if (custom && !defaultLike.has(normalized)) return custom;
      const m = /_(L[1-3])_/.exec(field.k || "");
      const phase = m ? `${m[1]}-` : "";
      if (field.k.endsWith("_V")) return `${phase}V`;
      if (field.k.endsWith("_A")) return `${phase}A`;
      if (field.k.endsWith("_KW")) return `${phase}KW`;
      if (field.k.endsWith("_temp")) return "온도";
    }
    return field.label || field.k;
  }

  function clearWarn(wrap) {
    wrap.classList.remove("warn");
    const msg = wrap.querySelector(".warnmsg");
    if (msg) msg.remove();
  }

  function setWarn(wrap, text) {
    wrap.classList.add("warn");
    let msg = wrap.querySelector(".warnmsg");
    if (!msg) {
      msg = document.createElement("div");
      msg.className = "warnmsg";
      wrap.appendChild(msg);
    }
    msg.textContent = text || "범위 벗어남";
  }

  function validateFieldValue(wrap, input) {
    const wmin = input.dataset.warnMin;
    const wmax = input.dataset.warnMax;
    if (wmin === undefined && wmax === undefined) return;
    const raw = (input.value || "").trim();
    if (!raw) {
      clearWarn(wrap);
      return;
    }
    const v = Number(raw);
    if (Number.isNaN(v)) {
      clearWarn(wrap);
      return;
    }
    const minv = wmin !== undefined && wmin !== "" ? Number(wmin) : null;
    const maxv = wmax !== undefined && wmax !== "" ? Number(wmax) : null;
    const bad = (minv !== null && v < minv) || (maxv !== null && v > maxv);
    if (!bad) {
      clearWarn(wrap);
      return;
    }
    const t =
      minv !== null && maxv !== null
        ? `${minv}~${maxv}`
        : minv !== null
        ? `>= ${minv}`
        : maxv !== null
        ? `<= ${maxv}`
        : "범위";
    setWarn(wrap, `허용범위: ${t}`);
  }

  function countWarnings() {
    return document.querySelectorAll(".input.warn").length;
  }

  function createFieldWrap(tabKey, field) {
    const wrap = document.createElement("div");
    wrap.className = COMPACT_TABS.has(tabKey) ? "input compact" : "input";
    wrap.dataset.tab = tabKey;
    wrap.dataset.field = field.k;

    const lab = document.createElement("label");
    lab.textContent = shortLabel(tabKey, field);
    wrap.appendChild(lab);

    let input;
    if (field.type === "textarea") {
      input = document.createElement("textarea");
      input.placeholder = field.placeholder || "";
    } else if (field.type === "select") {
      input = document.createElement("select");
      const opts = Array.isArray(field.options) ? field.options : [];
      for (const opt of opts) {
        const o = document.createElement("option");
        o.value = opt;
        o.textContent = opt;
        input.appendChild(o);
      }
    } else {
      input = document.createElement("input");
      input.type = field.type || "text";
      input.placeholder = field.placeholder || "";
      if (field.type === "number") {
        input.inputMode = "decimal";
        if (field.step !== undefined) input.step = String(field.step);
        if (field.min !== undefined) input.min = String(field.min);
        if (field.max !== undefined) input.max = String(field.max);
      }
    }

    input.id = `f-${tabKey}-${field.k}`;
    if (field.readonly) {
      if (input instanceof HTMLSelectElement) {
        input.disabled = true;
      } else {
        input.readOnly = true;
        input.setAttribute("aria-readonly", "true");
      }
    }
    if (field.warn_min !== undefined) input.dataset.warnMin = String(field.warn_min);
    if (field.warn_max !== undefined) input.dataset.warnMax = String(field.warn_max);
    input.addEventListener("input", () => validateFieldValue(wrap, input));
    input.addEventListener("change", () => validateFieldValue(wrap, input));
    wrap.appendChild(input);
    setTimeout(() => validateFieldValue(wrap, input), 0);
    return wrap;
  }

  function render() {
    const tabsEl = $("#tabs");
    const panelsEl = $("#panels");
    if (!tabsEl || !panelsEl) {
      alert("앱 초기화 오류: 화면 요소를 찾지 못했습니다.");
      return;
    }
    tabsEl.innerHTML = "";
    panelsEl.innerHTML = "";

    for (const tab of TABS) {
      const b = document.createElement("button");
      b.className = "tabbtn";
      b.type = "button";
      b.dataset.tab = tab.key;
      b.textContent = tab.title;
      tabsEl.appendChild(b);

      const p = document.createElement("section");
      p.className = "panel";
      p.id = `panel-${tab.key}`;
      const h = document.createElement("h2");
      h.textContent = tab.title;
      p.appendChild(h);

      const grid = document.createElement("div");
      grid.className = "grid";
      const byKey = {};
      for (const f of tab.fields) byKey[f.k] = f;

      for (const rowKeys of inferRows(tab)) {
        const row = document.createElement("div");
        row.className = "field-row";
        row.dataset.count = String(rowKeys.length);
        for (const k of rowKeys) {
          if (byKey[k]) row.appendChild(createFieldWrap(tab.key, byKey[k]));
        }
        if (row.childElementCount) grid.appendChild(row);
      }

      p.appendChild(grid);
      panelsEl.appendChild(p);
    }

    if (TABS.length) activateTab(TABS[0].key);
    else setTabRunStatus("", false);
  }

  function activateTab(tabKey, announce = false) {
    let activatedTitle = "";
    for (const b of document.querySelectorAll(".tabbtn")) {
      const isActive = b.dataset.tab === tabKey;
      b.classList.toggle("active", isActive);
      b.setAttribute("aria-pressed", isActive ? "true" : "false");
      if (isActive) activatedTitle = String(b.textContent || tabKey);
    }
    for (const p of document.querySelectorAll(".panel")) {
      p.classList.toggle("active", p.id === `panel-${tabKey}`);
    }
    if (activatedTitle) setTabRunStatus(activatedTitle, true);
    else setTabRunStatus("", false);
    if (announce && activatedTitle) {
      toast(`탭 실행: ${activatedTitle}`);
    }
  }

  function collectAllTabs() {
    const out = {};
    for (const tab of TABS) {
      out[tab.key] = {};
      for (const field of tab.fields) {
        const el = document.getElementById(`f-${tab.key}-${field.k}`);
        out[tab.key][field.k] = el ? el.value ?? "" : "";
      }
    }
    return out;
  }

  function fillTabs(tabs) {
    const keepWorkType = getSelectedWorkType() || DEFAULT_WORK_TYPE;
    for (const tab of TABS) {
      const values = tabs && tabs[tab.key] ? tabs[tab.key] : {};
      for (const field of tab.fields) {
        const el = document.getElementById(`f-${tab.key}-${field.k}`);
        if (!el) continue;
        if (tab.key === "home" && field.k === "work_type") {
          el.value = normalizeWorkTypeValue(values[field.k], keepWorkType);
          continue;
        }
        el.value = values[field.k] ?? "";
      }
    }
    syncHomeSiteIdentityDisplay();
    enforceHomeSiteIdentityPolicy();
  }

  function getActivePanel() {
    return document.querySelector(".panel.active");
  }

  function listFocusableInPanel(panel) {
    if (!panel) return [];
    const els = Array.from(panel.querySelectorAll("input, select, textarea"));
    return els.filter((el) => !el.disabled && el.offsetParent !== null);
  }

  function focusNextInActive(current) {
    const panel = getActivePanel();
    const els = listFocusableInPanel(panel);
    const idx = els.indexOf(current);
    if (idx >= 0 && idx < els.length - 1) {
      const next = els[idx + 1];
      next.focus();
      if (next.tagName === "INPUT" || next.tagName === "TEXTAREA") {
        try {
          next.select?.();
        } catch (_e) {}
      }
    }
  }

  function initAutoAdvance() {
    document.addEventListener(
      "keydown",
      (e) => {
        const panel = getActivePanel();
        if (!panel) return;
        const t = e.target;
        if (!panel.contains(t)) return;
        if (e.key === "Enter") {
          if (t && t.tagName === "TEXTAREA") return;
          e.preventDefault();
          focusNextInActive(t);
        }
      },
      true
    );
  }

  function saveHomeDraft() {
    try {
      const home = TABS.find((t) => t.key === "home");
      if (!home) return;
      const obj = {};
      for (const f of home.fields) {
        if (f.readonly) continue;
        const el = document.getElementById(`f-home-${f.k}`);
        if (!el) continue;
        if (f.k === "work_type") obj[f.k] = normalizeWorkTypeValue(el.value, DEFAULT_WORK_TYPE);
        else obj[f.k] = el.value ?? "";
      }
      localStorage.setItem(HOME_DRAFT_KEY, JSON.stringify(obj));
    } catch (_e) {}
  }

  function loadHomeDraft() {
    try {
      const raw = localStorage.getItem(HOME_DRAFT_KEY);
      if (!raw) return null;
      const obj = JSON.parse(raw);
      return obj && typeof obj === "object" ? obj : null;
    } catch (_e) {
      return null;
    }
  }

  function applyHomeDraft(obj) {
    if (!obj) return;
    const home = TABS.find((t) => t.key === "home");
    if (!home) return;
    for (const f of home.fields) {
      if (f.readonly) continue;
      const el = document.getElementById(`f-home-${f.k}`);
      if (!el || obj[f.k] === undefined) continue;
      if (f.k === "work_type") el.value = normalizeWorkTypeValue(obj[f.k], getSelectedWorkType() || DEFAULT_WORK_TYPE);
      else el.value = obj[f.k];
    }
    syncHomeSiteIdentityDisplay();
    enforceHomeSiteIdentityPolicy();
  }

  function clearHomeDraft() {
    try {
      localStorage.removeItem(HOME_DRAFT_KEY);
    } catch (_e) {}
    const home = TABS.find((t) => t.key === "home");
    if (!home) return;
    for (const f of home.fields) {
      if (f.readonly) continue;
      const el = document.getElementById(`f-home-${f.k}`);
      if (!el) continue;
      if (f.k === "work_type") el.value = normalizeWorkTypeValue("", DEFAULT_WORK_TYPE);
      else el.value = "";
    }
    syncHomeSiteIdentityDisplay();
    enforceHomeSiteIdentityPolicy();
  }

  async function loadOne(site, date, siteCode = "") {
    const qs = buildSiteQuery(site, siteCode || getSiteCode());
    const url = appendWorkTypeQuery(`/api/load?${qs}&date=${encodeURIComponent(date)}`);
    const data = await jfetch(url);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(String(data.site_name || "").trim());
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    fillTabs(data.tabs || {});
  }

  async function loadRange() {
    const site = getSiteNameRaw() || getSiteName();
    const siteCode = getSiteCodeRaw() || getSiteCode();
    const df = getDateStart();
    const dt = getDateEnd();
    rangeQueryFrom = df || "";
    rangeQueryTo = dt || "";
    updateContextLine();
    const qs = buildSiteQuery(site, siteCode);
    const url = appendWorkTypeQuery(`/api/list_range?${qs}&date_from=${encodeURIComponent(df)}&date_to=${encodeURIComponent(dt)}`);
    const data = await jfetch(url);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(String(data.site_name || "").trim());
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    rangeDates = data && Array.isArray(data.dates) ? data.dates : [];
    if (!rangeDates.length) {
      fillTabs({
        home: {
          work_type: getSelectedWorkType() || DEFAULT_WORK_TYPE,
        },
      });
      rangeIndex = -1;
      toast("해당 기간에 기록이 없습니다.");
      return;
    }
    rangeIndex = rangeDates.length - 1;
    const showDate = rangeDates[rangeIndex];
    const ds = $("#dateStart");
    if (ds) ds.value = showDate;
    updateContextLine();
    await loadOne(site, showDate, siteCode);
    toast(`기간 ${df}~${dt} · ${rangeDates.length}건 · 표시 ${showDate}`);
  }

  async function doLoad() {
    await syncSiteIdentity({ requireInput: true });
    await loadRange();
  }

  async function doPrev() {
    if (!rangeDates.length) {
      await loadRange();
      return;
    }
    if (rangeIndex <= 0) {
      toast("처음 날짜입니다.");
      return;
    }
    rangeIndex -= 1;
    const showDate = rangeDates[rangeIndex];
    const ds = $("#dateStart");
    if (ds) ds.value = showDate;
    updateContextLine();
    await loadOne(getSiteNameRaw() || getSiteName(), showDate, getSiteCodeRaw() || getSiteCode());
    toast(`표시 ${showDate} (${rangeIndex + 1}/${rangeDates.length})`);
  }

  async function doNext() {
    if (!rangeDates.length) {
      await loadRange();
      return;
    }
    if (rangeIndex >= rangeDates.length - 1) {
      toast("마지막 날짜입니다.");
      return;
    }
    rangeIndex += 1;
    const showDate = rangeDates[rangeIndex];
    const ds = $("#dateStart");
    if (ds) ds.value = showDate;
    updateContextLine();
    await loadOne(getSiteNameRaw() || getSiteName(), showDate, getSiteCodeRaw() || getSiteCode());
    toast(`표시 ${showDate} (${rangeIndex + 1}/${rangeDates.length})`);
  }

  async function doSave() {
    if (isPublicAccessUser(authUser)) {
      showPublicAccessSignupNotice();
      return;
    }
    if (!getSiteNameRaw() && !getSiteCodeRaw()) {
      const msg = "단지명이 없으면 단지명과 단지코드를 확인후 다시 저장하세요";
      alert(msg);
      toast(msg);
      return;
    }
    await syncSiteIdentity({ requireInput: true });
    const siteNameForSave = resolveSiteNameForSave();
    if (!siteNameForSave) {
      const msg = "단지명이 없으면 단지명과 단지코드를 확인후 다시 저장하세요";
      alert(msg);
      toast(msg);
      return;
    }

    const wc = countWarnings();
    if (wc > 0) {
      const ok = confirm(`허용범위를 벗어난 값이 ${wc}개 있습니다. 저장할까요?`);
      if (!ok) return;
    }
    const payload = {
      site_id: getSiteId(),
      site_name: siteNameForSave,
      site_code: getSiteCodeRaw() || getSiteCode(),
      date: getPickedDate(),
      tabs: collectAllTabs(),
    };
    await jfetch("/api/save", { method: "POST", body: JSON.stringify(payload) });
    await loadRange().catch(() => {});
    toast("저장 완료");
  }

  async function doDelete() {
    await syncSiteIdentity({ requireInput: true });
    const date = getPickedDate();
    const ok = confirm(`${date} 데이터를 삭제할까요?`);
    if (!ok) return;
    const qs = buildSiteQuery(getSiteNameRaw() || getSiteName(), getSiteCodeRaw() || getSiteCode());
    const url = appendWorkTypeQuery(`/api/delete?${qs}&date=${encodeURIComponent(date)}`);
    await jfetch(url, { method: "DELETE" });
    await loadRange().catch(() => {});
    toast("삭제 완료");
  }

  async function doExport() {
    await syncSiteIdentity({ requireInput: true });
    const qs = buildSiteQuery(getSiteNameRaw() || getSiteName(), getSiteCodeRaw() || getSiteCode());
    const url = appendWorkTypeQuery(`/api/export?${qs}&date_from=${encodeURIComponent(getDateStart())}&date_to=${encodeURIComponent(getDateEnd())}`);
    await downloadWithAuth(url, "export.xlsx");
  }

  async function doPdf() {
    await syncSiteIdentity({ requireInput: true });
    const qs = buildSiteQuery(getSiteNameRaw() || getSiteName(), getSiteCodeRaw() || getSiteCode());
    const url = appendWorkTypeQuery(`/api/pdf?${qs}&date=${encodeURIComponent(getPickedDate())}`);
    await downloadWithAuth(url, "report.pdf");
  }

  function syncStickyOffset() {
    const header = document.querySelector(".top");
    const h = header ? header.offsetHeight : 0;
    document.documentElement.style.setProperty("--tabs-top", `${h}px`);
  }

  function renderMaintenanceBanner(payload) {
    const banner = $("#maintenanceBanner");
    if (!banner) return;
    const maintenance = payload && payload.maintenance ? payload.maintenance : payload;
    const active = !!(maintenance && maintenance.active);
    if (!active) {
      banner.classList.remove("show");
      banner.textContent = "";
      return;
    }
    const msg = String(maintenance.message || "서버 점검 중입니다. 잠시 후 다시 시도해 주세요.");
    banner.textContent = msg;
    banner.classList.add("show");
  }

  async function refreshMaintenanceStatus() {
    try {
      const data = await jfetch("/api/backup/status");
      renderMaintenanceBanner(data);
    } catch (_e) {
      // ignore poll errors
    }
  }

  function startMaintenancePolling() {
    refreshMaintenanceStatus().catch(() => {});
    if (maintenancePollTimer) {
      clearInterval(maintenancePollTimer);
      maintenancePollTimer = null;
    }
    maintenancePollTimer = setInterval(() => {
      refreshMaintenanceStatus().catch(() => {});
    }, 20000);
  }

  function wire() {
    const today = ymdToday();
    const ds = $("#dateStart");
    const de = $("#dateEnd");
    if (ds && !ds.value) ds.value = today;
    if (de && !de.value) de.value = today;

    initAutoAdvance();
    wireMenuDrawer();
    syncStickyOffset();
    window.addEventListener("resize", () => {
      relocateFiltersBlock();
      updateContextLine();
      syncStickyOffset();
    });

    $("#tabs")?.addEventListener("click", (e) => {
      const btn = e.target.closest(".tabbtn");
      if (!btn) return;
      activateTab(btn.dataset.tab, true);
    });

    const onSiteIdentityChange = async (source) => {
      const isAdmin = authUser && hasAdminPermission(authUser);
      rangeQueryFrom = "";
      rangeQueryTo = "";
      if (!isAdmin) {
        setSiteId(assignedSiteIdForUser());
        setSiteName(assignedSiteNameForUser());
        setSiteCode(assignedSiteCodeForUser());
        enforceSiteIdentityPolicy();
        enforceHomeSiteIdentityPolicy();
        toast("단지명/단지코드 입력·수정은 관리자만 가능합니다.");
        return;
      }
      if (source === "siteName") {
        setSiteName(getSiteNameRaw());
        setSiteId(0);
      } else if (source === "siteCode") {
        setSiteCode(getSiteCodeRaw());
        setSiteId(0);
      }
      await syncSiteIdentity({ requireInput: true });
      await loadRange().catch(() => {});
      toast("단지명/단지코드가 자동 매핑되었습니다.");
    };

    $("#siteName")?.addEventListener("change", () => {
      onSiteIdentityChange("siteName").catch((err) => alert("단지정보 동기화 오류: " + err.message));
    });
    $("#siteCode")?.addEventListener("change", () => {
      onSiteIdentityChange("siteCode").catch((err) => alert("단지정보 동기화 오류: " + err.message));
    });
    const resetRangeQuery = () => {
      rangeQueryFrom = "";
      rangeQueryTo = "";
      updateContextLine();
    };
    $("#dateStart")?.addEventListener("change", resetRangeQuery);
    $("#dateEnd")?.addEventListener("change", resetRangeQuery);

    const onHomePanelValueChanged = (e) => {
      const t = e.target;
      if (t instanceof HTMLInputElement || t instanceof HTMLTextAreaElement || t instanceof HTMLSelectElement) {
        saveHomeDraft();
        if (t.id === "f-home-work_type") {
          t.value = normalizeWorkTypeValue(t.value, getSelectedWorkType() || DEFAULT_WORK_TYPE);
          rangeQueryFrom = "";
          rangeQueryTo = "";
          updateContextLine();
          loadRange().catch(() => {});
        }
      }
    };
    const homePanel = document.getElementById("panel-home");
    homePanel?.addEventListener("input", onHomePanelValueChanged);
    homePanel?.addEventListener("change", onHomePanelValueChanged);

    $("#btnPrev")?.addEventListener("click", () => doPrev().catch((err) => alert("이전 오류: " + err.message)));
    $("#btnLoad")?.addEventListener("click", () => doLoad().catch((err) => alert("조회 오류: " + err.message)));
    $("#btnNext")?.addEventListener("click", () => doNext().catch((err) => alert("다음 오류: " + err.message)));
    $("#btnSave")?.addEventListener("click", () => doSave().catch((err) => alert("저장 오류: " + err.message)));
    $("#btnDelete")?.addEventListener("click", () => doDelete().catch((err) => alert("삭제 오류: " + err.message)));
    $("#btnExport")?.addEventListener("click", () => {
      doExport().catch((err) => alert("엑셀 오류: " + err.message));
    });
    $("#btnPdf")?.addEventListener("click", () => {
      doPdf().catch((err) => alert("PDF 오류: " + err.message));
    });
    $("#btnProfile")?.addEventListener("click", () => {
      window.location.href = "/pwa/profile.html";
    });
    $("#btnUsers")?.addEventListener("click", () => {
      const site = getSiteName();
      const siteCode = getSiteCode();
      const qs = canViewSiteIdentity(authUser) ? buildSiteQuery(site, siteCode) : buildSiteQuery("", "", getSiteId());
      window.location.href = qs ? `/pwa/users.html?${qs}` : "/pwa/users.html";
    });
    $("#btnSpecEnv")?.addEventListener("click", () => {
      const site = getSiteName();
      const siteCode = getSiteCode();
      const qs = canViewSiteIdentity(authUser) ? buildSiteQuery(site, siteCode) : buildSiteQuery("", "", getSiteId());
      window.location.href = qs ? `/pwa/spec_env.html?${qs}` : "/pwa/spec_env.html";
    });
    $("#btnApartmentInfo")?.addEventListener("click", () => {
      const site = getSiteName();
      const siteCode = getSiteCode();
      const qs = canViewSiteIdentity(authUser) ? buildSiteQuery(site, siteCode) : buildSiteQuery("", "", getSiteId());
      window.location.href = qs ? `/pwa/apartment_info.html?${qs}` : "/pwa/apartment_info.html";
    });
    $("#btnComplaints")?.addEventListener("click", () => {
      const site = getSiteName();
      const siteCode = getSiteCode();
      const qs = canViewSiteIdentity(authUser) ? buildSiteQuery(site, siteCode) : buildSiteQuery("", "", getSiteId());
      window.location.href = qs ? `/pwa/complaints.html?${qs}` : "/pwa/complaints.html";
    });
    $("#btnInspection")?.addEventListener("click", () => {
      const site = getSiteName();
      const siteCode = getSiteCode();
      const qs = canViewSiteIdentity(authUser) ? buildSiteQuery(site, siteCode) : buildSiteQuery("", "", getSiteId());
      window.location.href = qs ? `/pwa/inspection.html?${qs}` : "/pwa/inspection.html";
    });
    $("#btnBackup")?.addEventListener("click", () => {
      const site = getSiteName();
      const siteCode = getSiteCode();
      const qs = canViewSiteIdentity(authUser) ? buildSiteQuery(site, siteCode) : buildSiteQuery("", "", getSiteId());
      window.location.href = qs ? `/pwa/backup.html?${qs}` : "/pwa/backup.html";
    });
    $("#btnParking")?.addEventListener("click", () => {
      const run = async () => {
        const identity = await syncSiteIdentity({ requireInput: false });
        const siteName = String(identity.site_name || getSiteNameRaw() || getSiteName()).trim();
        const siteCode = String(identity.site_code || getSiteCodeRaw() || getSiteCode()).trim().toUpperCase();
        const qs = buildSiteQuery(siteName, siteCode);
        const endpoint = qs ? `/api/parking/context?${qs}` : "/api/parking/context";
        const data = await jfetch(endpoint);
        const url = data && data.url ? String(data.url) : "/parking/admin2";
        window.location.href = url;
      };
      run().catch((err) => {
        alert("주차관리 접속 오류: " + err.message);
      });
    });
    $("#btnLogout")?.addEventListener("click", () => {
      const run = async () => {
        await window.KAAuth.logout("/pwa/");
      };
      run().catch(() => {});
    });
  }

  async function init() {
    await ensureAuth();
    resolveSiteName();
    resolveSiteCode();
    resolveSiteId();
    await syncSiteIdentity({ requireInput: false });
    const data = await jfetch(`/api/schema?${buildSiteQuery(getSiteNameRaw(), getSiteCode())}`);
    const schema = data && data.schema ? data.schema : null;
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && data.site_name) setSiteName(String(data.site_name));
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    if (!schema) throw new Error("스키마를 불러오지 못했습니다.");
    TABS = normalizeTabsFromSchema(schema);
    if (!TABS.length) throw new Error("스키마 탭이 비어있습니다.");
    render();
    enforceHomeSiteIdentityPolicy();
    wire();
    startMaintenancePolling();
    syncStickyOffset();
    applyHomeDraft(loadHomeDraft());
    await doLoad().catch(() => {});
    applyHomeDraft(loadHomeDraft());

    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/pwa/sw.js?v=20260208a").catch(() => {});
    }
  }

  init().catch((err) => {
    const msg = err && err.message ? err.message : String(err);
    if (msg.includes("로그인이 필요") || msg.includes("단지코드/단지명 충돌로 로그아웃되었습니다.") || msg.includes("모듈 전환 중")) return;
    alert("앱 초기화 오류: " + msg);
  });
})();

