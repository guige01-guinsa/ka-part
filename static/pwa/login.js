(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const PENDING_SITE_REGISTER_KEY = "ka_pending_site_register_v1";
  let bootstrapRequired = false;

  function readSessionJson(key) {
    try {
      const raw = window.sessionStorage ? window.sessionStorage.getItem(key) : "";
      if (!raw) return null;
      const obj = JSON.parse(raw);
      return obj && typeof obj === "object" ? obj : null;
    } catch (_e) {
      return null;
    }
  }

  function writeSessionJson(key, value) {
    try {
      if (window.sessionStorage) {
        window.sessionStorage.setItem(key, JSON.stringify(value || {}));
      }
    } catch (_e) {}
  }

  function clearSessionKey(key) {
    try {
      if (window.sessionStorage) window.sessionStorage.removeItem(key);
    } catch (_e) {}
  }

  function normalizePath(raw) {
    const txt = String(raw || "").trim();
    if (!txt) return "";
    try {
      const u = new URL(txt, window.location.origin);
      if (u.origin !== window.location.origin) return "";
      return `${u.pathname}${u.search}${u.hash}`;
    } catch (_e) {
      return "";
    }
  }

  function nextPath() {
    const u = new URL(window.location.href);
    return normalizePath(u.searchParams.get("next")) || "/pwa/";
  }

  function isResidentRoleText(role) {
    const txt = String(role || "").trim();
    return txt === "입주민" || txt === "주민" || txt === "세대주민";
  }

  function isBoardRoleText(role) {
    const txt = String(role || "").trim();
    return txt === "입대의" || txt === "입주자대표" || txt === "입주자대표회의";
  }

  function isComplaintsRoleText(role) {
    return isResidentRoleText(role) || isBoardRoleText(role);
  }

  function isSecurityRoleText(role) {
    const txt = String(role || "").trim();
    if (!txt) return false;
    const compact = txt.replaceAll(" ", "");
    if (compact === "보안/경비") return true;
    return txt.includes("보안") || txt.includes("경비");
  }

  function permissionLevelText(user) {
    return String((user && user.permission_level) || "").trim().toLowerCase();
  }

  function adminScopeText(user) {
    return String((user && user.admin_scope) || "").trim().toLowerCase();
  }

  function isSuperAdmin(user) {
    const level = permissionLevelText(user);
    return level === "admin" && adminScopeText(user) === "super_admin";
  }

  function defaultLandingPath(user) {
    const fromServer = normalizePath(user && user.default_landing_path);
    if (fromServer) return fromServer;
    const level = permissionLevelText(user);
    if (level === "security_guard") return "/parking/admin2";
    if (level === "resident" || level === "board_member") return "/pwa/complaints.html";
    const role = String((user && user.role) || "").trim();
    if (isSecurityRoleText(role)) return "/parking/admin2";
    if (isComplaintsRoleText(role)) return "/pwa/complaints.html";
    return "/pwa/";
  }

  function resolveNextPath(user) {
    const requested = nextPath();
    const level = permissionLevelText(user);
    if (level === "security_guard") {
      return requested.startsWith("/parking") ? requested : defaultLandingPath(user);
    }
    if (level === "resident" || level === "board_member") {
      return requested.startsWith("/pwa/complaints.html") ? requested : defaultLandingPath(user);
    }
    const role = String((user && user.role) || "").trim();
    if (isSecurityRoleText(role)) {
      return requested.startsWith("/parking") ? requested : defaultLandingPath(user);
    }
    if (isComplaintsRoleText(role)) {
      return requested.startsWith("/pwa/complaints.html") ? requested : defaultLandingPath(user);
    }
    return requested || defaultLandingPath(user);
  }

  function goNext(user) {
    window.location.href = resolveNextPath(user || null);
  }

  function setMsg(el, msg, isErr = false) {
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function setSiteRegMsg(msg, isErr = false) {
    setMsg($("#siteRegMsg"), msg, isErr);
  }

  function showSignupResult(text) {
    const el = $("#signupResult");
    if (!el) return;
    el.textContent = text || "";
    el.classList.toggle("hidden", !text);
  }

  function showSiteRegisterAssist(show) {
    const el = $("#siteRegAssist");
    if (!el) return;
    el.classList.toggle("hidden", !show);
  }

  function pendingSiteRegister() {
    const pending = readSessionJson(PENDING_SITE_REGISTER_KEY);
    if (!pending) return null;
    const site_name = String(pending.site_name || "").trim();
    const site_code = String(pending.site_code || "").trim().toUpperCase();
    if (!site_name) return null;
    return { site_name, site_code };
  }

  function savePendingSiteRegister(siteName, siteCode) {
    writeSessionJson(PENDING_SITE_REGISTER_KEY, {
      site_name: String(siteName || "").trim(),
      site_code: String(siteCode || "").trim().toUpperCase(),
      requested_at: new Date().toISOString(),
    });
  }

  function clearPendingSiteRegister() {
    clearSessionKey(PENDING_SITE_REGISTER_KEY);
  }

  function isMissingSiteCodeMessage(message) {
    const msg = String(message || "").trim();
    if (!msg) return false;
    return (
      msg.includes("단지코드가 등록되지 않았습니다") ||
      msg.includes("해당 단지코드가 등록되어 있지 않습니다") ||
      msg.includes("site_code mapping not found")
    );
  }

  function syncAssistSiteName() {
    const su = $("#suSiteName");
    const sr = $("#srSiteName");
    if (!su || !sr) return;
    const current = String(sr.value || "").trim();
    const source = String(su.value || "").trim();
    if (!current && source) sr.value = source;
  }

  function showMissingSiteCodeAssist() {
    showSiteRegisterAssist(true);
    syncAssistSiteName();
    if (bootstrapRequired) {
      setSiteRegMsg("아직 최고관리자 계정이 없습니다. 아래 '최초 관리자 설정' 완료 후 간편등록 예약을 눌러주세요.");
      return;
    }
    const pending = pendingSiteRegister();
    if (pending) {
      if ($("#srSiteName")) $("#srSiteName").value = pending.site_name;
      if ($("#srSiteCode")) $("#srSiteCode").value = pending.site_code || "";
      setSiteRegMsg("간편등록 예약이 있습니다. 최고관리자로 로그인하면 단지코드를 자동 등록합니다.");
      return;
    }
    setSiteRegMsg("간편등록 예약을 누른 뒤 최고관리자로 로그인하면 단지코드를 자동 등록합니다.");
  }

  function signupPayloadFromForm() {
    return {
      name: ($("#suName").value || "").trim(),
      phone: ($("#suPhone").value || "").trim(),
      site_name: ($("#suSiteName").value || "").trim(),
      role: ($("#suRole").value || "").trim(),
      unit_label: ($("#suUnitLabel").value || "").trim(),
      address: ($("#suAddress").value || "").trim(),
      office_phone: ($("#suOfficePhone").value || "").trim(),
      office_fax: ($("#suOfficeFax").value || "").trim(),
    };
  }

  async function requestSignupCode() {
    const body = signupPayloadFromForm();
    if (!body.name || !body.phone || !body.site_name || !body.role || !body.address || !body.office_phone || !body.office_fax) {
      setMsg($("#signupMsg"), "필수 항목을 모두 입력하세요.", true);
      return;
    }
    if (body.role === "최고/운영관리자") {
      setMsg($("#signupMsg"), "최고/운영관리자 계정은 자가가입할 수 없습니다.", true);
      return;
    }
    if (isResidentRoleText(body.role) && !body.unit_label) {
      setMsg($("#signupMsg"), "입주민은 동/호를 입력해야 합니다.", true);
      return;
    }
    const data = await KAAuth.requestJson("/api/auth/signup/request_phone_verification", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify(body),
      headers: {},
    });
    let msg = data.message || "인증번호를 전송했습니다.";
    if (data.debug_code) msg += ` (개발용 인증번호: ${data.debug_code})`;
    setMsg($("#signupMsg"), msg);
    showSignupResult("");
  }

  async function verifySignupAndIssueId() {
    const phone = ($("#suPhone").value || "").trim();
    const code = ($("#suCode").value || "").trim();
    if (!phone || !code) {
      setMsg($("#signupMsg"), "휴대폰번호와 인증번호를 입력하세요.", true);
      return;
    }
    const data = await KAAuth.requestJson("/api/auth/signup/verify_phone_and_issue_id", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({ phone, code }),
      headers: {},
    });
    const lines = [];
    lines.push(data.message || "아이디 발급이 완료되었습니다.");
    lines.push(`아이디: ${data.login_id || "-"}`);
    if (data.temporary_password) {
      lines.push(`임시비밀번호: ${data.temporary_password}`);
      lines.push("로그인 후 비밀번호를 변경하세요.");
    }
    showSignupResult(lines.join("\n"));
    setMsg($("#signupMsg"), "처리 완료");
    if (data.login_id) $("#loginId").value = String(data.login_id);
    if (data.temporary_password) $("#password").value = String(data.temporary_password);
  }

  async function logoutSilently() {
    try {
      await KAAuth.requestJson("/api/auth/logout", { method: "POST" });
    } catch (_e) {}
    KAAuth.clearSession();
  }

  async function tryCompletePendingSiteRegister(user) {
    const pending = pendingSiteRegister();
    if (!pending) return false;
    if (!isSuperAdmin(user)) return false;

    const payload = { site_name: pending.site_name };
    if (pending.site_code) payload.site_code = pending.site_code;

    const data = await KAAuth.requestJson("/api/site_registry/register", {
      method: "POST",
      body: JSON.stringify(payload),
      headers: {},
    });

    clearPendingSiteRegister();
    await logoutSilently();

    const u = new URL("/pwa/login.html", window.location.origin);
    u.searchParams.set("site_registered", "1");
    u.searchParams.set("site_name", String(data.site_name || pending.site_name));
    if (data.site_code) u.searchParams.set("site_code", String(data.site_code));
    window.location.href = `${u.pathname}${u.search}`;
    return true;
  }

  async function login() {
    const login_id = ($("#loginId").value || "").trim().toLowerCase();
    const password = ($("#password").value || "").trim();
    if (!login_id || !password) {
      setMsg($("#loginMsg"), "아이디와 비밀번호를 입력하세요.", true);
      return;
    }
    const data = await KAAuth.requestJson("/api/auth/login", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({ login_id, password }),
      headers: {},
    });
    KAAuth.setSession(data.token, data.user);
    const handled = await tryCompletePendingSiteRegister(data.user || null);
    if (handled) return;
    setMsg($("#loginMsg"), "로그인 성공");
    goNext(data.user || null);
  }

  async function bootstrap() {
    const login_id = ($("#bsLoginId").value || "").trim().toLowerCase();
    const name = ($("#bsName").value || "").trim();
    const role = ($("#bsRole").value || "").trim();
    const password = ($("#bsPassword").value || "").trim();
    const password2 = ($("#bsPassword2").value || "").trim();
    if (password !== password2) {
      setMsg($("#bootstrapMsg"), "비밀번호 확인이 일치하지 않습니다.", true);
      return;
    }
    const data = await KAAuth.requestJson("/api/auth/bootstrap", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({ login_id, name, role, password }),
      headers: {},
    });
    KAAuth.setSession(data.token, data.user);
    const handled = await tryCompletePendingSiteRegister(data.user || null);
    if (handled) return;
    setMsg($("#bootstrapMsg"), "초기 관리자 생성 완료");
    goNext(data.user || null);
  }

  async function checkAlreadyLoggedIn() {
    const token = KAAuth.getToken();
    try {
      const me = await KAAuth.requestJson("/api/auth/me", { noAuth: !token });
      if (me && me.user) {
        KAAuth.setSession(token, me.user);
        const handled = await tryCompletePendingSiteRegister(me.user);
        if (handled) return;
        goNext(me.user);
      }
    } catch (_e) {
      KAAuth.clearSession();
    }
  }

  async function loadBootstrapStatus() {
    const data = await fetch("/api/auth/bootstrap_status").then((r) => r.json());
    bootstrapRequired = !!data.required;
    const card = $("#bootstrapCard");
    if (card) card.classList.toggle("hidden", !bootstrapRequired);
  }

  function restoreSiteRegisterResultFromQuery() {
    const u = new URL(window.location.href);
    const done = String(u.searchParams.get("site_registered") || "").trim() === "1";
    if (!done) return;

    const siteName = String(u.searchParams.get("site_name") || "").trim();
    const siteCode = String(u.searchParams.get("site_code") || "").trim().toUpperCase();

    if (siteName && $("#suSiteName")) $("#suSiteName").value = siteName;
    if (siteName && $("#srSiteName")) $("#srSiteName").value = siteName;
    if (siteCode && $("#srSiteCode")) $("#srSiteCode").value = siteCode;

    showSiteRegisterAssist(true);
    const completeMsg = siteCode
      ? `단지코드 등록 완료: ${siteName || "단지"} (${siteCode})`
      : "단지코드 등록이 완료되었습니다.";
    setMsg($("#signupMsg"), `${completeMsg}. 인증번호 받기를 다시 진행하세요.`);
    setSiteRegMsg("간편등록이 완료되었습니다.");

    u.searchParams.delete("site_registered");
    u.searchParams.delete("site_name");
    u.searchParams.delete("site_code");
    const cleaned = `${u.pathname}${u.search}`;
    window.history.replaceState({}, "", cleaned);
  }

  function restorePendingSiteRegisterAssist() {
    const pending = pendingSiteRegister();
    if (!pending) return;
    showSiteRegisterAssist(true);
    if ($("#srSiteName")) $("#srSiteName").value = pending.site_name;
    if ($("#srSiteCode")) $("#srSiteCode").value = pending.site_code || "";
    setSiteRegMsg("간편등록 예약이 있습니다. 최고관리자로 로그인하면 자동 등록됩니다.");
  }

  function prepareSiteRegisterReservation() {
    const siteNameInput = $("#srSiteName");
    const siteCodeInput = $("#srSiteCode");
    const siteName = String((siteNameInput && siteNameInput.value) || $("#suSiteName")?.value || "").trim();
    const siteCode = String((siteCodeInput && siteCodeInput.value) || "").trim().toUpperCase();
    if (!siteName) {
      setSiteRegMsg("단지명을 입력하세요.", true);
      return;
    }
    savePendingSiteRegister(siteName, siteCode);
    if ($("#suSiteName")) $("#suSiteName").value = siteName;
    if (siteNameInput) siteNameInput.value = siteName;
    if (siteCodeInput) siteCodeInput.value = siteCode;

    if (bootstrapRequired) {
      setSiteRegMsg("예약했습니다. 아래 '최초 관리자 설정'을 완료한 뒤 로그인하면 단지코드를 자동 등록합니다.");
      const card = $("#bootstrapCard");
      if (card) card.scrollIntoView({ behavior: "smooth", block: "center" });
      return;
    }

    setSiteRegMsg("예약했습니다. 최고관리자로 로그인하면 단지코드를 자동 등록합니다.");
    const loginId = $("#loginId");
    if (loginId) {
      loginId.focus();
      loginId.select();
    }
  }

  function handleSignupError(err) {
    const msg = err && err.message ? String(err.message) : String(err || "오류가 발생했습니다.");
    setMsg($("#signupMsg"), msg, true);
    if (isMissingSiteCodeMessage(msg)) {
      showMissingSiteCodeAssist();
    }
  }

  function wire() {
    $("#btnLogin").addEventListener("click", () => {
      login().catch((e) => setMsg($("#loginMsg"), e.message || String(e), true));
    });
    $("#btnBootstrap").addEventListener("click", () => {
      bootstrap().catch((e) => setMsg($("#bootstrapMsg"), e.message || String(e), true));
    });
    $("#btnReqCode").addEventListener("click", () => {
      requestSignupCode().catch((e) => handleSignupError(e));
    });
    $("#btnVerifySignup").addEventListener("click", () => {
      verifySignupAndIssueId().catch((e) => handleSignupError(e));
    });
    $("#btnPrepareSiteReg")?.addEventListener("click", () => {
      prepareSiteRegisterReservation();
    });
    $("#suSiteName")?.addEventListener("input", () => {
      const sr = $("#srSiteName");
      if (!sr) return;
      if (!String(sr.value || "").trim()) {
        sr.value = String($("#suSiteName")?.value || "").trim();
      }
    });
    $("#password").addEventListener("keydown", (e) => {
      if (e.key === "Enter") login().catch((err) => setMsg($("#loginMsg"), err.message || String(err), true));
    });
    $("#suCode").addEventListener("keydown", (e) => {
      if (e.key === "Enter") verifySignupAndIssueId().catch((err) => handleSignupError(err));
    });
    $("#bsPassword2").addEventListener("keydown", (e) => {
      if (e.key === "Enter") bootstrap().catch((err) => setMsg($("#bootstrapMsg"), err.message || String(err), true));
    });
  }

  async function init() {
    wire();
    restoreSiteRegisterResultFromQuery();
    restorePendingSiteRegisterAssist();
    await loadBootstrapStatus();
    await checkAlreadyLoggedIn();
  }

  init().catch((e) => {
    setMsg($("#loginMsg"), e.message || String(e), true);
  });
})();

