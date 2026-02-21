(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  let bootstrapRequired = false;
  let signupFinalizeToken = "";
  let lastCheckedLoginId = "";
  let lastCheckedAvailable = false;
  let loginIdCheckTimer = 0;
  let signupReadyMode = false;
  let signupPasswordPolicy = {
    min_length: 10,
    rules: [
      "10자 이상",
      "영문 대/소문자, 숫자, 특수문자 중 3종 이상 포함",
      "아이디/휴대폰번호 포함 금지",
      "같은 문자 3회 연속 금지",
    ],
  };
  const SIGNUP_LOGIN_ID_REGEX = /^[a-z0-9][a-z0-9_]{7,24}$/;
  const SIGNUP_TUTORIAL_AUTO_ADVANCE = (() => {
    const u = new URL(window.location.href);
    const flag = String(u.searchParams.get("tutorial") || "").trim().toLowerCase();
    return flag === "1" || flag === "true" || flag === "yes" || flag === "y";
  })();

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

  function wantsGuestAutoEntry() {
    const u = new URL(window.location.href);
    const flag = String(u.searchParams.get("guest") || "").trim().toLowerCase();
    return flag === "1" || flag === "true" || flag === "yes" || flag === "y";
  }

  function signupReadyContextFromQuery() {
    const u = new URL(window.location.href);
    const flag = String(u.searchParams.get("signup_ready") || "").trim().toLowerCase();
    const enabled = flag === "1" || flag === "true" || flag === "yes" || flag === "y";
    if (!enabled) return null;
    return {
      phone: String(u.searchParams.get("phone") || "").trim(),
      loginId: normalizeSignupLoginId(u.searchParams.get("login_id") || ""),
      requestId: String(u.searchParams.get("request_id") || "").trim(),
    };
  }

  function forceStayOnLoginPage() {
    const u = new URL(window.location.href);
    const force = String(u.searchParams.get("force") || "").trim().toLowerCase();
    const mode = String(u.searchParams.get("mode") || "").trim().toLowerCase();
    const hash = String(u.hash || "").trim().toLowerCase();
    if (force === "1" || force === "true" || force === "yes" || force === "y") return true;
    if (mode === "login" || mode === "signup") return true;
    if (hash === "#signupcard" || hash.startsWith("#signup")) return true;
    return false;
  }

  function enableSignupReadyMode(opts = {}) {
    signupReadyMode = true;
    const card = $("#signupCard");
    if (card) card.scrollIntoView({ behavior: "smooth", block: "start" });
    const sub = $("#signupCardSub");
    if (sub) {
      sub.textContent = "관리자 등록처리가 완료되었습니다. 문자 인증번호 확인 후 비밀번호를 설정하세요.";
    }
    const phone = String((opts && opts.phone) || "").trim();
    const loginId = normalizeSignupLoginId((opts && opts.loginId) || "");
    if (phone && $("#suPhone")) $("#suPhone").value = phone;
    if (loginId && $("#suLoginId")) {
      $("#suLoginId").value = loginId;
      scheduleSignupLoginIdCheck();
    }
    showSiteRegisterAssist(false);
    const reqBtn = $("#btnReqCode");
    if (reqBtn) reqBtn.textContent = "인증번호 재요청";
    const modeBtn = $("#btnEnableReadyMode");
    if (modeBtn) {
      modeBtn.disabled = true;
      modeBtn.textContent = "등록처리 모드";
    }
    const reqTag = String((opts && opts.requestId) || "").trim();
    const tagText = reqTag ? ` (요청 #${reqTag})` : "";
    const intro = opts && opts.fromQuery
      ? `등록처리 완료${tagText}. 문자로 받은 인증번호를 입력하고 [인증확인]을 누르세요.`
      : "등록처리된 휴대폰번호를 입력하고 [인증번호 재요청]을 눌러 비밀번호 설정을 진행하세요.";
    setMsg($("#signupMsg"), intro);
    showSignupResult("인증번호 확인 후 비밀번호를 설정하면 최종 가입이 완료됩니다.");
  }

  function applySignupReadyContext() {
    const ctx = signupReadyContextFromQuery();
    if (!ctx) return;
    enableSignupReadyMode({
      fromQuery: true,
      phone: ctx.phone,
      loginId: ctx.loginId,
      requestId: ctx.requestId,
    });
    showSignupResult("최고관리자가 등록처리를 완료했습니다.\n인증번호 확인 후 비밀번호를 설정하면 최종 가입이 완료됩니다.");
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

  function setLoginIdMsg(msg, isErr = false) {
    setMsg($("#suLoginIdMsg"), msg, isErr);
  }

  function setSignupCompleteMsg(msg, isErr = false) {
    setMsg($("#signupCompleteMsg"), msg, isErr);
  }

  function showSignupResult(text) {
    const el = $("#signupResult");
    if (!el) return;
    el.textContent = text || "";
    el.classList.toggle("hidden", !text);
  }

  function showSignupPasswordPanel(show) {
    const el = $("#signupPasswordPanel");
    if (!el) return;
    el.classList.toggle("hidden", !show);
  }

  function showSiteRegisterAssist(show) {
    const el = $("#siteRegAssist");
    if (!el) return;
    el.classList.toggle("hidden", !show);
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
    setSiteRegMsg("간편등록 예약을 접수하면 자동 등록 처리됩니다. 인증번호 받기를 다시 눌러 계속 진행하세요.");
  }

  function normalizeSignupLoginId(value) {
    return String(value || "").trim().toLowerCase();
  }

  function validateSignupLoginIdFormat(loginId) {
    const value = normalizeSignupLoginId(loginId);
    if (!value) return "아이디를 입력하세요.";
    if (!/^[a-z0-9_]+$/.test(value)) return "아이디는 소문자/숫자/_만 사용할 수 있습니다.";
    if (value.length < 8 || value.length > 25) return "아이디는 8~25자여야 합니다.";
    if (!SIGNUP_LOGIN_ID_REGEX.test(value)) return "아이디 형식을 확인하세요.";
    return "";
  }

  function signupLoginId() {
    const el = $("#suLoginId");
    if (!el) return "";
    const normalized = normalizeSignupLoginId(el.value || "");
    if (el.value !== normalized) el.value = normalized;
    return normalized;
  }

  function markLoginIdUnchecked() {
    lastCheckedLoginId = "";
    lastCheckedAvailable = false;
  }

  function phoneDigits(value) {
    return String(value || "").replace(/\D/g, "");
  }

  function isLikelyValidPhone(value) {
    const digits = phoneDigits(value);
    return digits.length >= 9 && digits.length <= 11;
  }

  function isLikelyValidUnitLabel(value) {
    const compact = String(value || "").trim().replace(/\s+/g, "");
    if (!compact) return false;
    return /^(\d{2,4}[-/]\d{3,4}|\d{2,4}동\d{3,4}호?)$/.test(compact);
  }

  function isStepFocusable(el) {
    if (!el) return false;
    if (el.disabled) return false;
    if (el.closest(".hidden")) return false;
    return true;
  }

  function focusSignupStep(stepId) {
    const el = $(`#${stepId}`);
    if (!isStepFocusable(el)) return false;
    try {
      el.focus({ preventScroll: false });
    } catch (_e) {
      try {
        el.focus();
      } catch (_e2) {}
    }
    if (
      typeof el.select === "function" &&
      (el.tagName === "INPUT" || el.tagName === "TEXTAREA") &&
      (el.type || "").toLowerCase() !== "date"
    ) {
      try {
        el.select();
      } catch (_e) {}
    }
    return true;
  }

  function signupTutorialStepOrder() {
    const role = String($("#suRole")?.value || "").trim();
    const order = ["suName", "suPhone", "suLoginId", "suSiteName", "suRole"];
    if (isResidentRoleText(role)) order.push("suUnitLabel");
    order.push("suAddress", "suOfficePhone", "suOfficeFax", "btnReqCode", "suCode", "btnVerifySignup");
    if (!$("#signupPasswordPanel")?.classList.contains("hidden")) {
      order.push("suPassword", "suPassword2", "btnCompleteSignup");
    }
    return order;
  }

  function nextSignupTutorialStepId(currentId) {
    const order = signupTutorialStepOrder();
    const idx = order.indexOf(String(currentId || "").trim());
    if (idx < 0) return "";
    for (let i = idx + 1; i < order.length; i += 1) {
      const stepId = order[i];
      const el = $(`#${stepId}`);
      if (isStepFocusable(el)) return stepId;
    }
    return "";
  }

  function isSignupStepValidForTutorial(stepId) {
    const id = String(stepId || "").trim();
    const role = String($("#suRole")?.value || "").trim();
    switch (id) {
      case "suName":
        return String($("#suName")?.value || "").trim().length >= 2;
      case "suPhone":
        return isLikelyValidPhone($("#suPhone")?.value || "");
      case "suLoginId":
        return !validateSignupLoginIdFormat(signupLoginId());
      case "suSiteName":
        return String($("#suSiteName")?.value || "").trim().length >= 2;
      case "suRole":
        return !!role;
      case "suUnitLabel":
        if (!isResidentRoleText(role)) return true;
        return isLikelyValidUnitLabel($("#suUnitLabel")?.value || "");
      case "suAddress":
        return String($("#suAddress")?.value || "").trim().length >= 5;
      case "suOfficePhone":
        return isLikelyValidPhone($("#suOfficePhone")?.value || "");
      case "suOfficeFax":
        return isLikelyValidPhone($("#suOfficeFax")?.value || "");
      case "suCode":
        return /^\d{6}$/.test(String($("#suCode")?.value || "").trim());
      case "suPassword":
        return String($("#suPassword")?.value || "").trim().length >= Number(signupPasswordPolicy.min_length || 10);
      case "suPassword2": {
        const pw = String($("#suPassword")?.value || "").trim();
        const pw2 = String($("#suPassword2")?.value || "").trim();
        return !!pw && pw === pw2;
      }
      default:
        return true;
    }
  }

  async function tryTutorialAutoAdvance(stepId) {
    if (!SIGNUP_TUTORIAL_AUTO_ADVANCE) return;
    const id = String(stepId || "").trim();
    if (!id) return;
    if (!isSignupStepValidForTutorial(id)) return;
    if (id === "suLoginId") {
      try {
        const available = await checkSignupLoginIdAvailability({ force: true });
        if (!available) return;
      } catch (e) {
        setLoginIdMsg(e.message || String(e), true);
        return;
      }
    }
    const nextId = nextSignupTutorialStepId(id);
    if (!nextId) return;
    focusSignupStep(nextId);
  }

  function focusTutorialStartField() {
    if (!SIGNUP_TUTORIAL_AUTO_ADVANCE) return;
    if (signupReadyMode) {
      focusSignupStep("suCode");
      return;
    }
    const u = new URL(window.location.href);
    const mode = String(u.searchParams.get("mode") || "").trim().toLowerCase();
    const hash = String(u.hash || "").trim().toLowerCase();
    if (mode !== "signup" && !hash.startsWith("#signup")) return;
    const order = signupTutorialStepOrder();
    for (const id of order) {
      if (id.startsWith("btn")) continue;
      const el = $(`#${id}`);
      if (!isStepFocusable(el)) continue;
      const value = "value" in el ? String(el.value || "").trim() : "";
      if (!value) {
        focusSignupStep(id);
        return;
      }
    }
  }

  function wireSignupTutorialAutoAdvance() {
    if (!SIGNUP_TUTORIAL_AUTO_ADVANCE) return;
    const simpleStepIds = [
      "suName",
      "suPhone",
      "suLoginId",
      "suSiteName",
      "suAddress",
      "suOfficePhone",
      "suOfficeFax",
      "suUnitLabel",
      "suPassword",
      "suPassword2",
      "suCode",
    ];
    for (const stepId of simpleStepIds) {
      const el = $(`#${stepId}`);
      if (!el) continue;
      el.addEventListener("blur", () => {
        tryTutorialAutoAdvance(stepId).catch(() => {});
      });
      el.addEventListener("keydown", (e) => {
        if (e.key !== "Enter") return;
        if (stepId === "suCode" || stepId === "suPassword2") return;
        e.preventDefault();
        tryTutorialAutoAdvance(stepId).catch(() => {});
      });
    }
    $("#suRole")?.addEventListener("change", () => {
      tryTutorialAutoAdvance("suRole").catch(() => {});
    });
  }

  function applyIssuedVerificationCode(data) {
    const code = String((data && data.debug_code) || "").trim();
    if (!/^\d{6}$/.test(code)) return false;
    const codeEl = $("#suCode");
    if (codeEl) codeEl.value = code;
    const hint = "인증번호가 자동 입력되었습니다. [인증확인]을 누르세요.";
    const current = String($("#signupMsg")?.textContent || "").trim();
    if (!current) setMsg($("#signupMsg"), hint);
    else if (!current.includes("인증번호가 자동 입력되었습니다")) setMsg($("#signupMsg"), `${current} ${hint}`);
    focusSignupStep("btnVerifySignup");
    return true;
  }

  function moveToLoginCardForSignin(loginId, password, note = "") {
    const cleanLoginId = String(loginId || "").trim();
    const cleanPassword = String(password || "").trim();
    if (cleanLoginId && $("#loginId")) $("#loginId").value = cleanLoginId;
    if (cleanPassword && $("#password")) $("#password").value = cleanPassword;
    setMsg($("#loginMsg"), note || "가입 완료. 로그인 버튼을 누르면 바로 접속됩니다.");
    const btn = $("#btnLogin");
    const card = btn ? btn.closest(".card") : null;
    if (card) card.scrollIntoView({ behavior: "smooth", block: "start" });
    if (btn) {
      window.setTimeout(() => {
        try {
          btn.focus({ preventScroll: true });
        } catch (_e) {
          try { btn.focus(); } catch (_e2) {}
        }
      }, card ? 220 : 0);
    }
  }

  function applySignupPasswordPolicy(policy) {
    if (policy && typeof policy === "object") {
      signupPasswordPolicy = {
        min_length: Number(policy.min_length || signupPasswordPolicy.min_length || 10),
        rules: Array.isArray(policy.rules) && policy.rules.length ? policy.rules : signupPasswordPolicy.rules,
      };
    }
    const policyEl = $("#suPwPolicy");
    if (!policyEl) return;
    const rules = Array.isArray(signupPasswordPolicy.rules) ? signupPasswordPolicy.rules : [];
    policyEl.textContent = rules.join(" · ");
  }

  function scorePasswordStrength(password, loginId, phone) {
    const pw = String(password || "");
    const lid = String(loginId || "").trim().toLowerCase();
    const phoneNum = phoneDigits(phone);
    let score = 0;

    if (pw.length >= Number(signupPasswordPolicy.min_length || 10)) score += 2;
    else if (pw.length >= 8) score += 1;

    if (/[A-Z]/.test(pw)) score += 1;
    if (/[a-z]/.test(pw)) score += 1;
    if (/[0-9]/.test(pw)) score += 1;
    if (/[^A-Za-z0-9]/.test(pw)) score += 1;
    if (/\s/.test(pw)) score -= 2;
    if (/(.)\1\1/.test(pw)) score -= 2;
    if (lid && lid.length >= 3 && pw.toLowerCase().includes(lid)) score -= 2;
    if (phoneNum) {
      const tail = phoneNum.slice(-4);
      const longTail = phoneNum.slice(-8);
      if ((tail && pw.includes(tail)) || (longTail && pw.includes(longTail)) || pw.includes(phoneNum)) score -= 2;
    }

    if (score <= 2) return { label: "약함", color: "#ff9e9e" };
    if (score <= 4) return { label: "보통", color: "#ffd37a" };
    if (score <= 6) return { label: "좋음", color: "#a9d4ff" };
    return { label: "강함", color: "#8de3b0" };
  }

  function refreshSignupPasswordStrength() {
    const el = $("#suPwStrength");
    if (!el) return;
    const pw = ($("#suPassword")?.value || "").trim();
    if (!pw) {
      el.textContent = "강도: -";
      el.style.color = "";
      return;
    }
    const loginId = signupLoginId();
    const phone = ($("#suPhone")?.value || "").trim();
    const meta = scorePasswordStrength(pw, loginId, phone);
    el.textContent = `강도: ${meta.label}`;
    el.style.color = meta.color;
  }

  async function checkSignupLoginIdAvailability(opts = {}) {
    const force = !!opts.force;
    const silent = !!opts.silent;
    const loginId = signupLoginId();
    const phone = ($("#suPhone")?.value || "").trim();
    const phoneNum = phoneDigits(phone);

    if (!loginId) {
      markLoginIdUnchecked();
      if (!silent) setLoginIdMsg("아이디를 입력하세요.", true);
      return false;
    }
    const formatError = validateSignupLoginIdFormat(loginId);
    if (formatError) {
      markLoginIdUnchecked();
      if (!silent) setLoginIdMsg(formatError, true);
      return false;
    }
    if (!force && lastCheckedLoginId === loginId && lastCheckedAvailable) {
      if (!silent) setLoginIdMsg("사용 가능한 아이디입니다.");
      return true;
    }

    const qs = new URLSearchParams();
    qs.set("login_id", loginId);
    if (phoneNum.length >= 9) qs.set("phone", phone);
    const data = await KAAuth.requestJson(`/api/auth/signup/check_login_id?${qs.toString()}`, { noAuth: true });
    const available = !!(data && data.available);
    lastCheckedLoginId = loginId;
    lastCheckedAvailable = available;
    if (!silent) {
      setLoginIdMsg(
        String(data && data.message ? data.message : available ? "사용 가능한 아이디입니다." : "이미 사용 중인 아이디입니다."),
        !available
      );
    }
    return available;
  }

  function scheduleSignupLoginIdCheck() {
    if (loginIdCheckTimer) {
      window.clearTimeout(loginIdCheckTimer);
      loginIdCheckTimer = 0;
    }
    markLoginIdUnchecked();
    const loginId = signupLoginId();
    if (!loginId) {
      setLoginIdMsg("");
      return;
    }
    const formatError = validateSignupLoginIdFormat(loginId);
    if (formatError) {
      setLoginIdMsg(formatError, true);
      return;
    }
    loginIdCheckTimer = window.setTimeout(() => {
      checkSignupLoginIdAvailability({ force: true }).catch((e) => setLoginIdMsg(e.message || String(e), true));
    }, 320);
  }

  function resetSignupFinalizeState() {
    signupFinalizeToken = "";
    showSignupPasswordPanel(false);
    setSignupCompleteMsg("");
    if ($("#suPassword")) $("#suPassword").value = "";
    if ($("#suPassword2")) $("#suPassword2").value = "";
    refreshSignupPasswordStrength();
  }

  function signupPayloadFromForm() {
    return {
      name: ($("#suName").value || "").trim(),
      phone: ($("#suPhone").value || "").trim(),
      login_id: signupLoginId(),
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
    if (signupReadyMode) {
      if (!body.phone) {
        setMsg($("#signupMsg"), "휴대폰번호를 입력하세요.", true);
        return;
      }
      resetSignupFinalizeState();
      const data = await KAAuth.requestJson("/api/auth/signup/request_ready_verification", {
        method: "POST",
        noAuth: true,
        body: JSON.stringify({ phone: body.phone }),
        headers: {},
      });
      if (data && data.already_registered) {
        const lines = [];
        lines.push(data.message || "이미 등록된 사용자입니다.");
        lines.push(`아이디: ${data.login_id || "-"}`);
        showSignupResult(lines.join("\n"));
        setMsg($("#signupMsg"), "처리 완료");
        if (data.login_id) $("#loginId").value = String(data.login_id);
        return;
      }
      let msg = data.message || "인증번호를 전송했습니다.";
      if (data.debug_code) msg += ` (개발용 인증번호: ${data.debug_code})`;
      setMsg($("#signupMsg"), msg);
      showSignupResult("문자로 받은 인증번호를 입력하고 [인증확인]을 누르세요.");
      if (!applyIssuedVerificationCode(data) && SIGNUP_TUTORIAL_AUTO_ADVANCE) focusSignupStep("suCode");
      return;
    }
    const missingNormalFields =
      !body.name || !body.phone || !body.login_id || !body.site_name || !body.role || !body.address || !body.office_phone || !body.office_fax;
    if (missingNormalFields) {
      // If the account was already approved by an admin (site registry flow), allow a phone-only path
      // without exposing a separate "ready" button in the UI.
      if (body.phone) {
        try {
          resetSignupFinalizeState();
          const data = await KAAuth.requestJson("/api/auth/signup/request_ready_verification", {
            method: "POST",
            noAuth: true,
            body: JSON.stringify({ phone: body.phone }),
            headers: {},
          });
          if (data && data.already_registered) {
            const lines = [];
            lines.push(data.message || "이미 등록된 사용자입니다.");
            lines.push(`아이디: ${data.login_id || "-"}`);
            showSignupResult(lines.join("\n"));
            setMsg($("#signupMsg"), "처리 완료");
            if (data.login_id) $("#loginId").value = String(data.login_id);
            return;
          }
          enableSignupReadyMode({
            fromQuery: false,
            phone: body.phone,
            loginId: body.login_id,
            requestId: data && data.request_id ? String(data.request_id) : "",
          });
          let msg = data.message || "인증번호를 전송했습니다.";
          if (data.debug_code) msg += ` (개발용 인증번호: ${data.debug_code})`;
          setMsg($("#signupMsg"), msg);
          showSignupResult("문자로 받은 인증번호를 입력하고 [인증확인]을 누르세요.");
          if (!applyIssuedVerificationCode(data) && SIGNUP_TUTORIAL_AUTO_ADVANCE) focusSignupStep("suCode");
          return;
        } catch (_e) {
          // fall through: user is in normal signup flow
        }
      }
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
    const formatError = validateSignupLoginIdFormat(body.login_id);
    if (formatError) {
      setLoginIdMsg(formatError, true);
      setMsg($("#signupMsg"), formatError, true);
      return;
    }
    const available = await checkSignupLoginIdAvailability({ force: true });
    if (!available) {
      setMsg($("#signupMsg"), "아이디 중복을 확인하고 다른 아이디를 입력하세요.", true);
      return;
    }
    resetSignupFinalizeState();
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
    if (!applyIssuedVerificationCode(data) && SIGNUP_TUTORIAL_AUTO_ADVANCE) focusSignupStep("suCode");
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
    if (data && data.already_registered) {
      const lines = [];
      lines.push(data.message || "이미 등록된 사용자입니다.");
      lines.push(`아이디: ${data.login_id || "-"}`);
      showSignupResult(lines.join("\n"));
      resetSignupFinalizeState();
      setMsg($("#signupMsg"), "처리 완료");
      if (data.login_id) $("#loginId").value = String(data.login_id);
      return;
    }

    signupFinalizeToken = String((data && data.signup_token) || "").trim();
    if (!signupFinalizeToken) {
      throw new Error("인증은 완료되었지만 가입토큰 발급에 실패했습니다. 다시 시도하세요.");
    }
    if (data && data.login_id_suggestion) {
      const suggested = normalizeSignupLoginId(data.login_id_suggestion);
      if ($("#suLoginId")) $("#suLoginId").value = suggested;
      markLoginIdUnchecked();
      checkSignupLoginIdAvailability({ force: true, silent: true }).catch(() => {});
    }
    applySignupPasswordPolicy(data && data.password_policy ? data.password_policy : null);
    showSignupPasswordPanel(true);
    setSignupCompleteMsg("");
    setMsg($("#signupMsg"), data.message || "휴대폰 인증이 완료되었습니다. 비밀번호를 설정하세요.");
    const lines = [];
    lines.push("휴대폰 인증이 완료되었습니다.");
    lines.push("비밀번호를 설정하면 가입이 완료됩니다.");
    showSignupResult(lines.join("\n"));
    refreshSignupPasswordStrength();
    if (SIGNUP_TUTORIAL_AUTO_ADVANCE) focusSignupStep("suPassword");
  }

  async function completeSignup() {
    if (!signupFinalizeToken) {
      setSignupCompleteMsg("먼저 휴대폰 인증을 완료하세요.", true);
      return;
    }
    const loginId = signupLoginId();
    const password = ($("#suPassword").value || "").trim();
    const password2 = ($("#suPassword2").value || "").trim();

    if (!loginId) {
      setSignupCompleteMsg("아이디를 입력하세요.", true);
      return;
    }
    const formatError = validateSignupLoginIdFormat(loginId);
    if (formatError) {
      setSignupCompleteMsg(formatError, true);
      return;
    }
    const available = await checkSignupLoginIdAvailability({ force: true });
    if (!available) {
      setSignupCompleteMsg("이미 사용 중인 아이디입니다. 다른 아이디를 입력하세요.", true);
      return;
    }
    if (!password || !password2) {
      setSignupCompleteMsg("비밀번호와 비밀번호 확인을 입력하세요.", true);
      return;
    }
    if (password !== password2) {
      setSignupCompleteMsg("비밀번호 확인이 일치하지 않습니다.", true);
      return;
    }

    const data = await KAAuth.requestJson("/api/auth/signup/complete", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({
        signup_token: signupFinalizeToken,
        login_id: loginId,
        password,
        password_confirm: password2,
      }),
      headers: {},
    });

    const lines = [];
    if (data && data.already_registered) {
      lines.push(data.message || "이미 등록된 사용자입니다.");
      lines.push(`아이디: ${data.login_id || "-"}`);
      showSignupResult(lines.join("\n"));
      setMsg($("#signupMsg"), "처리 완료");
      setSignupCompleteMsg("");
      resetSignupFinalizeState();
      if (data.login_id) $("#loginId").value = String(data.login_id);
      return;
    }

    lines.push(data.message || "가입이 완료되었습니다.");
    lines.push(`아이디: ${data.login_id || loginId || "-"}`);
    lines.push("설정한 비밀번호로 로그인하세요.");
    showSignupResult(lines.join("\n"));
    setMsg($("#signupMsg"), "가입 완료");
    setSignupCompleteMsg("");
    const finalLoginId = String(data.login_id || loginId || "").trim();
    moveToLoginCardForSignin(
      finalLoginId,
      password,
      finalLoginId
        ? `가입 완료. 아이디 ${finalLoginId}로 로그인 버튼만 누르시면 됩니다.`
        : "가입 완료. 로그인 버튼만 누르시면 됩니다."
    );
    resetSignupFinalizeState();
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
    setMsg($("#bootstrapMsg"), "초기 관리자 생성 완료");
    goNext(data.user || null);
  }

  async function guestAccess() {
    const data = await KAAuth.requestJson("/api/auth/public_access", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({}),
      headers: {},
    });
    KAAuth.setSession(data.token, data.user);
    setMsg($("#loginMsg"), "로그인 없이 접속했습니다.");
    goNext(data.user || null);
  }

  async function checkAlreadyLoggedIn() {
    if (forceStayOnLoginPage()) return;
    const token = KAAuth.getToken();
    try {
      const me = await KAAuth.requestJson("/api/auth/me", { noAuth: !token });
      if (me && me.user) {
        KAAuth.setSession(token, me.user);
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

  async function prepareSiteRegisterReservation() {
    const siteNameInput = $("#srSiteName");
    const siteCodeInput = $("#srSiteCode");
    const siteName = String((siteNameInput && siteNameInput.value) || $("#suSiteName")?.value || "").trim();
    const siteCode = String((siteCodeInput && siteCodeInput.value) || "").trim().toUpperCase();
    const signup = signupPayloadFromForm();
    if (!siteName) {
      setSiteRegMsg("단지명을 입력하세요.", true);
      return;
    }
    if (!signup.name || !signup.phone || !signup.login_id || !signup.role || !signup.address || !signup.office_phone || !signup.office_fax) {
      setSiteRegMsg("간편등록 예약 전 필수 가입정보(이름/휴대폰/아이디/분류/주소/관리소 연락처)를 입력하세요.", true);
      return;
    }
    if (isResidentRoleText(signup.role) && !signup.unit_label) {
      setSiteRegMsg("입주민은 동/호를 입력해야 합니다.", true);
      return;
    }
    const formatError = validateSignupLoginIdFormat(signup.login_id);
    if (formatError) {
      setSiteRegMsg(formatError, true);
      setLoginIdMsg(formatError, true);
      return;
    }
    if ($("#suSiteName")) $("#suSiteName").value = siteName;
    if (siteNameInput) siteNameInput.value = siteName;
    if (siteCodeInput) siteCodeInput.value = siteCode;

    const payload = {
      site_name: siteName,
      auto_process: true,
      requester_name: signup.name,
      requester_phone: signup.phone,
      requester_login_id: signup.login_id,
      requester_role: signup.role,
      requester_unit_label: signup.unit_label,
      requester_note: "login.html 간편등록 예약",
      signup_name: signup.name,
      signup_phone: signup.phone,
      signup_login_id: signup.login_id,
      signup_role: signup.role,
      signup_unit_label: signup.unit_label,
      signup_address: signup.address,
      signup_office_phone: signup.office_phone,
      signup_office_fax: signup.office_fax,
    };
    if (siteCode) payload.site_code = siteCode;
    const data = await KAAuth.requestJson("/api/site_registry/request", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify(payload),
      headers: {},
    });
    const reqId = Number(data && data.request_id ? data.request_id : 0);
    const doneMsg =
      reqId > 0
        ? `접수 완료 #${reqId}. 다시한번 인증번호 받기를 누르면 단지대표자인증 번호가 다시 발송되고 인증확인후 계속 진행`
        : "접수 완료. 다시한번 인증번호 받기를 누르면 단지대표자인증 번호가 다시 발송되고 인증확인후 계속 진행";
    showSiteRegisterAssist(true);
    setSiteRegMsg(doneMsg);
    setMsg($("#signupMsg"), doneMsg);
    const autoError = String((data && data.auto_error) || "").trim();
    if (autoError) {
      setSiteRegMsg(`접수는 완료되었지만 자동 처리에 실패했습니다. 최고관리자 요청함에서 처리해 주세요. (${autoError})`, true);
    }
  }

  function handleSignupError(err) {
    const msg = err && err.message ? String(err.message) : String(err || "오류가 발생했습니다.");
    setMsg($("#signupMsg"), msg, true);
    if (msg.includes("이미 사용 중인 아이디")) {
      setLoginIdMsg(msg, true);
    }
    if (isMissingSiteCodeMessage(msg)) {
      showMissingSiteCodeAssist();
    }
    if (signupReadyMode && msg.includes("등록처리 완료 내역을 찾을 수 없습니다")) {
      showSignupResult("등록처리 상태를 찾지 못했습니다.\n가입정보를 다시 입력하고 인증번호 받기를 진행해 주세요.");
    }
  }

  function wire() {
    $("#btnLogin").addEventListener("click", () => {
      login().catch((e) => setMsg($("#loginMsg"), e.message || String(e), true));
    });
    $("#btnGuestAccess")?.addEventListener("click", () => {
      guestAccess().catch((e) => setMsg($("#loginMsg"), e.message || String(e), true));
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
    $("#btnCompleteSignup")?.addEventListener("click", () => {
      completeSignup().catch((e) => setSignupCompleteMsg(e.message || String(e), true));
    });
    $("#btnCheckLoginId")?.addEventListener("click", () => {
      checkSignupLoginIdAvailability({ force: true })
        .then((available) => {
          if (available) tryTutorialAutoAdvance("suLoginId").catch(() => {});
        })
        .catch((e) => setLoginIdMsg(e.message || String(e), true));
    });
    $("#suLoginId")?.addEventListener("input", () => {
      scheduleSignupLoginIdCheck();
    });
    $("#suPhone")?.addEventListener("input", () => {
      if (lastCheckedLoginId) scheduleSignupLoginIdCheck();
      refreshSignupPasswordStrength();
    });
    $("#btnPrepareSiteReg")?.addEventListener("click", () => {
      prepareSiteRegisterReservation().catch((e) => setSiteRegMsg(e.message || String(e), true));
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
    $("#suPassword")?.addEventListener("input", () => {
      refreshSignupPasswordStrength();
    });
    $("#suPassword2")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") completeSignup().catch((err) => setSignupCompleteMsg(err.message || String(err), true));
    });
    $("#bsPassword2").addEventListener("keydown", (e) => {
      if (e.key === "Enter") bootstrap().catch((err) => setMsg($("#bootstrapMsg"), err.message || String(err), true));
    });
    wireSignupTutorialAutoAdvance();
  }

  async function init() {
    applySignupPasswordPolicy(null);
    resetSignupFinalizeState();
    wire();
    if (wantsGuestAutoEntry()) {
      await guestAccess();
      return;
    }
    applySignupReadyContext();
    await loadBootstrapStatus();
    await checkAlreadyLoggedIn();
    focusTutorialStartField();
  }

  try {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/pwa/sw.js?v=20260208a").catch(() => {});
    }
  } catch (_e) {}

  init().catch((e) => {
    setMsg($("#loginMsg"), e.message || String(e), true);
  });
})();

