(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);

  function nextPath() {
    const u = new URL(window.location.href);
    return u.searchParams.get("next") || "/pwa/";
  }

  function goNext() {
    window.location.href = nextPath();
  }

  function setMsg(el, msg, isErr = false) {
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function showSignupResult(text) {
    const el = $("#signupResult");
    if (!el) return;
    el.textContent = text || "";
    el.classList.toggle("hidden", !text);
  }

  function signupPayloadFromForm() {
    return {
      name: ($("#suName").value || "").trim(),
      phone: ($("#suPhone").value || "").trim(),
      site_name: ($("#suSiteName").value || "").trim(),
      role: ($("#suRole").value || "").trim(),
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
    goNext();
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
    goNext();
  }

  async function checkAlreadyLoggedIn() {
    const token = KAAuth.getToken();
    if (!token) return;
    try {
      const me = await KAAuth.requestJson("/api/auth/me");
      if (me && me.user) {
        KAAuth.setSession(token, me.user);
        goNext();
      }
    } catch (_e) {
      KAAuth.clearSession();
    }
  }

  async function loadBootstrapStatus() {
    const data = await fetch("/api/auth/bootstrap_status").then((r) => r.json());
    const card = $("#bootstrapCard");
    if (card) card.classList.toggle("hidden", !data.required);
  }

  function wire() {
    $("#btnLogin").addEventListener("click", () => {
      login().catch((e) => setMsg($("#loginMsg"), e.message || String(e), true));
    });
    $("#btnBootstrap").addEventListener("click", () => {
      bootstrap().catch((e) => setMsg($("#bootstrapMsg"), e.message || String(e), true));
    });
    $("#btnReqCode").addEventListener("click", () => {
      requestSignupCode().catch((e) => setMsg($("#signupMsg"), e.message || String(e), true));
    });
    $("#btnVerifySignup").addEventListener("click", () => {
      verifySignupAndIssueId().catch((e) => setMsg($("#signupMsg"), e.message || String(e), true));
    });
    $("#password").addEventListener("keydown", (e) => {
      if (e.key === "Enter") login().catch((err) => setMsg($("#loginMsg"), err.message || String(err), true));
    });
    $("#suCode").addEventListener("keydown", (e) => {
      if (e.key === "Enter") verifySignupAndIssueId().catch((err) => setMsg($("#signupMsg"), err.message || String(err), true));
    });
    $("#bsPassword2").addEventListener("keydown", (e) => {
      if (e.key === "Enter") bootstrap().catch((err) => setMsg($("#bootstrapMsg"), err.message || String(err), true));
    });
  }

  async function init() {
    wire();
    await checkAlreadyLoggedIn();
    await loadBootstrapStatus();
  }

  init().catch((e) => {
    setMsg($("#loginMsg"), e.message || String(e), true);
  });
})();

