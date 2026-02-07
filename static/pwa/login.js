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
    $("#password").addEventListener("keydown", (e) => {
      if (e.key === "Enter") login().catch((err) => setMsg($("#loginMsg"), err.message || String(err), true));
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
