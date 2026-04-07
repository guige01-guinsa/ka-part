(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);

  function setMessage(selector, message, isError = false) {
    const el = $(selector);
    if (!el) return;
    el.textContent = String(message || "");
    el.classList.toggle("error", !!isError);
  }

  function nextPath() {
    try {
      const url = new URL(window.location.href);
      const next = String(url.searchParams.get("next") || "").trim();
      return next || "/pwa/";
    } catch (_e) {
      return "/pwa/";
    }
  }

  function clearRegisterForm() {
    ["#regName", "#regLoginId", "#regPhone", "#regPassword", "#regPassword2"].forEach((sel) => {
      const el = $(sel);
      if (el) el.value = "";
    });
  }

  function renderRegisterOptions(items) {
    const select = $("#regTenantId");
    if (!select) return;
    const rows = Array.isArray(items) ? items : [];
    select.innerHTML = "";
    if (!rows.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "가입 가능한 단지가 없습니다.";
      select.appendChild(option);
      return;
    }
    rows.forEach((item) => {
      const option = document.createElement("option");
      option.value = String(item.id || "").trim();
      const name = String(item.name || item.site_name || item.id || "").trim();
      option.textContent = String(item.site_code || "").trim()
        ? `${name} (${String(item.site_code || "").trim()})`
        : name;
      select.appendChild(option);
    });
  }

  async function bootstrapStatus() {
    const data = await window.KAAuth.requestJson("/api/auth/bootstrap_status", { noAuth: true });
    const needsBootstrap = !!(data && data.needs_bootstrap);
    $("#bootstrapCard")?.classList.toggle("hidden", !needsBootstrap);
    return needsBootstrap;
  }

  async function registerOptions() {
    const data = await window.KAAuth.requestJson("/api/auth/register_options", { noAuth: true });
    const enabled = !!(data && data.enabled);
    renderRegisterOptions((data && data.items) || []);
    $("#btnRegister")?.toggleAttribute("disabled", !enabled);
    if (!enabled) {
      setMessage("#registerMsg", "현재는 회원등록 가능한 단지가 없습니다.", true);
    }
    return enabled;
  }

  async function doLogin() {
    const loginId = String($("#loginId").value || "").trim().toLowerCase();
    const password = String($("#password").value || "");
    if (!loginId || !password) throw new Error("아이디와 비밀번호를 입력하세요.");
    const data = await window.KAAuth.requestJson("/api/auth/login", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({ login_id: loginId, password }),
    });
    if (!data || !data.user) throw new Error("로그인 응답이 올바르지 않습니다.");
    window.KAAuth.setSession("", data.user);
    window.location.replace(String(nextPath() || data.landing_path || "/pwa/"));
  }

  async function doBootstrap() {
    const loginId = String($("#bsLoginId").value || "").trim().toLowerCase();
    const name = String($("#bsName").value || "").trim();
    const password = String($("#bsPassword").value || "");
    const password2 = String($("#bsPassword2").value || "");
    if (!loginId || !name || !password || !password2) throw new Error("모든 항목을 입력하세요.");
    if (password !== password2) throw new Error("비밀번호 확인이 일치하지 않습니다.");
    const data = await window.KAAuth.requestJson("/api/auth/bootstrap", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({ login_id: loginId, name, password }),
    });
    if (!data || !data.user) throw new Error("초기 관리자 생성에 실패했습니다.");
    window.KAAuth.setSession("", data.user);
    window.location.replace(String(data.landing_path || "/pwa/"));
  }

  async function doRegister() {
    const tenantId = String($("#regTenantId").value || "").trim().toLowerCase();
    const name = String($("#regName").value || "").trim();
    const loginId = String($("#regLoginId").value || "").trim().toLowerCase();
    const phone = String($("#regPhone").value || "").trim();
    const password = String($("#regPassword").value || "");
    const password2 = String($("#regPassword2").value || "");
    if (!tenantId || !name || !loginId || !password || !password2) throw new Error("모든 회원등록 항목을 입력하세요.");
    if (password !== password2) throw new Error("비밀번호 확인이 일치하지 않습니다.");
    const data = await window.KAAuth.requestJson("/api/auth/register", {
      method: "POST",
      noAuth: true,
      body: JSON.stringify({ tenant_id: tenantId, name, login_id: loginId, phone, password }),
    });
    clearRegisterForm();
    setMessage("#registerMsg", data.message || "회원등록 요청이 접수되었습니다.");
  }

  function wire() {
    $("#btnLogin")?.addEventListener("click", () => {
      setMessage("#loginMsg", "");
      doLogin().catch((error) => setMessage("#loginMsg", error.message || String(error), true));
    });
    $("#btnShowRegister")?.addEventListener("click", () => {
      const card = $("#registerCard");
      if (!card) {
        return;
      }
      card.scrollIntoView({ behavior: "smooth", block: "start" });
      $("#regTenantId")?.focus();
    });
    $("#btnRegister")?.addEventListener("click", () => {
      setMessage("#registerMsg", "");
      doRegister().catch((error) => setMessage("#registerMsg", error.message || String(error), true));
    });
    $("#btnBootstrap")?.addEventListener("click", () => {
      setMessage("#bootstrapMsg", "");
      doBootstrap().catch((error) => setMessage("#bootstrapMsg", error.message || String(error), true));
    });
  }

  async function init() {
    const token = window.KAAuth.getToken();
    if (token) {
      try {
        const me = await window.KAAuth.requestJson("/api/auth/me");
        if (me && me.user) {
          window.KAAuth.setSession(token, me.user);
          window.location.replace(String(me.landing_path || "/pwa/"));
          return;
        }
      } catch (_e) {
        window.KAAuth.clearSession({ includeSensitive: true, broadcast: false });
      }
    }
    await bootstrapStatus();
    await registerOptions();
  }

  wire();
  init().catch((error) => {
    setMessage("#loginMsg", error.message || String(error), true);
  });
})();
