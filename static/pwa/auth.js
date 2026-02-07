(() => {
  "use strict";

  const TOKEN_KEY = "ka_part_auth_token_v1";
  const USER_KEY = "ka_part_auth_user_v1";

  function getToken() {
    return (localStorage.getItem(TOKEN_KEY) || "").trim();
  }

  function getUser() {
    try {
      return JSON.parse(localStorage.getItem(USER_KEY) || "null");
    } catch (_e) {
      return null;
    }
  }

  function setSession(token, user) {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    if (user) localStorage.setItem(USER_KEY, JSON.stringify(user));
  }

  function clearSession() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  }

  function loginUrl(nextPath) {
    const next = nextPath || `${window.location.pathname}${window.location.search}`;
    return `/pwa/login.html?next=${encodeURIComponent(next)}`;
  }

  function redirectLogin(nextPath) {
    window.location.href = loginUrl(nextPath);
  }

  function _errorMessage(body, fallback) {
    if (!body) return fallback;
    if (typeof body === "string") return body || fallback;
    if (body.detail) return String(body.detail);
    if (body.message) return String(body.message);
    return fallback;
  }

  async function requestJson(url, opts = {}) {
    const noAuth = !!opts.noAuth;
    const headers = { ...(opts.headers || {}) };
    const hasBody = Object.prototype.hasOwnProperty.call(opts, "body");
    if (hasBody && !headers["Content-Type"]) {
      headers["Content-Type"] = "application/json";
    }
    const token = getToken();
    if (!noAuth && token && !headers.Authorization) {
      headers.Authorization = `Bearer ${token}`;
    }

    const fetchOpts = { ...opts, headers };
    delete fetchOpts.noAuth;
    const res = await fetch(url, fetchOpts);
    const ct = res.headers.get("content-type") || "";
    const body = ct.includes("application/json") ? await res.json() : await res.text();

    if (res.status === 401) {
      if (!noAuth && token) {
        clearSession();
        redirectLogin();
        throw new Error("로그인이 필요합니다.");
      }
      throw new Error(_errorMessage(body, "401"));
    }
    if (!res.ok) {
      throw new Error(_errorMessage(body, `${res.status}`));
    }
    return body;
  }

  async function requireAuth() {
    const token = getToken();
    if (!token) {
      redirectLogin();
      throw new Error("로그인이 필요합니다.");
    }
    const me = await requestJson("/api/auth/me");
    if (!me || !me.user) {
      clearSession();
      redirectLogin();
      throw new Error("로그인이 필요합니다.");
    }
    setSession(token, me.user);
    return me.user;
  }

  window.KAAuth = {
    TOKEN_KEY,
    USER_KEY,
    getToken,
    getUser,
    setSession,
    clearSession,
    loginUrl,
    redirectLogin,
    requestJson,
    requireAuth,
  };
})();
