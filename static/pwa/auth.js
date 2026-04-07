(() => {
  "use strict";

  const TOKEN_KEY = "ka_part_auth_token_v1";
  const USER_KEY = "ka_part_auth_user_v1";
  const LOGOUT_BROADCAST_KEY = "ka_part_logout_event_v1";
  const SENSITIVE_STORAGE_KEYS = [
    "ka_current_site_name_v1",
    "ka_current_site_code_v1",
    "ka_current_site_id_v1",
  ];

  function safeStorage(kind) {
    try {
      const store = window[kind];
      if (!store) return null;
      const probe = "__ka_probe__";
      store.setItem(probe, "1");
      store.removeItem(probe);
      return store;
    } catch (_e) {
      return null;
    }
  }

  const sessionStore = safeStorage("sessionStorage");
  const localStore = safeStorage("localStorage");

  function readStore(store, key) {
    try {
      const raw = store ? store.getItem(key) : "";
      return (raw || "").trim();
    } catch (_e) {
      return "";
    }
  }

  function writeStore(store, key, value) {
    try {
      if (store) store.setItem(key, value);
    } catch (_e) {}
  }

  function removeStore(store, key) {
    try {
      if (store) store.removeItem(key);
    } catch (_e) {}
  }

  function getToken() {
    return readStore(sessionStore, TOKEN_KEY) || readStore(localStore, TOKEN_KEY);
  }

  function getUser() {
    const raw = readStore(sessionStore, USER_KEY) || readStore(localStore, USER_KEY);
    try {
      return JSON.parse(raw || "null");
    } catch (_e) {
      return null;
    }
  }

  function setSession(token, user) {
    if (token) {
      if (sessionStore) writeStore(sessionStore, TOKEN_KEY, token);
      else writeStore(localStore, TOKEN_KEY, token);
    } else {
      removeStore(sessionStore, TOKEN_KEY);
      removeStore(localStore, TOKEN_KEY);
    }

    if (typeof user !== "undefined") {
      if (user) {
        const serialized = JSON.stringify(user);
        if (sessionStore) writeStore(sessionStore, USER_KEY, serialized);
        else writeStore(localStore, USER_KEY, serialized);
      } else {
        removeStore(sessionStore, USER_KEY);
        removeStore(localStore, USER_KEY);
      }
    }

    if (sessionStore && localStore) {
      removeStore(localStore, TOKEN_KEY);
      removeStore(localStore, USER_KEY);
    }
  }

  function clearSensitiveClientState() {
    for (const key of SENSITIVE_STORAGE_KEYS) {
      removeStore(sessionStore, key);
      removeStore(localStore, key);
    }
  }

  function broadcastLogout() {
    if (!localStore) return;
    try {
      localStore.setItem(LOGOUT_BROADCAST_KEY, String(Date.now()));
      localStore.removeItem(LOGOUT_BROADCAST_KEY);
    } catch (_e) {}
  }

  function clearSession(options = {}) {
    const includeSensitive = !!(options && options.includeSensitive);
    const broadcast = !!(options && options.broadcast);
    removeStore(sessionStore, TOKEN_KEY);
    removeStore(sessionStore, USER_KEY);
    removeStore(localStore, TOKEN_KEY);
    removeStore(localStore, USER_KEY);
    if (includeSensitive) clearSensitiveClientState();
    if (broadcast) broadcastLogout();
  }

  function loginUrl(nextPath) {
    const next = nextPath || `${window.location.pathname}${window.location.search}`;
    return `/pwa/login.html?next=${encodeURIComponent(next)}`;
  }

  function redirectLogin(nextPath, opts = {}) {
    const target = loginUrl(nextPath);
    const useReplace = !(opts && opts.replace === false);
    if (useReplace) {
      window.location.replace(target);
      return;
    }
    window.location.href = target;
  }

  function isLoginPage() {
    const p = String(window.location.pathname || "").toLowerCase();
    return p.endsWith("/pwa/login.html");
  }

  async function logout(nextPath = "", opts = {}) {
    const includeSensitive = !(opts && opts.includeSensitive === false);
    const broadcast = !(opts && opts.broadcast === false);
    const redirect = !(opts && opts.redirect === false);
    try {
      const headers = {};
      const token = getToken();
      if (token) headers.Authorization = `Bearer ${token}`;
      await fetch("/api/auth/logout", { method: "POST", credentials: "same-origin", headers });
    } catch (_e) {}

    clearSession({ includeSensitive, broadcast });
    if (redirect) redirectLogin(nextPath);
  }

  function errorMessage(body, fallback) {
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

    const fetchOpts = { ...opts, headers, credentials: "same-origin" };
    delete fetchOpts.noAuth;
    const res = await fetch(url, fetchOpts);
    const ct = res.headers.get("content-type") || "";
    const body = ct.includes("application/json") ? await res.json() : await res.text();

    if (res.status === 401) {
      if (!noAuth) {
        clearSession({ includeSensitive: true, broadcast: true });
        redirectLogin();
      }
      throw new Error(errorMessage(body, "401"));
    }
    if (!res.ok) {
      throw new Error(errorMessage(body, `${res.status}`));
    }
    return body;
  }

  async function requireAuth() {
    const me = await requestJson("/api/auth/me");
    if (!me || !me.user) {
      clearSession({ includeSensitive: true, broadcast: false });
      redirectLogin();
      throw new Error("로그인이 필요합니다.");
    }
    setSession(getToken(), me.user);
    return me.user;
  }

  if (window.addEventListener) {
    window.addEventListener("storage", (event) => {
      if (!event || event.key !== LOGOUT_BROADCAST_KEY) return;
      clearSession({ includeSensitive: true, broadcast: false });
      if (!isLoginPage()) redirectLogin();
    });
  }

  window.KAAuth = {
    TOKEN_KEY,
    USER_KEY,
    LOGOUT_BROADCAST_KEY,
    getToken,
    getUser,
    setSession,
    clearSession,
    clearSensitiveClientState,
    logout,
    loginUrl,
    redirectLogin,
    requestJson,
    requireAuth,
  };
})();
