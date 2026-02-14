(() => {
  "use strict";

  const TOKEN_KEY = "ka_part_auth_token_v1";
  const USER_KEY = "ka_part_auth_user_v1";
  const LOGOUT_BROADCAST_KEY = "ka_part_logout_event_v1";
  const SENSITIVE_STORAGE_KEYS = [
    "ka_current_site_name_v1",
    "ka_current_site_code_v1",
    "ka_current_site_id_v1",
    "ka_home_draft_v2",
    "ka_unit_selector_recent_v1",
    "ka_unit_selector_favorites_v1",
  ];

  function _safeStorage(kind) {
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

  const sessionStore = _safeStorage("sessionStorage");
  const localStore = _safeStorage("localStorage");

  function _readStore(store, key) {
    try {
      const raw = store ? store.getItem(key) : "";
      return (raw || "").trim();
    } catch (_e) {
      return "";
    }
  }

  function _writeStore(store, key, value) {
    try {
      if (store) store.setItem(key, value);
    } catch (_e) {}
  }

  function _removeStore(store, key) {
    try {
      if (store) store.removeItem(key);
    } catch (_e) {}
  }

  function _migrateLegacySession() {
    if (!sessionStore || !localStore) return;
    const legacyToken = _readStore(localStore, TOKEN_KEY);
    const legacyUser = _readStore(localStore, USER_KEY);
    if (legacyToken && !_readStore(sessionStore, TOKEN_KEY)) {
      _writeStore(sessionStore, TOKEN_KEY, legacyToken);
    }
    if (legacyUser && !_readStore(sessionStore, USER_KEY)) {
      _writeStore(sessionStore, USER_KEY, legacyUser);
    }
    if (legacyToken || legacyUser) {
      _removeStore(localStore, TOKEN_KEY);
      _removeStore(localStore, USER_KEY);
    }
  }

  function getToken() {
    _migrateLegacySession();
    const token = _readStore(sessionStore, TOKEN_KEY);
    if (token) return token;
    return _readStore(localStore, TOKEN_KEY);
  }

  function getUser() {
    _migrateLegacySession();
    const raw = _readStore(sessionStore, USER_KEY) || _readStore(localStore, USER_KEY);
    try {
      return JSON.parse(raw || "null");
    } catch (_e) {
      return null;
    }
  }

  function setSession(token, user) {
    if (token) {
      if (sessionStore) _writeStore(sessionStore, TOKEN_KEY, token);
      else _writeStore(localStore, TOKEN_KEY, token);
    } else {
      _removeStore(sessionStore, TOKEN_KEY);
      _removeStore(localStore, TOKEN_KEY);
    }

    if (typeof user !== "undefined") {
      if (user) {
        const serialized = JSON.stringify(user);
        if (sessionStore) _writeStore(sessionStore, USER_KEY, serialized);
        else _writeStore(localStore, USER_KEY, serialized);
      } else {
        _removeStore(sessionStore, USER_KEY);
        _removeStore(localStore, USER_KEY);
      }
    }

    if (sessionStore && localStore) {
      _removeStore(localStore, TOKEN_KEY);
      _removeStore(localStore, USER_KEY);
    }
  }

  function clearSensitiveClientState() {
    for (const key of SENSITIVE_STORAGE_KEYS) {
      _removeStore(sessionStore, key);
      _removeStore(localStore, key);
    }
  }

  function _broadcastLogout() {
    if (!localStore) return;
    try {
      localStore.setItem(LOGOUT_BROADCAST_KEY, String(Date.now()));
      localStore.removeItem(LOGOUT_BROADCAST_KEY);
    } catch (_e) {}
  }

  function clearSession(options = {}) {
    const includeSensitive = !!(options && options.includeSensitive);
    const broadcast = !!(options && options.broadcast);
    _removeStore(sessionStore, TOKEN_KEY);
    _removeStore(sessionStore, USER_KEY);
    _removeStore(localStore, TOKEN_KEY);
    _removeStore(localStore, USER_KEY);
    if (includeSensitive) clearSensitiveClientState();
    if (broadcast) _broadcastLogout();
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

  function _isLoginPage() {
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
    try {
      const creds = navigator.credentials;
      if (creds && typeof creds.preventSilentAccess === "function") {
        await creds.preventSilentAccess();
      }
    } catch (_e) {}
    if (redirect) redirectLogin(nextPath);
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
      if (!noAuth) {
        clearSession();
        redirectLogin();
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
    try {
      const me = await requestJson("/api/auth/me", { noAuth: !token });
      if (!me || !me.user) throw new Error("로그인이 필요합니다.");
      setSession(token, me.user);
      return me.user;
    } catch (_e) {
      clearSession();
      redirectLogin();
      throw new Error("로그인이 필요합니다.");
    }
  }

  if (window.addEventListener) {
    window.addEventListener("storage", (event) => {
      if (!event || event.key !== LOGOUT_BROADCAST_KEY) return;
      clearSession({ includeSensitive: true, broadcast: false });
      if (!_isLoginPage()) redirectLogin();
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
