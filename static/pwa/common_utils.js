(() => {
  "use strict";

  function isSuperAdmin(user) {
    if (!user || !user.is_admin) return false;
    return String(user.admin_scope || "").trim().toLowerCase() === "super_admin";
  }

  function canViewSiteIdentity(user) {
    return isSuperAdmin(user);
  }

  function normalizeSiteId(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    const id = Math.trunc(n);
    return id > 0 ? id : 0;
  }

  function stripSiteIdentityFromUrl() {
    try {
      const u = new URL(window.location.href);
      let changed = false;
      if (u.searchParams.has("site_name")) {
        u.searchParams.delete("site_name");
        changed = true;
      }
      if (u.searchParams.has("site_code")) {
        u.searchParams.delete("site_code");
        changed = true;
      }
      if (!changed) return;
      const next = `${u.pathname}${u.searchParams.toString() ? `?${u.searchParams.toString()}` : ""}`;
      window.history.replaceState({}, "", next);
    } catch (_e) {}
  }

  function parseDownloadFilename(contentDisposition, fallbackName) {
    const value = String(contentDisposition || "");
    const utf8 = /filename\*=UTF-8''([^;]+)/i.exec(value);
    if (utf8 && utf8[1]) {
      try {
        return decodeURIComponent(utf8[1]);
      } catch (_e) {}
    }
    const plain = /filename=\"?([^\";]+)\"?/i.exec(value);
    if (plain && plain[1]) return plain[1];
    return String(fallbackName || "download.bin");
  }

  async function authJson(url, opts = {}) {
    if (!window.KAAuth || typeof window.KAAuth.requestJson !== "function") {
      throw new Error("KAAuth.requestJson is not available");
    }
    return window.KAAuth.requestJson(url, opts);
  }

  window.KAUtil = {
    authJson,
    canViewSiteIdentity,
    isSuperAdmin,
    normalizeSiteId,
    parseDownloadFilename,
    stripSiteIdentityFromUrl,
  };
})();

