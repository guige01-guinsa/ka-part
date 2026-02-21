(() => {
  "use strict";

  const NOTICE_SELECTOR = ".msg, .feed-msg, .toast, .maintenance-banner";
  const NOTICE_FLASH_CLASS = "msg-attention";
  const NOTICE_FLASH_ERR_CLASS = "msg-attention-err";

  function pulseNoticeElement(el) {
    if (!el || !el.classList) return;
    const text = String(el.textContent || "").trim();
    if (!text) return;
    const isErr = el.classList.contains("err");
    try {
      el.classList.remove(NOTICE_FLASH_CLASS, NOTICE_FLASH_ERR_CLASS);
      // Force reflow so repeated same message can replay the animation.
      void el.offsetWidth;
      el.classList.add(NOTICE_FLASH_CLASS);
      if (isErr) el.classList.add(NOTICE_FLASH_ERR_CLASS);
      if (el._kaNoticeTimer) window.clearTimeout(el._kaNoticeTimer);
      el._kaNoticeTimer = window.setTimeout(() => {
        el.classList.remove(NOTICE_FLASH_CLASS, NOTICE_FLASH_ERR_CLASS);
      }, 900);
    } catch (_e) {}
  }

  function findNoticeHost(node) {
    let cur = node;
    if (cur && cur.nodeType === Node.TEXT_NODE) cur = cur.parentElement;
    while (cur && cur.nodeType === Node.ELEMENT_NODE) {
      if (cur.matches && cur.matches(NOTICE_SELECTOR)) return cur;
      cur = cur.parentElement;
    }
    return null;
  }

  function installNoticePulseObserver() {
    if (window.__KA_NOTICE_PULSE_INSTALLED) return;
    window.__KA_NOTICE_PULSE_INSTALLED = true;
    const root = document.body || document.documentElement;
    if (!root || typeof MutationObserver !== "function") return;
    const observer = new MutationObserver((mutations) => {
      const targets = new Set();
      for (const m of mutations) {
        const host = findNoticeHost(m.target);
        if (host) targets.add(host);
        if (m.type === "childList" && m.addedNodes) {
          for (const n of m.addedNodes) {
            if (!n || n.nodeType !== Node.ELEMENT_NODE) continue;
            const el = n.matches && n.matches(NOTICE_SELECTOR) ? n : (n.querySelector ? n.querySelector(NOTICE_SELECTOR) : null);
            if (el) targets.add(el);
          }
        }
      }
      if (!targets.size) return;
      window.requestAnimationFrame(() => {
        for (const el of targets) pulseNoticeElement(el);
      });
    });
    observer.observe(root, { subtree: true, childList: true, characterData: true });
  }

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
    pulseNoticeElement,
    stripSiteIdentityFromUrl,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", installNoticePulseObserver, { once: true });
  } else {
    installNoticePulseObserver();
  }
})();
