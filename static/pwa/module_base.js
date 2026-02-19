(() => {
  "use strict";

  const EMPTY_SCOPE = {
    site_code: "",
    site_name: "",
    site_id: 0,
  };
  const EMPTY_CONTRACT_PAYLOAD = {
    ok: false,
    allowed_modules: [],
    contracts: [],
  };
  let contractsPromise = null;

  function normalizePositiveInt(value, fallback = 0) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return Number(fallback || 0);
    return Math.trunc(n);
  }

  function normalizeSiteScope(scope = {}) {
    return {
      site_code: String(scope.site_code || "").trim().toUpperCase(),
      site_name: String(scope.site_name || "").trim(),
      site_id: normalizePositiveInt(scope.site_id || 0, 0),
    };
  }

  function parseSiteFromQuery() {
    try {
      const q = new URLSearchParams(window.location.search || "");
      return normalizeSiteScope({
        site_code: q.get("site_code") || "",
        site_name: q.get("site_name") || "",
        site_id: q.get("site_id") || 0,
      });
    } catch (_e) {
      return { ...EMPTY_SCOPE };
    }
  }

  function appendQuery(basePath, params = {}) {
    const raw = String(basePath || "").trim() || "/";
    const base = raw.startsWith("http://") || raw.startsWith("https://") ? raw : `${window.location.origin}${raw.startsWith("/") ? "" : "/"}${raw}`;
    const url = new URL(base);
    for (const [key, value] of Object.entries(params || {})) {
      const cleanKey = String(key || "").trim();
      if (!cleanKey) continue;
      const str = String(value == null ? "" : value).trim();
      if (!str) {
        url.searchParams.delete(cleanKey);
        continue;
      }
      url.searchParams.set(cleanKey, str);
    }
    if (raw.startsWith("http://") || raw.startsWith("https://")) {
      return url.toString();
    }
    return `${url.pathname}${url.search}${url.hash}`;
  }

  function normalizePolicy(policy = {}, fallbackDefault = 100, fallbackMax = 500) {
    const baseDefault = normalizePositiveInt(fallbackDefault, 100) || 100;
    const baseMaxRaw = normalizePositiveInt(fallbackMax, 500) || 500;
    const baseMax = baseMaxRaw >= baseDefault ? baseMaxRaw : baseDefault;
    const d = normalizePositiveInt(policy.default_limit ?? policy.defaultLimit, baseDefault) || baseDefault;
    let m = normalizePositiveInt(policy.max_limit ?? policy.maxLimit, baseMax) || baseMax;
    if (m < d) m = d;
    return {
      default_limit: d,
      max_limit: m,
      defaultLimit: d,
      maxLimit: m,
    };
  }

  function clampLimit(value, policy = {}, fallback = 0) {
    const fallbackDefault = normalizePositiveInt(fallback, 100) || 100;
    const normalized = normalizePolicy(policy, fallbackDefault, Math.max(fallbackDefault, 500));
    let out = normalizePositiveInt(value, 0);
    if (out <= 0) out = fallbackDefault;
    if (out > normalized.max_limit) out = normalized.max_limit;
    if (out < 1) out = 1;
    return out;
  }

  async function fetchContracts() {
    if (!window.KAAuth || typeof window.KAAuth.requestJson !== "function") {
      return EMPTY_CONTRACT_PAYLOAD;
    }
    if (!contractsPromise) {
      contractsPromise = window.KAAuth.requestJson("/api/modules/contracts")
        .then((data) => (data && typeof data === "object" ? data : EMPTY_CONTRACT_PAYLOAD))
        .catch(() => EMPTY_CONTRACT_PAYLOAD);
    }
    return await contractsPromise;
  }

  function contractByKey(payload, moduleKey) {
    const key = String(moduleKey || "").trim();
    if (!key) return null;
    const items = Array.isArray(payload && payload.contracts) ? payload.contracts : [];
    for (const item of items) {
      if (String(item && item.module_key || "").trim() === key) return item;
    }
    return null;
  }

  async function bootstrap(moduleKey, opts = {}) {
    if (!window.KAAuth || typeof window.KAAuth.requireAuth !== "function") {
      throw new Error("auth.js가 로드되지 않았습니다.");
    }
    const user = await window.KAAuth.requireAuth();
    const payload = await fetchContracts();
    const contract = contractByKey(payload, moduleKey);

    const q = parseSiteFromQuery();
    const scope = normalizeSiteScope({
      site_code: q.site_code || user.site_code || "",
      site_name: q.site_name || user.site_name || "",
      site_id: q.site_id || user.site_id || 0,
    });

    const fallbackDefault = normalizePositiveInt(opts.defaultLimit || opts.default_limit, 100) || 100;
    const fallbackMax = normalizePositiveInt(opts.maxLimit || opts.max_limit, 500) || 500;
    const policy = normalizePolicy(contract || {}, fallbackDefault, fallbackMax);

    const withSite = (basePath, overrides = {}) => {
      const overrideScope = normalizeSiteScope({
        site_code: overrides.site_code ?? scope.site_code,
        site_name: overrides.site_name ?? scope.site_name,
        site_id: overrides.site_id ?? scope.site_id,
      });
      const extra = { ...(overrides || {}) };
      delete extra.site_code;
      delete extra.site_name;
      delete extra.site_id;
      return appendQuery(basePath, {
        ...overrideScope,
        ...extra,
      });
    };

    const allowedModules = Array.isArray(payload && payload.allowed_modules)
      ? payload.allowed_modules.map((x) => String(x || "").trim()).filter(Boolean)
      : [];

    return {
      user,
      moduleKey: String(moduleKey || "").trim(),
      contract: contract || null,
      allowedModules,
      siteCode: scope.site_code,
      siteName: scope.site_name,
      siteId: scope.site_id,
      policy,
      clampLimit(value, fallback = 0) {
        return clampLimit(value, policy, fallback);
      },
      withSite,
    };
  }

  window.KAModuleBase = {
    normalizePositiveInt,
    normalizeSiteScope,
    parseSiteFromQuery,
    appendQuery,
    normalizePolicy,
    clampLimit,
    fetchContracts,
    bootstrap,
  };
})();
