(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  const SITE_ID_KEY = "ka_current_site_id_v1";
  const KAUtil = window.KAUtil;
  const TAB_ORDER = [
    "home",
    "tr1",
    "tr2",
    "tr3",
    "tr4",
    "tr5",
    "tr6",
    "main_vcb",
    "dc_panel",
    "temperature",
    "meter",
    "facility",
    "facility_check",
    "facility_fire",
    "facility_mechanical",
    "facility_telecom",
  ];
  const DEFAULT_PDF_PROFILE_ID = "substation_daily_a4";
  const PDF_PROFILE_OPTIONS = [
    { id: "substation_daily_a4", label: "수변전 점검일지 A4 (기본)" },
    { id: "substation_daily_ami4_a4", label: "수배전반(검침)점검일지 A4" },
    { id: "substation_daily_generic_a4", label: "범용 점검일지 A4" },
  ];

  let me = null;
  let moduleCtx = null;
  let templates = [];
  let baseSchema = {};
  let activeSchema = {};
  let handlingSiteCodeConflict = false;
  let specEnvManageCodePolicy = {
    loaded: false,
    enabled: false,
    required: false,
    role_bucket: "",
    role_label: "",
    ttl_sec: 0,
  };
  let specEnvManageCodeToken = "";
  let specEnvManageCodeTokenExpiresAtTs = 0;
  const ACTION_SUCCESS_BUTTON_IDS = ["btnTemplateApply", "btnSave", "btnPreview", "btnGoMain"];

  function canManageSpecEnv(user) {
    return !!(user && (user.is_admin || user.is_site_admin));
  }

  function isAdmin(user) {
    return !!(user && user.is_admin);
  }

  function canManageSiteCodeMigration(user) {
    return KAUtil.isSuperAdmin(user);
  }

  function applySiteIdentityVisibility() {
    const show = KAUtil.canViewSiteIdentity(me);
    const siteWrap = $("#siteName")?.closest(".field");
    const codeWrap = $("#siteCode")?.closest(".field");
    if (siteWrap) siteWrap.classList.toggle("hidden", !show);
    if (codeWrap) codeWrap.classList.toggle("hidden", !show);

    const listCard = $("#siteList")?.closest("section.card");
    if (listCard) listCard.hidden = !show;
    if (!show) KAUtil.stripSiteIdentityFromUrl();
  }

  function setMsg(msg, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function setMigrationMsg(msg, isErr = false) {
    const el = $("#migMsg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function resetSpecEnvManageCodeToken() {
    specEnvManageCodeToken = "";
    specEnvManageCodeTokenExpiresAtTs = 0;
  }

  function setSpecEnvManageCodeToken(token, expiresAt, expiresInSec = 0) {
    const cleanToken = String(token || "").trim();
    const tsFromDate = Date.parse(String(expiresAt || "").trim());
    const nowMs = Date.now();
    let expiresMs = Number.isFinite(tsFromDate) ? tsFromDate : 0;
    if (expiresMs <= nowMs) {
      const addSec = Number(expiresInSec || 0);
      if (Number.isFinite(addSec) && addSec > 0) {
        expiresMs = nowMs + addSec * 1000;
      }
    }
    specEnvManageCodeToken = cleanToken;
    specEnvManageCodeTokenExpiresAtTs = expiresMs > nowMs ? expiresMs : 0;
  }

  function hasValidSpecEnvManageCodeToken() {
    const token = String(specEnvManageCodeToken || "").trim();
    if (!token) return false;
    if (specEnvManageCodeTokenExpiresAtTs <= 0) return false;
    return Date.now() + 1500 < specEnvManageCodeTokenExpiresAtTs;
  }

  function isSpecEnvManageCodeErrorMessage(msg) {
    const text = String(msg || "").toLowerCase();
    return text.includes("관리코드") || text.includes("manage code");
  }

  function showMigrationCard(scrollIntoView = false) {
    const card = $("#migrationCard");
    if (!card) return false;
    const allowed = canManageSiteCodeMigration(me);
    card.hidden = !allowed;
    if (allowed && scrollIntoView) {
      card.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    return allowed;
  }

  function clearActionSuccess(exceptButtonId = "") {
    for (const id of ACTION_SUCCESS_BUTTON_IDS) {
      if (exceptButtonId && id === exceptButtonId) continue;
      const btn = document.getElementById(id);
      if (!btn) continue;
      btn.classList.remove("action-success", "action-success-pulse");
      btn.removeAttribute("data-ok-icon");
      btn.removeAttribute("aria-current");
    }
  }

  function markActionSuccess(button, icon = "✓") {
    if (!button) return;
    const targetId = String(button.id || "");
    clearActionSuccess(targetId);
    button.classList.add("action-success");
    button.setAttribute("data-ok-icon", icon);
    button.setAttribute("aria-current", "true");
    button.classList.remove("action-success-pulse");
    void button.offsetWidth;
    button.classList.add("action-success-pulse");
    clearTimeout(button._actionPulseTimer);
    button._actionPulseTimer = setTimeout(() => button.classList.remove("action-success-pulse"), 560);
  }

  function clone(v) {
    return JSON.parse(JSON.stringify(v || {}));
  }

  function sortTabKeys(keys) {
    return [...keys].sort((a, b) => {
      const ia = TAB_ORDER.indexOf(a);
      const ib = TAB_ORDER.indexOf(b);
      const va = ia >= 0 ? ia : 999;
      const vb = ib >= 0 ? ib : 999;
      return va - vb || a.localeCompare(b);
    });
  }

  function escapeHtml(v) {
    return String(v)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function escapeHtmlAttr(v) {
    return String(v)
      .replaceAll("&", "&amp;")
      .replaceAll('"', "&quot;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function getSiteName() {
    return ($("#siteName").value || "").trim();
  }

  function setSiteName(name) {
    const v = (name || "").trim();
    $("#siteName").value = v;
    if (v) localStorage.setItem(SITE_KEY, v);
  }

  function getSiteCode() {
    return ($("#siteCode").value || "").trim().toUpperCase();
  }

  function setSiteCode(code) {
    const v = (code || "").trim().toUpperCase();
    $("#siteCode").value = v;
    if (v) localStorage.setItem(SITE_CODE_KEY, v);
    else localStorage.removeItem(SITE_CODE_KEY);
  }

  function getSiteId() {
    return KAUtil.normalizeSiteId(localStorage.getItem(SITE_ID_KEY) || "");
  }

  function setSiteId(siteId) {
    const clean = KAUtil.normalizeSiteId(siteId);
    if (clean > 0) localStorage.setItem(SITE_ID_KEY, String(clean));
    else localStorage.removeItem(SITE_ID_KEY);
    return clean;
  }

  function buildSiteQuery(siteName, siteCode, siteId = null) {
    const qs = new URLSearchParams();
    const site = String(siteName || "").trim();
    const code = String(siteCode || "").trim().toUpperCase();
    const sid = KAUtil.normalizeSiteId(siteId == null ? getSiteId() : siteId);
    if (sid > 0) qs.set("site_id", String(sid));
    if (site) qs.set("site_name", site);
    if (code) qs.set("site_code", code);
    return qs.toString();
  }

  function withSitePath(basePath, overrides = {}) {
    const scope = {
      site_id: Object.prototype.hasOwnProperty.call(overrides, "site_id") ? overrides.site_id : getSiteId(),
      site_name: Object.prototype.hasOwnProperty.call(overrides, "site_name") ? overrides.site_name : getSiteName(),
      site_code: Object.prototype.hasOwnProperty.call(overrides, "site_code") ? overrides.site_code : getSiteCode(),
    };
    const extra = { ...(overrides || {}) };
    delete extra.site_id;
    delete extra.site_name;
    delete extra.site_code;

    if (moduleCtx && typeof moduleCtx.withSite === "function") {
      return moduleCtx.withSite(basePath, { ...scope, ...extra });
    }

    const qs = buildSiteQuery(scope.site_name, scope.site_code, scope.site_id);
    const params = new URLSearchParams(qs || "");
    for (const [k, v] of Object.entries(extra)) {
      const key = String(k || "").trim();
      const val = String(v == null ? "" : v).trim();
      if (!key || !val) continue;
      params.set(key, val);
    }
    if (!params.toString()) return basePath;
    const sep = String(basePath || "").includes("?") ? "&" : "?";
    return `${basePath}${sep}${params.toString()}`;
  }

  async function syncSiteIdentity(silent = true) {
    const site = getSiteName();
    const siteCode = getSiteCode();
    const siteId = getSiteId();
    if (!site && !siteCode && siteId <= 0) {
      if (!silent) setMsg("site_name 또는 site_code를 입력하세요.", true);
      return null;
    }
    const data = await jfetch(withSitePath("/api/site_identity", { site_name: site, site_code: siteCode, site_id: siteId }));
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(data.site_name || "");
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    return data || null;
  }

  function getConfigFromEditor() {
    const raw = ($("#envJson").value || "").trim();
    if (!raw) return {};
    const v = JSON.parse(raw);
    return v && typeof v === "object" ? v : {};
  }

  function setConfigToEditor(config, opts = {}) {
    $("#envJson").value = JSON.stringify(config || {}, null, 2);
    if (opts && opts.syncPdfProfile === false) return;
    syncPdfProfileSelectFromConfig(config || {});
  }

  function renderPdfProfileSelect(selectedProfileId = DEFAULT_PDF_PROFILE_ID) {
    const sel = $("#pdfProfileSelect");
    if (!sel) return;
    const selected = String(selectedProfileId || "").trim() || DEFAULT_PDF_PROFILE_ID;
    const options = [...PDF_PROFILE_OPTIONS];
    if (!options.some((x) => String(x.id || "").trim() === selected)) {
      options.push({ id: selected, label: `현재 설정 (${selected})` });
    }
    sel.innerHTML = "";
    for (const opt of options) {
      const o = document.createElement("option");
      o.value = String(opt.id || "").trim();
      o.textContent = String(opt.label || opt.id || "").trim();
      sel.appendChild(o);
    }
    sel.value = selected;
  }

  function getPdfProfileIdFromConfig(config) {
    const cfg = config && typeof config === "object" ? config : {};
    const report = cfg.report && typeof cfg.report === "object" ? cfg.report : {};
    return String(report.pdf_profile_id || "").trim() || DEFAULT_PDF_PROFILE_ID;
  }

  function syncPdfProfileSelectFromConfig(config = null) {
    let cfg = config;
    if (!cfg || typeof cfg !== "object") {
      try {
        cfg = compactConfig(getConfigFromEditor());
      } catch (_e) {
        cfg = {};
      }
    }
    renderPdfProfileSelect(getPdfProfileIdFromConfig(cfg));
  }

  function applyPdfProfileToEditor(profileId) {
    const selected = String(profileId || "").trim() || DEFAULT_PDF_PROFILE_ID;
    let cfg = {};
    try {
      cfg = compactConfig(getConfigFromEditor());
    } catch (e) {
      setMsg(`JSON 파싱 오류: ${e.message}`, true);
      syncPdfProfileSelectFromConfig({});
      return false;
    }
    if (!cfg.report || typeof cfg.report !== "object") cfg.report = {};
    cfg.report.pdf_profile_id = selected;
    setConfigToEditor(cfg, { syncPdfProfile: false });
    renderPdfProfileSelect(selected);
    return true;
  }

  function compactConfig(config) {
    const cfg = clone(config);
    if (!cfg || typeof cfg !== "object") return {};

    if (Array.isArray(cfg.hide_tabs)) {
      cfg.hide_tabs = [...new Set(cfg.hide_tabs.map((x) => String(x || "").trim()).filter(Boolean))];
      if (!cfg.hide_tabs.length) delete cfg.hide_tabs;
    } else {
      delete cfg.hide_tabs;
    }

    if (cfg.report && typeof cfg.report === "object") {
      const report = {};
      const profileId = String(cfg.report.pdf_profile_id || "").trim();
      if (profileId) report.pdf_profile_id = profileId;
      const lockedProfileId = String(cfg.report.locked_profile_id || "").trim();
      if (lockedProfileId) report.locked_profile_id = lockedProfileId;

      const rawTemplate = String(cfg.report.pdf_template_name || "").trim();
      const templateName = rawTemplate.replace(/\\/g, "/").split("/").pop().trim();
      if (templateName && /\.html$/i.test(templateName)) report.pdf_template_name = templateName;

      if (Object.keys(report).length) cfg.report = report;
      else delete cfg.report;
    } else {
      delete cfg.report;
    }

    if (!cfg.tabs || typeof cfg.tabs !== "object") {
      delete cfg.tabs;
      return cfg;
    }

    for (const [tabKey, rawTab] of Object.entries(cfg.tabs)) {
      if (!rawTab || typeof rawTab !== "object") {
        delete cfg.tabs[tabKey];
        continue;
      }
      const t = rawTab;
      if (typeof t.title === "string") {
        t.title = t.title.trim();
        if (!t.title) delete t.title;
      } else {
        delete t.title;
      }
      if (Array.isArray(t.hide_fields)) {
        t.hide_fields = [...new Set(t.hide_fields.map((x) => String(x || "").trim()).filter(Boolean))];
        if (!t.hide_fields.length) delete t.hide_fields;
      } else {
        delete t.hide_fields;
      }
      if (t.field_labels && typeof t.field_labels === "object") {
        const o = {};
        for (const [k, v] of Object.entries(t.field_labels)) {
          const kk = String(k || "").trim();
          const vv = String(v || "").trim();
          if (kk && vv) o[kk] = vv;
        }
        if (Object.keys(o).length) t.field_labels = o;
        else delete t.field_labels;
      } else {
        delete t.field_labels;
      }
      if (t.field_overrides && typeof t.field_overrides === "object") {
        if (!Object.keys(t.field_overrides).length) delete t.field_overrides;
      } else {
        delete t.field_overrides;
      }
      if (Array.isArray(t.add_fields)) {
        t.add_fields = t.add_fields.filter((x) => x && typeof x === "object" && String(x.k || "").trim());
        if (!t.add_fields.length) delete t.add_fields;
      } else {
        delete t.add_fields;
      }
      if (Array.isArray(t.rows)) {
        t.rows = t.rows
          .filter((r) => Array.isArray(r))
          .map((r) => r.map((x) => String(x || "").trim()).filter(Boolean))
          .filter((r) => r.length);
        if (!t.rows.length) delete t.rows;
      } else {
        delete t.rows;
      }
      if (!Object.keys(t).length) delete cfg.tabs[tabKey];
    }
    if (!Object.keys(cfg.tabs).length) delete cfg.tabs;
    return cfg;
  }

  function schemaFieldCount(schema) {
    let count = 0;
    for (const tabDef of Object.values(schema || {})) {
      const fields = Array.isArray((tabDef || {}).fields) ? tabDef.fields : [];
      count += fields.length;
    }
    return count;
  }

  function setActiveSchema(schema, rerenderTemplateScope = true) {
    activeSchema = schema && typeof schema === "object" ? clone(schema) : {};
    renderActiveSyncInfo();
    if (rerenderTemplateScope && templates.length) {
      updateTemplateDescAndScope();
      return;
    }
    syncTemplateSelectionFromActiveSchema();
  }

  function renderActiveSyncInfo() {
    const el = $("#liveSyncInfo");
    if (!el) return;
    const site = getSiteName();
    const tabCount = Object.keys(activeSchema || {}).length;
    const fieldCount = schemaFieldCount(activeSchema || {});
    el.textContent = `동기화 기준 단지: ${site || "-"} | 현재 사용 양식: 탭 ${tabCount}개 / 항목 ${fieldCount}개`;
  }

  function mergeScopeSchemaCatalog(target, source) {
    if (!target || typeof target !== "object" || !source || typeof source !== "object") return target || {};
    for (const [tabKey, tabDefRaw] of Object.entries(source)) {
      const tabDef = tabDefRaw && typeof tabDefRaw === "object" ? tabDefRaw : {};
      if (!target[tabKey] || typeof target[tabKey] !== "object") {
        target[tabKey] = { title: String(tabDef.title || tabKey), fields: [] };
      }
      const dst = target[tabKey];
      if (tabDef.title) dst.title = String(tabDef.title);
      const curFields = Array.isArray(dst.fields) ? dst.fields : [];
      const byKey = {};
      for (const f of curFields) {
        const k = String((f || {}).k || "").trim();
        if (k) byKey[k] = f;
      }
      const srcFields = Array.isArray(tabDef.fields) ? tabDef.fields : [];
      for (const f of srcFields) {
        const k = String((f || {}).k || "").trim();
        if (!k) continue;
        if (byKey[k]) {
          Object.assign(byKey[k], f);
        } else {
          const item = clone(f);
          curFields.push(item);
          byKey[k] = item;
        }
      }
      dst.fields = curFields;
    }
    return target;
  }

  function buildScopeSchema(templateCfg) {
    const catalog = clone(baseSchema || {});
    mergeScopeSchemaCatalog(catalog, activeSchema || {});
    const templated = applyConfigToSchema(baseSchema || {}, templateCfg || {});
    mergeScopeSchemaCatalog(catalog, templated || {});
    return catalog;
  }

  function applyConfigToSchema(base, cfg) {
    const schema = clone(base || {});
    const config = compactConfig(cfg || {});

    const hiddenTabs = new Set((config.hide_tabs || []).map((x) => String(x)));
    for (const t of hiddenTabs) delete schema[t];

    for (const [tabKey, tabCfg] of Object.entries(config.tabs || {})) {
      if (!schema[tabKey]) schema[tabKey] = { title: tabKey, fields: [] };
      const tab = schema[tabKey];
      if (tabCfg.title) tab.title = String(tabCfg.title);

      let fields = Array.isArray(tab.fields) ? clone(tab.fields) : [];
      const hideFields = new Set((tabCfg.hide_fields || []).map((x) => String(x)));
      if (hideFields.size) fields = fields.filter((f) => !hideFields.has(String(f.k)));

      const byKey = {};
      for (const f of fields) if (f && f.k) byKey[String(f.k)] = f;

      for (const [k, v] of Object.entries(tabCfg.field_labels || {})) {
        if (byKey[k]) byKey[k].label = String(v);
      }
      for (const [k, ov] of Object.entries(tabCfg.field_overrides || {})) {
        if (byKey[k] && ov && typeof ov === "object") Object.assign(byKey[k], ov);
      }
      for (const f of tabCfg.add_fields || []) {
        if (!f || typeof f !== "object" || !f.k) continue;
        const k = String(f.k);
        if (byKey[k]) Object.assign(byKey[k], f);
        else {
          const item = clone(f);
          fields.push(item);
          byKey[k] = item;
        }
      }
      tab.fields = fields;
      if (Array.isArray(tabCfg.rows)) tab.rows = clone(tabCfg.rows);
    }
    for (const [tabKey, tabDef] of Object.entries(schema)) {
      const fields = Array.isArray((tabDef || {}).fields) ? tabDef.fields : [];
      if (!fields.length) delete schema[tabKey];
    }
    return schema;
  }

  function renderPreview(data) {
    const schema = (data && data.schema) || {};
    const lines = [];
    const sid = KAUtil.normalizeSiteId((data && data.site_id) || getSiteId());
    lines.push(`site_id: ${sid > 0 ? sid : "-"}`);
    if (KAUtil.canViewSiteIdentity(me)) {
      lines.push(`site_name: ${data.site_name || getSiteName()}`);
      lines.push(`site_code: ${data.site_code || getSiteCode() || "-"}`);
    } else {
      lines.push("site: (숨김)");
    }
    lines.push(`tab_count: ${Object.keys(schema).length}`);
    for (const [tabKey, tabDef] of Object.entries(schema)) {
      const title = tabDef.title || tabKey;
      const fields = Array.isArray(tabDef.fields) ? tabDef.fields : [];
      lines.push(`- ${tabKey} (${title}): ${fields.length} fields`);
      for (const f of fields) lines.push(`  * ${f.k} : ${f.label || f.k} [${f.type || "text"}]`);
    }
    $("#preview").textContent = lines.join("\n");
  }

  function isSiteCodeConflictMessage(message) {
    const msg = String(message || "").trim();
    if (!msg) return false;
    return (
      msg.includes("site_code is immutable for existing site_name") ||
      msg.includes("site_code is immutable for existing site_env") ||
      msg.includes("site_code already mapped to another site_name") ||
      msg.includes("site_name currently mapped to another site_code") ||
      msg.includes("단지코드/단지명 충돌")
    );
  }

  function migrationStatusBadge(status) {
    const raw = String(status || "").trim().toLowerCase();
    if (raw === "pending") return { cls: "pending", text: "대기" };
    if (raw === "approved") return { cls: "approved", text: "승인됨" };
    if (raw === "executed") return { cls: "executed", text: "실행됨" };
    return { cls: "", text: raw || "-" };
  }

  function renderMigrationList(items) {
    const wrap = $("#migList");
    if (!wrap) return;
    const list = Array.isArray(items) ? items : [];
    if (!list.length) {
      wrap.textContent = "등록된 마이그레이션 요청이 없습니다.";
      return;
    }
    wrap.innerHTML = list
      .map((item) => {
        const id = Number(item.id || 0) || 0;
        const statusRaw = String(item.status || "").trim().toLowerCase();
        const status = migrationStatusBadge(statusRaw);
        const payload = item && typeof item.payload === "object" ? item.payload : {};
        const siteName = String(payload.site_name || item.target_site_name || "").trim();
        const oldCode = String(payload.old_site_code || item.target_site_code || "").trim().toUpperCase();
        const newCode = String(payload.new_site_code || "").trim().toUpperCase();
        const reason = String(payload.reason || item.reason || "").trim();
        const createdAt = String(item.created_at || "").trim();
        const approvedAt = String(item.approved_at || "").trim();
        const executedAt = String(item.executed_at || "").trim();
        const approveBtn =
          statusRaw === "pending"
            ? `<button class="btn" type="button" data-mig-act="approve" data-id="${id}">승인</button>`
            : "";
        const executeBtn =
          statusRaw === "approved"
            ? `<button class="btn primary" type="button" data-mig-act="execute" data-id="${id}">실행</button>`
            : "";
        return `
          <div class="mig-item">
            <div class="mig-head">
              <strong>#${id}</strong>
              <span class="mig-status ${status.cls}">${status.text}</span>
              <span>${escapeHtml(siteName || "-")} [${escapeHtml(oldCode || "-")} -> ${escapeHtml(newCode || "-")}]</span>
            </div>
            <div class="hint">사유: ${escapeHtml(reason || "-")}</div>
            <div class="hint">요청: ${escapeHtml(createdAt || "-")} / 승인: ${escapeHtml(approvedAt || "-")} / 실행: ${escapeHtml(executedAt || "-")}</div>
            <div class="row">${approveBtn}${executeBtn}</div>
          </div>
        `;
      })
      .join("");
  }

  async function prefillMigrationContext() {
    if (!canManageSiteCodeMigration(me)) return null;
    const siteNameEl = $("#migSiteName");
    const oldCodeEl = $("#migOldCode");
    const newCodeEl = $("#migNewCode");
    const reasonEl = $("#migReason");
    if (!siteNameEl || !oldCodeEl || !newCodeEl || !reasonEl) return null;

    const siteId = getSiteId();
    const siteName = getSiteName();

    let resolved = null;
    if (siteId > 0 || siteName) {
      try {
        resolved = await KAAuth.requestJson(
          withSitePath("/api/site_identity", { site_id: siteId, site_name: siteName, site_code: getSiteCode() }),
        );
      } catch (_e) {
        resolved = null;
      }
    }
    const resolvedName = String((resolved && resolved.site_name) || siteName || "").trim();
    const resolvedCode = String((resolved && resolved.site_code) || getSiteCode() || "").trim().toUpperCase();
    siteNameEl.value = resolvedName;
    oldCodeEl.value = resolvedCode;
    if (!newCodeEl.value || newCodeEl.value.toUpperCase() === resolvedCode) {
      const current = String(getSiteCode() || "").trim().toUpperCase();
      newCodeEl.value = current && current !== resolvedCode ? current : "";
    }
    if (!reasonEl.value.trim()) {
      reasonEl.value = "단지코드 충돌 복구";
    }
    return { site_name: resolvedName, old_site_code: resolvedCode, new_site_code: newCodeEl.value.trim().toUpperCase() };
  }

  async function loadMigrationRequests() {
    if (!canManageSiteCodeMigration(me)) return [];
    const data = await jfetch("/api/site_code/migration/requests?limit=100");
    const items = Array.isArray(data.items) ? data.items : [];
    renderMigrationList(items);
    return items;
  }

  async function createMigrationRequest() {
    if (!canManageSiteCodeMigration(me)) {
      throw new Error("최고관리자만 마이그레이션 요청을 만들 수 있습니다.");
    }
    await prefillMigrationContext();
    const siteName = String($("#migSiteName")?.value || "").trim();
    const oldCode = String($("#migOldCode")?.value || "").trim().toUpperCase();
    const newCode = String($("#migNewCode")?.value || "").trim().toUpperCase();
    const reason = String($("#migReason")?.value || "").trim();
    const expiresRaw = Number($("#migExpiresHours")?.value || 24);
    const expiresHours = Number.isFinite(expiresRaw) ? Math.max(1, Math.min(72, Math.trunc(expiresRaw))) : 24;
    if (!siteName || !oldCode || !newCode) {
      throw new Error("site_name / 기존 site_code / 변경 site_code를 모두 입력하세요.");
    }
    if (oldCode === newCode) {
      throw new Error("변경 site_code는 기존 값과 달라야 합니다.");
    }
    if (reason.length < 4) {
      throw new Error("변경 사유를 4자 이상 입력하세요.");
    }
    const data = await jfetch("/api/site_code/migration/request", {
      method: "POST",
      body: JSON.stringify({
        site_name: siteName,
        old_site_code: oldCode,
        new_site_code: newCode,
        reason,
        expires_hours: expiresHours,
        mfa_confirmed: true,
      }),
    });
    const req = data && data.request ? data.request : null;
    setMigrationMsg(`요청 생성 완료: #${Number((req && req.id) || 0) || "-"} (${oldCode} -> ${newCode})`);
    await loadMigrationRequests().catch(() => {});
    return req;
  }

  async function approveMigrationRequest(requestId) {
    const rid = Number(requestId || 0);
    if (rid <= 0) throw new Error("request_id가 올바르지 않습니다.");
    await jfetch("/api/site_code/migration/approve", {
      method: "POST",
      body: JSON.stringify({ request_id: rid, mfa_confirmed: true }),
    });
    setMigrationMsg(`요청 #${rid} 승인 완료`);
    await loadMigrationRequests().catch(() => {});
  }

  async function executeMigrationRequest(requestId) {
    const rid = Number(requestId || 0);
    if (rid <= 0) throw new Error("request_id가 올바르지 않습니다.");
    await jfetch("/api/site_code/migration/execute", {
      method: "POST",
      body: JSON.stringify({ request_id: rid, mfa_confirmed: true }),
    });
    setMigrationMsg(`요청 #${rid} 실행 완료`);
    await syncSiteIdentity(true).catch(() => {});
    await loadMigrationRequests().catch(() => {});
    await reloadConfig().catch(() => {});
  }

  async function openMigrationFlowFromConflict(detail) {
    const msg = String(detail || "").trim();
    if (!canManageSiteCodeMigration(me)) {
      setMsg(`단지코드 충돌: ${msg}. 최고관리자에게 단지코드 마이그레이션 요청이 필요합니다.`, true);
      return;
    }
    if (!showMigrationCard(true)) return;
    await prefillMigrationContext().catch(() => {});
    setMigrationMsg(`충돌 감지: ${msg}`, true);
    await loadMigrationRequests().catch(() => {});
  }

  async function jfetch(url, opts = {}) {
    try {
      return await KAAuth.requestJson(url, opts);
    } catch (err) {
      const msg = err && err.message ? String(err.message) : String(err || "");
      if (!handlingSiteCodeConflict && isSiteCodeConflictMessage(msg)) {
        handlingSiteCodeConflict = true;
        try {
          await openMigrationFlowFromConflict(msg);
        } catch (_e) {
          // ignore
        } finally {
          handlingSiteCodeConflict = false;
        }
      }
      throw err;
    }
  }

  async function loadSpecEnvManageCodePolicy(force = false) {
    if (!force && specEnvManageCodePolicy.loaded) return specEnvManageCodePolicy;
    const data = await jfetch("/api/site_env/manage_code/policy");
    specEnvManageCodePolicy = {
      loaded: true,
      enabled: !!(data && data.enabled),
      required: !!(data && data.required),
      role_bucket: String((data && data.role_bucket) || ""),
      role_label: String((data && data.role_label) || ""),
      verify_mode: String((data && data.verify_mode) || ""),
      ttl_sec: Number((data && data.ttl_sec) || 0),
    };
    if (!specEnvManageCodePolicy.required) resetSpecEnvManageCodeToken();
    return specEnvManageCodePolicy;
  }

  async function ensureSpecEnvManageCodeToken() {
    const policy = await loadSpecEnvManageCodePolicy();
    if (!policy.required) return "";
    if (hasValidSpecEnvManageCodeToken()) return specEnvManageCodeToken;
    const roleLabel = String(policy.role_label || "").trim();
    const verifyMode = String(policy.verify_mode || "");
    let msg = roleLabel
      ? `${roleLabel} 제원설정 관리코드를 입력하세요.`
      : "제원설정 관리코드를 입력하세요.";
    let emptyMsg = "제원설정 관리코드를 입력하세요.";
    if (verifyMode === "password") {
      msg = roleLabel
        ? `${roleLabel} 계정 비밀번호를 입력하세요.`
        : "계정 비밀번호를 입력하세요.";
      emptyMsg = "비밀번호를 입력하세요.";
    } else if (verifyMode === "code_or_password") {
      msg = roleLabel
        ? `${roleLabel} 제원설정 관리코드 또는 계정 비밀번호를 입력하세요.`
        : "제원설정 관리코드 또는 계정 비밀번호를 입력하세요.";
      emptyMsg = "관리코드 또는 비밀번호를 입력하세요.";
    }
    const raw = window.prompt(msg, "");
    if (raw == null) throw new Error("인증 입력이 취소되었습니다.");
    const code = String(raw || "").trim();
    if (!code) throw new Error(emptyMsg);
    const data = await jfetch("/api/site_env/manage_code/verify", {
      method: "POST",
      body: JSON.stringify({ code }),
    });
    if (data && data.required === false) {
      specEnvManageCodePolicy = { ...specEnvManageCodePolicy, required: false };
      resetSpecEnvManageCodeToken();
      return "";
    }
    setSpecEnvManageCodeToken(data && data.token, data && data.expires_at, data && data.expires_in_sec);
    if (!hasValidSpecEnvManageCodeToken()) {
      throw new Error("제원설정 관리코드 인증 토큰을 발급받지 못했습니다. 다시 시도하세요.");
    }
    return specEnvManageCodeToken;
  }

  async function loadBaseSchema() {
    const data = await jfetch("/api/base_schema");
    baseSchema = (data && data.schema) || {};
  }

  function getSelectedTemplate() {
    const key = $("#templateSelect").value;
    return templates.find((x) => x.key === key) || null;
  }

  function renderTemplateSelect() {
    const sel = $("#templateSelect");
    sel.innerHTML = "";
    for (const t of templates) {
      const o = document.createElement("option");
      o.value = t.key;
      o.textContent = `${t.name} (${t.key})`;
      sel.appendChild(o);
    }
    if (templates.length) {
      sel.value = templates[0].key;
      updateTemplateDescAndScope();
    }
  }

  function updateTemplateDescAndScope() {
    const t = getSelectedTemplate();
    $("#templateDesc").textContent = t ? t.description || "" : "";
    renderTemplateScope();
  }

  function renderTemplateScope() {
    const wrap = $("#templateScopeWrap");
    const t = getSelectedTemplate();
    if (!t) {
      wrap.innerHTML = '<div class="hint">템플릿이 없습니다.</div>';
      return;
    }
    const schema = buildScopeSchema(t.config || {});
    const tabs = sortTabKeys(Object.keys(schema));
    if (!tabs.length) {
      wrap.innerHTML = '<div class="hint">선택 가능한 탭이 없습니다.</div>';
      return;
    }

    wrap.innerHTML = "";
    for (const tabKey of tabs) {
      const tab = schema[tabKey] || {};
      const title = String(tab.title || tabKey);
      const fields = Array.isArray(tab.fields) ? tab.fields : [];
      const block = document.createElement("div");
      block.className = "tpl-tab-block";
      block.innerHTML = `
        <label class="tpl-tab-head">
          <input type="checkbox" class="tpl-tab" data-tab="${escapeHtmlAttr(tabKey)}" />
          <span>${escapeHtml(title)} <code>${escapeHtml(tabKey)}</code></span>
        </label>
        <div class="tpl-fields"></div>
      `;
      const fwrap = block.querySelector(".tpl-fields");
      for (const f of fields) {
        const k = String(f.k || "").trim();
        if (!k) continue;
        const label = String(f.label || k);
        const item = document.createElement("label");
        item.className = "tpl-field-item";
        item.innerHTML = `<input type="checkbox" class="tpl-field" data-tab="${escapeHtmlAttr(tabKey)}" data-field="${escapeHtmlAttr(
          k
        )}" /><span>${escapeHtml(label)} <code>${escapeHtml(k)}</code></span>`;
        fwrap.appendChild(item);
      }
      wrap.appendChild(block);
    }
    syncTemplateSelectionFromActiveSchema();
  }

  function setTemplateSelectionAll(checked) {
    for (const x of document.querySelectorAll("#templateScopeWrap input.tpl-tab, #templateScopeWrap input.tpl-field")) {
      x.checked = !!checked;
      x.disabled = false;
    }
  }

  function templateFieldInputsByTab(tabKey) {
    const tab = String(tabKey || "").trim();
    const out = [];
    if (!tab) return out;
    for (const f of document.querySelectorAll("#templateScopeWrap input.tpl-field")) {
      if (String(f.dataset.tab || "").trim() === tab) out.push(f);
    }
    return out;
  }

  function setTemplateFieldsByTab(tabKey, checked) {
    const on = !!checked;
    for (const f of templateFieldInputsByTab(tabKey)) {
      f.disabled = !on;
      f.checked = on;
    }
  }

  function syncTemplateTabFromFields(tabKey) {
    const tab = String(tabKey || "").trim();
    if (!tab) return;
    let tabEl = null;
    for (const t of document.querySelectorAll("#templateScopeWrap input.tpl-tab")) {
      if (String(t.dataset.tab || "").trim() === tab) {
        tabEl = t;
        break;
      }
    }
    if (!tabEl) return;
    const fields = templateFieldInputsByTab(tab);
    if (!fields.length) return;
    const anyChecked = fields.some((f) => !!f.checked);
    tabEl.checked = anyChecked;
    if (!anyChecked) {
      for (const f of fields) {
        f.checked = false;
        f.disabled = true;
      }
    }
  }

  function syncTemplateFieldDisables() {
    const tabMap = {};
    for (const t of document.querySelectorAll("#templateScopeWrap input.tpl-tab")) {
      tabMap[String(t.dataset.tab || "")] = !!t.checked;
    }
    for (const f of document.querySelectorAll("#templateScopeWrap input.tpl-field")) {
      const tab = String(f.dataset.tab || "");
      const on = !!tabMap[tab];
      f.disabled = !on;
      if (!on) f.checked = false;
    }
  }

  function syncTemplateSelectionFromActiveSchema() {
    const tabEls = [...document.querySelectorAll("#templateScopeWrap input.tpl-tab")];
    const fieldEls = [...document.querySelectorAll("#templateScopeWrap input.tpl-field")];
    if (!tabEls.length && !fieldEls.length) return;
    const schema = activeSchema && typeof activeSchema === "object" ? activeSchema : {};
    const activeTabKeys = Object.keys(schema);

    if (!activeTabKeys.length) {
      const rawEditor = ($("#envJson")?.value || "").trim();
      // Before loading any site config, keep all selected as an initial default.
      if (!rawEditor) {
        setTemplateSelectionAll(true);
        syncTemplateFieldDisables();
        return;
      }
    }

    const fieldsByTab = {};
    for (const [tabKey, tabDef] of Object.entries(schema)) {
      const fields = Array.isArray((tabDef || {}).fields) ? tabDef.fields : [];
      fieldsByTab[tabKey] = new Set(fields.map((f) => String((f || {}).k || "").trim()).filter(Boolean));
    }

    for (const t of tabEls) {
      const tabKey = String(t.dataset.tab || "").trim();
      const activeFields = tabKey ? fieldsByTab[tabKey] : null;
      t.checked = !!(activeFields && activeFields.size);
    }

    for (const f of fieldEls) {
      const tabKey = String(f.dataset.tab || "").trim();
      const fieldKey = String(f.dataset.field || "").trim();
      const activeFields = tabKey ? fieldsByTab[tabKey] : null;
      const tabOn = !!(activeFields && activeFields.size);
      f.disabled = !tabOn;
      f.checked = !!(tabOn && fieldKey && activeFields.has(fieldKey));
    }
  }

  function collectTemplateSelection() {
    const tabs = new Set();
    const fieldsByTab = {};
    for (const t of document.querySelectorAll("#templateScopeWrap input.tpl-tab")) {
      const tab = String(t.dataset.tab || "").trim();
      if (tab && t.checked) tabs.add(tab);
    }
    for (const f of document.querySelectorAll("#templateScopeWrap input.tpl-field")) {
      const tab = String(f.dataset.tab || "").trim();
      const key = String(f.dataset.field || "").trim();
      if (!tab || !key || f.disabled || !f.checked) continue;
      if (!fieldsByTab[tab]) fieldsByTab[tab] = new Set();
      fieldsByTab[tab].add(key);
    }
    return { tabs, fieldsByTab };
  }

  function filterTemplateConfigBySelection(templateCfg, selection) {
    const cfg = clone(templateCfg || {});
    const tabs = selection.tabs;
    const fieldsByTab = selection.fieldsByTab || {};

    if (Array.isArray(cfg.hide_tabs)) {
      cfg.hide_tabs = cfg.hide_tabs.filter((t) => tabs.has(String(t)));
    }

    const outTabs = {};
    for (const [tabKey, tabCfgRaw] of Object.entries(cfg.tabs || {})) {
      if (!tabs.has(tabKey)) continue;
      const tabCfg = clone(tabCfgRaw || {});
      const fieldSet = fieldsByTab[tabKey] || new Set();
      if (Array.isArray(tabCfg.hide_fields)) {
        tabCfg.hide_fields = tabCfg.hide_fields.filter((k) => fieldSet.has(String(k)));
      }
      if (tabCfg.field_labels && typeof tabCfg.field_labels === "object") {
        const o = {};
        for (const [k, v] of Object.entries(tabCfg.field_labels)) if (fieldSet.has(String(k))) o[k] = v;
        tabCfg.field_labels = o;
      }
      if (tabCfg.field_overrides && typeof tabCfg.field_overrides === "object") {
        const o = {};
        for (const [k, v] of Object.entries(tabCfg.field_overrides)) if (fieldSet.has(String(k))) o[k] = v;
        tabCfg.field_overrides = o;
      }
      if (Array.isArray(tabCfg.add_fields)) {
        tabCfg.add_fields = tabCfg.add_fields.filter((f) => fieldSet.has(String((f || {}).k || "")));
      }
      if (Array.isArray(tabCfg.rows)) {
        const rows = [];
        for (const row of tabCfg.rows) {
          if (!Array.isArray(row)) continue;
          const rr = row.map((x) => String(x || "")).filter((k) => fieldSet.has(k));
          if (rr.length) rows.push(rr);
        }
        tabCfg.rows = rows;
      }
      outTabs[tabKey] = tabCfg;
    }
    cfg.tabs = outTabs;
    return compactConfig(cfg);
  }

  function buildVisibilityConfigBySelection(selection, scopedSchema) {
    const tabs = selection.tabs || new Set();
    const fieldsByTab = selection.fieldsByTab || {};
    const hideTabs = new Set();
    const outTabs = {};

    for (const [tabKey, tabDef] of Object.entries(scopedSchema || {})) {
      const fields = Array.isArray((tabDef || {}).fields) ? tabDef.fields : [];
      const allKeys = fields.map((f) => String((f || {}).k || "").trim()).filter(Boolean);
      if (!tabs.has(tabKey)) {
        hideTabs.add(tabKey);
        continue;
      }

      const selected = fieldsByTab[tabKey] || new Set();
      const visibleKeys = allKeys.filter((k) => selected.has(k));
      if (!visibleKeys.length) {
        hideTabs.add(tabKey);
        continue;
      }

      const hiddenKeys = allKeys.filter((k) => !selected.has(k));
      const tabCfg = { title: String((tabDef || {}).title || tabKey) };
      if (hiddenKeys.length) tabCfg.hide_fields = hiddenKeys;
      outTabs[tabKey] = tabCfg;
    }

    return compactConfig({
      hide_tabs: [...hideTabs],
      tabs: outTabs,
    });
  }

  function buildAllSelectionFromSchema(scopedSchema) {
    const tabs = new Set();
    const fieldsByTab = {};
    for (const [tabKey, tabDef] of Object.entries(scopedSchema || {})) {
      tabs.add(tabKey);
      const fields = Array.isArray((tabDef || {}).fields) ? tabDef.fields : [];
      fieldsByTab[tabKey] = new Set(
        fields.map((f) => String((f || {}).k || "").trim()).filter(Boolean)
      );
    }
    return { tabs, fieldsByTab };
  }

  function mergeConfig(baseCfg, applyCfg) {
    const out = compactConfig(baseCfg || {});
    const add = compactConfig(applyCfg || {});

    const hideTabs = new Set([...(out.hide_tabs || []), ...(add.hide_tabs || [])].map((x) => String(x)));
    if (hideTabs.size) out.hide_tabs = [...hideTabs];

    if (add.report && typeof add.report === "object") {
      const curReport = out.report && typeof out.report === "object" ? out.report : {};
      out.report = { ...curReport, ...add.report };
    }

    if (!out.tabs || typeof out.tabs !== "object") out.tabs = {};
    for (const [tabKey, tabCfg] of Object.entries(add.tabs || {})) {
      if (!out.tabs[tabKey] || typeof out.tabs[tabKey] !== "object") out.tabs[tabKey] = {};
      const t = out.tabs[tabKey];
      if (tabCfg.title !== undefined) t.title = tabCfg.title;

      const setUnion = (k) => {
        const set = new Set([...(t[k] || []), ...(tabCfg[k] || [])].map((x) => String(x)));
        if (set.size) t[k] = [...set];
      };
      setUnion("hide_fields");

      if (tabCfg.field_labels && typeof tabCfg.field_labels === "object") {
        t.field_labels = { ...(t.field_labels || {}), ...tabCfg.field_labels };
      }
      if (tabCfg.field_overrides && typeof tabCfg.field_overrides === "object") {
        t.field_overrides = { ...(t.field_overrides || {}), ...tabCfg.field_overrides };
      }
      if (Array.isArray(tabCfg.add_fields)) {
        const map = {};
        for (const x of t.add_fields || []) {
          const k = String((x || {}).k || "");
          if (k) map[k] = x;
        }
        for (const x of tabCfg.add_fields) {
          const k = String((x || {}).k || "");
          if (k) map[k] = x;
        }
        t.add_fields = Object.values(map);
      }
      if (Array.isArray(tabCfg.rows)) t.rows = clone(tabCfg.rows);
    }
    return compactConfig(out);
  }

  function mergeConfigWithScope(baseCfg, applyCfg, scopeTabsInput) {
    const out = compactConfig(baseCfg || {});
    const add = compactConfig(applyCfg || {});
    const scopeTabs = new Set(
      [...(scopeTabsInput || [])]
        .map((x) => String(x || "").trim())
        .filter(Boolean)
    );

    const hideTabs = new Set((out.hide_tabs || []).map((x) => String(x)));
    for (const tab of scopeTabs) hideTabs.delete(tab);
    for (const tab of add.hide_tabs || []) hideTabs.add(String(tab));
    if (hideTabs.size) out.hide_tabs = [...hideTabs];
    else delete out.hide_tabs;

    if (add.report && typeof add.report === "object") {
      const curReport = out.report && typeof out.report === "object" ? out.report : {};
      out.report = { ...curReport, ...add.report };
    }

    if (!out.tabs || typeof out.tabs !== "object") out.tabs = {};
    for (const tab of scopeTabs) delete out.tabs[tab];
    for (const [tabKey, tabCfg] of Object.entries(add.tabs || {})) {
      out.tabs[tabKey] = clone(tabCfg || {});
    }
    if (!Object.keys(out.tabs).length) delete out.tabs;

    return compactConfig(out);
  }

  async function applySelectedTemplate(opts = {}) {
    const allowDirectSaveOnNoSelection = !!(opts && opts.allowDirectSaveOnNoSelection);
    const t = getSelectedTemplate();
    const templateCfg = t && t.config ? t.config : {};
    const templateSchema = buildScopeSchema(templateCfg);
    let selection = collectTemplateSelection();
    if (!selection.tabs.size && allowDirectSaveOnNoSelection) {
      selection = buildAllSelectionFromSchema(templateSchema);
    }
    if (!selection.tabs.size) {
      if (allowDirectSaveOnNoSelection) {
        setMsg("선택 범위가 비어 있어 전체 탭/항목 기준으로 저장합니다...");
        selection = buildAllSelectionFromSchema(templateSchema);
      } else {
        setMsg("최소 1개 탭을 선택하세요.", true);
        return;
      }
    }
    const filtered = filterTemplateConfigBySelection(templateCfg, selection);
    const visibility = buildVisibilityConfigBySelection(selection, templateSchema);
    const templateScoped = mergeConfig(filtered, visibility);
    const scopeTabs = Object.keys(templateSchema || {});
    const mode = $("#templateMode").value || "merge";

    let current = {};
    try {
      current = compactConfig(getConfigFromEditor());
    } catch (e) {
      setMsg(`JSON 파싱 오류: ${e.message}`, true);
      return;
    }
    const next = mode === "replace" ? templateScoped : mergeConfigWithScope(current, templateScoped, scopeTabs);
    setConfigToEditor(next);
    const schemaPreview = applyConfigToSchema(baseSchema, next);
    renderPreview({ site_name: getSiteName(), site_code: getSiteCode(), schema: schemaPreview });
    setMsg("선택한 탭메뉴와 항목을 적용했습니다. 저장 중입니다...");
    await saveConfig();
  }

  async function loadTemplates() {
    let items = [];
    try {
      const data = await jfetch("/api/site_env_templates");
      items = Array.isArray(data.items) ? data.items : [];
    } catch (_e) {
      const fallback = await jfetch("/api/site_env_template");
      items = [{ key: "default", name: "기본", description: "", config: fallback.template || {} }];
    }
    templates = items.map((x) => ({
      key: String(x.key || ""),
      name: String(x.name || x.key || "template"),
      description: String(x.description || ""),
      config: x.config && typeof x.config === "object" ? x.config : {},
    }));
    if (!templates.length) templates = [{ key: "blank", name: "빈 템플릿", description: "", config: {} }];
    renderTemplateSelect();
  }

  async function loadSiteList() {
    const data = await jfetch("/api/site_env_list");
    const items = Array.isArray(data.items) ? data.items : [];
    const wrap = $("#siteList");
    if (!items.length) {
      wrap.textContent = "설정된 단지가 없습니다.";
      return;
    }
    wrap.innerHTML = items
      .map(
        (x) =>
          `<button class="btn site-item" type="button" data-site="${escapeHtmlAttr(x.site_name || "")}" data-code="${escapeHtmlAttr(
            x.site_code || ""
          )}" data-site-id="${escapeHtmlAttr(x.site_id || "")}">${escapeHtml(x.site_name || "")}${x.site_code ? ` <code>[${escapeHtml(x.site_code)}]</code>` : ""}</button> <span style="opacity:.75">${escapeHtml(
            x.updated_at || ""
          )}</span>`
      )
      .join("<br/>");
  }

  async function reloadConfig() {
    const rawSite = getSiteName();
    const rawSiteCode = getSiteCode();
    const rawSiteId = getSiteId();
    if (!rawSite && !rawSiteCode && rawSiteId <= 0) {
      setMsg("site_name 또는 site_code를 입력하세요.", true);
      return;
    }
    await syncSiteIdentity(true);
    const siteId = getSiteId();
    const site = getSiteName();
    const siteCode = getSiteCode();
    if (!site && siteId <= 0) {
      setMsg("site_name/site_id를 확인하세요.", true);
      return;
    }
    if (site) localStorage.setItem(SITE_KEY, site);
    setSiteCode(siteCode);
    const data = await jfetch(withSitePath("/api/site_env", { site_name: site, site_code: siteCode, site_id: siteId }));
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(data.site_name || "");
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    setConfigToEditor(data.config || {});
    setActiveSchema(data.schema || {});
    renderPreview(data);
    setMsg("환경변수를 불러왔습니다.");
    await loadSiteList().catch(() => {});
  }

  async function saveConfig() {
    const rawSite = getSiteName();
    const rawSiteCode = getSiteCode();
    const rawSiteId = getSiteId();
    if (!rawSite && !rawSiteCode && rawSiteId <= 0) {
      setMsg("site_name 또는 site_code를 입력하세요.", true);
      return;
    }
    await syncSiteIdentity(true);
    const siteId = getSiteId();
    const site = getSiteName();
    const siteCode = getSiteCode();
    if (!site && siteId <= 0) {
      setMsg("site_name/site_id를 확인하세요.", true);
      return;
    }
    let cfg = {};
    try {
      cfg = compactConfig(getConfigFromEditor());
    } catch (e) {
      setMsg(`JSON 파싱 오류: ${e.message}`, true);
      return;
    }

    let manageCodeToken = "";
    try {
      manageCodeToken = await ensureSpecEnvManageCodeToken();
    } catch (e) {
      setMsg(e.message || String(e), true);
      return;
    }

    let data = null;
    try {
      data = await jfetch("/api/site_env", {
        method: "PUT",
        body: JSON.stringify({
          site_id: siteId || 0,
          site_name: site,
          site_code: siteCode || "",
          config: cfg,
          mfa_confirmed: true,
          manage_code_token: manageCodeToken || "",
          reason: "spec_env_save",
        }),
      });
    } catch (err) {
      const msg = err && err.message ? String(err.message) : String(err || "");
      if (isSpecEnvManageCodeErrorMessage(msg)) resetSpecEnvManageCodeToken();
      throw err;
    }
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(data.site_name || "");
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    setConfigToEditor(data.config || {});
    setActiveSchema(data.schema || {});
    renderPreview(data);
    markActionSuccess($("#btnSave"), "✓");
    setMsg("저장이 완료되었습니다. 이제 [메인으로]를 눌러 이동하세요.");
    await loadSiteList().catch(() => {});
  }

  async function deleteConfig() {
    const rawSite = getSiteName();
    const rawSiteCode = getSiteCode();
    const rawSiteId = getSiteId();
    if (!rawSite && !rawSiteCode && rawSiteId <= 0) {
      setMsg("site_name 또는 site_code를 입력하세요.", true);
      return;
    }
    await syncSiteIdentity(true);
    const siteId = getSiteId();
    const site = getSiteName();
    const siteCode = getSiteCode();
    if (!site && siteId <= 0) {
      setMsg("site_name/site_id를 확인하세요.", true);
      return;
    }
    const ok = confirm(`${site}${siteCode ? ` [${siteCode}]` : ""} 단지의 제원설정을 삭제할까요?`);
    if (!ok) return;

    let manageCodeToken = "";
    try {
      manageCodeToken = await ensureSpecEnvManageCodeToken();
    } catch (e) {
      setMsg(e.message || String(e), true);
      return;
    }

    const headers = { "X-KA-MFA-VERIFIED": "1" };
    if (manageCodeToken) headers["X-KA-SPEC-ENV-CODE-TOKEN"] = manageCodeToken;

    let del = null;
    try {
      del = await jfetch(withSitePath("/api/site_env", { site_name: site, site_code: siteCode, site_id: siteId }), {
        method: "DELETE",
        headers,
      });
    } catch (err) {
      const msg = err && err.message ? String(err.message) : String(err || "");
      if (isSpecEnvManageCodeErrorMessage(msg)) resetSpecEnvManageCodeToken();
      throw err;
    }
    if (del && Object.prototype.hasOwnProperty.call(del, "site_id")) setSiteId(del.site_id);
    const data = await jfetch(withSitePath("/api/schema", { site_name: site, site_code: siteCode, site_id: siteId }));
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(data.site_name || "");
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    setConfigToEditor((data && data.site_env_config) || {});
    setActiveSchema((data && data.schema) || {});
    renderPreview({ site_name: site, site_code: (data && data.site_code) || siteCode, schema: (data && data.schema) || {} });
    setMsg("삭제되었습니다.");
    await loadSiteList().catch(() => {});
  }

  async function previewSchema() {
    if (!baseSchema || !Object.keys(baseSchema).length) {
      await loadBaseSchema();
    }

    // Site identity sync is best-effort; preview itself should work without it.
    const rawSite = getSiteName();
    const rawSiteCode = getSiteCode();
    const rawSiteId = getSiteId();
    if (rawSite || rawSiteCode || rawSiteId > 0) {
      await syncSiteIdentity(true).catch(() => {});
    }

    const siteId = getSiteId();
    const site = getSiteName();
    const siteCode = getSiteCode();

    let cfg = {};
    try {
      cfg = compactConfig(getConfigFromEditor());
    } catch (e) {
      setMsg(`JSON 파싱 오류: ${e.message}`, true);
      return;
    }

    const schema = applyConfigToSchema(baseSchema || {}, cfg || {});
    setActiveSchema(schema);
    renderPreview({ site_id: siteId, site_name: site, site_code: siteCode, schema });
    syncPdfProfileSelectFromConfig(cfg);
    markActionSuccess($("#btnPreview"), "✓");
    setMsg("양식 미리보기를 갱신했습니다. (현재 편집 중인 JSON 기준)");
  }

  function wire() {
    $("#btnGoMain")?.addEventListener("click", (e) => {
      e.preventDefault();
      const btn = e.currentTarget instanceof HTMLElement ? e.currentTarget : $("#btnGoMain");
      markActionSuccess(btn, "↗");
      setMsg("메인으로 이동합니다.");
      const site = getSiteName();
      const siteCode = getSiteCode();
      const target = withSitePath("/pwa/", { site_name: site, site_code: siteCode, site_id: getSiteId() });
      window.setTimeout(() => {
        window.location.href = target;
      }, 240);
    });

    $("#btnReload").addEventListener("click", () => reloadConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnSave").addEventListener("click", () =>
      applySelectedTemplate({ allowDirectSaveOnNoSelection: true }).catch((e) => setMsg(e.message || String(e), true))
    );
    $("#btnDelete").addEventListener("click", () => deleteConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnPreview").addEventListener("click", () => previewSchema().catch((e) => setMsg(e.message || String(e), true)));

    $("#templateSelect").addEventListener("change", updateTemplateDescAndScope);
    $("#pdfProfileSelect")?.addEventListener("change", (e) => {
      const selected = String(e.target && e.target.value ? e.target.value : "").trim() || DEFAULT_PDF_PROFILE_ID;
      if (!applyPdfProfileToEditor(selected)) return;
      setMsg(`PDF 프로파일을 '${selected}'로 설정했습니다. 저장을 누르세요.`);
    });
    $("#envJson")?.addEventListener("change", () => syncPdfProfileSelectFromConfig());
    $("#btnTemplateApply")?.addEventListener("click", () =>
      applySelectedTemplate().catch((e) => setMsg(e.message || String(e), true))
    );
    $("#siteName")?.addEventListener("change", () => {
      setSiteName(getSiteName());
      setSiteId(0);
      syncSiteIdentity(true).catch(() => {});
      prefillMigrationContext().catch(() => {});
    });
    $("#siteCode")?.addEventListener("change", () => {
      setSiteCode(getSiteCode());
      setSiteId(0);
      syncSiteIdentity(true).catch(() => {});
      prefillMigrationContext().catch(() => {});
    });
    $("#btnTplAllOn").addEventListener("click", () => {
      setTemplateSelectionAll(true);
      syncTemplateFieldDisables();
    });
    $("#btnTplAllOff").addEventListener("click", () => {
      setTemplateSelectionAll(false);
      syncTemplateFieldDisables();
    });
    $("#templateScopeWrap").addEventListener("change", (e) => {
      const tabInput = e.target.closest("input.tpl-tab");
      if (tabInput) {
        const tabKey = String(tabInput.dataset.tab || "").trim();
        setTemplateFieldsByTab(tabKey, !!tabInput.checked);
        syncTemplateFieldDisables();
        return;
      }
      const fieldInput = e.target.closest("input.tpl-field");
      if (fieldInput) {
        const tabKey = String(fieldInput.dataset.tab || "").trim();
        syncTemplateTabFromFields(tabKey);
        syncTemplateFieldDisables();
      }
    });

    $("#btnMigRequest")?.addEventListener("click", () =>
      createMigrationRequest().catch((e) => setMigrationMsg(e.message || String(e), true))
    );
    $("#btnMigRefresh")?.addEventListener("click", () =>
      loadMigrationRequests()
        .then(() => setMigrationMsg("요청 목록을 새로고침했습니다."))
        .catch((e) => setMigrationMsg(e.message || String(e), true))
    );
    $("#migList")?.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-mig-act][data-id]");
      if (!btn) return;
      const action = String(btn.dataset.migAct || "").trim().toLowerCase();
      const requestId = Number(btn.dataset.id || 0);
      if (requestId <= 0) return;
      if (action === "approve") {
        approveMigrationRequest(requestId).catch((err) => setMigrationMsg(err.message || String(err), true));
        return;
      }
      if (action === "execute") {
        executeMigrationRequest(requestId).catch((err) => setMigrationMsg(err.message || String(err), true));
      }
    });

    $("#siteList").addEventListener("click", (e) => {
      const btn = e.target.closest("button.site-item[data-site]");
      if (!btn) return;
      const site = String(btn.dataset.site || "").trim();
      const code = String(btn.dataset.code || "").trim().toUpperCase();
      const siteId = KAUtil.normalizeSiteId(btn.dataset.siteId || "");
      if (!site) return;
      setSiteId(siteId);
      setSiteName(site);
      setSiteCode(code);
      reloadConfig().catch((err) => setMsg(err.message || String(err), true));
      prefillMigrationContext().catch(() => {});
    });
  }

  function enforceSitePolicy() {
    if (!me) return;
    const siteInput = $("#siteName");
    const codeInput = $("#siteCode");
    if (!siteInput || !codeInput) return;
    showMigrationCard(false);
    if (isAdmin(me)) {
      siteInput.readOnly = false;
      siteInput.removeAttribute("aria-readonly");
      siteInput.title = "";
      if (KAUtil.isSuperAdmin(me)) {
        codeInput.readOnly = false;
        codeInput.removeAttribute("aria-readonly");
        codeInput.title = "";
      } else {
        codeInput.readOnly = true;
        codeInput.setAttribute("aria-readonly", "true");
        codeInput.title = "단지코드 생성/변경은 최고관리자만 가능합니다.";
      }
      applySiteIdentityVisibility();
      return;
    }

    const assignedSite = String(me.site_name || "").trim();
    if (!assignedSite) {
      throw new Error("계정에 소속 단지가 지정되지 않았습니다. 관리자에게 문의하세요.");
    }
    setSiteId(KAUtil.normalizeSiteId(me.site_id));
    setSiteName(assignedSite);
    const assignedCode = String(me.site_code || "").trim().toUpperCase();
    setSiteCode(assignedCode || "");
    siteInput.readOnly = true;
    codeInput.readOnly = true;
    siteInput.setAttribute("aria-readonly", "true");
    codeInput.setAttribute("aria-readonly", "true");
    siteInput.title = "소속 단지는 관리자만 변경할 수 있습니다.";
    codeInput.title = "소속 단지코드는 관리자만 변경할 수 있습니다.";
    applySiteIdentityVisibility();
  }

  async function init() {
    if (window.KAModuleBase && typeof window.KAModuleBase.bootstrap === "function") {
      moduleCtx = await window.KAModuleBase.bootstrap("main", {
        defaultLimit: 100,
        maxLimit: 500,
      });
      me = moduleCtx.user || null;
    } else {
      me = await KAAuth.requireAuth();
    }
    if (!canManageSpecEnv(me)) {
      alert("관리자/단지대표자만 접근할 수 있습니다.");
      window.location.href = "/pwa/";
      return;
    }
    const u = new URL(window.location.href);
    const qSite = (u.searchParams.get("site_name") || "").trim();
    const qCode = (u.searchParams.get("site_code") || "").trim().toUpperCase();
    const qSiteId = KAUtil.normalizeSiteId(u.searchParams.get("site_id") || "");
    const ctxSite = String(moduleCtx && moduleCtx.siteName ? moduleCtx.siteName : "").trim();
    const ctxCode = String(moduleCtx && moduleCtx.siteCode ? moduleCtx.siteCode : "").trim().toUpperCase();
    const ctxSiteId = KAUtil.normalizeSiteId(moduleCtx && moduleCtx.siteId ? moduleCtx.siteId : 0);
    const stored = (localStorage.getItem(SITE_KEY) || "").trim();
    const storedCode = (localStorage.getItem(SITE_CODE_KEY) || "").trim().toUpperCase();
    const storedSiteId = KAUtil.normalizeSiteId(localStorage.getItem(SITE_ID_KEY) || "");
    const meSiteId = KAUtil.normalizeSiteId(me.site_id);
    setSiteId(qSiteId || ctxSiteId || storedSiteId || meSiteId || 0);
    setSiteName(qSite || ctxSite || stored || String(me.site_name || "").trim());
    setSiteCode(qCode || ctxCode || storedCode || "");
    enforceSitePolicy();

    wire();
    loadSpecEnvManageCodePolicy().catch(() => {});
    if (canManageSiteCodeMigration(me)) {
      await prefillMigrationContext().catch(() => {});
      await loadMigrationRequests().catch(() => {});
    }
    await loadBaseSchema();
    await loadTemplates();
    renderPdfProfileSelect(DEFAULT_PDF_PROFILE_ID);
    if (KAUtil.canViewSiteIdentity(me)) {
      await loadSiteList().catch(() => {});
    } else {
      applySiteIdentityVisibility();
    }
    if (getSiteName() || getSiteCode()) {
      await syncSiteIdentity(true).catch(() => {});
      try {
        await reloadConfig();
      } catch (e) {
        setMsg(e.message || String(e), true);
      }
      return;
    }
    setConfigToEditor({});
    setActiveSchema({});
    renderPreview({ site_name: "", site_code: "", schema: {} });
    setMsg("site_name 또는 site_code를 입력한 뒤 [불러오기]를 누르세요.");
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();

