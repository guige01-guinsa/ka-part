(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  const TAB_ORDER = [
    "home",
    "tr450",
    "tr400",
    "meter",
    "facility",
    "facility_check",
    "facility_fire",
    "facility_mechanical",
    "facility_telecom",
  ];

  let me = null;
  let templates = [];
  let baseSchema = {};
  let activeSchema = {};
  const ACTION_SUCCESS_BUTTON_IDS = ["btnTemplateApply", "btnSave", "btnGoMain"];

  function canManageSpecEnv(user) {
    return !!(user && (user.is_admin || user.is_site_admin));
  }

  function isAdmin(user) {
    return !!(user && user.is_admin);
  }

  function setMsg(msg, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
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

  function buildSiteQuery(siteName, siteCode) {
    const qs = new URLSearchParams();
    const site = String(siteName || "").trim();
    const code = String(siteCode || "").trim().toUpperCase();
    if (site) qs.set("site_name", site);
    if (code) qs.set("site_code", code);
    return qs.toString();
  }

  function getConfigFromEditor() {
    const raw = ($("#envJson").value || "").trim();
    if (!raw) return {};
    const v = JSON.parse(raw);
    return v && typeof v === "object" ? v : {};
  }

  function setConfigToEditor(config) {
    $("#envJson").value = JSON.stringify(config || {}, null, 2);
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
    lines.push(`site_name: ${data.site_name || getSiteName()}`);
    lines.push(`site_code: ${data.site_code || getSiteCode() || "-"}`);
    lines.push(`tab_count: ${Object.keys(schema).length}`);
    for (const [tabKey, tabDef] of Object.entries(schema)) {
      const title = tabDef.title || tabKey;
      const fields = Array.isArray(tabDef.fields) ? tabDef.fields : [];
      lines.push(`- ${tabKey} (${title}): ${fields.length} fields`);
      for (const f of fields) lines.push(`  * ${f.k} : ${f.label || f.k} [${f.type || "text"}]`);
    }
    $("#preview").textContent = lines.join("\n");
  }

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
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

    if (!Object.keys(activeSchema || {}).length) {
      setTemplateSelectionAll(false);
      syncTemplateFieldDisables();
      return;
    }

    const activeFieldMap = {};
    for (const [tabKey, tabDef] of Object.entries(activeSchema || {})) {
      const fields = Array.isArray((tabDef || {}).fields) ? tabDef.fields : [];
      activeFieldMap[tabKey] = new Set(fields.map((f) => String((f || {}).k || "").trim()).filter(Boolean));
    }

    for (const t of tabEls) {
      const tab = String(t.dataset.tab || "").trim();
      t.checked = !!activeFieldMap[tab];
    }
    for (const f of fieldEls) {
      const tab = String(f.dataset.tab || "").trim();
      const key = String(f.dataset.field || "").trim();
      const set = activeFieldMap[tab];
      f.checked = !!set && set.has(key);
    }
    syncTemplateFieldDisables();
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

  function mergeConfig(baseCfg, applyCfg) {
    const out = compactConfig(baseCfg || {});
    const add = compactConfig(applyCfg || {});

    const hideTabs = new Set([...(out.hide_tabs || []), ...(add.hide_tabs || [])].map((x) => String(x)));
    if (hideTabs.size) out.hide_tabs = [...hideTabs];

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

    if (!out.tabs || typeof out.tabs !== "object") out.tabs = {};
    for (const tab of scopeTabs) delete out.tabs[tab];
    for (const [tabKey, tabCfg] of Object.entries(add.tabs || {})) {
      out.tabs[tabKey] = clone(tabCfg || {});
    }
    if (!Object.keys(out.tabs).length) delete out.tabs;

    return compactConfig(out);
  }

  function applySelectedTemplate() {
    const t = getSelectedTemplate();
    if (!t) {
      setMsg("템플릿을 선택하세요.", true);
      return;
    }
    const selection = collectTemplateSelection();
    if (!selection.tabs.size) {
      setMsg("최소 1개 탭을 선택하세요.", true);
      return;
    }
    const templateSchema = buildScopeSchema(t.config || {});
    const filtered = filterTemplateConfigBySelection(t.config || {}, selection);
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
    markActionSuccess($("#btnTemplateApply"), "✓");
    setMsg("선택한 탭메뉴와 항목들을 불러왔습니다.이제 [저장]을 누르고 [메인으로]를 누르세요");
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
          )}">${escapeHtml(x.site_name || "")}${x.site_code ? ` <code>[${escapeHtml(x.site_code)}]</code>` : ""}</button> <span style="opacity:.75">${escapeHtml(
            x.updated_at || ""
          )}</span>`
      )
      .join("<br/>");
  }

  async function reloadConfig() {
    const site = getSiteName();
    const siteCode = getSiteCode();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    localStorage.setItem(SITE_KEY, site);
    setSiteCode(siteCode);
    const qs = buildSiteQuery(site, siteCode);
    const data = await jfetch(`/api/site_env?${qs}`);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    setConfigToEditor(data.config || {});
    setActiveSchema(data.schema || {});
    renderPreview(data);
    setMsg("환경변수를 불러왔습니다.");
    await loadSiteList().catch(() => {});
  }

  async function saveConfig() {
    const site = getSiteName();
    const siteCode = getSiteCode();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    let cfg = {};
    try {
      cfg = compactConfig(getConfigFromEditor());
    } catch (e) {
      setMsg(`JSON 파싱 오류: ${e.message}`, true);
      return;
    }
    const data = await jfetch("/api/site_env", {
      method: "PUT",
      body: JSON.stringify({ site_name: site, site_code: siteCode || "", config: cfg }),
    });
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    setConfigToEditor(data.config || {});
    setActiveSchema(data.schema || {});
    renderPreview(data);
    markActionSuccess($("#btnSave"), "✓");
    setMsg("저장이 완료되었습니다. 이제 [메인으로]를 눌러 이동하세요.");
    await loadSiteList().catch(() => {});
  }

  async function deleteConfig() {
    const site = getSiteName();
    const siteCode = getSiteCode();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    const ok = confirm(`${site}${siteCode ? ` [${siteCode}]` : ""} 단지의 제원설정을 삭제할까요?`);
    if (!ok) return;
    const qs = buildSiteQuery(site, siteCode);
    await jfetch(`/api/site_env?${qs}`, { method: "DELETE" });
    const data = await jfetch(`/api/schema?${qs}`);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    setConfigToEditor((data && data.site_env_config) || {});
    setActiveSchema((data && data.schema) || {});
    renderPreview({ site_name: site, site_code: (data && data.site_code) || siteCode, schema: (data && data.schema) || {} });
    setMsg("삭제되었습니다.");
    await loadSiteList().catch(() => {});
  }

  async function previewSchema() {
    const site = getSiteName();
    const siteCode = getSiteCode();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    const qs = buildSiteQuery(site, siteCode);
    const data = await jfetch(`/api/schema?${qs}`);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    setActiveSchema(data.schema || {});
    renderPreview({ site_name: site, site_code: data.site_code || siteCode, schema: data.schema || {} });
    setMsg("미리보기를 갱신했습니다.");
  }

  function wire() {
    $("#btnGoMain")?.addEventListener("click", (e) => {
      e.preventDefault();
      const btn = e.currentTarget instanceof HTMLElement ? e.currentTarget : $("#btnGoMain");
      markActionSuccess(btn, "↗");
      setMsg("메인으로 이동합니다.");
      const site = getSiteName();
      const siteCode = getSiteCode();
      const qs = buildSiteQuery(site, siteCode);
      const target = qs ? `/pwa/?${qs}` : "/pwa/";
      window.setTimeout(() => {
        window.location.href = target;
      }, 240);
    });

    $("#btnReload").addEventListener("click", () => reloadConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnSave").addEventListener("click", () => saveConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnDelete").addEventListener("click", () => deleteConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnPreview").addEventListener("click", () => previewSchema().catch((e) => setMsg(e.message || String(e), true)));

    $("#templateSelect").addEventListener("change", updateTemplateDescAndScope);
    $("#btnTemplateApply").addEventListener("click", applySelectedTemplate);
    $("#siteCode")?.addEventListener("change", () => {
      setSiteCode(getSiteCode());
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
      if (e.target.closest("input.tpl-tab")) syncTemplateFieldDisables();
    });

    $("#siteList").addEventListener("click", (e) => {
      const btn = e.target.closest("button.site-item[data-site]");
      if (!btn) return;
      const site = String(btn.dataset.site || "").trim();
      const code = String(btn.dataset.code || "").trim().toUpperCase();
      if (!site) return;
      setSiteName(site);
      setSiteCode(code);
      reloadConfig().catch((err) => setMsg(err.message || String(err), true));
    });
  }

  function enforceSitePolicy() {
    if (!me) return;
    const siteInput = $("#siteName");
    const codeInput = $("#siteCode");
    if (!siteInput || !codeInput) return;
    if (isAdmin(me)) {
      siteInput.readOnly = false;
      codeInput.readOnly = false;
      siteInput.removeAttribute("aria-readonly");
      codeInput.removeAttribute("aria-readonly");
      siteInput.title = "";
      codeInput.title = "";
      return;
    }

    const assignedSite = String(me.site_name || "").trim();
    if (!assignedSite) {
      throw new Error("계정에 소속 단지가 지정되지 않았습니다. 관리자에게 문의하세요.");
    }
    setSiteName(assignedSite);
    const assignedCode = String(me.site_code || "").trim().toUpperCase();
    setSiteCode(assignedCode || "");
    siteInput.readOnly = true;
    codeInput.readOnly = true;
    siteInput.setAttribute("aria-readonly", "true");
    codeInput.setAttribute("aria-readonly", "true");
    siteInput.title = "소속 단지는 관리자만 변경할 수 있습니다.";
    codeInput.title = "소속 단지코드는 관리자만 변경할 수 있습니다.";
  }

  async function init() {
    me = await KAAuth.requireAuth();
    if (!canManageSpecEnv(me)) {
      alert("관리자/단지관리자만 접근할 수 있습니다.");
      window.location.href = "/pwa/";
      return;
    }
    const u = new URL(window.location.href);
    const qSite = (u.searchParams.get("site_name") || "").trim();
    const qCode = (u.searchParams.get("site_code") || "").trim().toUpperCase();
    const stored = (localStorage.getItem(SITE_KEY) || "").trim();
    const storedCode = (localStorage.getItem(SITE_CODE_KEY) || "").trim().toUpperCase();
    setSiteName(qSite || stored || "미지정단지");
    setSiteCode(qCode || storedCode || "");
    enforceSitePolicy();

    wire();
    await loadBaseSchema();
    await reloadConfig().catch((e) => setMsg(e.message || String(e), true));
    await loadTemplates();
    await loadSiteList().catch(() => {});
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();
