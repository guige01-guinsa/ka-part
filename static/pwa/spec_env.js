(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_KEY = "ka_current_site_name_v1";

  let me = null;
  let templateObj = {};

  function setMsg(msg, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function getSiteName() {
    return ($("#siteName").value || "").trim();
  }

  function setSiteName(name) {
    const v = (name || "").trim();
    $("#siteName").value = v;
    if (v) localStorage.setItem(SITE_KEY, v);
  }

  function getConfigFromEditor() {
    const raw = ($("#envJson").value || "").trim();
    if (!raw) return {};
    return JSON.parse(raw);
  }

  function setConfigToEditor(config) {
    $("#envJson").value = JSON.stringify(config || {}, null, 2);
  }

  function renderPreview(data) {
    const schema = (data && data.schema) || {};
    const lines = [];
    lines.push(`site_name: ${data.site_name || getSiteName()}`);
    lines.push(`tab_count: ${Object.keys(schema).length}`);
    for (const [tabKey, tabDef] of Object.entries(schema)) {
      const title = tabDef.title || tabKey;
      const fields = Array.isArray(tabDef.fields) ? tabDef.fields : [];
      lines.push(`- ${tabKey} (${title}): ${fields.length} fields`);
      for (const f of fields) {
        lines.push(`  * ${f.k} : ${f.label || f.k} [${f.type || "text"}]`);
      }
    }
    $("#preview").textContent = lines.join("\n");
  }

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
  }

  async function loadTemplate() {
    const data = await jfetch("/api/site_env_template");
    templateObj = data.template || {};
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
      .map((x) => `<div>${escapeHtml(x.site_name || "")} <span style="opacity:.75">(${escapeHtml(x.updated_at || "")})</span></div>`)
      .join("");
  }

  function escapeHtml(v) {
    return String(v)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  async function reloadConfig() {
    const site = getSiteName();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    localStorage.setItem(SITE_KEY, site);
    const data = await jfetch(`/api/site_env?site_name=${encodeURIComponent(site)}`);
    setConfigToEditor(data.config || {});
    renderPreview(data);
    setMsg("환경변수를 불러왔습니다.");
    await loadSiteList().catch(() => {});
  }

  async function saveConfig() {
    const site = getSiteName();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    let cfg = {};
    try {
      cfg = getConfigFromEditor();
    } catch (e) {
      setMsg(`JSON 파싱 오류: ${e.message}`, true);
      return;
    }
    const data = await jfetch("/api/site_env", {
      method: "PUT",
      body: JSON.stringify({ site_name: site, config: cfg }),
    });
    setConfigToEditor(data.config || {});
    renderPreview(data);
    setMsg("저장되었습니다.");
    await loadSiteList().catch(() => {});
  }

  async function deleteConfig() {
    const site = getSiteName();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    const ok = confirm(`${site} 단지의 제원설정을 삭제할까요?`);
    if (!ok) return;
    await jfetch(`/api/site_env?site_name=${encodeURIComponent(site)}`, { method: "DELETE" });
    setConfigToEditor({});
    $("#preview").textContent = "";
    setMsg("삭제되었습니다.");
    await loadSiteList().catch(() => {});
  }

  async function previewSchema() {
    const site = getSiteName();
    if (!site) {
      setMsg("site_name을 입력하세요.", true);
      return;
    }
    const data = await jfetch(`/api/schema?site_name=${encodeURIComponent(site)}`);
    renderPreview({ site_name: site, schema: data.schema || {} });
    setMsg("미리보기를 갱신했습니다.");
  }

  function wire() {
    $("#btnReload").addEventListener("click", () => reloadConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnSave").addEventListener("click", () => saveConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnDelete").addEventListener("click", () => deleteConfig().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnPreview").addEventListener("click", () => previewSchema().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnTemplate").addEventListener("click", () => {
      setConfigToEditor(templateObj || {});
      setMsg("템플릿을 입력했습니다. 필요 항목만 수정 후 저장하세요.");
    });
  }

  async function init() {
    me = await KAAuth.requireAuth();
    if (!me.is_admin) {
      alert("관리자만 접근할 수 있습니다.");
      window.location.href = "/pwa/";
      return;
    }
    const u = new URL(window.location.href);
    const qSite = (u.searchParams.get("site_name") || "").trim();
    const stored = (localStorage.getItem(SITE_KEY) || "").trim();
    setSiteName(qSite || stored || "미지정단지");
    wire();
    await loadTemplate();
    await reloadConfig().catch(() => {});
    await loadSiteList().catch(() => {});
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();
