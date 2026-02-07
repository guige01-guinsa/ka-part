(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const PREFERRED_TAB_ORDER = [
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
  const LOCKED_ROWS = {
    tr450: [
      ["lv1_L1_V", "lv1_L1_A", "lv1_L1_KW"],
      ["lv1_L2_V", "lv1_L2_A", "lv1_L2_KW"],
      ["lv1_L3_V", "lv1_L3_A", "lv1_L3_KW"],
      ["lv1_temp"],
    ],
    tr400: [
      ["lv2_L1_V", "lv2_L1_A", "lv2_L1_KW"],
      ["lv2_L2_V", "lv2_L2_A", "lv2_L2_KW"],
      ["lv2_L3_V", "lv2_L3_A", "lv2_L3_KW"],
      ["lv2_temp"],
    ],
    meter: [
      ["AISS_L1_A", "AISS_L2_A", "AISS_L3_A"],
      ["main_kwh", "industry_kwh", "street_kwh"],
    ],
    facility_check: [
      ["tank_level_1", "tank_level_2"],
      ["hydrant_pressure", "sp_pump_pressure"],
      ["high_pressure", "low_pressure"],
      ["office_pressure", "shop_pressure"],
    ],
  };
  const COMPACT_TABS = new Set(["tr450", "tr400", "meter", "facility_check"]);
  const HOME_DRAFT_KEY = "ka_home_draft_v2";
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const DEFAULT_SITE_NAME = "미지정단지";

  let TABS = [];
  let rangeDates = [];
  let rangeIndex = -1;
  let authUser = null;

  function toast(msg) {
    const el = $("#toast");
    if (!el) return;
    el.textContent = msg;
    el.classList.add("show");
    clearTimeout(el._t);
    el._t = setTimeout(() => el.classList.remove("show"), 2200);
  }

  function ymdToday() {
    const d = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  }

  async function jfetch(url, opts = {}) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    return window.KAAuth.requestJson(url, opts);
  }

  function setCurrentUserChip(user) {
    const chip = $("#currentUser");
    if (!chip) return;
    if (!user) {
      chip.textContent = "미로그인";
      return;
    }
    const role = user.is_admin ? "관리자" : "일반";
    chip.textContent = `${user.name || user.login_id} (${role})`;
  }

  async function ensureAuth() {
    authUser = await window.KAAuth.requireAuth();
    setCurrentUserChip(authUser);
    const btnUsers = $("#btnUsers");
    if (btnUsers && !authUser.is_admin) btnUsers.style.display = "none";
    const btnSpec = $("#btnSpecEnv");
    if (btnSpec && !authUser.is_admin) btnSpec.style.display = "none";
  }

  function parseFilename(contentDisposition, fallback) {
    const value = String(contentDisposition || "");
    const utf8 = /filename\*=UTF-8''([^;]+)/i.exec(value);
    if (utf8 && utf8[1]) {
      try {
        return decodeURIComponent(utf8[1]);
      } catch (_e) {}
    }
    const plain = /filename=\"?([^\";]+)\"?/i.exec(value);
    if (plain && plain[1]) return plain[1];
    return fallback;
  }

  async function downloadWithAuth(url, fallbackName) {
    const token = window.KAAuth.getToken();
    if (!token) {
      window.KAAuth.redirectLogin();
      return;
    }
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (res.status === 401) {
      window.KAAuth.clearSession();
      window.KAAuth.redirectLogin();
      return;
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`${res.status} ${res.statusText} ${txt}`.trim());
    }
    const blob = await res.blob();
    const filename = parseFilename(res.headers.get("content-disposition"), fallbackName);
    const href = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = href;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(href);
  }

  function getSiteName() {
    const el = $("#siteName");
    return (el && el.value ? el.value : "").trim() || DEFAULT_SITE_NAME;
  }

  function setSiteName(name) {
    const clean = (name || "").trim() || DEFAULT_SITE_NAME;
    const el = $("#siteName");
    if (el) el.value = clean;
    localStorage.setItem(SITE_NAME_KEY, clean);
    return clean;
  }

  function resolveSiteName() {
    const u = new URL(window.location.href);
    const q = (u.searchParams.get("site_name") || u.searchParams.get("site") || "").trim();
    if (q) return setSiteName(q);
    const stored = (localStorage.getItem(SITE_NAME_KEY) || "").trim();
    if (stored) return setSiteName(stored);
    return setSiteName(DEFAULT_SITE_NAME);
  }

  function getDateStart() {
    const el = $("#dateStart");
    return el && el.value ? el.value : ymdToday();
  }

  function getDateEnd() {
    const el = $("#dateEnd");
    return el && el.value ? el.value : getDateStart();
  }

  function getPickedDate() {
    return getDateStart();
  }

  function sortSchemaKeys(keys) {
    return [...keys].sort((a, b) => {
      const ia = PREFERRED_TAB_ORDER.indexOf(a);
      const ib = PREFERRED_TAB_ORDER.indexOf(b);
      const va = ia >= 0 ? ia : PREFERRED_TAB_ORDER.length + 100;
      const vb = ib >= 0 ? ib : PREFERRED_TAB_ORDER.length + 100;
      return va - vb || a.localeCompare(b);
    });
  }

  function normalizeTabsFromSchema(schema) {
    if (!schema || typeof schema !== "object") return [];
    const keys = sortSchemaKeys(Object.keys(schema));
    const tabs = [];
    for (const key of keys) {
      const def = schema[key];
      if (!def || !Array.isArray(def.fields)) continue;
      tabs.push({
        key,
        title: def.title || key,
        fields: def.fields,
        rows: Array.isArray(def.rows) ? def.rows : null,
      });
    }
    return tabs;
  }

  function chunk(arr, n) {
    const out = [];
    for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
    return out;
  }

  function inferRows(tab) {
    const locked = LOCKED_ROWS[tab.key];
    if (locked) return locked;
    if (Array.isArray(tab.rows) && tab.rows.length) return tab.rows;
    const keys = tab.fields.map((f) => f.k);
    const allNumeric = tab.fields.every((f) => f.type === "number");
    if (COMPACT_TABS.has(tab.key) || (allNumeric && keys.length >= 6)) return chunk(keys, 3);
    if (keys.length >= 4) return chunk(keys, 2);
    return keys.map((k) => [k]);
  }

  function shortLabel(tabKey, field) {
    if (tabKey === "tr450" || tabKey === "tr400") {
      const m = /_(L[1-3])_/.exec(field.k || "");
      const phase = m ? `${m[1]}-` : "";
      if (field.k.endsWith("_V")) return `${phase}V`;
      if (field.k.endsWith("_A")) return `${phase}A`;
      if (field.k.endsWith("_KW")) return `${phase}KW`;
      if (field.k.endsWith("_temp")) return "온도";
    }
    return field.label || field.k;
  }

  function clearWarn(wrap) {
    wrap.classList.remove("warn");
    const msg = wrap.querySelector(".warnmsg");
    if (msg) msg.remove();
  }

  function setWarn(wrap, text) {
    wrap.classList.add("warn");
    let msg = wrap.querySelector(".warnmsg");
    if (!msg) {
      msg = document.createElement("div");
      msg.className = "warnmsg";
      wrap.appendChild(msg);
    }
    msg.textContent = text || "범위 벗어남";
  }

  function validateFieldValue(wrap, input) {
    const wmin = input.dataset.warnMin;
    const wmax = input.dataset.warnMax;
    if (wmin === undefined && wmax === undefined) return;
    const raw = (input.value || "").trim();
    if (!raw) {
      clearWarn(wrap);
      return;
    }
    const v = Number(raw);
    if (Number.isNaN(v)) {
      clearWarn(wrap);
      return;
    }
    const minv = wmin !== undefined && wmin !== "" ? Number(wmin) : null;
    const maxv = wmax !== undefined && wmax !== "" ? Number(wmax) : null;
    const bad = (minv !== null && v < minv) || (maxv !== null && v > maxv);
    if (!bad) {
      clearWarn(wrap);
      return;
    }
    const t =
      minv !== null && maxv !== null
        ? `${minv}~${maxv}`
        : minv !== null
        ? `>= ${minv}`
        : maxv !== null
        ? `<= ${maxv}`
        : "범위";
    setWarn(wrap, `허용범위: ${t}`);
  }

  function countWarnings() {
    return document.querySelectorAll(".input.warn").length;
  }

  function createFieldWrap(tabKey, field) {
    const wrap = document.createElement("div");
    wrap.className = COMPACT_TABS.has(tabKey) ? "input compact" : "input";
    wrap.dataset.tab = tabKey;
    wrap.dataset.field = field.k;

    const lab = document.createElement("label");
    lab.textContent = shortLabel(tabKey, field);
    wrap.appendChild(lab);

    let input;
    if (field.type === "textarea") {
      input = document.createElement("textarea");
      input.placeholder = field.placeholder || "";
    } else if (field.type === "select") {
      input = document.createElement("select");
      const opts = Array.isArray(field.options) ? field.options : [];
      for (const opt of opts) {
        const o = document.createElement("option");
        o.value = opt;
        o.textContent = opt;
        input.appendChild(o);
      }
    } else {
      input = document.createElement("input");
      input.type = field.type || "text";
      input.placeholder = field.placeholder || "";
      if (field.type === "number") {
        input.inputMode = "decimal";
        if (field.step !== undefined) input.step = String(field.step);
        if (field.min !== undefined) input.min = String(field.min);
        if (field.max !== undefined) input.max = String(field.max);
      }
    }

    input.id = `f-${tabKey}-${field.k}`;
    if (field.warn_min !== undefined) input.dataset.warnMin = String(field.warn_min);
    if (field.warn_max !== undefined) input.dataset.warnMax = String(field.warn_max);
    input.addEventListener("input", () => validateFieldValue(wrap, input));
    input.addEventListener("change", () => validateFieldValue(wrap, input));
    wrap.appendChild(input);
    setTimeout(() => validateFieldValue(wrap, input), 0);
    return wrap;
  }

  function render() {
    const tabsEl = $("#tabs");
    const panelsEl = $("#panels");
    if (!tabsEl || !panelsEl) {
      alert("앱 초기화 오류: 화면 요소를 찾지 못했습니다.");
      return;
    }
    tabsEl.innerHTML = "";
    panelsEl.innerHTML = "";

    for (const tab of TABS) {
      const b = document.createElement("button");
      b.className = "tabbtn";
      b.type = "button";
      b.dataset.tab = tab.key;
      b.textContent = tab.title;
      tabsEl.appendChild(b);

      const p = document.createElement("section");
      p.className = "panel";
      p.id = `panel-${tab.key}`;
      const h = document.createElement("h2");
      h.textContent = tab.title;
      p.appendChild(h);

      const grid = document.createElement("div");
      grid.className = "grid";
      const byKey = {};
      for (const f of tab.fields) byKey[f.k] = f;

      for (const rowKeys of inferRows(tab)) {
        const row = document.createElement("div");
        row.className = "field-row";
        row.dataset.count = String(rowKeys.length);
        for (const k of rowKeys) {
          if (byKey[k]) row.appendChild(createFieldWrap(tab.key, byKey[k]));
        }
        if (row.childElementCount) grid.appendChild(row);
      }

      p.appendChild(grid);
      panelsEl.appendChild(p);
    }

    if (TABS.length) activateTab(TABS[0].key);
  }

  function activateTab(tabKey) {
    for (const b of document.querySelectorAll(".tabbtn")) {
      b.classList.toggle("active", b.dataset.tab === tabKey);
    }
    for (const p of document.querySelectorAll(".panel")) {
      p.classList.toggle("active", p.id === `panel-${tabKey}`);
    }
  }

  function collectAllTabs() {
    const out = {};
    for (const tab of TABS) {
      out[tab.key] = {};
      for (const field of tab.fields) {
        const el = document.getElementById(`f-${tab.key}-${field.k}`);
        out[tab.key][field.k] = el ? el.value ?? "" : "";
      }
    }
    return out;
  }

  function fillTabs(tabs) {
    for (const tab of TABS) {
      const values = tabs && tabs[tab.key] ? tabs[tab.key] : {};
      for (const field of tab.fields) {
        const el = document.getElementById(`f-${tab.key}-${field.k}`);
        if (!el) continue;
        el.value = values[field.k] ?? "";
      }
    }
  }

  function getActivePanel() {
    return document.querySelector(".panel.active");
  }

  function listFocusableInPanel(panel) {
    if (!panel) return [];
    const els = Array.from(panel.querySelectorAll("input, select, textarea"));
    return els.filter((el) => !el.disabled && el.offsetParent !== null);
  }

  function focusNextInActive(current) {
    const panel = getActivePanel();
    const els = listFocusableInPanel(panel);
    const idx = els.indexOf(current);
    if (idx >= 0 && idx < els.length - 1) {
      const next = els[idx + 1];
      next.focus();
      if (next.tagName === "INPUT" || next.tagName === "TEXTAREA") {
        try {
          next.select?.();
        } catch (_e) {}
      }
    }
  }

  function initAutoAdvance() {
    document.addEventListener(
      "keydown",
      (e) => {
        const panel = getActivePanel();
        if (!panel) return;
        const t = e.target;
        if (!panel.contains(t)) return;
        if (e.key === "Enter") {
          if (t && t.tagName === "TEXTAREA") return;
          e.preventDefault();
          focusNextInActive(t);
        }
      },
      true
    );
  }

  function saveHomeDraft() {
    try {
      const home = TABS.find((t) => t.key === "home");
      if (!home) return;
      const obj = {};
      for (const f of home.fields) {
        const el = document.getElementById(`f-home-${f.k}`);
        if (el) obj[f.k] = el.value ?? "";
      }
      localStorage.setItem(HOME_DRAFT_KEY, JSON.stringify(obj));
    } catch (_e) {}
  }

  function loadHomeDraft() {
    try {
      const raw = localStorage.getItem(HOME_DRAFT_KEY);
      if (!raw) return null;
      const obj = JSON.parse(raw);
      return obj && typeof obj === "object" ? obj : null;
    } catch (_e) {
      return null;
    }
  }

  function applyHomeDraft(obj) {
    if (!obj) return;
    const home = TABS.find((t) => t.key === "home");
    if (!home) return;
    for (const f of home.fields) {
      const el = document.getElementById(`f-home-${f.k}`);
      if (el && obj[f.k] !== undefined) el.value = obj[f.k];
    }
  }

  function clearHomeDraft() {
    try {
      localStorage.removeItem(HOME_DRAFT_KEY);
    } catch (_e) {}
    const home = TABS.find((t) => t.key === "home");
    if (!home) return;
    for (const f of home.fields) {
      const el = document.getElementById(`f-home-${f.k}`);
      if (el) el.value = "";
    }
  }

  async function loadOne(site, date) {
    const url = `/api/load?site_name=${encodeURIComponent(site)}&date=${encodeURIComponent(date)}`;
    const data = await jfetch(url);
    fillTabs(data.tabs || {});
  }

  async function loadRange() {
    const site = getSiteName();
    const df = getDateStart();
    const dt = getDateEnd();
    const url = `/api/list_range?site_name=${encodeURIComponent(site)}&date_from=${encodeURIComponent(df)}&date_to=${encodeURIComponent(dt)}`;
    const data = await jfetch(url);
    rangeDates = data && Array.isArray(data.dates) ? data.dates : [];
    if (!rangeDates.length) {
      fillTabs({});
      rangeIndex = -1;
      toast("해당 기간에 기록이 없습니다.");
      return;
    }
    rangeIndex = rangeDates.length - 1;
    const showDate = rangeDates[rangeIndex];
    const ds = $("#dateStart");
    if (ds) ds.value = showDate;
    await loadOne(site, showDate);
    toast(`기간 ${df}~${dt} · ${rangeDates.length}건 · 표시 ${showDate}`);
  }

  async function doLoad() {
    await loadRange();
  }

  async function doPrev() {
    if (!rangeDates.length) {
      await loadRange();
      return;
    }
    if (rangeIndex <= 0) {
      toast("처음 날짜입니다.");
      return;
    }
    rangeIndex -= 1;
    const showDate = rangeDates[rangeIndex];
    const ds = $("#dateStart");
    if (ds) ds.value = showDate;
    await loadOne(getSiteName(), showDate);
    toast(`표시 ${showDate} (${rangeIndex + 1}/${rangeDates.length})`);
  }

  async function doNext() {
    if (!rangeDates.length) {
      await loadRange();
      return;
    }
    if (rangeIndex >= rangeDates.length - 1) {
      toast("마지막 날짜입니다.");
      return;
    }
    rangeIndex += 1;
    const showDate = rangeDates[rangeIndex];
    const ds = $("#dateStart");
    if (ds) ds.value = showDate;
    await loadOne(getSiteName(), showDate);
    toast(`표시 ${showDate} (${rangeIndex + 1}/${rangeDates.length})`);
  }

  async function doSave() {
    const wc = countWarnings();
    if (wc > 0) {
      const ok = confirm(`허용범위를 벗어난 값이 ${wc}개 있습니다. 저장할까요?`);
      if (!ok) return;
    }
    const payload = {
      site_name: getSiteName(),
      date: getPickedDate(),
      tabs: collectAllTabs(),
    };
    await jfetch("/api/save", { method: "POST", body: JSON.stringify(payload) });
    await loadRange().catch(() => {});
    toast("저장 완료");
  }

  async function doDelete() {
    const date = getPickedDate();
    const ok = confirm(`${date} 데이터를 삭제할까요?`);
    if (!ok) return;
    const url = `/api/delete?site_name=${encodeURIComponent(getSiteName())}&date=${encodeURIComponent(date)}`;
    await jfetch(url, { method: "DELETE" });
    await loadRange().catch(() => {});
    toast("삭제 완료");
  }

  async function doExport() {
    const url = `/api/export?site_name=${encodeURIComponent(getSiteName())}&date_from=${encodeURIComponent(
      getDateStart()
    )}&date_to=${encodeURIComponent(getDateEnd())}`;
    await downloadWithAuth(url, "export.xlsx");
  }

  async function doPdf() {
    const url = `/api/pdf?site_name=${encodeURIComponent(getSiteName())}&date=${encodeURIComponent(getPickedDate())}`;
    await downloadWithAuth(url, "report.pdf");
  }

  function syncStickyOffset() {
    const header = document.querySelector(".top");
    const h = header ? header.offsetHeight : 0;
    document.documentElement.style.setProperty("--tabs-top", `${h}px`);
  }

  function wire() {
    const today = ymdToday();
    const ds = $("#dateStart");
    const de = $("#dateEnd");
    if (ds && !ds.value) ds.value = today;
    if (de && !de.value) de.value = today;

    initAutoAdvance();
    syncStickyOffset();
    window.addEventListener("resize", syncStickyOffset);

    $("#tabs")?.addEventListener("click", (e) => {
      const btn = e.target.closest(".tabbtn");
      if (!btn) return;
      activateTab(btn.dataset.tab);
    });

    $("#siteName")?.addEventListener("change", () => {
      const next = setSiteName(getSiteName());
      const u = new URL(window.location.href);
      u.searchParams.set("site_name", next);
      window.location.href = `${u.pathname}?${u.searchParams.toString()}`;
    });

    document.getElementById("panel-home")?.addEventListener("input", (e) => {
      const t = e.target;
      if (t instanceof HTMLInputElement || t instanceof HTMLTextAreaElement || t instanceof HTMLSelectElement) {
        saveHomeDraft();
      }
    });

    $("#btnPrev")?.addEventListener("click", () => doPrev().catch((err) => alert("이전 오류: " + err.message)));
    $("#btnLoad")?.addEventListener("click", () => doLoad().catch((err) => alert("조회 오류: " + err.message)));
    $("#btnNext")?.addEventListener("click", () => doNext().catch((err) => alert("다음 오류: " + err.message)));
    $("#btnSave")?.addEventListener("click", () => doSave().catch((err) => alert("저장 오류: " + err.message)));
    $("#btnDelete")?.addEventListener("click", () => doDelete().catch((err) => alert("삭제 오류: " + err.message)));
    $("#btnExport")?.addEventListener("click", () => {
      doExport().catch((err) => alert("엑셀 오류: " + err.message));
    });
    $("#btnPdf")?.addEventListener("click", () => {
      doPdf().catch((err) => alert("PDF 오류: " + err.message));
    });
    $("#btnUsers")?.addEventListener("click", () => {
      const site = getSiteName();
      window.location.href = `/pwa/users.html?site_name=${encodeURIComponent(site)}`;
    });
    $("#btnSpecEnv")?.addEventListener("click", () => {
      const site = getSiteName();
      window.location.href = `/pwa/spec_env.html?site_name=${encodeURIComponent(site)}`;
    });
    $("#btnLogout")?.addEventListener("click", () => {
      const run = async () => {
        try {
          await jfetch("/api/auth/logout", { method: "POST" });
        } catch (_e) {}
        window.KAAuth.clearSession();
        window.KAAuth.redirectLogin("/pwa/");
      };
      run().catch(() => {});
    });
    $("#btnExit")?.addEventListener("click", () => {
      const ok = confirm("홈 입력값을 비우고 종료할까요?\n(종료 버튼을 누르기 전까지 입력값은 유지됩니다.)");
      if (!ok) return;
      clearHomeDraft();
      activateTab("home");
      window.scrollTo(0, 0);
      toast("종료 처리");
    });
  }

  async function init() {
    await ensureAuth();
    const siteName = resolveSiteName();
    const data = await jfetch(`/api/schema?site_name=${encodeURIComponent(siteName)}`);
    const schema = data && data.schema ? data.schema : null;
    if (data && data.site_name) setSiteName(String(data.site_name));
    if (!schema) throw new Error("스키마를 불러오지 못했습니다.");
    TABS = normalizeTabsFromSchema(schema);
    if (!TABS.length) throw new Error("스키마 탭이 비어있습니다.");
    render();
    wire();
    syncStickyOffset();
    applyHomeDraft(loadHomeDraft());
    await doLoad().catch(() => {});
    applyHomeDraft(loadHomeDraft());

    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/pwa/sw.js?v=20260208a").catch(() => {});
    }
  }

  init().catch((err) => {
    const msg = err && err.message ? err.message : String(err);
    if (msg.includes("로그인이 필요")) return;
    alert("앱 초기화 오류: " + msg);
  });
})();

