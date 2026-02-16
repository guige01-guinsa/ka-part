(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  const SITE_ID_KEY = "ka_current_site_id_v1";
  const ACTION_SUCCESS_BUTTON_IDS = ["btnReload", "btnSave", "btnGoMain"];

  let me = null;
  let buildingOverrides = {};

  function canManage(user) {
    return !!(user && (user.is_admin || user.is_site_admin));
  }

  function isAdmin(user) {
    return !!(user && user.is_admin);
  }

  function isSuperAdmin(user) {
    if (!user || !user.is_admin) return false;
    return String(user.admin_scope || "").trim().toLowerCase() === "super_admin";
  }

  function canViewSiteIdentity(user) {
    return isSuperAdmin(user);
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

  function setMsg(msg, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function clearActionSuccess(exceptId = "") {
    for (const id of ACTION_SUCCESS_BUTTON_IDS) {
      if (exceptId && id === exceptId) continue;
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

  async function jfetch(url, opts = {}) {
    return window.KAAuth.requestJson(url, opts);
  }

  function normalizeSiteId(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    const id = Math.trunc(n);
    return id > 0 ? id : 0;
  }

  function getSiteId() {
    return normalizeSiteId(localStorage.getItem(SITE_ID_KEY) || "");
  }

  function setSiteId(siteId) {
    const clean = normalizeSiteId(siteId);
    if (clean > 0) localStorage.setItem(SITE_ID_KEY, String(clean));
    else localStorage.removeItem(SITE_ID_KEY);
    return clean;
  }

  function getSiteName() {
    return ($("#siteName").value || "").trim();
  }

  function setSiteName(name) {
    const v = String(name || "").trim();
    $("#siteName").value = v;
    if (v) localStorage.setItem(SITE_NAME_KEY, v);
    else localStorage.removeItem(SITE_NAME_KEY);
  }

  function getSiteCode() {
    return ($("#siteCode").value || "").trim().toUpperCase();
  }

  function setSiteCode(code) {
    const v = String(code || "").trim().toUpperCase();
    $("#siteCode").value = v;
    if (v) localStorage.setItem(SITE_CODE_KEY, v);
    else localStorage.removeItem(SITE_CODE_KEY);
  }

  function buildSiteQuery(siteName, siteCode) {
    const qs = new URLSearchParams();
    const siteId = getSiteId();
    if (siteId > 0) qs.set("site_id", String(siteId));
    const s = String(siteName || "").trim();
    const c = String(siteCode || "").trim().toUpperCase();
    if (s) qs.set("site_name", s);
    if (c) qs.set("site_code", c);
    return qs.toString();
  }

  function intOr(value, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.trunc(n);
  }

  function cleanInt(value, fallback, minV, maxV, field) {
    const n = intOr(value, fallback);
    if (n < minV || n > maxV) throw new Error(`${field} 값은 ${minV}~${maxV} 범위여야 합니다.`);
    return n;
  }

  function computedBuildings() {
    const start = intOr($("#buildingStart").value, 101);
    const count = intOr($("#buildingCount").value, 20);
    const s = Math.max(1, Math.min(start, 9999));
    const c = Math.max(0, Math.min(count, 500));
    const out = [];
    for (let i = 0; i < c; i += 1) out.push(String(s + i));
    return out;
  }

  function updateBuildingsPreview() {
    const el = $("#buildingsPreview");
    if (!el) return;
    const b = computedBuildings();
    if (!b.length) {
      el.textContent = "동 목록: (미설정)";
      return;
    }
    const head = b.slice(0, 4).join(", ");
    const tail = b.length > 8 ? b.slice(-3).join(", ") : "";
    el.textContent = b.length > 8 ? `동 목록: ${head} ... ${tail} (총 ${b.length}동)` : `동 목록: ${b.join(", ")} (총 ${b.length}동)`;
  }

  function updateDefaultsPreview() {
    const el = $("#defaultsPreview");
    if (!el) return;
    const lc = intOr($("#defaultLineCount").value, 8);
    const mf = intOr($("#defaultMaxFloor").value, 60);
    const bf = intOr($("#defaultBasementFloors").value, 0);
    el.textContent = `기본 구조: 1~${mf}층, 01~${String(Math.max(1, Math.min(lc, 8))).padStart(2, "0")}라인, 지하 ${Math.max(0, bf)}층`;
  }

  function renderOverrides() {
    const wrap = $("#overrideList");
    if (!wrap) return;
    const keys = Object.keys(buildingOverrides || {}).map((x) => String(x || "").trim()).filter(Boolean);
    keys.sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));
    if (!keys.length) {
      wrap.innerHTML = '<div class="hint">동별 설정이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = keys
      .map((b) => {
        const it = buildingOverrides[b] && typeof buildingOverrides[b] === "object" ? buildingOverrides[b] : {};
        const lineCount = intOr(it.line_count, "");
        const maxFloor = intOr(it.max_floor, "");
        const basement = intOr(it.basement_floors, "");
        const lineMax = it.line_max_floors && typeof it.line_max_floors === "object" ? it.line_max_floors : {};
        const lcEffective = lineCount ? Math.max(1, Math.min(lineCount, 8)) : intOr($("#defaultLineCount").value, 8);

        const lineInputs = Array.from({ length: 8 }, (_x, i) => String(i + 1).padStart(2, "0")).map((l) => {
          const v = Object.prototype.hasOwnProperty.call(lineMax, l) ? intOr(lineMax[l], "") : "";
          const disabled = Number(l) > lcEffective;
          return `
            <label class="mini-field">
              <label>${l}라인</label>
              <input class="ov-line-max" data-building="${b}" data-line="${l}" type="number" min="1" max="60" value="${v}" ${disabled ? "disabled" : ""} />
            </label>
          `;
        });

        return `
          <div class="ov-item" data-building="${b}">
            <div class="ov-head">
              <div class="ov-title">${b}동</div>
              <button class="btn danger ov-del" type="button" data-building="${b}">삭제</button>
            </div>
            <div class="ov-grid">
              <label class="mini-field">
                <label>라인수</label>
                <input class="ov-field" data-building="${b}" data-field="line_count" type="number" min="1" max="8" value="${lineCount}" placeholder="(기본값 사용)" />
              </label>
              <label class="mini-field">
                <label>최고층</label>
                <input class="ov-field" data-building="${b}" data-field="max_floor" type="number" min="1" max="60" value="${maxFloor}" placeholder="(기본값 사용)" />
              </label>
              <label class="mini-field">
                <label>지하층수</label>
                <input class="ov-field" data-building="${b}" data-field="basement_floors" type="number" min="0" max="20" value="${basement}" placeholder="(기본값 사용)" />
              </label>
            </div>
            <details>
              <summary>라인별 최고층(선택)</summary>
              <div class="line-grid">
                ${lineInputs.join("")}
              </div>
              <div class="hint">비워두면 동별/기본 최고층이 적용됩니다.</div>
            </details>
          </div>
        `;
      })
      .join("");
  }

  function collectProfile() {
    const households_total = cleanInt($("#householdsTotal").value, 0, 0, 200000, "세대수");
    const building_start = cleanInt($("#buildingStart").value, 101, 1, 9999, "동 시작번호");
    const building_count = cleanInt($("#buildingCount").value, 20, 0, 500, "동 수");
    const default_line_count = cleanInt($("#defaultLineCount").value, 8, 1, 8, "기본 라인수");
    const default_max_floor = cleanInt($("#defaultMaxFloor").value, 60, 1, 60, "기본 최고층");
    const default_basement_floors = cleanInt($("#defaultBasementFloors").value, 0, 0, 20, "기본 지하층수");

    const outOverrides = {};
    const src = buildingOverrides && typeof buildingOverrides === "object" ? buildingOverrides : {};
    for (const [b, raw] of Object.entries(src)) {
      const key = String(b || "").trim();
      if (!key) continue;
      if (!raw || typeof raw !== "object") continue;
      const item = {};
      if (Object.prototype.hasOwnProperty.call(raw, "line_count")) {
        item.line_count = cleanInt(intOr(raw.line_count, 0), 0, 1, 8, "라인수");
      }
      if (Object.prototype.hasOwnProperty.call(raw, "max_floor")) {
        item.max_floor = cleanInt(intOr(raw.max_floor, 0), 0, 1, 60, "최고층");
      }
      if (Object.prototype.hasOwnProperty.call(raw, "basement_floors")) {
        item.basement_floors = cleanInt(intOr(raw.basement_floors, 0), 0, 0, 20, "지하층수");
      }

      const lm = raw.line_max_floors && typeof raw.line_max_floors === "object" ? raw.line_max_floors : {};
      const lineOut = {};
      for (const [lk, lv] of Object.entries(lm)) {
        const lineKey = String(lk || "").trim().padStart(2, "0");
        if (!/^\d{2}$/.test(lineKey)) continue;
        if (Number(lineKey) < 1 || Number(lineKey) > 8) continue;
        const v = intOr(lv, 0);
        if (!v) continue;
        lineOut[lineKey] = cleanInt(v, 0, 1, 60, `라인별 최고층(${lineKey})`);
      }
      if (Object.keys(lineOut).length) item.line_max_floors = lineOut;

      if (Object.keys(item).length) outOverrides[key] = item;
    }

    return {
      households_total,
      building_start,
      building_count,
      default_line_count,
      default_max_floor,
      default_basement_floors,
      building_overrides: outOverrides,
    };
  }

  function updatePreview() {
    updateBuildingsPreview();
    updateDefaultsPreview();
    try {
      const profile = collectProfile();
      $("#jsonPreview").textContent = JSON.stringify(profile, null, 2);
      setMsg("");
    } catch (e) {
      $("#jsonPreview").textContent = "";
      setMsg(e.message || String(e), true);
    }
  }

  function applyProfileToForm(data) {
    $("#householdsTotal").value = intOr(data.households_total, 0);
    $("#buildingStart").value = intOr(data.building_start, 101);
    $("#buildingCount").value = intOr(data.building_count, 20);
    $("#defaultLineCount").value = String(intOr(data.default_line_count, 8));
    $("#defaultMaxFloor").value = intOr(data.default_max_floor, 60);
    $("#defaultBasementFloors").value = intOr(data.default_basement_floors, 0);
    buildingOverrides = data.building_overrides && typeof data.building_overrides === "object" ? data.building_overrides : {};
    renderOverrides();
    updatePreview();
  }

  async function syncSiteIdentity(silent = true) {
    const site = getSiteName();
    const code = getSiteCode();
    const siteId = getSiteId();
    if (!site && !code && siteId <= 0) {
      if (!silent) setMsg("site_name 또는 site_code를 입력하세요.", true);
      return null;
    }
    const qs = buildSiteQuery(site, code);
    const data = await jfetch(`/api/site_identity?${qs}`);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_id")) setSiteId(data.site_id);
    if (data && Object.prototype.hasOwnProperty.call(data, "site_name")) setSiteName(data.site_name || "");
    if (data && Object.prototype.hasOwnProperty.call(data, "site_code")) setSiteCode(data.site_code || "");
    return data || null;
  }

  async function loadProfile() {
    await syncSiteIdentity(false);
    const qs = buildSiteQuery(getSiteName(), getSiteCode());
    const data = await jfetch(`/api/apartment_profile?${qs}`);
    applyProfileToForm(data || {});
    markActionSuccess($("#btnReload"), "↺");
    setMsg("아파트 정보를 불러왔습니다.");
  }

  async function saveProfile() {
    await syncSiteIdentity(false);
    const profile = collectProfile();
    const data = await jfetch("/api/apartment_profile", {
      method: "PUT",
      body: JSON.stringify({
        site_id: getSiteId(),
        site_name: getSiteName(),
        site_code: getSiteCode(),
        profile,
        mfa_confirmed: true,
        reason: "apartment_profile_save",
      }),
    });
    applyProfileToForm(data || {});
    markActionSuccess($("#btnSave"), "✓");
    setMsg("저장이 완료되었습니다. 민원 동/호 선택 기본정보로 사용됩니다.");
  }

  async function deleteProfile() {
    await syncSiteIdentity(false);
    const site = getSiteName();
    const code = getSiteCode();
    const label = canViewSiteIdentity(me) ? `${site}${code ? ` [${code}]` : ""}` : "현재 단지";
    const ok = confirm(`${label}의 아파트 정보 설정을 삭제할까요?`);
    if (!ok) return;
    const qs = buildSiteQuery(site, code);
    await jfetch(`/api/apartment_profile?${qs}`, { method: "DELETE", headers: { "X-KA-MFA-VERIFIED": "1" } });
    applyProfileToForm({
      households_total: 0,
      building_start: 101,
      building_count: 20,
      default_line_count: 8,
      default_max_floor: 60,
      default_basement_floors: 0,
      building_overrides: {},
    });
    markActionSuccess($("#btnDelete"), "✕");
    setMsg("삭제되었습니다.");
  }

  function addBuildingOverride() {
    const all = computedBuildings();
    const used = new Set(Object.keys(buildingOverrides || {}));
    const candidate = all.find((b) => !used.has(b)) || all[0] || "";
    if (!candidate) {
      setMsg("동 목록(동 시작번호/동 수)을 먼저 설정하세요.", true);
      return;
    }
    const defaults = {
      line_count: intOr($("#defaultLineCount").value, 8),
      max_floor: intOr($("#defaultMaxFloor").value, 60),
      basement_floors: intOr($("#defaultBasementFloors").value, 0),
      line_max_floors: {},
    };
    buildingOverrides = buildingOverrides && typeof buildingOverrides === "object" ? buildingOverrides : {};
    buildingOverrides[candidate] = buildingOverrides[candidate] || defaults;
    renderOverrides();
    updatePreview();
    setMsg(`${candidate}동 설정을 추가했습니다.`);
  }

  function wireOverrideEvents() {
    const wrap = $("#overrideList");
    if (!wrap) return;

    wrap.addEventListener("click", (e) => {
      const delBtn = e.target.closest(".ov-del[data-building]");
      if (delBtn) {
        const b = String(delBtn.getAttribute("data-building") || "").trim();
        if (b && buildingOverrides && Object.prototype.hasOwnProperty.call(buildingOverrides, b)) {
          delete buildingOverrides[b];
          renderOverrides();
          updatePreview();
        }
      }
    });

    wrap.addEventListener("input", (e) => {
      const t = e.target;
      if (!(t instanceof HTMLInputElement)) return;
      const b = String(t.getAttribute("data-building") || "").trim();
      if (!b) return;
      buildingOverrides = buildingOverrides && typeof buildingOverrides === "object" ? buildingOverrides : {};
      const row = buildingOverrides[b] && typeof buildingOverrides[b] === "object" ? buildingOverrides[b] : {};
      const field = String(t.getAttribute("data-field") || "").trim();
      const line = String(t.getAttribute("data-line") || "").trim();

      if (field) {
        const raw = String(t.value || "").trim();
        if (!raw) {
          delete row[field];
        } else {
          row[field] = intOr(raw, 0);
        }
        buildingOverrides[b] = row;
        renderOverrides();
        updatePreview();
        return;
      }

      if (line) {
        row.line_max_floors = row.line_max_floors && typeof row.line_max_floors === "object" ? row.line_max_floors : {};
        const raw = String(t.value || "").trim();
        if (!raw) {
          delete row.line_max_floors[line];
        } else {
          row.line_max_floors[line] = intOr(raw, 0);
        }
        buildingOverrides[b] = row;
        updatePreview();
      }
    });
  }

  function applyPermissionPolicy() {
    const canEditSite = isSuperAdmin(me);
    $("#siteName").readOnly = !canEditSite;
    $("#siteCode").readOnly = !canEditSite;
    if (!canEditSite) {
      $("#siteName").title = "단지명 입력/수정은 관리자만 가능합니다.";
      $("#siteCode").title = "단지코드 입력/수정은 관리자만 가능합니다.";
    }

    const show = canViewSiteIdentity(me);
    const nameWrap = $("#siteName")?.closest(".field");
    const codeWrap = $("#siteCode")?.closest(".field");
    if (nameWrap) nameWrap.classList.toggle("hidden", !show);
    if (codeWrap) codeWrap.classList.toggle("hidden", !show);
  }

  function loadSiteFromQueryOrStorage() {
    const u = new URL(window.location.href);
    const qName = (u.searchParams.get("site_name") || "").trim();
    const qCode = (u.searchParams.get("site_code") || "").trim().toUpperCase();
    const qId = normalizeSiteId(u.searchParams.get("site_id") || "");
    if (qId > 0) setSiteId(qId);
    if (qName) setSiteName(qName);
    else setSiteName((localStorage.getItem(SITE_NAME_KEY) || "").trim());
    if (qCode) setSiteCode(qCode);
    else setSiteCode((localStorage.getItem(SITE_CODE_KEY) || "").trim());
  }

  function wire() {
    $("#btnGoMain")?.addEventListener("click", (e) => {
      e.preventDefault();
      markActionSuccess(e.currentTarget, "↗");
      const qs = canViewSiteIdentity(me) ? buildSiteQuery(getSiteName(), getSiteCode()) : "";
      window.setTimeout(() => {
        window.location.href = qs ? `/pwa/?${qs}` : "/pwa/";
      }, 200);
    });
    $("#btnReload")?.addEventListener("click", () => loadProfile().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnSave")?.addEventListener("click", () => saveProfile().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnDelete")?.addEventListener("click", () => deleteProfile().catch((e) => setMsg(e.message || String(e), true)));
    $("#btnAddBuilding")?.addEventListener("click", () => addBuildingOverride());
    $("#btnClearOverrides")?.addEventListener("click", () => {
      const ok = confirm("동별설정을 전체 삭제할까요?");
      if (!ok) return;
      buildingOverrides = {};
      renderOverrides();
      updatePreview();
    });

    document.body.addEventListener("input", (e) => {
      const t = e.target;
      if (!(t instanceof HTMLInputElement) && !(t instanceof HTMLSelectElement)) return;
      if (["householdsTotal", "buildingStart", "buildingCount", "defaultLineCount", "defaultMaxFloor", "defaultBasementFloors"].includes(t.id)) {
        updatePreview();
      }
    });

    wireOverrideEvents();
  }

  async function init() {
    me = await window.KAAuth.requireAuth();
    if (!canManage(me)) {
      alert("아파트 정보 설정은 관리자/단지대표자만 사용할 수 있습니다.");
      window.location.href = "/pwa/";
      return;
    }
    loadSiteFromQueryOrStorage();
    if (!canViewSiteIdentity(me)) {
      stripSiteIdentityFromUrl();
      if (me && me.site_id) setSiteId(me.site_id);
      if (me && me.site_name) setSiteName(me.site_name);
      if (me && me.site_code) setSiteCode(me.site_code);
    }
    if (getSiteId() <= 0) setSiteId(me && me.site_id);
    if (!getSiteName()) setSiteName(me && me.site_name);
    if (!getSiteCode()) setSiteCode(me && me.site_code);
    applyPermissionPolicy();
    renderOverrides();
    updatePreview();
    wire();
    await loadProfile().catch(() => {});
  }

  init().catch((e) => {
    setMsg(e.message || String(e), true);
  });
})();
