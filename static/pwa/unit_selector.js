(() => {
  "use strict";

  const MODE_SMART = "smart";
  const MODE_STRUCTURE = "structure";
  const MODE_SEARCH = "search";
  const MODE_GRID = "grid";
  const LS_RECENT = "ka_unit_selector_recent_v1";
  const LS_FAVORITES = "ka_unit_selector_favorites_v1";
  const MAX_RECENT = 8;
  const MAX_FAVORITES = 12;
  const DEFAULT_BUILDINGS = Array.from({ length: 20 }, (_x, i) => String(101 + i));
  const DEFAULT_LINES = ["01", "02", "03", "04", "05", "06"];
  const DEFAULT_FLOORS = Array.from({ length: 60 }, (_x, i) => i + 1);
  const PROFILE_ENDPOINT = "/api/v1/apartment_profile";
  const PROFILE_CACHE_TTL_MS = 5 * 60 * 1000;
  const PROFILE_CACHE = new Map(); // siteKey -> { ts:number, data:any }

  const esc = (v) =>
    String(v || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

  const byId = (root, id) => root.querySelector(`#${id}`);

  function clampInt(value, fallback, minValue, maxValue) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    const i = Math.trunc(n);
    if (i < minValue) return minValue;
    if (i > maxValue) return maxValue;
    return i;
  }

  function buildBuildings(buildingStart, buildingCount) {
    const start = clampInt(buildingStart, 101, 1, 9999);
    const count = clampInt(buildingCount, 0, 0, 500);
    const out = [];
    for (let i = 0; i < count; i += 1) out.push(String(start + i));
    return out;
  }

  function buildLines(lineCount) {
    const n = clampInt(lineCount, 6, 1, 6);
    const out = [];
    for (let i = 1; i <= n; i += 1) out.push(String(i).padStart(2, "0"));
    return out;
  }

  function floorsUpTo(maxFloor) {
    const n = clampInt(maxFloor, 60, 1, 60);
    return Array.from({ length: n }, (_x, i) => i + 1);
  }

  function normalizeLineLabel(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const m = raw.match(/^(\d{1,2})$/);
    if (!m) return raw;
    const n = clampInt(m[1], 0, 0, 99);
    if (n <= 0) return "";
    return String(n).padStart(2, "0");
  }

  function floorLineToHo(floor, line) {
    const f = Number(floor);
    const l = String(line || "").padStart(2, "0");
    if (!Number.isFinite(f) || f < 1) return "";
    return `${f}${l}`;
  }

  function normalizeUnitText(rawValue, fallbackBuilding = "") {
    const raw = String(rawValue || "").trim();
    if (!raw) return { raw: "", building: "", ho: "", normalized: "", complete: false };
    const compact = raw.replace(/\s+/g, "");

    let m = compact.match(/^(\d{2,4})[-](\d{3,4})$/);
    if (m) {
      return { raw, building: m[1], ho: m[2], normalized: `${m[1]}-${m[2]}`, complete: true };
    }
    m = compact.match(/^(\d{2,4})동(\d{3,4})호?$/);
    if (m) {
      return { raw, building: m[1], ho: m[2], normalized: `${m[1]}-${m[2]}`, complete: true };
    }
    m = compact.match(/^(\d{2,4})(\d{3,4})$/);
    if (m && m[2].length >= 3) {
      return { raw, building: m[1], ho: m[2], normalized: `${m[1]}-${m[2]}`, complete: true };
    }
    m = compact.match(/^(\d{3,4})호?$/);
    if (m) {
      const ho = m[1];
      if (fallbackBuilding) {
        return { raw, building: fallbackBuilding, ho, normalized: `${fallbackBuilding}-${ho}`, complete: true };
      }
      return { raw, building: "", ho, normalized: ho, complete: false };
    }

    return { raw, building: "", ho: "", normalized: raw, complete: false };
  }

  function safeJsonParse(raw, fallback) {
    try {
      const x = JSON.parse(String(raw || ""));
      return x && typeof x === "object" ? x : fallback;
    } catch (_e) {
      return fallback;
    }
  }

  function getStoreMap(key) {
    return safeJsonParse(localStorage.getItem(key), {});
  }

  function setStoreMap(key, mapObj) {
    localStorage.setItem(key, JSON.stringify(mapObj || {}));
  }

  function uniquePushFront(list, value, maxLen) {
    const v = String(value || "").trim();
    if (!v) return list;
    const next = [v, ...list.filter((x) => String(x) !== v)];
    return next.slice(0, maxLen);
  }

  function create(opts = {}) {
    const mount =
      typeof opts.mount === "string" ? document.querySelector(opts.mount) : opts.mount instanceof HTMLElement ? opts.mount : null;
    const input =
      typeof opts.input === "string" ? document.querySelector(opts.input) : opts.input instanceof HTMLInputElement ? opts.input : null;
    if (!mount || !input) return null;

    const siteCodeInput =
      typeof opts.siteCodeInput === "string"
        ? document.querySelector(opts.siteCodeInput)
        : opts.siteCodeInput instanceof HTMLInputElement
          ? opts.siteCodeInput
          : null;
    const siteNameInput =
      typeof opts.siteNameInput === "string"
        ? document.querySelector(opts.siteNameInput)
        : opts.siteNameInput instanceof HTMLInputElement
          ? opts.siteNameInput
          : null;

    const state = {
      mode: MODE_SMART,
      activePanel: MODE_STRUCTURE,
      selected: String(input.value || "").trim(),
      buildings: [...(Array.isArray(opts.buildings) && opts.buildings.length ? opts.buildings : DEFAULT_BUILDINGS)],
      profile: null,
      profile_site_key: "",
      profile_revision: 0,
      searchText: "",
    };

    function siteKey() {
      const sc = String(siteCodeInput && siteCodeInput.value ? siteCodeInput.value : "").trim().toUpperCase();
      const sn = String(siteNameInput && siteNameInput.value ? siteNameInput.value : "").trim();
      return sc || sn || "GLOBAL";
    }

    const buildingCfgCache = new Map(); // building -> cfg

    function activeProfile() {
      const p = state.profile;
      return p && typeof p === "object" ? p : null;
    }

    function profileValueInt(p, key, fallback, minV, maxV) {
      if (!p || typeof p !== "object") return fallback;
      return clampInt(p[key], fallback, minV, maxV);
    }

    function buildingOverride(building) {
      const p = activeProfile();
      const map = p && p.building_overrides && typeof p.building_overrides === "object" ? p.building_overrides : null;
      const key = String(building || "").trim();
      if (!map || !key) return null;
      const item = map[key];
      return item && typeof item === "object" ? item : null;
    }

    function lineCountForBuilding(building) {
      const p = activeProfile();
      const base = profileValueInt(p, "default_line_count", DEFAULT_LINES.length, 1, 6);
      const ov = buildingOverride(building);
      if (ov && Object.prototype.hasOwnProperty.call(ov, "line_count")) {
        return clampInt(ov.line_count, base, 1, 6);
      }
      return base;
    }

    function maxFloorForBuildingDefault(building) {
      const p = activeProfile();
      const base = profileValueInt(p, "default_max_floor", DEFAULT_FLOORS.length, 1, 60);
      const ov = buildingOverride(building);
      if (ov && Object.prototype.hasOwnProperty.call(ov, "max_floor")) {
        return clampInt(ov.max_floor, base, 1, 60);
      }
      return base;
    }

    function maxFloorForLine(building, lineLabel) {
      const base = maxFloorForBuildingDefault(building);
      const ov = buildingOverride(building);
      if (!ov) return base;
      const lineKey = normalizeLineLabel(lineLabel);
      const map = ov.line_max_floors && typeof ov.line_max_floors === "object" ? ov.line_max_floors : null;
      if (!map || !lineKey) return base;
      if (!Object.prototype.hasOwnProperty.call(map, lineKey)) return base;
      return clampInt(map[lineKey], base, 1, 60);
    }

    function buildingConfig(building) {
      const key = String(building || "").trim();
      const cached = buildingCfgCache.get(key);
      if (cached) return cached;
      const lines = buildLines(lineCountForBuilding(key));
      const maxFloorByLine = {};
      let maxAll = 1;
      for (const l of lines) {
        const mf = maxFloorForLine(key, l);
        maxFloorByLine[l] = mf;
        if (mf > maxAll) maxAll = mf;
      }
      const cfg = {
        lines,
        maxFloorAll: maxAll,
        maxFloorByLine,
        floors: floorsUpTo(maxAll),
      };
      buildingCfgCache.set(key, cfg);
      return cfg;
    }

    function setProfile(profileData) {
      const p = profileData && typeof profileData === "object" ? profileData : null;
      state.profile = p;
      state.profile_site_key = siteKey();
      state.profile_revision += 1;
      buildingCfgCache.clear();
    }

    function derivedBuildingsFromProfile(profileData) {
      const p = profileData && typeof profileData === "object" ? profileData : null;
      if (!p) return [...DEFAULT_BUILDINGS];
      const base = buildBuildings(p.building_start, p.building_count);
      const overrides = p.building_overrides && typeof p.building_overrides === "object" ? p.building_overrides : null;
      const overrideKeys = overrides ? Object.keys(overrides).map((x) => String(x || "").trim()).filter(Boolean) : [];
      const rows = base.length ? base : (overrideKeys.length ? overrideKeys : [...DEFAULT_BUILDINGS]);
      const merged = Array.from(new Set([...rows, ...overrideKeys]));
      merged.sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));
      return merged;
    }

    function refreshPanels() {
      mergeBuildingsFromHistory();
      renderModeTabs();
      renderStructurePanel();
      renderSearchPanel();
      renderGridPanel();
    }

    async function fetchApartmentProfile(force = false) {
      const key = siteKey();
      if (!key || key === "GLOBAL") return null;
      const now = Date.now();
      const cached = PROFILE_CACHE.get(key);
      if (!force && cached && typeof cached === "object" && (now - (cached.ts || 0)) < PROFILE_CACHE_TTL_MS) {
        return cached.data || null;
      }
      if (!window.KAAuth || typeof window.KAAuth.requestJson !== "function") return null;
      const sc = String(siteCodeInput && siteCodeInput.value ? siteCodeInput.value : "").trim().toUpperCase();
      const sn = String(siteNameInput && siteNameInput.value ? siteNameInput.value : "").trim();
      const qs = new URLSearchParams();
      if (sc) qs.set("site_code", sc);
      if (sn) qs.set("site_name", sn);
      const url = qs.toString() ? `${PROFILE_ENDPOINT}?${qs.toString()}` : PROFILE_ENDPOINT;
      try {
        const data = await window.KAAuth.requestJson(url);
        if (!data || data.ok !== true) return null;
        PROFILE_CACHE.set(key, { ts: now, data });
        return data;
      } catch (_e) {
        return null;
      }
    }

    async function ensureApartmentProfileLoaded(force = false) {
      const key = siteKey();
      if (!force && state.profile && state.profile_site_key === key) return;
      const data = await fetchApartmentProfile(force);
      if (!data) return;
      setProfile(data);
      state.buildings = derivedBuildingsFromProfile(data);
      refreshPanels();
    }

    function readRecent() {
      const mapObj = getStoreMap(LS_RECENT);
      const rows = Array.isArray(mapObj[siteKey()]) ? mapObj[siteKey()] : [];
      return rows.map((x) => String(x || "").trim()).filter(Boolean);
    }

    function writeRecent(unitLabel) {
      const v = String(unitLabel || "").trim();
      if (!v) return;
      const mapObj = getStoreMap(LS_RECENT);
      const key = siteKey();
      const prev = Array.isArray(mapObj[key]) ? mapObj[key] : [];
      mapObj[key] = uniquePushFront(prev, v, MAX_RECENT);
      setStoreMap(LS_RECENT, mapObj);
    }

    function readFavorites() {
      const mapObj = getStoreMap(LS_FAVORITES);
      const rows = Array.isArray(mapObj[siteKey()]) ? mapObj[siteKey()] : [];
      return rows.map((x) => String(x || "").trim()).filter(Boolean);
    }

    function writeFavorites(rows) {
      const mapObj = getStoreMap(LS_FAVORITES);
      mapObj[siteKey()] = rows.slice(0, MAX_FAVORITES);
      setStoreMap(LS_FAVORITES, mapObj);
    }

    function toggleFavorite(unitLabel) {
      const v = String(unitLabel || "").trim();
      if (!v) return;
      const prev = readFavorites();
      if (prev.includes(v)) {
        writeFavorites(prev.filter((x) => x !== v));
      } else {
        writeFavorites(uniquePushFront(prev, v, MAX_FAVORITES));
      }
    }

    function mergeBuildingsFromHistory() {
      const all = [...readRecent(), ...readFavorites(), String(input.value || "").trim()];
      all.forEach((txt) => {
        const n = normalizeUnitText(txt);
        if (n.building && !state.buildings.includes(n.building)) {
          state.buildings.push(n.building);
        }
      });
      state.buildings.sort((a, b) => Number(a) - Number(b));
    }

    function allUnitCandidates(limit = 12000) {
      const list = [];
      for (const b of state.buildings) {
        const cfg = buildingConfig(b);
        for (const f of cfg.floors) {
          for (const l of cfg.lines) {
            const mf = cfg.maxFloorByLine[l] || cfg.maxFloorAll;
            if (f > mf) continue;
            list.push(`${b}-${floorLineToHo(f, l)}`);
            if (list.length >= limit) return list;
          }
        }
      }
      return list;
    }

    function setSelected(unitLabel, source = "manual") {
      const v = String(unitLabel || "").trim();
      state.selected = v;
      input.value = v;
      if (source !== "init" && v) writeRecent(v);
      renderHeader();
      renderSearchPanel();
      renderStructurePanel();
      renderGridPanel();
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }

    function currentBuildingFromSelected() {
      const n = normalizeUnitText(state.selected);
      return n.building || state.buildings[0] || "";
    }

    function resolveActivePanel() {
      if (state.mode !== MODE_SMART) return state.mode;
      const txt = String(byId(mount, "usSearchInput")?.value || "").trim();
      if (txt.length >= 2) return MODE_SEARCH;
      if (window.matchMedia("(max-width: 880px)").matches) return MODE_GRID;
      return MODE_STRUCTURE;
    }

    function renderHeader() {
      const current = state.selected || "-";
      const favorites = readFavorites();
      const isFav = favorites.includes(current);
      byId(mount, "usCurrentValue").textContent = current;
      const favBtn = byId(mount, "usFavToggle");
      favBtn.textContent = isFav ? "즐겨찾기 해제" : "즐겨찾기";
      favBtn.disabled = !state.selected;
      const modeHint = byId(mount, "usModeHint");
      if (state.mode === MODE_SMART) {
        const mapped = { structure: "구조형(A)", search: "검색형(B)", grid: "그리드형(C)" }[state.activePanel] || "-";
        modeHint.textContent = `추천 모드: 현재 ${mapped}`;
      } else {
        modeHint.textContent = "수동 모드 선택 중";
      }
    }

    function renderModeTabs() {
      const wrap = byId(mount, "usModeTabs");
      const tabs = [
        { key: MODE_SMART, label: "추천 모드" },
        { key: MODE_STRUCTURE, label: "A 구조형" },
        { key: MODE_SEARCH, label: "B 검색형" },
        { key: MODE_GRID, label: "C 그리드형" },
      ];
      wrap.innerHTML = tabs
        .map(
          (t) =>
            `<button class="mode-tab ${state.mode === t.key ? "active" : ""}" type="button" data-mode="${t.key}">${esc(t.label)}</button>`
        )
        .join("");
      state.activePanel = resolveActivePanel();
      [MODE_STRUCTURE, MODE_SEARCH, MODE_GRID].forEach((k) => {
        const panel = byId(mount, `usPanel_${k}`);
        panel.classList.toggle("active", state.activePanel === k);
      });
      renderHeader();
    }

    function buildStructureHoOptions(building, lineOptRaw) {
      const b = String(building || "").trim();
      const lineOpt = normalizeLineLabel(lineOptRaw);
      const cfg = buildingConfig(b);
      const useLines = lineOpt ? [lineOpt] : cfg.lines;
      const items = [];
      cfg.floors.forEach((f) => {
        useLines.forEach((l) => {
          const mf = cfg.maxFloorByLine[l] || cfg.maxFloorAll;
          if (f > mf) return;
          const ho = floorLineToHo(f, l);
          items.push({ value: `${b}-${ho}`, label: `${String(f).padStart(2, "0")}층 ${l}라인 (${ho})` });
        });
      });
      return items;
    }

    function renderStructurePanel() {
      const bSel = byId(mount, "usStructureBuilding");
      const lSel = byId(mount, "usStructureLine");
      const hSel = byId(mount, "usStructureHo");
      if (!bSel || !lSel || !hSel) return;

      const prevLine = String(lSel.value || "").trim();
      const selectedBuilding = currentBuildingFromSelected();
      bSel.innerHTML = state.buildings.map((b) => `<option value="${esc(b)}">${esc(b)}동</option>`).join("");
      if (state.buildings.includes(selectedBuilding)) bSel.value = selectedBuilding;

      const b = String(bSel.value || selectedBuilding);
      const cfg = buildingConfig(b);
      const inferredLine = (() => {
        const n = normalizeUnitText(state.selected);
        const ho = String(n.ho || "");
        if (ho.length < 2) return "";
        return ho.slice(-2);
      })();
      const preferredLine = normalizeLineLabel(prevLine || inferredLine);
      lSel.innerHTML =
        `<option value="">라인 미선택(전체)</option>` + cfg.lines.map((l) => `<option value="${esc(l)}">${esc(l)}라인</option>`).join("");
      if (preferredLine && cfg.lines.includes(preferredLine)) {
        lSel.value = preferredLine;
      }
      const lineOpt = String(lSel.value || "").trim();
      const options = buildStructureHoOptions(b, lineOpt);
      hSel.innerHTML = `<option value="">호 선택</option>` + options.map((x) => `<option value="${esc(x.value)}">${esc(x.label)}</option>`).join("");

      if (state.selected && options.some((x) => x.value === state.selected)) {
        hSel.value = state.selected;
      }
    }

    function candidateScore(row, queryNorm, fallbackBuilding) {
      const txt = String(row || "");
      if (!queryNorm) return 1;
      if (txt === queryNorm.normalized) return 1000;
      if (queryNorm.ho && txt.endsWith(`-${queryNorm.ho}`)) {
        return queryNorm.building && txt.startsWith(`${queryNorm.building}-`) ? 900 : 700;
      }
      if (queryNorm.building && txt.startsWith(`${queryNorm.building}-`)) return 450;
      if (queryNorm.ho && txt.includes(queryNorm.ho)) return 350;
      if (queryNorm.raw && txt.includes(queryNorm.raw.replace(/\s+/g, ""))) return 250;
      if (fallbackBuilding && txt.startsWith(`${fallbackBuilding}-`)) return 100;
      return 0;
    }

    function searchCandidates(rawQuery) {
      const query = String(rawQuery || "").trim();
      const fallbackBuilding = currentBuildingFromSelected();
      const queryNorm = normalizeUnitText(query, fallbackBuilding);
      const base = Array.from(new Set([...readFavorites(), ...readRecent(), ...allUnitCandidates()]));
      let rows = base;
      if (query) {
        rows = base
          .map((x) => ({ value: x, score: candidateScore(x, queryNorm, fallbackBuilding) }))
          .filter((x) => x.score > 0)
          .sort((a, b) => b.score - a.score || a.value.localeCompare(b.value))
          .map((x) => x.value);
      } else {
        rows = [...readFavorites(), ...readRecent(), ...allUnitCandidates().slice(0, 60)];
      }
      return Array.from(new Set(rows)).slice(0, 60);
    }

    function renderSearchPanel() {
      const favWrap = byId(mount, "usFavChips");
      const recentWrap = byId(mount, "usRecentChips");
      const resultWrap = byId(mount, "usSearchResults");
      const qInput = byId(mount, "usSearchInput");
      if (!favWrap || !recentWrap || !resultWrap || !qInput) return;

      const favorites = readFavorites();
      const recents = readRecent();
      favWrap.innerHTML = favorites.length
        ? favorites.map((x) => `<button class="chip favorite" type="button" data-set="${esc(x)}">★ ${esc(x)}</button>`).join("")
        : '<span class="help">즐겨찾기 없음</span>';
      recentWrap.innerHTML = recents.length
        ? recents.map((x) => `<button class="chip" type="button" data-set="${esc(x)}">${esc(x)}</button>`).join("")
        : '<span class="help">최근 사용 없음</span>';

      const rows = searchCandidates(qInput.value);
      if (!rows.length) {
        resultWrap.innerHTML = '<div class="search-item">후보가 없습니다.</div>';
        return;
      }
      resultWrap.innerHTML = rows
        .map((x) => {
          const normalized = normalizeUnitText(x);
          const source = favorites.includes(x) ? "즐겨찾기" : recents.includes(x) ? "최근" : "추천";
          return `
            <button class="search-item" type="button" data-set="${esc(x)}">
              <div class="result-head"><span>${esc(x)}</span><span>${esc(source)}</span></div>
              <div class="help">${esc(normalized.building ? `${normalized.building}동 ${normalized.ho}호` : normalized.ho || "-")}</div>
            </button>
          `;
        })
        .join("");
    }

    function renderGridPanel() {
      const bSel = byId(mount, "usGridBuilding");
      const table = byId(mount, "usGridTable");
      const info = byId(mount, "usGridInfo");
      if (!bSel || !table) return;
      const selectedBuilding = currentBuildingFromSelected();
      bSel.innerHTML = state.buildings.map((b) => `<option value="${esc(b)}">${esc(b)}동</option>`).join("");
      if (state.buildings.includes(selectedBuilding)) bSel.value = selectedBuilding;
      const b = String(bSel.value || selectedBuilding);
      const cfg = buildingConfig(b);
      if (info) {
        const lineHint = cfg.lines.length ? `${cfg.lines[0]}~${cfg.lines[cfg.lines.length - 1]}라인(가로)` : "라인없음";
        info.value = `1~${cfg.maxFloorAll}층(세로) x ${lineHint}`;
      }

      const header = `<tr><th>층/라인</th>${cfg.lines.map((l) => `<th>${esc(l)}</th>`).join("")}</tr>`;
      const body = cfg.floors
        .slice()
        .reverse()
        .map((f) => {
          const cells = cfg.lines
            .map((l) => {
              const mf = cfg.maxFloorByLine[l] || cfg.maxFloorAll;
              if (f > mf) {
                return `<td><button type="button" class="grid-cell disabled" disabled aria-disabled="true"></button></td>`;
              }
              const ho = floorLineToHo(f, l);
              const value = `${b}-${ho}`;
              const active = state.selected === value;
              return `<td><button type="button" class="grid-cell ${active ? "active" : ""}" data-set="${esc(value)}">${esc(ho)}</button></td>`;
            })
            .join("");
          return `<tr><th>${esc(String(f).padStart(2, "0"))}층</th>${cells}</tr>`;
        })
        .join("");
      table.innerHTML = `<thead>${header}</thead><tbody>${body}</tbody>`;
    }

    function render() {
      mergeBuildingsFromHistory();
      mount.innerHTML = `
        <div class="unit-selector">
          <div class="mode-tabs" id="usModeTabs"></div>
          <div class="result-head">
            <span class="current" id="usCurrentValue">-</span>
            <div class="tools">
              <button class="tool-btn" id="usFavToggle" type="button">즐겨찾기</button>
              <button class="tool-btn" id="usClearBtn" type="button">초기화</button>
            </div>
          </div>
          <div class="mode-hint" id="usModeHint"></div>

          <div class="panel" id="usPanel_structure">
            <div class="row">
              <label>
                <span>동 선택</span>
                <select id="usStructureBuilding"></select>
              </label>
              <label>
                <span>라인(선택)</span>
                <select id="usStructureLine"></select>
              </label>
              <label>
                <span>호 선택</span>
                <select id="usStructureHo"></select>
              </label>
            </div>
            <div class="help">A 구조형: 동 -> 라인(선택) -> 호</div>
          </div>

          <div class="panel" id="usPanel_search">
            <div class="row two">
              <label>
                <span>검색 입력</span>
                <input id="usSearchInput" type="text" placeholder="예: 101-1203 / 102동 904호 / 1203" />
              </label>
              <label>
                <span>즉시 변환</span>
                <input id="usSearchNormalized" type="text" readonly />
              </label>
            </div>
            <div class="help">B 검색형: 자동 정규화 + 즉시 후보</div>
            <div class="help">즐겨찾기</div>
            <div class="chip-wrap" id="usFavChips"></div>
            <div class="help">최근 사용</div>
            <div class="chip-wrap" id="usRecentChips"></div>
            <div class="search-results" id="usSearchResults"></div>
          </div>

          <div class="panel" id="usPanel_grid">
            <div class="row two">
              <label>
                <span>동 선택</span>
                <select id="usGridBuilding"></select>
              </label>
              <label>
                <span>그리드 정보</span>
                <input id="usGridInfo" type="text" value="" readonly />
              </label>
            </div>
            <div class="grid-wrap">
              <table id="usGridTable"></table>
            </div>
            <div class="help">C 그리드형: 터치로 즉시 선택</div>
          </div>
        </div>
      `;

      renderModeTabs();
      renderStructurePanel();
      renderSearchPanel();
      renderGridPanel();

      const modeTabs = byId(mount, "usModeTabs");
      const structureBuilding = byId(mount, "usStructureBuilding");
      const structureLine = byId(mount, "usStructureLine");
      const structureHo = byId(mount, "usStructureHo");
      const searchInput = byId(mount, "usSearchInput");
      const searchNorm = byId(mount, "usSearchNormalized");
      const favToggle = byId(mount, "usFavToggle");
      const clearBtn = byId(mount, "usClearBtn");
      const gridBuilding = byId(mount, "usGridBuilding");
      const gridTable = byId(mount, "usGridTable");

      modeTabs?.addEventListener("click", (e) => {
        const btn = e.target.closest(".mode-tab[data-mode]");
        if (!btn) return;
        state.mode = String(btn.dataset.mode || MODE_SMART);
        state.activePanel = resolveActivePanel();
        renderModeTabs();
      });

      structureBuilding?.addEventListener("change", () => {
        renderStructurePanel();
      });
      structureLine?.addEventListener("change", () => {
        renderStructurePanel();
      });
      structureHo?.addEventListener("change", () => {
        const v = String(structureHo.value || "").trim();
        if (!v) return;
        setSelected(v);
      });

      searchInput?.addEventListener("input", () => {
        state.searchText = String(searchInput.value || "");
        const n = normalizeUnitText(searchInput.value, currentBuildingFromSelected());
        searchNorm.value = n.normalized || "";
        if (state.mode === MODE_SMART) {
          state.activePanel = resolveActivePanel();
          renderModeTabs();
        }
        renderSearchPanel();
      });
      searchInput?.addEventListener("keydown", (e) => {
        if (e.key !== "Enter") return;
        const n = normalizeUnitText(searchInput.value, currentBuildingFromSelected());
        if (!n.normalized) return;
        setSelected(n.normalized);
      });

      mount.addEventListener("click", (e) => {
        const setBtn = e.target.closest("[data-set]");
        if (setBtn) {
          const v = String(setBtn.getAttribute("data-set") || "").trim();
          if (v) setSelected(v);
          return;
        }
      });

      favToggle?.addEventListener("click", () => {
        if (!state.selected) return;
        toggleFavorite(state.selected);
        renderHeader();
        renderSearchPanel();
      });

      clearBtn?.addEventListener("click", () => {
        setSelected("");
      });

      gridBuilding?.addEventListener("change", () => {
        renderGridPanel();
      });

      gridTable?.addEventListener("click", (e) => {
        const btn = e.target.closest(".grid-cell[data-set]");
        if (!btn) return;
        const v = String(btn.getAttribute("data-set") || "").trim();
        if (!v) return;
        setSelected(v);
      });

      window.addEventListener("resize", () => {
        if (state.mode === MODE_SMART) {
          state.activePanel = resolveActivePanel();
          renderModeTabs();
        }
      });
    }

    render();
    if (state.selected) {
      const n = normalizeUnitText(state.selected, currentBuildingFromSelected());
      if (n.normalized) {
        setSelected(n.normalized, "init");
      }
    } else {
      renderHeader();
    }

    // Load per-site apartment profile (if available) to drive building/line/floor candidates.
    ensureApartmentProfileLoaded(false).catch(() => {});
    siteCodeInput?.addEventListener("change", () => ensureApartmentProfileLoaded(true).catch(() => {}));
    siteNameInput?.addEventListener("change", () => ensureApartmentProfileLoaded(true).catch(() => {}));

    return {
      getValue() {
        return String(state.selected || "").trim();
      },
      setValue(value) {
        const n = normalizeUnitText(value, currentBuildingFromSelected());
        setSelected(n.normalized || String(value || "").trim());
      },
      refresh() {
        mergeBuildingsFromHistory();
        renderModeTabs();
        renderStructurePanel();
        renderSearchPanel();
        renderGridPanel();
      },
      normalize(value) {
        return normalizeUnitText(value, currentBuildingFromSelected());
      },
    };
  }

  window.KAUnitSelector = { create, normalizeUnitText };
})();
