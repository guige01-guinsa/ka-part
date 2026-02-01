// static/works.js
// 목표:
// 1) 기본 모드 = ALL
// 2) 첫 진입에서도 1건 보이게(오늘 0건이면 ALL로 자동 폴백)
// 3) /api/works 는 서버에서 mode/today/all 등을 혼용할 수 있으니,
//    클라이언트는 "가장 안전한 파라미터"로 요청한다.

(function () {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  const elLogin = $("#login");
  const elMeLine = $("#meLine");
  const elQ = $("#q");
  const elStatus = $("#status");
  const elReload = $("#btnReload");

  const elCards = $("#cards");
  const elEmpty = $("#empty");
  const elErr = $("#err");

  const elSToday = $("#sToday");
  const elSOpen = $("#sOpen");
  const elSUrgent = $("#sUrgent");
  const elSDone = $("#sDone");

  const elNewTitle = $("#newTitle");
  const elNewCategory = $("#newCategory");
  const elNewLocation = $("#newLocation");
  const elNewPriority = $("#newPriority");
  const elCreate = $("#btnCreate");
  const elCreateMsg = $("#createMsg");

  let state = {
    mode: "all",      // ✅ 기본 ALL
    q: "",
    status: "",
    inited: false,
  };

  function esc(s) {
    return String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function showErr(msg) {
    if (!msg) {
      elErr.style.display = "none";
      elErr.textContent = "";
      return;
    }
    elErr.style.display = "block";
    elErr.textContent = msg;
  }

  function setActivePill(key) {
    $$(".pill").forEach((b) => b.classList.remove("active"));
    const btn = $(`.pill[data-quick="${key}"]`);
    if (btn) btn.classList.add("active");
  }

  function headerLogin() {
    const login = (elLogin.value || "").trim() || "admin";
    return { "X-User-Login": login };
  }

  async function apiGet(url) {
    const login = (elLogin.value || "").trim() || "admin";
    let finalUrl = url;
    if (login) {
      try {
        const u = new URL(url, location.origin);
        if (!u.searchParams.get("login")) u.searchParams.set("login", login);
        finalUrl = u.pathname + u.search + u.hash;
      } catch (_) {}
    }
    const res = await fetch(finalUrl, { headers: headerLogin() });
    const text = await res.text();
    let data = null;
    try { data = JSON.parse(text); } catch (e) {}
    if (!res.ok) {
      const errMsg = data?.error || data?.detail || text || `HTTP ${res.status}`;
      throw new Error(errMsg);
    }
    return data ?? {};
  }

  async function apiPost(url, body) {
    const login = (elLogin.value || "").trim() || "admin";
    let finalUrl = url;
    if (login) {
      try {
        const u = new URL(url, location.origin);
        if (!u.searchParams.get("login")) u.searchParams.set("login", login);
        finalUrl = u.pathname + u.search + u.hash;
      } catch (_) {}
    }
    const res = await fetch(finalUrl, {
      method: "POST",
      headers: { ...headerLogin(), "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    const text = await res.text();
    let data = null;
    try { data = JSON.parse(text); } catch (e) {}
    if (!res.ok) {
      const errMsg = data?.error || data?.detail || text || `HTTP ${res.status}`;
      throw new Error(errMsg);
    }
    return data ?? {};
  }

  async function loadMasterData() {
    try {
      const cats = await apiGet("/api/master/categories");
      const locs = await apiGet("/api/master/locations");

      if (elNewCategory) {
        elNewCategory.innerHTML = "";
        const opt0 = document.createElement("option");
        opt0.value = "";
        opt0.textContent = "분류 선택";
        elNewCategory.appendChild(opt0);
        (cats.items || []).forEach((c) => {
          const opt = document.createElement("option");
          opt.value = c.id;
          opt.textContent = `${c.name} (#${c.id})`;
          elNewCategory.appendChild(opt);
        });
      }

      if (elNewLocation) {
        elNewLocation.innerHTML = "";
        const opt0 = document.createElement("option");
        opt0.value = "";
        opt0.textContent = "위치 선택";
        elNewLocation.appendChild(opt0);
        (locs.items || []).forEach((l) => {
          const opt = document.createElement("option");
          opt.value = l.id;
          opt.textContent = `${l.name} (#${l.id})`;
          elNewLocation.appendChild(opt);
        });
      }
    } catch (e) {
      if (elCreateMsg) elCreateMsg.textContent = "분류/위치 불러오기 실패";
    }
  }

  function buildWorksUrl(mode) {
    // 서버가 /api/works?mode=all|today... 형태로 동작하므로 그 포맷을 따른다.
    const params = new URLSearchParams();
    if (mode) params.set("mode", mode);

    const q = (elQ.value || "").trim();
    const st = (elStatus.value || "").trim();

    if (q) params.set("q", q);
    if (st) params.set("status", st);

    const qs = params.toString();
    return "/api/works" + (qs ? `?${qs}` : "");
  }

  function badgeHtml(w) {
    const st = (w.status || "").toUpperCase();
    const urgent = Number(w.urgent || 0);

    let cls = "badge";
    if (st === "DONE") cls += " bDone";
    else if (urgent) cls += " bUrgent";
    else if (st && st !== "NEW") cls += " bWarn";

    const label = urgent ? `URGENT · ${st || "-"}` : (st || "-");
    return `<span class="${cls}">${esc(label)}</span>`;
  }

  function renderStats(counts) {
    // 서버가 counts를 내려주기도/안 내려주기도 하므로 안전하게 처리
    const c = counts || {};
    elSToday.textContent = String(c.today ?? "-");
    elSOpen.textContent = String(c.open ?? "-");
    elSUrgent.textContent = String(c.urgent ?? "-");
    elSDone.textContent = String(c.done ?? "-");
  }

  function renderCards(items) {
    elCards.innerHTML = "";
    if (!items || items.length === 0) {
      elEmpty.style.display = "block";
      return;
    }
    elEmpty.style.display = "none";

    for (const w of items) {
      const loc = w.location_name || "-";
      const cat = w.category_name || "-";
      const due = w.due_date ? `기한: ${w.due_date}` : "";
      const src = w.source_type ? `출처: ${w.source_type}` : "";
      const meta2 = [due, src].filter(Boolean).join(" · ");

      const html = `
        <div class="work" data-id="${esc(w.id)}">
          <div class="topline">
            <span class="code">${esc(w.work_code || "")}</span>
            ${badgeHtml(w)}
          </div>
          <div class="title">${esc(w.title || "")}</div>
          <div class="meta">
            <span>위치: ${esc(loc)}</span>
            <span>분류: ${esc(cat)}</span>
          </div>
          ${meta2 ? `<div class="small" style="margin-top:10px;">${esc(meta2)}</div>` : ""}
          <div class="small" style="margin-top:6px;">업데이트: ${esc(w.updated_at || w.created_at || "")}</div>
        </div>
      `;
      elCards.insertAdjacentHTML("beforeend", html);
    }

    // 클릭 → 상세로 이동(기존 UI 라우트 패턴 유지)
    $$(".work").forEach((card) => {
      card.addEventListener("click", () => {
        const id = card.getAttribute("data-id");
        const login = encodeURIComponent((elLogin.value || "admin").trim() || "admin");
        window.location.href = `/ui/works/${id}?login=${login}`;
      });
    });
  }

  async function loadMe() {
    try {
      const me = await apiGet("/api/me");
      const who = me?.user?.login || me?.login || (elLogin.value || "admin");
      const role = me?.user?.role ? ` (${me.user.role})` : "";
      elMeLine.textContent = `인증 OK: ${who}${role}`;
      showErr("");
    } catch (e) {
      elMeLine.textContent = "인증 확인 실패(/api/me)";
      showErr(String(e.message || e));
    }
  }

  async function loadWorks(mode) {
    const url = buildWorksUrl(mode);
    const data = await apiGet(url);

    // 응답 포맷 방어: items, counts
    const items = data.items || [];
    const counts = data.counts || null;

    renderStats(counts);
    renderCards(items);

    return items.length;
  }

  async function refresh() {
    showErr("");

    const mode = state.mode;
    setActivePill(mode.toUpperCase());

    try {
      // 1) 먼저 현재 모드로 로드
      const n = await loadWorks(mode);

      // 2) ✅ "첫 진입 1건" 보장: 첫 진입에만 TODAY가 비면 ALL로 폴백
      if (!state.inited) {
        state.inited = true;

        if (mode === "today" && n === 0) {
          state.mode = "all";
          setActivePill("ALL");
          await loadWorks("all");
        }
      }
    } catch (e) {
      showErr(String(e.message || e));
      // 오류 나면 카드 비움
      renderCards([]);
      renderStats(null);
    }
  }

  function wire() {
    // quick pill
    $$(".pill").forEach((b) => {
      b.addEventListener("click", async () => {
        const key = b.getAttribute("data-quick") || "ALL";

        // UI 키 → API mode
        // 서버는 mode=all|today|open|urgent|done 를 지원하므로 그대로 전달
        if (key === "TODAY") {
          state.mode = "today";
          elStatus.value = "";
        } else if (key === "DONE") {
          state.mode = "done";
          elStatus.value = "";
        } else if (key === "ALL") {
          state.mode = "all";
          elStatus.value = "";
        } else if (key === "OPEN") {
          state.mode = "open";
          elStatus.value = "";
        } else if (key === "URGENT") {
          state.mode = "urgent";
          elStatus.value = "";
        } else {
          state.mode = "all";
        }

        setActivePill(key);
        await refresh();
      });
    });

    // reload
    elReload.addEventListener("click", refresh);

    // create
    if (elCreate) {
      elCreate.addEventListener("click", async () => {
        if (elCreateMsg) elCreateMsg.textContent = "";
        try {
          const title = (elNewTitle.value || "").trim();
          const category_id = parseInt(elNewCategory.value || "0", 10);
          const location_id = parseInt(elNewLocation.value || "0", 10);
          const priority = parseInt(elNewPriority.value || "3", 10);
          if (!title || !category_id || !location_id) {
            throw new Error("제목/분류ID/위치ID를 입력하세요.");
          }
          await apiPost("/api/works", {
            title,
            category_id,
            location_id,
            priority,
          });
          elNewTitle.value = "";
          if (elCreateMsg) elCreateMsg.textContent = "생성 완료";
          await refresh();
        } catch (e) {
          if (elCreateMsg) elCreateMsg.textContent = String(e.message || e);
        }
      });
    }

    // search inputs
    let t = null;
    function debounceRefresh() {
      clearTimeout(t);
      t = setTimeout(() => refresh(), 220);
    }
    elQ.addEventListener("input", debounceRefresh);
    elStatus.addEventListener("change", refresh);

    // login changes → re-check me and reload
    elLogin.addEventListener("change", async () => {
      await loadMe();
      await refresh();
    });
  }

  async function init() {
    // ✅ 첫 진입에서 TODAY로 시작하지 않는다: ALL 기본
    setActivePill("ALL");
    await loadMe();
    await loadMasterData();
    await refresh();
  }

  wire();
  init();
})();

