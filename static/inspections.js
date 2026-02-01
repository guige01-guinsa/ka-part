(function () {
  const $ = (id) => document.getElementById(id);

  const loginEl = $("login");
  const meLine = $("meLine");
  const errBox = $("err");
  const listEl = $("list");
  const emptyEl = $("empty");

  const qEl = $("q");
  const statusEl = $("status");
  const btnReload = $("btnReload");

  function login() {
    return (loginEl && loginEl.value ? loginEl.value : "admin").trim() || "admin";
  }

  async function apiGet(path) {
    const res = await fetch(path, {
      headers: { "X-User-Login": login() },
    });
    const text = await res.text();
    let data = null;
    try { data = JSON.parse(text); } catch (_) {}
    if (!res.ok) {
      const msg = (data && (data.detail || data.error)) ? (data.detail || data.error) : text;
      throw new Error(msg);
    }
    return data || {};
  }

  function showErr(msg) {
    if (!errBox) return;
    errBox.style.display = "block";
    errBox.textContent = msg;
  }
  function clearErr() {
    if (!errBox) return;
    errBox.style.display = "none";
    errBox.textContent = "";
  }

  function renderItems(items) {
    listEl.innerHTML = "";
    if (!items || items.length === 0) {
      emptyEl.style.display = "block";
      return;
    }
    emptyEl.style.display = "none";

    for (const it of items) {
      const div = document.createElement("div");
      div.style.border = "1px solid #25324a";
      div.style.borderRadius = "14px";
      div.style.padding = "12px";
      div.style.background = "rgba(0,0,0,.08)";
      div.innerHTML = `
        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
          <div style="font-weight:900;">${escapeHtml(it.insp_code || it.id || "-")}</div>
          <div style="font-size:12px;border:1px solid #25324a;border-radius:999px;padding:4px 10px;">
            ${escapeHtml(it.status || "-")}
          </div>
        </div>
        <div style="margin-top:8px;font-weight:800;">${escapeHtml(it.title || "-")}</div>
        <div style="margin-top:8px;color:#94a3b8;font-size:12px;display:flex;gap:12px;flex-wrap:wrap;">
          <div>위치: ${escapeHtml(it.location_name || "-")}</div>
          <div>분류: ${escapeHtml(it.category_name || "-")}</div>
          <div>일시: ${escapeHtml(it.performed_at || it.created_at || "-")}</div>
        </div>
      `;
      // 상세 라우트는 추후 연결 (예: /ui/inspection_detail?id=..)
      listEl.appendChild(div);
    }
  }

  function escapeHtml(s) {
    return String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  async function loadMe() {
    try {
      const me = await apiGet("/api/me");
      if (meLine) meLine.textContent = `로그인: ${login()} (${(me.user && (me.user.role || "")) || ""})`;
    } catch (e) {
      if (meLine) meLine.textContent = "/api/me 실패";
      showErr(String(e.message || e));
    }
  }

  async function loadList() {
    clearErr();

    const q = (qEl && qEl.value ? qEl.value : "").trim();
    const st = (statusEl && statusEl.value ? statusEl.value : "").trim();

    // NOTE: 아직 inspections API가 없다면, 여기서 404/500이 날 수 있음.
    // API 붙이면 아래 path만 맞추면 즉시 UI가 살아난다.
    const params = new URLSearchParams();
    if (q) params.set("q", q);
    if (st) params.set("status", st);

    const path = "/api/inspections" + (params.toString() ? `?${params.toString()}` : "");

    try {
      const data = await apiGet(path);
      renderItems(data.items || []);
    } catch (e) {
      showErr(
        "점검 API가 아직 연결되지 않았습니다.\n" +
        "다음 단계에서 /api/inspections 라우트를 붙이면 즉시 정상 동작합니다.\n\n" +
        String(e.message || e)
      );
      renderItems([]);
    }
  }

  function bind() {
    if (btnReload) btnReload.addEventListener("click", loadList);
    if (qEl) qEl.addEventListener("keydown", (e) => { if (e.key === "Enter") loadList(); });
    if (statusEl) statusEl.addEventListener("change", loadList);

    // login 바꾸면 base.html에서 페이지가 재진입됨 (Enter)
    // 여기서는 별도 처리 불필요.
  }

  (async function init() {
    bind();
    await loadMe();
    await loadList();
  })();
})();
