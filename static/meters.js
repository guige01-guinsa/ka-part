(function () {
  const $ = (id) => document.getElementById(id);

  const loginEl = $("login");
  const meLine = $("meLine");
  const errBox = $("err");
  const listEl = $("list");
  const emptyEl = $("empty");

  const qEl = $("q");
  const typeEl = $("type");
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

  function escapeHtml(s) {
    return String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
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
          <div style="font-weight:900;">${escapeHtml(it.meter_code || it.id || "-")}</div>
          <div style="font-size:12px;border:1px solid #25324a;border-radius:999px;padding:4px 10px;">
            ${escapeHtml(it.meter_type || it.category || "-")}
          </div>
          ${it.active === 0 ? `<div style="font-size:12px;border:1px solid rgba(220,38,38,.5);border-radius:999px;padding:4px 10px;background:rgba(220,38,38,.15);">INACTIVE</div>` : ""}
        </div>

        <div style="margin-top:8px;font-weight:800;">${escapeHtml(it.name || "-")}</div>

        <div style="margin-top:8px;color:#94a3b8;font-size:12px;display:flex;gap:12px;flex-wrap:wrap;">
          <div>위치: ${escapeHtml(it.location_name || "-")}</div>
          <div>단위: ${escapeHtml(it.unit || "-")}</div>
          <div>자릿수: ${escapeHtml(it.digits ?? "-")}</div>
        </div>

        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
          <button class="pill" data-act="read">오늘 검침 입력(예정)</button>
          <button class="pill" data-act="history">이력 보기(예정)</button>
        </div>
      `;

      div.querySelectorAll("button").forEach((btn) => {
        btn.addEventListener("click", () => {
          const act = btn.getAttribute("data-act");
          alert("아직 API 연결 전입니다: " + act + "\n다음 단계에서 /api/meters + /api/meter_reads 붙이면 즉시 활성화됩니다.");
        });
      });

      listEl.appendChild(div);
    }
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
    const tp = (typeEl && typeEl.value ? typeEl.value : "").trim();

    const params = new URLSearchParams();
    if (q) params.set("q", q);
    if (tp) params.set("type", tp);

    const path = "/api/meters" + (params.toString() ? `?${params.toString()}` : "");

    try {
      const data = await apiGet(path);
      renderItems(data.items || []);
    } catch (e) {
      showErr(
        "검침 API가 아직 연결되지 않았습니다.\n" +
        "다음 단계에서 /api/meters, /api/meter_reads 라우트를 붙이면 즉시 정상 동작합니다.\n\n" +
        String(e.message || e)
      );
      renderItems([]);
    }
  }

  function bind() {
    if (btnReload) btnReload.addEventListener("click", loadList);
    if (qEl) qEl.addEventListener("keydown", (e) => { if (e.key === "Enter") loadList(); });
    if (typeEl) typeEl.addEventListener("change", loadList);
  }

  (async function init() {
    bind();
    await loadMe();
    await loadList();
  })();
})();
