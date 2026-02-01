(() => {
  const login = new URL(location.href).searchParams.get("login") || "admin";
  const headers = { "X-User-Login": login };

  const listEl = document.getElementById("queueList");
  const emptyEl = document.getElementById("empty");
  const btnReload = document.getElementById("btnReload");
  const statusFilter = document.getElementById("statusFilter");
  const qEl = document.getElementById("q");

  const api = (path, opts = {}) =>
    fetch(path, { ...opts, headers: { ...headers, ...(opts.headers || {}) } })
      .then((r) => r.json());

  function render(items) {
    listEl.innerHTML = "";
    if (!items.length) {
      emptyEl.style.display = "block";
      return;
    }
    emptyEl.style.display = "none";
    items.forEach((it) => {
      const div = document.createElement("div");
      div.style.border = "1px solid #25324a";
      div.style.borderRadius = "12px";
      div.style.padding = "10px";
      div.style.marginBottom = "8px";
      const payload = it.payload_json || "";
      div.innerHTML = `
        <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
          <div style="font-weight:800;">#${it.id} [${it.status}]</div>
          <button class="pill">재전송</button>
        </div>
        <div class="small" style="margin-top:6px;">recipient: ${it.recipient || "-"}</div>
        <div class="small">created: ${it.created_at || "-"}</div>
        <div class="small">error: ${it.error || "-"}</div>
        <details style="margin-top:6px;">
          <summary class="small">payload</summary>
          <pre style="white-space:pre-wrap;font-size:11px;">${payload}</pre>
        </details>
      `;
      div.querySelector("button").onclick = async () => {
        const res = await api(`/api/admin/notification-queue/${it.id}/resend`, { method: "POST" });
        if (res.ok) {
          load();
        } else {
          alert(res.error || res.detail || "재전송 실패");
        }
      };
      listEl.appendChild(div);
    });
  }

  async function load() {
    const status = encodeURIComponent(statusFilter.value || "");
    const q = encodeURIComponent(qEl.value || "");
    const res = await api(`/api/admin/notification-queue?limit=200&status=${status}&q=${q}`);
    if (!res.ok) {
      emptyEl.style.display = "block";
      return;
    }
    render(res.items || []);
  }

  btnReload.onclick = load;
  statusFilter.onchange = load;
  qEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") load();
  });
  load();
})();
