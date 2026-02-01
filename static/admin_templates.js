(() => {
  const login = new URL(location.href).searchParams.get("login") || "admin";
  const headers = { "X-User-Login": login };

  const tplList = document.getElementById("tplList");
  const eventKey = document.getElementById("eventKey");
  const templateCode = document.getElementById("templateCode");
  const enabled = document.getElementById("enabled");
  const messageFormat = document.getElementById("messageFormat");
  const msg = document.getElementById("msg");
  const btnSave = document.getElementById("btnSave");

  const api = (path, opts = {}) =>
    fetch(path, { ...opts, headers: { ...headers, ...(opts.headers || {}) } })
      .then((r) => r.json());

  function renderList(items) {
    tplList.innerHTML = "";
    items.forEach((t) => {
      const div = document.createElement("div");
      div.style.border = "1px solid #25324a";
      div.style.borderRadius = "12px";
      div.style.padding = "10px";
      div.style.marginBottom = "8px";
      div.innerHTML = `
        <div style="font-weight:800;">${t.event_key} <span class="small">(${t.template_code})</span></div>
        <div class="small">enabled: ${t.enabled}</div>
        <div class="small">format: ${t.message_format || "-"}</div>
      `;
      div.onclick = () => {
        eventKey.value = t.event_key || "";
        templateCode.value = t.template_code || "";
        enabled.value = String(t.enabled || 0);
        messageFormat.value = t.message_format || "";
      };
      tplList.appendChild(div);
    });
  }

  async function load() {
    const res = await api("/api/admin/notification-templates");
    if (!res.ok) return;
    renderList(res.items || []);
  }

  btnSave.onclick = async () => {
    msg.textContent = "";
    const payload = {
      event_key: eventKey.value.trim().toUpperCase(),
      template_code: templateCode.value.trim(),
      enabled: parseInt(enabled.value || "1", 10),
      message_format: messageFormat.value.trim() || null,
    };
    const res = await api("/api/admin/notification-templates", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify(payload),
    });
    if (res.ok) {
      msg.textContent = "저장 완료";
      load();
    } else {
      msg.textContent = res.detail || "저장 실패";
    }
  };

  load();
})();
