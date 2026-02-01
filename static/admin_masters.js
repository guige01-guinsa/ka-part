(() => {
  const login = new URL(location.href).searchParams.get("login") || "admin";
  const headers = { "X-User-Login": login };

  const catList = document.getElementById("catList");
  const locList = document.getElementById("locList");
  const catCode = document.getElementById("catCode");
  const catName = document.getElementById("catName");
  const locCode = document.getElementById("locCode");
  const locName = document.getElementById("locName");
  const locType = document.getElementById("locType");
  const btnCatAdd = document.getElementById("btnCatAdd");
  const btnLocAdd = document.getElementById("btnLocAdd");

  const api = (path, opts = {}) =>
    fetch(path, { ...opts, headers: { ...headers, ...(opts.headers || {}) } })
      .then((r) => r.json());

  function renderCats(items) {
    catList.innerHTML = "";
    items.forEach((c) => {
      const div = document.createElement("div");
      div.style.border = "1px solid #25324a";
      div.style.borderRadius = "12px";
      div.style.padding = "8px";
      div.style.marginBottom = "6px";
      div.innerHTML = `
        <div style="font-weight:800;">${c.name} <span class="small">(${c.code})</span></div>
        <div class="small">#${c.id}</div>
        <div style="margin-top:6px;display:flex;gap:6px;">
          <button class="pill" data-act="edit">수정</button>
          <button class="pill" data-act="del" style="border-color:#dc2626;color:#fecaca;">삭제</button>
        </div>
      `;
      div.querySelector("[data-act='edit']").onclick = async () => {
        const name = prompt("새 이름", c.name);
        if (!name) return;
        await api(`/api/admin/categories/${c.id}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json", ...headers },
          body: JSON.stringify({ name }),
        });
        load();
      };
      div.querySelector("[data-act='del']").onclick = async () => {
        if (!confirm("삭제할까요?")) return;
        await api(`/api/admin/categories/${c.id}`, { method: "DELETE" });
        load();
      };
      catList.appendChild(div);
    });
  }

  function renderLocs(items) {
    locList.innerHTML = "";
    items.forEach((l) => {
      const div = document.createElement("div");
      div.style.border = "1px solid #25324a";
      div.style.borderRadius = "12px";
      div.style.padding = "8px";
      div.style.marginBottom = "6px";
      div.innerHTML = `
        <div style="font-weight:800;">${l.name} <span class="small">(${l.code})</span></div>
        <div class="small">#${l.id} / ${l.type}</div>
        <div style="margin-top:6px;display:flex;gap:6px;">
          <button class="pill" data-act="edit">수정</button>
          <button class="pill" data-act="del" style="border-color:#dc2626;color:#fecaca;">삭제</button>
        </div>
      `;
      div.querySelector("[data-act='edit']").onclick = async () => {
        const name = prompt("새 이름", l.name);
        if (!name) return;
        await api(`/api/admin/locations/${l.id}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json", ...headers },
          body: JSON.stringify({ name }),
        });
        load();
      };
      div.querySelector("[data-act='del']").onclick = async () => {
        if (!confirm("삭제할까요?")) return;
        await api(`/api/admin/locations/${l.id}`, { method: "DELETE" });
        load();
      };
      locList.appendChild(div);
    });
  }

  async function load() {
    const cats = await api("/api/admin/categories");
    const locs = await api("/api/admin/locations");
    renderCats(cats.items || []);
    renderLocs(locs.items || []);
  }

  btnCatAdd.onclick = async () => {
    const code = (catCode.value || "").trim();
    const name = (catName.value || "").trim();
    if (!code || !name) return;
    await api("/api/admin/categories", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({ code, name }),
    });
    catCode.value = "";
    catName.value = "";
    load();
  };

  btnLocAdd.onclick = async () => {
    const code = (locCode.value || "").trim();
    const name = (locName.value || "").trim();
    const type = (locType.value || "COMMON").trim();
    if (!code || !name) return;
    await api("/api/admin/locations", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({ code, name, type }),
    });
    locCode.value = "";
    locName.value = "";
    load();
  };

  load();
})();
