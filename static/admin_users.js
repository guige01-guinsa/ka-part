(() => {
  const login = new URL(location.href).searchParams.get("login") || "admin";
  const headers = { "X-User-Login": login };

  const qEl = document.getElementById("q");
  const roleFilterEl = document.getElementById("roleFilter");
  const userListEl = document.getElementById("userList");
  const formLogin = document.getElementById("formLogin");
  const formName = document.getElementById("formName");
  const formPhone = document.getElementById("formPhone");
  const formActive = document.getElementById("formActive");
  const formVendor = document.getElementById("formVendor");
  const roleChips = document.getElementById("roleChips");
  const formMsg = document.getElementById("formMsg");
  const btnSave = document.getElementById("btnSave");
  const btnReload = document.getElementById("btnReload");

  let roles = [];
  let vendors = [];
  let selectedUserId = null;
  let selectedRoles = new Set();

  const api = (path, opts = {}) =>
    fetch(path, { ...opts, headers: { ...headers, ...(opts.headers || {}) } })
      .then((r) => r.json());

  function renderRoles() {
    roleChips.innerHTML = "";
    roles.forEach((r) => {
      const btn = document.createElement("button");
      btn.className = "pill";
      btn.textContent = `${r.code} (${r.name})`;
      if (selectedRoles.has(r.code)) btn.classList.add("active");
      btn.onclick = () => {
        if (selectedRoles.has(r.code)) selectedRoles.delete(r.code);
        else selectedRoles.add(r.code);
        renderRoles();
      };
      roleChips.appendChild(btn);
    });
  }

  function renderVendors() {
    formVendor.innerHTML = "";
    const opt0 = document.createElement("option");
    opt0.value = "";
    opt0.textContent = "외주업체 없음";
    formVendor.appendChild(opt0);
    vendors.forEach((v) => {
      const opt = document.createElement("option");
      opt.value = v.id;
      opt.textContent = `${v.name} (${v.phone || "-"})`;
      formVendor.appendChild(opt);
    });
  }

  function renderUserList(items) {
    userListEl.innerHTML = "";
    items.forEach((u) => {
      const div = document.createElement("div");
      div.style.border = "1px solid #25324a";
      div.style.borderRadius = "12px";
      div.style.padding = "10px";
      div.style.marginBottom = "8px";
      const rolesText = (u.roles || []).map((r) => r.code).join(", ");
      div.innerHTML = `
        <div style="font-weight:800;">${u.name} <span class="small">(${u.login})</span></div>
        <div class="small">roles: ${rolesText || "-"}</div>
        <div class="small">phone: ${u.phone || "-"} / active: ${u.is_active}</div>
      `;
      div.onclick = () => selectUser(u);
      userListEl.appendChild(div);
    });
  }

  function selectUser(u) {
    selectedUserId = u.id;
    formLogin.value = u.login;
    formLogin.disabled = true;
    formName.value = u.name || "";
    formPhone.value = u.phone || "";
    formActive.value = String(u.is_active);
    formVendor.value = u.vendor_id || "";
    selectedRoles = new Set((u.roles || []).map((r) => r.code));
    renderRoles();
  }

  function resetForm() {
    selectedUserId = null;
    formLogin.value = "";
    formLogin.disabled = false;
    formName.value = "";
    formPhone.value = "";
    formActive.value = "1";
    formVendor.value = "";
    selectedRoles = new Set();
    renderRoles();
  }

  async function loadAll() {
    const [rolesRes, vendorsRes, usersRes] = await Promise.all([
      api("/api/admin/roles"),
      api("/api/admin/vendors"),
      api(`/api/admin/users?q=${encodeURIComponent(qEl.value || "")}&role=${encodeURIComponent(roleFilterEl.value || "")}`),
    ]);

    roles = rolesRes.items || [];
    vendors = vendorsRes.items || [];
    renderRoles();
    renderVendors();

    roleFilterEl.innerHTML = "";
    const optAll = document.createElement("option");
    optAll.value = "";
    optAll.textContent = "전체";
    roleFilterEl.appendChild(optAll);
    roles.forEach((r) => {
      const opt = document.createElement("option");
      opt.value = r.code;
      opt.textContent = `${r.code} (${r.name})`;
      roleFilterEl.appendChild(opt);
    });

    renderUserList(usersRes.items || []);
  }

  btnReload.onclick = loadAll;
  roleFilterEl.onchange = loadAll;

  btnSave.onclick = async () => {
    formMsg.textContent = "";
    const payload = {
      login: formLogin.value.trim(),
      name: formName.value.trim(),
      phone: formPhone.value.trim() || null,
      is_active: parseInt(formActive.value || "1", 10),
      role_codes: Array.from(selectedRoles),
      vendor_id: formVendor.value ? parseInt(formVendor.value, 10) : null,
    };
    try {
      let res;
      if (selectedUserId) {
        res = await api(`/api/admin/users/${selectedUserId}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json", ...headers },
          body: JSON.stringify(payload),
        });
      } else {
        res = await api("/api/admin/users", {
          method: "POST",
          headers: { "Content-Type": "application/json", ...headers },
          body: JSON.stringify(payload),
        });
      }
      if (!res.ok) throw new Error(res.detail || "failed");
      formMsg.textContent = "저장 완료";
      resetForm();
      await loadAll();
    } catch (e) {
      formMsg.textContent = String(e);
    }
  };

  qEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") loadAll();
  });

  loadAll();
})();
