(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  let users = [];
  let roles = [];
  let editingId = null;
  let recommendedCount = 9;

  async function jfetch(url, opts = {}) {
    const res = await fetch(url, {
      headers: { "Content-Type": "application/json" },
      ...opts,
    });
    const ct = res.headers.get("content-type") || "";
    const body = ct.includes("application/json") ? await res.json() : await res.text();
    if (!res.ok) {
      const msg = typeof body === "string" ? body : body.detail || JSON.stringify(body);
      throw new Error(msg || `${res.status}`);
    }
    return body;
  }

  function setMsg(msg, isErr = false) {
    const el = $("#formMsg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function clearForm() {
    editingId = null;
    $("#formTitle").textContent = "사용자 등록";
    $("#loginId").value = "";
    $("#userName").value = "";
    $("#userPhone").value = "";
    $("#userNote").value = "";
    $("#isActive").checked = true;
    if (roles.length) $("#userRole").value = roles[0];
    setMsg("");
  }

  function fillForm(user) {
    editingId = user.id;
    $("#formTitle").textContent = `사용자 수정 #${user.id}`;
    $("#loginId").value = user.login_id || "";
    $("#userName").value = user.name || "";
    $("#userPhone").value = user.phone || "";
    $("#userNote").value = user.note || "";
    $("#isActive").checked = !!user.is_active;
    $("#userRole").value = user.role || roles[0] || "";
    setMsg("");
  }

  function updateMeta() {
    const el = $("#metaLine");
    if (!el) return;
    const count = users.length;
    const active = users.filter((u) => u.is_active).length;
    el.textContent = `등록 ${count}명 (활성 ${active}명) / 권장 ${recommendedCount}명`;
    el.classList.toggle("warn", active > recommendedCount);
  }

  function itemHtml(u) {
    const activeText = u.is_active ? "활성" : "비활성";
    return `
      <div class="item ${u.is_active ? "" : "inactive"}" data-id="${u.id}">
        <div class="line1">
          <div>
            <div class="name">${escapeHtml(u.name || "")} <span class="login">(${escapeHtml(u.login_id || "")})</span></div>
            <div class="line2">${escapeHtml(u.role || "")} / ${escapeHtml(u.phone || "-")} / ${activeText}</div>
          </div>
          <div class="actions">
            <button class="btn" data-action="edit" data-id="${u.id}" type="button">수정</button>
            <button class="btn danger" data-action="delete" data-id="${u.id}" type="button">삭제</button>
          </div>
        </div>
        <div class="line2">${escapeHtml(u.note || "")}</div>
      </div>
    `;
  }

  function escapeHtml(v) {
    return String(v)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function renderUsers() {
    const wrap = $("#userList");
    if (!wrap) return;
    if (!users.length) {
      wrap.innerHTML = '<div class="line2">등록된 사용자가 없습니다.</div>';
      return;
    }
    wrap.innerHTML = users.map(itemHtml).join("");
  }

  async function loadRoles() {
    const data = await jfetch("/api/user_roles");
    roles = Array.isArray(data.roles) ? data.roles : [];
    recommendedCount = Number(data.recommended_staff_count || 9);
    const sel = $("#userRole");
    sel.innerHTML = "";
    for (const role of roles) {
      const o = document.createElement("option");
      o.value = role;
      o.textContent = role;
      sel.appendChild(o);
    }
  }

  async function loadUsers() {
    const data = await jfetch("/api/users");
    users = Array.isArray(data.users) ? data.users : [];
    recommendedCount = Number(data.recommended_staff_count || recommendedCount);
    updateMeta();
    renderUsers();
  }

  function payloadFromForm() {
    return {
      login_id: ($("#loginId").value || "").trim(),
      name: ($("#userName").value || "").trim(),
      role: $("#userRole").value || "",
      phone: ($("#userPhone").value || "").trim(),
      note: ($("#userNote").value || "").trim(),
      is_active: !!$("#isActive").checked,
    };
  }

  async function saveUser() {
    const body = payloadFromForm();
    if (editingId == null) {
      await jfetch("/api/users", { method: "POST", body: JSON.stringify(body) });
      setMsg("등록되었습니다.");
    } else {
      await jfetch(`/api/users/${editingId}`, { method: "PATCH", body: JSON.stringify(body) });
      setMsg("수정되었습니다.");
    }
    await loadUsers();
    clearForm();
  }

  async function removeUser(id) {
    const u = users.find((x) => Number(x.id) === Number(id));
    if (!u) return;
    const ok = confirm(`사용자 '${u.name}'를 삭제할까요?`);
    if (!ok) return;
    await jfetch(`/api/users/${id}`, { method: "DELETE" });
    if (editingId === Number(id)) clearForm();
    setMsg("삭제되었습니다.");
    await loadUsers();
  }

  function wire() {
    $("#btnReload").addEventListener("click", () => loadUsers().catch((e) => setMsg(e.message, true)));
    $("#btnReset").addEventListener("click", clearForm);
    $("#btnSaveUser").addEventListener("click", () => saveUser().catch((e) => setMsg(e.message, true)));

    $("#userList").addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-action]");
      if (!btn) return;
      const id = Number(btn.dataset.id);
      if (btn.dataset.action === "edit") {
        const u = users.find((x) => Number(x.id) === id);
        if (u) fillForm(u);
      } else if (btn.dataset.action === "delete") {
        removeUser(id).catch((err) => setMsg(err.message, true));
      }
    });
  }

  async function init() {
    await loadRoles();
    clearForm();
    await loadUsers();
    wire();
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();
