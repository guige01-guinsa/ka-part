(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  let users = [];
  let roles = [];
  let editingId = null;
  let recommendedCount = 9;
  let me = null;

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
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
    $("#userSiteName").value = "";
    $("#userAddress").value = "";
    $("#userOfficePhone").value = "";
    $("#userOfficeFax").value = "";
    $("#userNote").value = "";
    $("#userPassword").value = "";
    $("#isAdmin").checked = false;
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
    $("#userSiteName").value = user.site_name || "";
    $("#userAddress").value = user.address || "";
    $("#userOfficePhone").value = user.office_phone || "";
    $("#userOfficeFax").value = user.office_fax || "";
    $("#userNote").value = user.note || "";
    $("#userPassword").value = "";
    $("#isAdmin").checked = !!user.is_admin;
    $("#isActive").checked = !!user.is_active;
    $("#userRole").value = user.role || roles[0] || "";
    setMsg("");
  }

  function updateMeta() {
    const el = $("#metaLine");
    if (!el) return;
    const count = users.length;
    const active = users.filter((u) => u.is_active).length;
    const adminCount = users.filter((u) => u.is_admin && u.is_active).length;
    el.textContent = `등록 ${count}명 (활성 ${active}명 / 관리자 ${adminCount}명 / 권장 ${recommendedCount}명)`;
    el.classList.toggle("warn", active > recommendedCount);
  }

  function itemHtml(u) {
    const activeText = u.is_active ? "활성" : "비활성";
    const adminTag = u.is_admin ? "<span class=\"badge\">관리자</span>" : "";
    return `
      <div class="item ${u.is_active ? "" : "inactive"}" data-id="${u.id}">
        <div class="line1">
          <div>
            <div class="name">${escapeHtml(u.name || "")} <span class="login">(${escapeHtml(u.login_id || "")})</span> ${adminTag}</div>
            <div class="line2">${escapeHtml(u.role || "")} / ${escapeHtml(u.phone || "-")} / ${activeText}</div>
            <div class="line2">${escapeHtml(u.site_name || "-")} / 관리소 ${escapeHtml(u.office_phone || "-")} / FAX ${escapeHtml(
      u.office_fax || "-"
    )}</div>
            <div class="line2">주소: ${escapeHtml(u.address || "-")}</div>
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
    const pw = ($("#userPassword").value || "").trim();
    const payload = {
      login_id: ($("#loginId").value || "").trim(),
      name: ($("#userName").value || "").trim(),
      role: $("#userRole").value || "",
      phone: ($("#userPhone").value || "").trim(),
      site_name: ($("#userSiteName").value || "").trim(),
      address: ($("#userAddress").value || "").trim(),
      office_phone: ($("#userOfficePhone").value || "").trim(),
      office_fax: ($("#userOfficeFax").value || "").trim(),
      note: ($("#userNote").value || "").trim(),
      is_admin: !!$("#isAdmin").checked,
      is_active: !!$("#isActive").checked,
    };
    if (pw) payload.password = pw;
    return payload;
  }

  async function saveUser() {
    const body = payloadFromForm();
    if (editingId == null && !body.password) {
      setMsg("신규 사용자는 비밀번호를 입력해야 합니다.", true);
      return;
    }
    if (body.password && body.password.length < 8) {
      setMsg("비밀번호는 8자 이상이어야 합니다.", true);
      return;
    }
    if (editingId == null) {
      await jfetch("/api/users", { method: "POST", body: JSON.stringify(body) });
      setMsg("등록했습니다.");
    } else {
      await jfetch(`/api/users/${editingId}`, { method: "PATCH", body: JSON.stringify(body) });
      setMsg("수정했습니다.");
    }
    await loadUsers();
    clearForm();
  }

  async function removeUser(id) {
    const u = users.find((x) => Number(x.id) === Number(id));
    if (!u) return;
    if (me && Number(me.id) === Number(id)) {
      setMsg("현재 로그인한 계정은 삭제할 수 없습니다.", true);
      return;
    }
    const ok = confirm(`사용자 '${u.name}'를 삭제할까요?`);
    if (!ok) return;
    await jfetch(`/api/users/${id}`, { method: "DELETE" });
    if (editingId === Number(id)) clearForm();
    setMsg("삭제했습니다.");
    await loadUsers();
  }

  function wire() {
    $("#btnReload").addEventListener("click", () => loadUsers().catch((e) => setMsg(e.message, true)));
    $("#btnReset").addEventListener("click", clearForm);
    $("#btnSaveUser").addEventListener("click", () => saveUser().catch((e) => setMsg(e.message, true)));
    $("#btnLogout").addEventListener("click", () => {
      const run = async () => {
        try {
          await jfetch("/api/auth/logout", { method: "POST" });
        } catch (_e) {}
        KAAuth.clearSession();
        KAAuth.redirectLogin("/pwa/users.html");
      };
      run().catch(() => {});
    });

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
    me = await KAAuth.requireAuth();
    if (!me.is_admin) {
      alert("관리자만 접근할 수 있습니다.");
      window.location.href = "/pwa/";
      return;
    }
    await loadRoles();
    clearForm();
    await loadUsers();
    wire();
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();
