(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  let me = null;
  let moduleCtx = null;
  let profile = null;

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
  }

  function setMsg(msg, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function permissionLabel(user) {
    const key = String(user && user.permission_level ? user.permission_level : user && user.is_admin ? "admin" : user && user.is_site_admin ? "site_admin" : "user");
    if (key === "admin") {
      const scopeLabel = String((user && user.admin_scope_label) || "").trim();
      if (scopeLabel) return scopeLabel;
      return "최고/운영관리자";
    }
    const map = {
      site_admin: "단지대표자",
      user: "사용자",
      security_guard: "보안/경비",
      resident: "입주민",
      board_member: "입대의",
    };
    return map[key] || key;
  }

  function isSuperAdmin(user) {
    if (!user || !user.is_admin) return false;
    return String(user.admin_scope || "").trim().toLowerCase() === "super_admin";
  }

  function isResidentRole(user) {
    const txt = String((user && user.role) || "").trim();
    return txt === "입주민" || txt === "주민" || txt === "세대주민";
  }

  function siteDisplay(user) {
    if (!isSuperAdmin(user)) return "(숨김)";
    const code = String((user && user.site_code) || "").trim();
    const name = String((user && user.site_name) || "").trim();
    if (code && name) return `${code} / ${name}`;
    if (name) return name;
    if (code) return code;
    return "-";
  }

  function updateMetaLine() {
    const el = $("#metaLine");
    if (!el) return;
    const u = profile || me || {};
    const name = String(u.name || u.login_id || "사용자");
    const level = permissionLabel(u);
    const site = siteDisplay(u);
    const unit = String(u.unit_label || "").trim();
    const unitText = unit ? ` · ${unit}` : "";
    el.textContent = `${name} (${level}) · ${site}${unitText}`;
  }

  function fillForm(u) {
    $("#loginId").value = String(u.login_id || "");
    $("#permissionLevel").value = permissionLabel(u);
    $("#siteDisplay").value = siteDisplay(u);

    const unitLabelSpan = $("#unitLabel")?.closest(".field")?.querySelector("span");
    if (unitLabelSpan) {
      unitLabelSpan.textContent = isResidentRole(u) ? "동/호(필수)" : "동/호(선택)";
    }
    $("#unitLabel").value = String(u.unit_label || "");

    $("#name").value = String(u.name || "");
    $("#phone").value = String(u.phone || "");
    $("#address").value = String(u.address || "");
    $("#officePhone").value = String(u.office_phone || "");
    $("#officeFax").value = String(u.office_fax || "");
    $("#note").value = String(u.note || "");
  }

  function payloadFromForm() {
    return {
      name: ($("#name").value || "").trim(),
      phone: ($("#phone").value || "").trim(),
      address: ($("#address").value || "").trim(),
      office_phone: ($("#officePhone").value || "").trim(),
      office_fax: ($("#officeFax").value || "").trim(),
      unit_label: ($("#unitLabel").value || "").trim(),
      note: ($("#note").value || "").trim(),
      current_password: ($("#currentPassword").value || "").trim(),
    };
  }

  async function loadProfile() {
    const data = await jfetch("/api/users/me");
    const u = data && data.user ? data.user : null;
    if (!u) throw new Error("내 정보를 불러오지 못했습니다.");
    profile = u;
    fillForm(u);
    updateMetaLine();
    setMsg("");
  }

  async function saveProfile() {
    const body = payloadFromForm();
    if (!body.current_password) {
      setMsg("저장하려면 현재 비밀번호를 입력하세요.", true);
      return;
    }
    const data = await jfetch("/api/users/me", { method: "PATCH", body: JSON.stringify(body) });
    const u = data && data.user ? data.user : null;
    if (u) {
      profile = u;
      fillForm(u);
      updateMetaLine();
    }
    $("#currentPassword").value = "";
    setMsg("저장했습니다.");
  }

  async function changePassword() {
    const oldPw = ($("#oldPassword").value || "").trim();
    const newPw = ($("#newPassword").value || "").trim();
    const newPw2 = ($("#newPassword2").value || "").trim();
    if (!oldPw) {
      setMsg("현재 비밀번호를 입력하세요.", true);
      return;
    }
    if (!newPw) {
      setMsg("새 비밀번호를 입력하세요.", true);
      return;
    }
    if (newPw !== newPw2) {
      setMsg("새 비밀번호 확인이 일치하지 않습니다.", true);
      return;
    }
    const data = await jfetch("/api/auth/change_password", {
      method: "POST",
      body: JSON.stringify({ old_password: oldPw, new_password: newPw }),
    });
    if (data && data.token && data.user) {
      KAAuth.setSession(String(data.token), data.user);
      profile = data.user;
      fillForm(profile);
      updateMetaLine();
    }
    $("#oldPassword").value = "";
    $("#newPassword").value = "";
    $("#newPassword2").value = "";
    setMsg("비밀번호를 변경했습니다.");
  }

  async function withdrawAccount() {
    const pw = ($("#withdrawPassword").value || "").trim();
    const confirmText = ($("#withdrawConfirm").value || "").trim();
    if (!pw) {
      setMsg("탈퇴하려면 현재 비밀번호를 입력하세요.", true);
      return;
    }
    if (confirmText !== "탈퇴") {
      setMsg("확인 문구에 '탈퇴'를 입력하세요.", true);
      return;
    }
    const ok = confirm("정말 탈퇴할까요? 탈퇴 후에는 로그인할 수 없습니다.");
    if (!ok) return;
    await jfetch("/api/users/me/withdraw", { method: "POST", body: JSON.stringify({ password: pw, confirm: "탈퇴" }) });
    KAAuth.clearSession({ includeSensitive: true, broadcast: true });
    window.location.replace("/pwa/login.html");
  }

  function wire() {
    $("#btnReload")?.addEventListener("click", () => {
      loadProfile().catch((e) => setMsg(e.message || String(e), true));
    });
    $("#btnReset")?.addEventListener("click", () => {
      loadProfile().catch((e) => setMsg(e.message || String(e), true));
    });
    $("#btnSave")?.addEventListener("click", () => {
      saveProfile().catch((e) => setMsg(e.message || String(e), true));
    });
    $("#btnChangePassword")?.addEventListener("click", () => {
      changePassword().catch((e) => setMsg(e.message || String(e), true));
    });
    $("#btnWithdraw")?.addEventListener("click", () => {
      withdrawAccount().catch((e) => setMsg(e.message || String(e), true));
    });
    $("#btnLogout")?.addEventListener("click", () => {
      KAAuth.logout("/pwa/profile.html").catch(() => {});
    });
  }

  async function init() {
    if (window.KAModuleBase && typeof window.KAModuleBase.bootstrap === "function") {
      moduleCtx = await window.KAModuleBase.bootstrap("main", {
        defaultLimit: 100,
        maxLimit: 500,
      });
      me = moduleCtx.user || null;
    } else {
      me = await KAAuth.requireAuth();
    }
    const back = $("#btnBack");
    if (back) {
      const fallbackPath = String((me && me.default_landing_path) || "/pwa/").trim() || "/pwa/";
      const target = (moduleCtx && typeof moduleCtx.withSite === "function" && (fallbackPath === "/pwa/" || fallbackPath === "/pwa"))
        ? moduleCtx.withSite("/pwa/")
        : fallbackPath;
      back.setAttribute("href", target);
    }
    await loadProfile();
    setMsg("준비 완료");
  }

  wire();
  init().catch((err) => {
    const msg = err && err.message ? err.message : String(err);
    if (msg.includes("로그인이 필요")) return;
    setMsg(msg, true);
  });
})();
