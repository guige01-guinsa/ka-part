(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  let me = null;
  let options = null;
  let pollTimer = null;

  function isAdmin(user) {
    return !!(user && user.is_admin);
  }

  function canManageBackup(user) {
    return !!(user && (user.is_admin || user.is_site_admin));
  }

  function setMsg(msg, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = msg || "";
    el.classList.toggle("err", !!isErr);
  }

  function escapeHtml(v) {
    return String(v || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function prettyBytes(bytes) {
    const n = Number(bytes || 0);
    if (!Number.isFinite(n) || n <= 0) return "-";
    const units = ["B", "KB", "MB", "GB"];
    let val = n;
    let idx = 0;
    while (val >= 1024 && idx < units.length - 1) {
      val /= 1024;
      idx += 1;
    }
    return `${val.toFixed(idx === 0 ? 0 : 1)} ${units[idx]}`;
  }

  function parseFilename(contentDisposition, fallbackName) {
    const value = String(contentDisposition || "");
    const utf8 = /filename\*=UTF-8''([^;]+)/i.exec(value);
    if (utf8 && utf8[1]) {
      try {
        return decodeURIComponent(utf8[1]);
      } catch (_e) {}
    }
    const plain = /filename=\"?([^\";]+)\"?/i.exec(value);
    if (plain && plain[1]) return plain[1];
    return fallbackName;
  }

  function setMetaLine() {
    const el = $("#metaLine");
    if (!el || !me) return;
    const level = isAdmin(me) ? "관리자" : "단지관리자";
    const code = (me.site_code || "").trim().toUpperCase();
    const name = (me.site_name || "").trim();
    const siteText = code ? `${code}${name ? ` / ${name}` : ""}` : (name || "-");
    el.textContent = `${me.name || me.login_id} (${level}) · 소속: ${siteText}`;
  }

  function setMaintenanceStatus(payload) {
    const line = $("#maintenanceLine");
    if (!line) return;
    const maintenance = payload && payload.maintenance ? payload.maintenance : payload;
    const active = !!(maintenance && maintenance.active);
    if (!active) {
      line.classList.remove("show");
      line.textContent = "";
      return;
    }
    line.textContent = String(maintenance.message || "서버 점검 중입니다.");
    line.classList.add("show");
  }

  function getScope() {
    return ($("#scopeSelect").value || "site").trim().toLowerCase();
  }

  function selectedTargetKeys() {
    return Array.from(document.querySelectorAll("input[name='backupTarget']:checked"))
      .map((el) => String(el.value || "").trim().toLowerCase())
      .filter(Boolean);
  }

  function renderTargets(targets) {
    const wrap = $("#targetList");
    if (!wrap) return;
    if (!targets || !targets.length) {
      wrap.innerHTML = '<div class="target-desc">사용 가능한 백업 대상이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = targets
      .map((t) => {
        const key = escapeHtml(t.key);
        const label = escapeHtml(t.label || t.key);
        const size = prettyBytes(t.size_bytes);
        const exists = !!t.exists;
        return `
          <div class="target-item">
            <label>
              <input type="checkbox" name="backupTarget" value="${key}" ${exists ? "checked" : ""} ${exists ? "" : "disabled"} />
              <strong>${label}</strong>
            </label>
            <div class="target-desc">파일 상태: ${exists ? "사용 가능" : "미존재"} · 크기: ${size}</div>
          </div>
        `;
      })
      .join("");
  }

  function applyRolePolicy() {
    const admin = isAdmin(me);
    const scopeEl = $("#scopeSelect");
    const siteCodeEl = $("#siteCode");
    const siteNameEl = $("#siteName");
    if (!scopeEl || !siteCodeEl || !siteNameEl) return;

    if (!admin) {
      scopeEl.value = "site";
      scopeEl.disabled = true;
      siteCodeEl.value = String(me.site_code || "").trim().toUpperCase();
      siteNameEl.value = String(me.site_name || "").trim();
      siteCodeEl.readOnly = true;
      siteNameEl.readOnly = true;
    } else {
      scopeEl.disabled = false;
      const q = new URL(window.location.href);
      const qCode = (q.searchParams.get("site_code") || "").trim().toUpperCase();
      const qName = (q.searchParams.get("site_name") || "").trim();
      const storedCode = (localStorage.getItem(SITE_CODE_KEY) || "").trim().toUpperCase();
      const storedName = (localStorage.getItem(SITE_NAME_KEY) || "").trim();
      if (!siteCodeEl.value) siteCodeEl.value = qCode || storedCode || "";
      if (!siteNameEl.value) siteNameEl.value = qName || storedName || "";
      siteCodeEl.readOnly = false;
      siteNameEl.readOnly = false;
    }
  }

  function renderScheduleInfo(schedules) {
    const el = $("#scheduleInfo");
    if (!el) return;
    const rows = Array.isArray(schedules) ? schedules : [];
    if (!rows.length) {
      el.textContent = "";
      return;
    }
    el.innerHTML = rows
      .map((x) => `${escapeHtml(x.label || x.key || "")}: <strong>${escapeHtml(x.when || "-")}</strong>`)
      .join(" / ");
  }

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
  }

  async function refreshStatus() {
    const data = await jfetch("/api/backup/status");
    setMaintenanceStatus(data);
    renderScheduleInfo(data.schedules || []);
    return data;
  }

  async function loadOptions() {
    const data = await jfetch("/api/backup/options");
    options = data;
    renderTargets(Array.isArray(data.targets) ? data.targets : []);
    if (!isAdmin(me)) {
      $("#scopeSelect").value = "site";
    }
    applyRolePolicy();
  }

  function historyItemHtml(item) {
    const fileName = escapeHtml(item.file_name || "-");
    const scope = escapeHtml(item.scope_label || item.scope || "-");
    const trigger = escapeHtml(item.trigger_label || item.trigger || "-");
    const when = escapeHtml(item.created_at || "-");
    const targetLabels = Array.isArray(item.target_labels) ? item.target_labels.join(", ") : "-";
    const sitePart = item.site_code ? ` · 단지코드 ${escapeHtml(item.site_code)}` : "";
    const rel = escapeHtml(item.relative_path || "");
    const size = prettyBytes(item.file_size_bytes);
    return `
      <div class="history-item">
        <div class="line">
          <div class="file">${fileName}</div>
          <div>
            <span class="tag">${scope}</span>
            <button class="btn" type="button" data-download="${rel}">다운로드</button>
          </div>
        </div>
        <div class="sub">${when} · ${trigger}${sitePart}</div>
        <div class="sub">대상: ${escapeHtml(targetLabels)} · 크기: ${size}</div>
      </div>
    `;
  }

  async function loadHistory() {
    const data = await jfetch("/api/backup/history?limit=40");
    const items = Array.isArray(data.items) ? data.items : [];
    const wrap = $("#historyList");
    if (!wrap) return;
    if (!items.length) {
      wrap.innerHTML = '<div class="sub">백업 이력이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = items.map(historyItemHtml).join("");
  }

  async function runBackup() {
    const targetKeys = selectedTargetKeys();
    if (!targetKeys.length) {
      setMsg("백업 대상을 1개 이상 선택하세요.", true);
      return;
    }

    const scope = getScope();
    const payload = { target_keys: targetKeys, scope };
    if (scope === "site") {
      payload.site_code = ($("#siteCode").value || "").trim().toUpperCase();
      payload.site_name = ($("#siteName").value || "").trim();
    }
    if (scope === "full") {
      const ok = confirm("전체 시스템 백업을 실행할까요?\n실행 중에는 임시 점검모드가 활성화될 수 있습니다.");
      if (!ok) return;
    }
    if (scope === "site" && !payload.site_code) {
      setMsg("단지코드를 입력하세요.", true);
      return;
    }

    const runBtn = $("#btnRunBackup");
    if (runBtn) runBtn.disabled = true;
    setMsg("백업 실행 중입니다...");
    try {
      const data = await jfetch("/api/backup/run", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      const result = data && data.result ? data.result : {};
      setMsg(`백업 완료: ${result.file_name || "-"}`);
      await Promise.all([loadHistory(), refreshStatus()]);
    } catch (e) {
      setMsg(e.message || String(e), true);
    } finally {
      if (runBtn) runBtn.disabled = false;
    }
  }

  async function downloadBackup(path) {
    const token = KAAuth.getToken();
    if (!token) {
      KAAuth.redirectLogin("/pwa/backup.html");
      return;
    }
    const url = `/api/backup/download?path=${encodeURIComponent(path)}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (res.status === 401) {
      KAAuth.clearSession();
      KAAuth.redirectLogin("/pwa/backup.html");
      return;
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error((txt || `${res.status}`).trim());
    }
    const blob = await res.blob();
    const name = parseFilename(res.headers.get("content-disposition"), "backup.zip");
    const href = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = href;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(href);
  }

  function wire() {
    $("#btnReload")?.addEventListener("click", () => {
      Promise.all([loadOptions(), loadHistory(), refreshStatus()]).catch((e) => setMsg(e.message, true));
    });
    $("#btnRunBackup")?.addEventListener("click", () => {
      runBackup().catch((e) => setMsg(e.message, true));
    });
    $("#scopeSelect")?.addEventListener("change", () => {
      const scope = getScope();
      if (!isAdmin(me) && scope !== "site") $("#scopeSelect").value = "site";
      const siteCodeEl = $("#siteCode");
      const siteNameEl = $("#siteName");
      if (!siteCodeEl || !siteNameEl) return;
      if (scope === "full") {
        siteCodeEl.readOnly = true;
        siteNameEl.readOnly = true;
      } else if (isAdmin(me)) {
        siteCodeEl.readOnly = false;
        siteNameEl.readOnly = false;
      }
    });
    $("#btnLogout")?.addEventListener("click", () => {
      const run = async () => {
        try {
          await jfetch("/api/auth/logout", { method: "POST" });
        } catch (_e) {}
        KAAuth.clearSession();
        KAAuth.redirectLogin("/pwa/backup.html");
      };
      run().catch(() => {});
    });
    $("#historyList")?.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-download]");
      if (!btn) return;
      const path = String(btn.dataset.download || "").trim();
      if (!path) return;
      downloadBackup(path).catch((err) => setMsg(err.message || String(err), true));
    });
  }

  async function init() {
    me = await KAAuth.requireAuth();
    if (!canManageBackup(me)) {
      alert("관리자/단지관리자만 접근할 수 있습니다.");
      window.location.href = "/pwa/";
      return;
    }
    setMetaLine();
    await Promise.all([loadOptions(), loadHistory(), refreshStatus()]);
    wire();
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(() => {
      refreshStatus().catch(() => {});
    }, 20000);
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();
