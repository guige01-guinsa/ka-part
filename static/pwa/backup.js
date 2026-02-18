(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  let me = null;
  let options = null;
  let pollTimer = null;
  let backupTimezone = "";
  let clientBackupDirHandle = null;
  const CLIENT_BACKUP_DIR_NAME = "backup_APT";

  function isAdmin(user) {
    return !!(user && user.is_admin);
  }

  function isSuperAdmin(user) {
    if (!user || !user.is_admin) return false;
    return String(user.admin_scope || "").trim().toLowerCase() === "super_admin";
  }

  function canViewSiteIdentity(user) {
    return isSuperAdmin(user);
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

  function formatDateTime(value) {
    const raw = String(value || "").trim();
    if (!raw) return "-";
    const normalized = raw.replace(" ", "T");
    const dt = new Date(normalized);
    if (Number.isNaN(dt.getTime())) return raw;
    return dt.toLocaleString("ko-KR", { hour12: false });
  }

  function setMetaLine() {
    const el = $("#metaLine");
    if (!el || !me) return;
    const level = isAdmin(me) ? "최고/운영관리자" : "단지관리자";
    const showSite = canViewSiteIdentity(me);
    const code = (me.site_code || "").trim().toUpperCase();
    const name = (me.site_name || "").trim();
    const siteText = showSite ? (code ? `${code}${name ? ` / ${name}` : ""}` : (name || "-")) : "(숨김)";
    el.textContent = `${me.name || me.login_id} (${level}) · 소속: ${siteText}`;
  }

  function applySiteIdentityVisibility() {
    const show = canViewSiteIdentity(me);
    const codeWrap = $("#siteCode")?.closest(".field");
    const nameWrap = $("#siteName")?.closest(".field");
    if (codeWrap) codeWrap.classList.toggle("hidden", !show);
    if (nameWrap) nameWrap.classList.toggle("hidden", !show);
  }

  function stripSiteIdentityFromUrl() {
    try {
      const u = new URL(window.location.href);
      let changed = false;
      if (u.searchParams.has("site_name")) {
        u.searchParams.delete("site_name");
        changed = true;
      }
      if (u.searchParams.has("site_code")) {
        u.searchParams.delete("site_code");
        changed = true;
      }
      if (!changed) return;
      const next = `${u.pathname}${u.searchParams.toString() ? `?${u.searchParams.toString()}` : ""}`;
      window.history.replaceState({}, "", next);
    } catch (_e) {}
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
    const superAdmin = isSuperAdmin(me);
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
    } else if (!superAdmin) {
      scopeEl.disabled = false;
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

    applySiteIdentityVisibility();
    if (!canViewSiteIdentity(me)) stripSiteIdentityFromUrl();
  }

  function renderScheduleInfo(schedules) {
    const el = $("#scheduleInfo");
    if (!el) return;
    const rows = Array.isArray(schedules) ? schedules : [];
    const tz = backupTimezone ? ` · 기준시간대: ${escapeHtml(backupTimezone)}` : "";
    if (!rows.length) {
      el.innerHTML = tz ? tz.slice(3) : "";
      return;
    }
    const body = rows
      .map((x) => `${escapeHtml(x.label || x.key || "")}: <strong>${escapeHtml(x.when || "-")}</strong>`)
      .join(" / ");
    el.innerHTML = `${body}${tz}`;
  }

  async function jfetch(url, opts = {}) {
    return KAAuth.requestJson(url, opts);
  }

  async function fetchBackupBlob(path) {
    const token = KAAuth.getToken();
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const url = `/api/backup/download?path=${encodeURIComponent(path)}`;
    const res = await fetch(url, { headers });
    if (res.status === 401) {
      KAAuth.clearSession();
      KAAuth.redirectLogin("/pwa/backup.html");
      return null;
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error((txt || `${res.status}`).trim());
    }
    const blob = await res.blob();
    const name = parseFilename(res.headers.get("content-disposition"), "backup.zip");
    return { blob, name };
  }

  async function ensureClientBackupFolder() {
    if (!$("#saveLocalCopy")?.checked) return { ok: false, skipped: true };
    if (typeof window.showDirectoryPicker !== "function") {
      return { ok: false, unsupported: true };
    }

    try {
      if (clientBackupDirHandle) {
        const q = await clientBackupDirHandle.queryPermission({ mode: "readwrite" });
        if (q === "granted") return { ok: true, created: false };
        const req = await clientBackupDirHandle.requestPermission({ mode: "readwrite" });
        if (req === "granted") return { ok: true, created: false };
      }
    } catch (_e) {}

    try {
      const rootHandle = await window.showDirectoryPicker({ mode: "readwrite" });
      let backupDir = null;
      let created = false;
      try {
        backupDir = await rootHandle.getDirectoryHandle(CLIENT_BACKUP_DIR_NAME, { create: false });
      } catch (e) {
        if (e && e.name === "NotFoundError") {
          alert(`선택한 위치에 '${CLIENT_BACKUP_DIR_NAME}' 폴더가 없어 자동 생성합니다.`);
          backupDir = await rootHandle.getDirectoryHandle(CLIENT_BACKUP_DIR_NAME, { create: true });
          created = true;
        } else {
          throw e;
        }
      }
      clientBackupDirHandle = backupDir;
      return { ok: true, created };
    } catch (e) {
      if (e && e.name === "AbortError") return { ok: false, cancelled: true };
      return { ok: false, error: e };
    }
  }

  async function saveBackupToClientFolder(path, fallbackName = "backup.zip") {
    if (!clientBackupDirHandle) return { ok: false, reason: "not-prepared" };
    const payload = await fetchBackupBlob(path);
    if (!payload) return { ok: false, reason: "no-payload" };
    const fileName = String(payload.name || fallbackName || "backup.zip").trim() || "backup.zip";
    const fileHandle = await clientBackupDirHandle.getFileHandle(fileName, { create: true });
    const writable = await fileHandle.createWritable();
    await writable.write(payload.blob);
    await writable.close();
    return { ok: true, fileName };
  }

  async function refreshStatus() {
    const data = await jfetch("/api/backup/status");
    backupTimezone = String(data.timezone || "").trim();
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
    const when = escapeHtml(formatDateTime(item.created_at || ""));
    const targetLabels = Array.isArray(item.target_labels) ? item.target_labels.join(", ") : "-";
    const sitePart = canViewSiteIdentity(me) && item.site_code ? ` · 단지코드 ${escapeHtml(item.site_code)}` : "";
    const rel = escapeHtml(item.relative_path || "");
    const size = prettyBytes(item.file_size_bytes);
    const rawScope = String(item.scope || "").trim().toLowerCase();
    const restoreBtn = isAdmin(me) && rawScope === "full"
      ? `<button class="btn danger" type="button" data-restore="${rel}">복구</button>`
      : "";
    return `
      <div class="history-item">
        <div class="line">
          <div class="file">${fileName}</div>
          <div>
            <span class="tag">${scope}</span>
            <button class="btn" type="button" data-download="${rel}">다운로드</button>
            ${restoreBtn}
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

    const wantsLocalCopy = !!$("#saveLocalCopy")?.checked;
    let localReady = { ok: false, skipped: true };
    if (wantsLocalCopy) {
      localReady = await ensureClientBackupFolder();
      if (localReady.cancelled) {
        setMsg("단말기 backup_APT 저장이 취소되어 서버 백업만 진행합니다.");
      } else if (localReady.unsupported) {
        setMsg("브라우저가 단말기 폴더 저장을 지원하지 않아 서버 백업만 진행합니다.");
      } else if (localReady.error) {
        setMsg(`단말기 backup_APT 준비 실패: ${localReady.error?.message || localReady.error}`, true);
      }
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
      let doneMsg = `백업 완료: ${result.file_name || "-"}`;
      if (result.server_backup_saved && result.server_backup_relative_path) {
        doneMsg += ` / 서버 backup_APT 저장: ${result.server_backup_relative_path}`;
      }
      if (wantsLocalCopy && localReady.ok && result.relative_path) {
        try {
          const local = await saveBackupToClientFolder(result.relative_path, result.file_name || "backup.zip");
          if (local.ok) {
            doneMsg += ` / 단말기 backup_APT 저장: ${local.fileName}`;
          }
        } catch (e) {
          doneMsg += " / 단말기 backup_APT 저장 실패(다운로드 버튼으로 수동 저장)";
        }
      }
      setMsg(doneMsg);
      await Promise.all([loadHistory(), refreshStatus()]);
    } catch (e) {
      setMsg(e.message || String(e), true);
    } finally {
      if (runBtn) runBtn.disabled = false;
    }
  }

  async function restoreBackup(path) {
    const safePath = String(path || "").trim();
    if (!safePath) {
      setMsg("복구할 백업 파일 경로가 없습니다.", true);
      return;
    }
    const ok = confirm(
      "선택한 백업으로 DB를 복구할까요?\n" +
      "- 전체 시스템 점검모드가 잠시 활성화됩니다.\n" +
      "- 복구 전 현재 DB 스냅샷(pre_restore)이 자동 생성됩니다."
    );
    if (!ok) return;

    setMsg("DB 복구 실행 중입니다...");
    const data = await jfetch("/api/backup/restore", {
      method: "POST",
      body: JSON.stringify({
        path: safePath,
        with_maintenance: true,
      }),
    });
    const result = data && data.result ? data.result : {};
    const targets = Array.isArray(result.target_labels) ? result.target_labels.join(", ") : "-";
    const rollback = String(result.rollback_relative_path || "").trim();
    const rollbackInfo = rollback ? `\n복구 전 스냅샷: ${rollback}` : "";
    setMsg(`DB 복구 완료 (대상: ${targets})${rollbackInfo}`);
    await Promise.all([loadHistory(), refreshStatus()]);
  }

  async function downloadBackup(path) {
    const payload = await fetchBackupBlob(path);
    if (!payload) return;
    const blob = payload.blob;
    const name = payload.name;
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
        await KAAuth.logout("/pwa/backup.html");
      };
      run().catch(() => {});
    });
    $("#historyList")?.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-download]");
      if (btn) {
        const path = String(btn.dataset.download || "").trim();
        if (!path) return;
        downloadBackup(path).catch((err) => setMsg(err.message || String(err), true));
        return;
      }
      const restoreBtn = e.target.closest("button[data-restore]");
      if (!restoreBtn) return;
      const path = String(restoreBtn.dataset.restore || "").trim();
      if (!path) return;
      restoreBackup(path).catch((err) => setMsg(err.message || String(err), true));
    });
  }

  async function init() {
    me = await KAAuth.requireAuth();
    if (!canManageBackup(me)) {
      alert("최고/운영관리자 또는 단지관리자만 접근할 수 있습니다.");
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
