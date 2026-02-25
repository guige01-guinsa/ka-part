(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const SITE_NAME_KEY = "ka_current_site_name_v1";
  const SITE_CODE_KEY = "ka_current_site_code_v1";
  const KAUtil = window.KAUtil;
  let me = null;
  let moduleCtx = null;
  let options = null;
  let pollTimer = null;
  let backupTimezone = "";
  let clientBackupDirHandle = null;
  let restoreRequests = [];
  const CLIENT_BACKUP_DIR_NAME = "backup_APT";

  function isAdmin(user) {
    return !!(user && user.is_admin);
  }

  function isSiteAdmin(user) {
    return !!(user && user.is_site_admin && !user.is_admin);
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

  function setRestoreReqMsg(msg, isErr = false) {
    const el = $("#restoreReqMsg");
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
    const showSite = KAUtil.canViewSiteIdentity(me);
    const code = (me.site_code || "").trim().toUpperCase();
    const name = (me.site_name || "").trim();
    const siteText = showSite ? (code ? `${code}${name ? ` / ${name}` : ""}` : (name || "-")) : "(숨김)";
    el.textContent = `${me.name || me.login_id} (${level}) · 소속: ${siteText}`;
  }

  function applySiteIdentityVisibility() {
    const show = KAUtil.canViewSiteIdentity(me);
    const codeWrap = $("#siteCode")?.closest(".field");
    const nameWrap = $("#siteName")?.closest(".field");
    if (codeWrap) codeWrap.classList.toggle("hidden", !show);
    if (nameWrap) nameWrap.classList.toggle("hidden", !show);
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

  function includeUserTablesForBackup() {
    if (!isAdmin(me)) return false;
    if (getScope() !== "site") return true;
    return !!$("#includeUserTablesBackup")?.checked;
  }

  function includeUserTablesForRestore(scope = "") {
    if (!isAdmin(me)) return false;
    const cleanScope = String(scope || "").trim().toLowerCase();
    if (cleanScope && cleanScope !== "site") return true;
    return !!$("#includeUserTablesRestore")?.checked;
  }

  function syncUserTableOptionState() {
    const admin = isAdmin(me);
    const backupWrap = $("#backupUserDataWrap");
    const restoreWrap = $("#restoreUserDataWrap");
    const backupBox = $("#includeUserTablesBackup");
    const restoreBox = $("#includeUserTablesRestore");
    if (backupWrap) backupWrap.classList.toggle("hidden", !admin);
    if (restoreWrap) restoreWrap.classList.toggle("hidden", !admin);
    if (!admin) return;

    const siteScope = getScope() === "site";
    if (backupBox) {
      if (!siteScope) {
        backupBox.checked = true;
        backupBox.disabled = true;
      } else {
        backupBox.disabled = false;
      }
    }
    if (restoreBox) restoreBox.disabled = false;
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
    const superAdmin = KAUtil.isSuperAdmin(me);
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
      const ctxCode = String(moduleCtx && moduleCtx.siteCode ? moduleCtx.siteCode : "").trim().toUpperCase();
      const ctxName = String(moduleCtx && moduleCtx.siteName ? moduleCtx.siteName : "").trim();
      const storedCode = (localStorage.getItem(SITE_CODE_KEY) || "").trim().toUpperCase();
      const storedName = (localStorage.getItem(SITE_NAME_KEY) || "").trim();
      if (!siteCodeEl.value) siteCodeEl.value = qCode || ctxCode || storedCode || "";
      if (!siteNameEl.value) siteNameEl.value = qName || ctxName || storedName || "";
      siteCodeEl.readOnly = false;
      siteNameEl.readOnly = false;
    }

    applySiteIdentityVisibility();
    if (!KAUtil.canViewSiteIdentity(me)) KAUtil.stripSiteIdentityFromUrl();
    $("#restoreUploadWrap")?.classList.toggle("hidden", !isAdmin(me));
    const reqCard = $("#restoreRequestCard");
    if (reqCard) reqCard.hidden = !canManageBackup(me);
    const reqHint = $("#restoreReqHint");
    if (reqHint) {
      if (isAdmin(me)) {
        reqHint.textContent = "최고/운영관리자는 요청을 승인(대표자 실행 권한 부여)하거나 즉시 복구를 실행할 수 있습니다.";
      } else {
        reqHint.textContent = "단지대표자는 본인 요청 상태를 확인하고, 승인된 요청에 한해 직접 복구를 실행할 수 있습니다.";
      }
    }
    syncUserTableOptionState();
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

  async function issueDownloadLink(path) {
    const safePath = String(path || "").trim();
    if (!safePath) throw new Error("다운로드 경로가 없습니다.");
    const data = await KAUtil.authJson("/api/backup/download/request", {
      method: "POST",
      body: JSON.stringify({ path: safePath }),
    });
    const download = data && data.download ? data.download : {};
    const url = String(download.url || "").trim();
    if (!url) throw new Error("다운로드 링크 발급에 실패했습니다.");
    return { ...download, url };
  }

  async function fetchBackupBlob(path) {
    const issued = await issueDownloadLink(path);
    const token = KAAuth.getToken();
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const res = await fetch(String(issued.url || ""), { headers });
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
    const name = KAUtil.parseDownloadFilename(res.headers.get("content-disposition"), "backup.zip");
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
    const data = await KAUtil.authJson("/api/backup/status");
    backupTimezone = String(data.timezone || "").trim();
    setMaintenanceStatus(data);
    renderScheduleInfo(data.schedules || []);
    return data;
  }

  async function loadOptions() {
    const data = await KAUtil.authJson("/api/backup/options");
    options = data;
    renderTargets(Array.isArray(data.targets) ? data.targets : []);
    const defaultIncludeUsers = data && data.include_user_tables_default !== false;
    const includeBackupEl = $("#includeUserTablesBackup");
    const includeRestoreEl = $("#includeUserTablesRestore");
    if (includeBackupEl) includeBackupEl.checked = !!defaultIncludeUsers;
    if (includeRestoreEl) includeRestoreEl.checked = !!defaultIncludeUsers;
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
    const sitePart = KAUtil.canViewSiteIdentity(me) && item.site_code ? ` · 단지코드 ${escapeHtml(item.site_code)}` : "";
    const rel = escapeHtml(item.relative_path || "");
    const size = prettyBytes(item.file_size_bytes);
    const rawScope = String(item.scope || "").trim().toLowerCase() || "full";
    const hasUserData = !!item.contains_user_data;
    const mySiteCode = String(me?.site_code || "").trim().toUpperCase();
    const itemSiteCode = String(item.site_code || "").trim().toUpperCase();
    const canDownloadItem = isAdmin(me) || !hasUserData;
    const canRestoreItem = isAdmin(me) && (rawScope === "full" || rawScope === "site");
    const canRestoreRequestItem = !isAdmin(me) && !hasUserData && rawScope === "site" && mySiteCode && itemSiteCode === mySiteCode;
    const downloadBtn = canDownloadItem
      ? `<button class="btn" type="button" data-download="${rel}">다운로드</button>`
      : "";
    const restoreBtn = canRestoreItem
      ? `<button class="btn danger" type="button" data-restore="${rel}" data-restore-scope="${escapeHtml(rawScope)}">복구</button>`
      : "";
    const restoreRequestBtn = canRestoreRequestItem
      ? `<button class="btn" type="button" data-restore-request="${rel}">복구요청</button>`
      : "";
    const userDataTag = hasUserData ? `<span class="tag">사용자정보 포함(관리자 전용)</span>` : "";
    return `
      <div class="history-item">
        <div class="line">
          <div class="file">${fileName}</div>
          <div>
            <span class="tag">${scope}</span>
            ${userDataTag}
            ${downloadBtn}
            ${restoreRequestBtn}
            ${restoreBtn}
          </div>
        </div>
        <div class="sub">${when} · ${trigger}${sitePart}</div>
        <div class="sub">대상: ${escapeHtml(targetLabels)} · 크기: ${size}</div>
      </div>
    `;
  }

  async function loadHistory() {
    const data = await KAUtil.authJson("/api/backup/history?limit=40");
    const items = Array.isArray(data.items) ? data.items : [];
    const wrap = $("#historyList");
    if (!wrap) return;
    if (!items.length) {
      wrap.innerHTML = '<div class="sub">백업 이력이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = items.map(historyItemHtml).join("");
  }

  function restoreRequestStatusInfo(status) {
    const raw = String(status || "").trim().toLowerCase();
    if (raw === "pending") return { cls: "pending", label: "대기" };
    if (raw === "approved") return { cls: "approved", label: "승인" };
    if (raw === "executed") return { cls: "executed", label: "실행완료" };
    return { cls: "", label: raw || "-" };
  }

  function isOwnRestoreRequest(item) {
    const myId = Number(me?.id || 0);
    const reqUserId = Number(item?.requested_by_user_id || 0);
    if (myId > 0 && reqUserId > 0 && myId === reqUserId) return true;
    const myLogin = String(me?.login_id || "").trim().toLowerCase();
    const reqLogin = String(item?.requested_by_login || "").trim().toLowerCase();
    return !!(myLogin && reqLogin && myLogin === reqLogin);
  }

  function restoreRequestItemHtml(item) {
    const reqId = Number(item?.id || 0);
    const statusRaw = String(item?.status || "").trim().toLowerCase();
    const status = restoreRequestStatusInfo(statusRaw);
    const payload = item && typeof item.payload === "object" ? item.payload : {};
    const path = String(payload.path || "").trim();
    const siteCode = String(item?.target_site_code || payload.site_code || "").trim().toUpperCase();
    const siteName = String(item?.target_site_name || payload.site_name || "").trim();
    const reason = String(item?.reason || payload.reason || "").trim();
    const requester = String(item?.requested_by_login || payload.requested_by || "-").trim();
    const createdAt = formatDateTime(item?.created_at || "");
    const approvedAt = formatDateTime(item?.approved_at || "");
    const executedAt = formatDateTime(item?.executed_at || "");
    const canApprove = isAdmin(me) && statusRaw === "pending";
    const canExecAdmin = isAdmin(me) && (statusRaw === "pending" || statusRaw === "approved");
    const canExecSelf = isSiteAdmin(me) && statusRaw === "approved" && isOwnRestoreRequest(item);
    const actionButtons = [
      canApprove ? `<button class="btn" type="button" data-req-act="approve" data-req-id="${reqId}">승인(대표자 실행)</button>` : "",
      canExecAdmin ? `<button class="btn primary" type="button" data-req-act="execute-admin" data-req-id="${reqId}">관리자 즉시 실행</button>` : "",
      canExecSelf ? `<button class="btn primary" type="button" data-req-act="execute-self" data-req-id="${reqId}">승인건 직접 복구</button>` : "",
    ]
      .filter(Boolean)
      .join("");
    return `
      <div class="history-item" data-req-id="${reqId}">
        <div class="line">
          <div class="file">요청 #${reqId} · ${escapeHtml(siteName || "-")} ${siteCode ? `[${escapeHtml(siteCode)}]` : ""}</div>
          <div class="status-pill ${status.cls}">${status.label}</div>
        </div>
        <div class="sub">요청자: ${escapeHtml(requester)} · 요청: ${escapeHtml(createdAt)}</div>
        <div class="sub">승인: ${escapeHtml(approvedAt)} · 실행: ${escapeHtml(executedAt)}</div>
        <div class="sub">대상: ${escapeHtml(path || "-")}</div>
        <div class="sub">사유: ${escapeHtml(reason || "-")}</div>
        ${actionButtons ? `<div class="restore-req-actions">${actionButtons}</div>` : ""}
      </div>
    `;
  }

  function renderRestoreRequestList() {
    const wrap = $("#restoreReqList");
    if (!wrap) return;
    if (!Array.isArray(restoreRequests) || !restoreRequests.length) {
      wrap.innerHTML = '<div class="sub">복구 요청이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = restoreRequests.map((item) => restoreRequestItemHtml(item)).join("");
  }

  async function loadRestoreRequests() {
    if (!canManageBackup(me)) return;
    const status = String($("#restoreReqStatus")?.value || "pending").trim().toLowerCase();
    const qs = new URLSearchParams();
    qs.set("limit", "120");
    qs.set("status", status || "pending");
    const data = await KAUtil.authJson(`/api/backup/restore/requests?${qs.toString()}`);
    restoreRequests = Array.isArray(data.items) ? data.items : [];
    renderRestoreRequestList();
  }

  async function approveRestoreRequest(requestId) {
    const rid = Number(requestId || 0);
    if (rid <= 0) throw new Error("request_id가 올바르지 않습니다.");
    const ok = confirm(`요청 #${rid}를 승인하여 단지대표자에게 복구 실행 권한을 부여할까요?`);
    if (!ok) return;
    const data = await KAUtil.authJson("/api/backup/restore/request/approve", {
      method: "POST",
      body: JSON.stringify({ request_id: rid }),
    });
    const req = data && data.request ? data.request : {};
    setRestoreReqMsg(`요청 #${rid} 승인 완료 (상태: ${String(req.status || "approved")})`);
    await Promise.all([loadRestoreRequests(), loadHistory(), refreshStatus()]);
  }

  async function executeRestoreRequestAsAdmin(requestId) {
    const rid = Number(requestId || 0);
    if (rid <= 0) throw new Error("request_id가 올바르지 않습니다.");
    const ok = confirm(`요청 #${rid}를 관리자 권한으로 즉시 복구 실행할까요?`);
    if (!ok) return;
    const data = await KAUtil.authJson("/api/backup/restore/request/execute", {
      method: "POST",
      body: JSON.stringify({
        request_id: rid,
        with_maintenance: false,
      }),
    });
    const rollback = String(data?.result?.rollback_relative_path || "").trim();
    setRestoreReqMsg(`요청 #${rid} 복구 실행 완료${rollback ? ` / 복구 전 스냅샷: ${rollback}` : ""}`);
    await Promise.all([loadRestoreRequests(), loadHistory(), refreshStatus()]);
  }

  async function executeRestoreRequestAsRequester(requestId) {
    const rid = Number(requestId || 0);
    if (rid <= 0) throw new Error("request_id가 올바르지 않습니다.");
    const ok = confirm(`승인된 요청 #${rid}를 지금 직접 복구 실행할까요?`);
    if (!ok) return;
    const data = await KAUtil.authJson("/api/backup/restore/request/execute_self", {
      method: "POST",
      body: JSON.stringify({ request_id: rid }),
    });
    const rollback = String(data?.result?.rollback_relative_path || "").trim();
    setRestoreReqMsg(`요청 #${rid} 직접 복구 완료${rollback ? ` / 복구 전 스냅샷: ${rollback}` : ""}`);
    await Promise.all([loadRestoreRequests(), loadHistory(), refreshStatus()]);
  }

  async function runBackup() {
    const targetKeys = selectedTargetKeys();
    if (!targetKeys.length) {
      setMsg("백업 대상을 1개 이상 선택하세요.", true);
      return;
    }

    const scope = getScope();
    const includeUserTables = includeUserTablesForBackup();
    const payload = { target_keys: targetKeys, scope, include_user_tables: includeUserTables };
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
      const data = await KAUtil.authJson("/api/backup/run", {
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

  async function restoreBackup(path, scope = "") {
    const safePath = String(path || "").trim();
    if (!safePath) {
      setMsg("복구할 백업 파일 경로가 없습니다.", true);
      return;
    }
    const cleanScope = String(scope || "").trim().toLowerCase();
    const isSiteRestore = cleanScope === "site";
    const includeUserTables = includeUserTablesForRestore(cleanScope);
    const ok = confirm(
      `선택한 백업(${isSiteRestore ? "단지코드 범위" : "전체 시스템"})으로 DB를 복구할까요?\n` +
      `- ${isSiteRestore ? "단지코드 데이터가 복구됩니다." : "전체 시스템 점검모드가 잠시 활성화됩니다."}\n` +
      `${isSiteRestore ? `- 사용자관리 레코드: ${includeUserTables ? "포함" : "제외"}\n` : ""}` +
      "- 복구 전 현재 DB 스냅샷(pre_restore)이 자동 생성됩니다."
    );
    if (!ok) return;

    setMsg("DB 복구 실행 중입니다...");
    const data = await KAUtil.authJson("/api/backup/restore", {
      method: "POST",
      body: JSON.stringify({
        path: safePath,
        with_maintenance: !isSiteRestore,
        include_user_tables: includeUserTables,
      }),
    });
    const result = data && data.result ? data.result : {};
    const targets = Array.isArray(result.target_labels) ? result.target_labels.join(", ") : "-";
    const rollback = String(result.rollback_relative_path || "").trim();
    const rollbackInfo = rollback ? `\n복구 전 스냅샷: ${rollback}` : "";
    setMsg(`DB 복구 완료 (대상: ${targets})${rollbackInfo}`);
    await Promise.all([loadHistory(), refreshStatus()]);
  }

  async function restoreBackupFromFile() {
    if (!canManageBackup(me)) {
      setMsg("백업파일 복구 권한이 없습니다.", true);
      return;
    }
    if (!isAdmin(me)) {
      setMsg("단지대표자는 직접 복구할 수 없습니다. 복구요청을 이용하세요.", true);
      return;
    }
    const fileEl = $("#restoreFile");
    const file = fileEl?.files?.[0];
    if (!file) {
      setMsg("복구할 ZIP 파일을 선택하세요.", true);
      return;
    }
    const name = String(file.name || "mobile-backup.zip").trim() || "mobile-backup.zip";
    const includeUserTables = includeUserTablesForRestore();

    const ok = confirm(
      `선택한 파일(${name})로 DB를 복구할까요?\n` +
      "- 파일 범위(full/site)는 서버에서 자동 판별합니다.\n" +
      `- 단지코드(site) 복구 시 사용자관리 레코드: ${includeUserTables ? "포함" : "제외"}\n` +
      "- 복구 전 현재 DB 스냅샷(pre_restore)이 자동 생성됩니다."
    );
    if (!ok) return;

    const token = KAAuth.getToken();
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const fd = new FormData();
    fd.append("backup_file", file, name);
    fd.append("with_maintenance", isAdmin(me) ? "true" : "false");
    fd.append("include_user_tables", includeUserTables ? "true" : "false");

    setMsg("업로드 복구 실행 중입니다...");
    const res = await fetch("/api/backup/restore/upload", {
      method: "POST",
      headers,
      body: fd,
    });
    if (res.status === 401) {
      KAAuth.clearSession();
      KAAuth.redirectLogin("/pwa/backup.html");
      return;
    }
    const raw = await res.text().catch(() => "");
    let body = null;
    try { body = raw ? JSON.parse(raw) : {}; } catch (_e) {}
    if (!res.ok) {
      throw new Error((body && body.detail) || raw || `${res.status}`);
    }
    const result = body && body.result ? body.result : {};
    const targets = Array.isArray(result.target_labels) ? result.target_labels.join(", ") : "-";
    const rollback = String(result.rollback_relative_path || "").trim();
    const uploadedRel = String(body?.uploaded?.relative_path || "").trim();
    const parts = [`업로드 복구 완료 (대상: ${targets})`];
    if (uploadedRel) parts.push(`업로드 파일: ${uploadedRel}`);
    if (rollback) parts.push(`복구 전 스냅샷: ${rollback}`);
    setMsg(parts.join(" / "));
    if (fileEl) fileEl.value = "";
    await Promise.all([loadHistory(), refreshStatus()]);
  }

  async function requestRestore(path) {
    const safePath = String(path || "").trim();
    if (!safePath) {
      setMsg("복구 요청 대상 경로가 없습니다.", true);
      return;
    }
    if (isAdmin(me)) {
      setMsg("관리자는 복구 요청 없이 직접 복구를 실행할 수 있습니다.");
      return;
    }
    const reasonRaw = window.prompt("복구 요청 사유를 입력하세요.", "단지 운영 데이터 복구 요청");
    if (reasonRaw === null) return;
    const reason = String(reasonRaw || "").trim();
    setMsg("복구 요청 등록 중입니다...");
    const data = await KAUtil.authJson("/api/backup/restore/request", {
      method: "POST",
      body: JSON.stringify({
        path: safePath,
        reason,
      }),
    });
    const req = data && data.request ? data.request : {};
    const reqId = Number(req.id || 0);
    const status = String(req.status || "pending");
    setMsg(`복구 요청이 등록되었습니다. 요청번호: ${reqId > 0 ? reqId : "-"} / 상태: ${status}`);
    await Promise.all([loadHistory(), loadRestoreRequests()]);
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
      Promise.all([loadOptions(), loadHistory(), loadRestoreRequests(), refreshStatus()]).catch((e) => setMsg(e.message, true));
    });
    $("#btnRunBackup")?.addEventListener("click", () => {
      runBackup().catch((e) => setMsg(e.message, true));
    });
    $("#btnRestoreFile")?.addEventListener("click", () => {
      restoreBackupFromFile().catch((e) => setMsg(e.message || String(e), true));
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
      syncUserTableOptionState();
    });
    $("#btnLogout")?.addEventListener("click", () => {
      const run = async () => {
        await KAAuth.logout("/pwa/backup.html");
      };
      run().catch(() => {});
    });
    $("#btnReloadRestoreReq")?.addEventListener("click", () => {
      loadRestoreRequests().catch((e) => setRestoreReqMsg(e.message || String(e), true));
    });
    $("#restoreReqStatus")?.addEventListener("change", () => {
      loadRestoreRequests().catch((e) => setRestoreReqMsg(e.message || String(e), true));
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
      if (restoreBtn) {
        const path = String(restoreBtn.dataset.restore || "").trim();
        const scope = String(restoreBtn.dataset.restoreScope || "").trim().toLowerCase();
        if (!path) return;
        restoreBackup(path, scope).catch((err) => setMsg(err.message || String(err), true));
        return;
      }
      const requestBtn = e.target.closest("button[data-restore-request]");
      if (!requestBtn) return;
      const path = String(requestBtn.dataset.restoreRequest || "").trim();
      if (!path) return;
      requestRestore(path).catch((err) => setMsg(err.message || String(err), true));
    });
    $("#restoreReqList")?.addEventListener("click", (e) => {
      const btn = e.target.closest("button[data-req-act][data-req-id]");
      if (!btn) return;
      const action = String(btn.dataset.reqAct || "").trim().toLowerCase();
      const reqId = Number(btn.dataset.reqId || 0);
      if (reqId <= 0) return;
      if (action === "approve") {
        approveRestoreRequest(reqId).catch((err) => setRestoreReqMsg(err.message || String(err), true));
        return;
      }
      if (action === "execute-admin") {
        executeRestoreRequestAsAdmin(reqId).catch((err) => setRestoreReqMsg(err.message || String(err), true));
        return;
      }
      if (action === "execute-self") {
        executeRestoreRequestAsRequester(reqId).catch((err) => setRestoreReqMsg(err.message || String(err), true));
      }
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
    if (!canManageBackup(me)) {
      alert("최고/운영관리자 또는 단지관리자만 접근할 수 있습니다.");
      window.location.href = "/pwa/";
      return;
    }
    setMetaLine();
    await Promise.all([loadOptions(), loadHistory(), loadRestoreRequests(), refreshStatus()]);
    wire();
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(() => {
      refreshStatus().catch(() => {});
    }, 20000);
  }

  init().catch((e) => setMsg(e.message || String(e), true));
})();

