(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const state = {
    user: null,
    siteCode: "",
    bootstrapUser: null,
    runs: [],
    selectedRunId: 0,
    selectedRun: null,
  };

  function todayYmd() {
    const d = new Date();
    const p = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
  }

  function msg(text, isErr = false) {
    const el = $("#msg");
    if (!el) return;
    el.textContent = String(text || "");
    el.style.color = isErr ? "#ff9db1" : "#cfe0ff";
  }

  function verifyMsg(text, isErr = false) {
    const el = $("#verifyMsg");
    if (!el) return;
    el.textContent = String(text || "");
    el.style.color = isErr ? "#ff9db1" : "#cfe0ff";
  }

  function getToken() {
    return window.KAAuth ? window.KAAuth.getToken() : "";
  }

  async function apiGet(path) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    return await window.KAAuth.requestJson(path);
  }

  async function apiPost(path, payload = {}) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    return await window.KAAuth.requestJson(path, { method: "POST", body: JSON.stringify(payload || {}) });
  }

  async function apiPatch(path, payload = {}) {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    return await window.KAAuth.requestJson(path, { method: "PATCH", body: JSON.stringify(payload || {}) });
  }

  async function uploadPhoto(runId, itemId, file) {
    const token = getToken();
    const fd = new FormData();
    fd.append("photo", file);
    const headers = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const res = await fetch(`/api/inspection/runs/${runId}/items/${itemId}/photo`, {
      method: "POST",
      headers,
      body: fd,
    });
    const text = await res.text();
    let body = null;
    try { body = JSON.parse(text); } catch (_e) {}
    if (!res.ok) throw new Error((body && body.detail) || text || `HTTP ${res.status}`);
    return body || {};
  }

  function escapeHtml(s) {
    return String(s ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function buildSiteQuery() {
    const qs = new URLSearchParams(window.location.search);
    const siteCode = (qs.get("site_code") || "").trim().toUpperCase();
    if (siteCode) return `site_code=${encodeURIComponent(siteCode)}`;
    return "";
  }

  function isManagerUser() {
    const u = state.bootstrapUser || {};
    const role = String(u.role || "").trim();
    if (role === "최고/운영관리자" || role === "최고관리자" || role === "운영관리자" || role === "단지대표자" || role === "단지관리자") {
      return true;
    }
    return !!u.is_admin || !!u.is_site_admin;
  }

  function canCreateRun() {
    const u = state.bootstrapUser || {};
    if (typeof u.can_create_run === "boolean") return !!u.can_create_run;
    const role = String(u.role || "").trim();
    if (role === "최고/운영관리자" || role === "최고관리자" || role === "운영관리자" || role === "단지대표자" || role === "단지관리자") {
      return true;
    }
    return !!u.is_admin || !!u.is_site_admin;
  }

  function defaultTemplateItems() {
    const base = [
      { item_key: "fire_extinguisher", item_text: "소화기 비치 및 압력상태 확인", category: "소방", severity: 2, requires_photo: false, requires_note: false },
      { item_key: "emergency_exit", item_text: "비상구 통로 적치물 여부 확인", category: "피난", severity: 2, requires_photo: true, requires_note: false },
      { item_key: "electrical_panel", item_text: "분전반 외관/표시/이상음 확인", category: "전기", severity: 2, requires_photo: false, requires_note: true },
      { item_key: "leak_check", item_text: "누수 및 배관 이상 여부 확인", category: "설비", severity: 1, requires_photo: false, requires_note: true },
      { item_key: "housekeeping", item_text: "정리정돈 및 위험물 방치 여부 확인", category: "일반", severity: 1, requires_photo: false, requires_note: false },
    ];
    return base.map((x, idx) => ({ ...x, sort_order: (idx + 1) * 10, is_active: true }));
  }

  function updateSetupState(data) {
    const targets = Array.isArray(data?.targets) ? data.targets : [];
    const templates = Array.isArray(data?.templates) ? data.templates : [];
    const hasTargets = targets.length > 0;
    const hasTemplates = templates.length > 0;
    const setupCard = $("#setupCard");
    const setupHint = $("#setupHint");
    const createBtn = $("#btnCreateRun");

    const canRunCreate = canCreateRun();
    if (createBtn) createBtn.disabled = !(hasTargets && hasTemplates && canRunCreate);

    if (!setupCard || !setupHint) return;

    const manager = isManagerUser();
    if (!manager) {
      setupCard.hidden = true;
      msg("점검대상/점검표가 아직 없습니다. 관리자 계정으로 기초정보를 먼저 등록해 주세요.", true);
      return;
    }

    setupCard.hidden = false;
    if (hasTargets && hasTemplates) {
      setupHint.textContent = "새 점검대상/점검표를 추가로 만들 수 있습니다. 항목 수를 정하고 세부리스트를 입력해 주세요.";
    } else if (!hasTargets && !hasTemplates) {
      setupHint.textContent = "현재 단지에 점검대상과 점검표가 없습니다. 아래 버튼으로 기본 구성을 생성해 주세요.";
    } else if (!hasTargets) {
      setupHint.textContent = "점검대상이 없습니다. 점검대상을 먼저 생성해 주세요.";
    } else {
      setupHint.textContent = "점검표가 없습니다. 점검표를 생성해야 점검을 만들 수 있습니다.";
    }
  }

  function buildDetailItemInputs(count) {
    const wrap = $("#detailItems");
    if (!wrap) return;
    const cleanCount = Math.max(1, Math.min(50, Number(count || 0) || 1));
    wrap.innerHTML = "";
    for (let i = 1; i <= cleanCount; i += 1) {
      const row = document.createElement("div");
      row.className = "detail-item-row";
      row.innerHTML = `
        <div class="detail-item-grid">
          <label class="field">
            <span>세부리스트 ${i} - 대분류</span>
            <input data-role="detail-major" type="text" maxlength="120" placeholder="예: 전기" />
          </label>
          <label class="field">
            <span>세부리스트 ${i} - 중분류</span>
            <input data-role="detail-middle" type="text" maxlength="120" placeholder="예: 수전설비" />
          </label>
          <label class="field">
            <span>세부리스트 ${i} - 소분류</span>
            <input data-role="detail-minor" type="text" maxlength="120" placeholder="예: 차단기 상태 확인" />
          </label>
        </div>
      `;
      wrap.appendChild(row);
    }
  }

  function collectDetailItemsFromForm() {
    const rows = Array.from(document.querySelectorAll("#detailItems .detail-item-row"));
    if (!rows.length) return defaultTemplateItems();

    const items = [];
    const missing = [];
    rows.forEach((row, idx) => {
      const major = String(row.querySelector("input[data-role='detail-major']")?.value || "").trim();
      const middle = String(row.querySelector("input[data-role='detail-middle']")?.value || "").trim();
      const minor = String(row.querySelector("input[data-role='detail-minor']")?.value || "").trim();
      if (!major || !middle || !minor) {
        missing.push(idx + 1);
        return;
      }
      items.push({
        item_key: `item_${String(idx + 1).padStart(2, "0")}`,
        item_text: `${middle} / ${minor}`,
        category: major,
        severity: 1,
        sort_order: (idx + 1) * 10,
        requires_photo: false,
        requires_note: false,
        is_active: true,
      });
    });
    if (missing.length) {
      throw new Error(`세부리스트 항목의 대/중/소분류를 모두 입력해 주세요: ${missing.join(", ")}번`);
    }
    return items;
  }

  async function loadBootstrap() {
    const q = buildSiteQuery();
    const data = await apiGet(`/api/inspection/bootstrap${q ? `?${q}` : ""}`);
    state.siteCode = String(data.site_code || "").trim().toUpperCase();
    state.bootstrapUser = data.user || null;

    const userLine = $("#userLine");
    if (userLine) {
      userLine.textContent = `사용자: ${data.user?.name || data.user?.login_id || "-"} (${data.user?.role || "-"}) / 단지코드: ${state.siteCode || "-"}`;
    }

    const targetSel = $("#targetId");
    if (targetSel) {
      targetSel.innerHTML = "";
      for (const t of data.targets || []) {
        const opt = document.createElement("option");
        opt.value = String(t.id);
        opt.textContent = `${t.name} (${t.site_code})`;
        targetSel.appendChild(opt);
      }
      if (!targetSel.options.length) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "등록된 점검대상이 없습니다.";
        targetSel.appendChild(opt);
      }
    }
    const tplSel = $("#templateId");
    if (tplSel) {
      tplSel.innerHTML = "";
      for (const t of data.templates || []) {
        const opt = document.createElement("option");
        opt.value = String(t.id);
        opt.textContent = `${t.name} [${t.period}] · ${t.item_count}항목`;
        tplSel.appendChild(opt);
      }
      if (!tplSel.options.length) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "등록된 점검표가 없습니다.";
        tplSel.appendChild(opt);
      }
    }

    updateSetupState(data);

    if ($("#quickTargetName") && !$("#quickTargetName").value.trim()) {
      $("#quickTargetName").value = "공용시설 기본 점검대상";
    }
    if ($("#quickTemplateName") && !$("#quickTemplateName").value.trim()) {
      $("#quickTemplateName").value = "월간 안전점검표";
    }
    if ($("#detailItems") && !document.querySelector("#detailItems .detail-item-row")) {
      buildDetailItemInputs(Number($("#detailItemCount")?.value || 5));
    }
  }

  function renderRuns() {
    const wrap = $("#runs");
    if (!wrap) return;
    wrap.innerHTML = "";
    if (!state.runs.length) {
      wrap.innerHTML = `<div class="muted">조회 결과가 없습니다.</div>`;
      return;
    }
    for (const r of state.runs) {
      const div = document.createElement("div");
      div.className = "run-item";
      div.innerHTML = `
        <div class="head">
          <div>
            <b>${escapeHtml(r.run_code || r.id)}</b>
            <span class="muted"> · ${escapeHtml(r.status || "-")} · ${escapeHtml(r.run_date || "-")}</span>
          </div>
          <button class="btn" type="button" data-id="${Number(r.id || 0)}">상세</button>
        </div>
        <div class="muted">${escapeHtml(r.target_name || "-")} / ${escapeHtml(r.template_name || "-")} / 부적합 ${Number(r.noncompliant_count || 0)}건</div>
      `;
      div.querySelector("button")?.addEventListener("click", () => openRun(Number(r.id || 0)));
      wrap.appendChild(div);
    }
  }

  async function loadRuns() {
    msg("");
    const params = new URLSearchParams();
    if (state.siteCode) params.set("site_code", state.siteCode);
    const st = ($("#qStatus")?.value || "").trim();
    if (st) params.set("status", st);
    const df = ($("#qDateFrom")?.value || "").trim();
    if (df) params.set("date_from", df);
    const dt = ($("#qDateTo")?.value || "").trim();
    if (dt) params.set("date_to", dt);
    const data = await apiGet(`/api/inspection/runs?${params.toString()}`);
    state.runs = Array.isArray(data.items) ? data.items : [];
    renderRuns();
  }

  async function createRun() {
    if (!canCreateRun()) {
      msg("점검 생성 권한은 최고/운영관리자와 단지대표자만 가능합니다.", true);
      return;
    }
    const targetId = Number($("#targetId")?.value || 0);
    const templateId = Number($("#templateId")?.value || 0);
    const runDate = ($("#runDate")?.value || "").trim() || todayYmd();
    const runNote = ($("#runNote")?.value || "").trim();
    if (targetId <= 0 || templateId <= 0) {
      const targetCount = Number($("#targetId")?.options?.length || 0);
      const templateCount = Number($("#templateId")?.options?.length || 0);
      if (targetCount <= 1 || templateCount <= 1) {
        const canSetup = isManagerUser();
        msg(
          canSetup
            ? "점검대상/점검표가 아직 없습니다. 아래 '기초정보 빠른 설정'에서 먼저 생성해 주세요."
            : "점검대상/점검표가 아직 없습니다. 관리자에게 기초정보 등록을 요청해 주세요.",
          true
        );
        return;
      }
      msg("점검대상과 점검표를 선택하세요.", true);
      return;
    }
    const payload = {
      site_code: state.siteCode,
      target_id: targetId,
      template_id: templateId,
      run_date: runDate,
      note: runNote,
    };
    const out = await apiPost("/api/inspection/runs", payload);
    msg(`점검 생성 완료: ${out.run_code || out.run_id}`);
    await loadRuns();
    if (Number(out.run_id || 0) > 0) await openRun(Number(out.run_id || 0));
  }

  async function quickSetup() {
    if (!isManagerUser()) {
      msg("최고/운영관리자 또는 단지대표자 권한에서만 기초정보를 생성할 수 있습니다.", true);
      return;
    }
    const targetName = ($("#quickTargetName")?.value || "").trim() || "공용시설 기본 점검대상";
    const templateName = ($("#quickTemplateName")?.value || "").trim() || "월간 안전점검표";
    const period = String($("#quickTemplatePeriod")?.value || "MONTHLY").trim().toUpperCase();
    const items = collectDetailItemsFromForm();

    const targetOut = await apiPost("/api/inspection/targets", {
      site_code: state.siteCode,
      name: targetName,
      location: "",
      description: "리스트 폼 자동 생성으로 추가된 점검대상",
      is_active: true,
      force_new: true,
    });
    const targetId = Number(targetOut?.item?.id || targetOut?.target_id || 0);
    if (targetId <= 0) {
      throw new Error("점검대상 생성에 실패했습니다.");
    }

    const templateOut = await apiPost("/api/inspection/templates", {
      site_code: state.siteCode,
      target_id: targetId,
      name: templateName,
      period,
      is_active: true,
      force_new: true,
      items,
    });
    const templateId = Number(templateOut?.template_id || templateOut?.item?.id || 0);
    if (templateId <= 0) {
      throw new Error("점검표 생성에 실패했습니다.");
    }

    await loadBootstrap();
    if (targetId > 0 && $("#targetId")) $("#targetId").value = String(targetId);
    if (templateId > 0 && $("#templateId")) $("#templateId").value = String(templateId);
    msg("리스트 폼 자동 생성 완료: 점검대상/점검표가 신규로 추가되었습니다.");
  }

  function collectItemPayloads() {
    const rows = [];
    for (const card of document.querySelectorAll(".item-card[data-id]")) {
      const id = Number(card.dataset.id || 0);
      if (id <= 0) continue;
      const result = String(card.querySelector("select[data-role='result']")?.value || "NA").trim().toUpperCase();
      const note = String(card.querySelector("textarea[data-role='note']")?.value || "").trim();
      rows.push({ id, result, note });
    }
    return rows;
  }

  function renderRunDetail() {
    const run = state.selectedRun;
    const card = $("#detailCard");
    if (!card || !run) return;
    card.hidden = false;

    const runMeta = $("#runMeta");
    if (runMeta) {
      runMeta.textContent = `코드 ${run.run.run_code} / 상태 ${run.run.status} / 점검일 ${run.run.run_date} / 점검자 ${run.run.inspector_name || run.run.inspector_login}`;
    }

    const itemsWrap = $("#runItems");
    if (itemsWrap) {
      itemsWrap.innerHTML = "";
      for (const it of run.items || []) {
        const iid = Number(it.id || 0);
        const row = document.createElement("div");
        row.className = "item-card";
        row.dataset.id = String(iid);
        const photoHref = iid > 0 ? `/api/inspection/runs/${run.run.id}/items/${iid}/photo` : "";
        row.innerHTML = `
          <div class="item-grid">
            <div>
              <div><b>${escapeHtml(it.item_text || it.item_key || "-")}</b> <span class="muted">(${escapeHtml(it.category || "-")})</span></div>
              <textarea data-role="note" placeholder="메모">${escapeHtml(it.note || "")}</textarea>
              <div class="photo-row">
                <input data-role="file" type="file" accept="image/*" />
                <button data-role="upload" class="btn" type="button">사진 업로드</button>
                ${it.photo_path ? `<a class="btn" href="${photoHref}" target="_blank">사진 보기</a>` : `<span class="muted">사진 없음</span>`}
              </div>
            </div>
            <div>
              <label class="field">
                <span>결과</span>
                <select data-role="result">
                  <option value="COMPLIANT" ${String(it.result || "").toUpperCase() === "COMPLIANT" ? "selected" : ""}>양호</option>
                  <option value="NONCOMPLIANT" ${String(it.result || "").toUpperCase() === "NONCOMPLIANT" ? "selected" : ""}>부적합</option>
                  <option value="NA" ${String(it.result || "").toUpperCase() === "NA" ? "selected" : ""}>해당없음</option>
                </select>
              </label>
            </div>
          </div>
        `;
        row.querySelector("button[data-role='upload']")?.addEventListener("click", async () => {
          try {
            const f = row.querySelector("input[data-role='file']")?.files?.[0];
            if (!f) {
              msg("업로드할 사진 파일을 선택하세요.", true);
              return;
            }
            await uploadPhoto(Number(run.run.id || 0), iid, f);
            msg("사진 업로드 완료");
            await openRun(Number(run.run.id || 0));
          } catch (e) {
            msg(`사진 업로드 실패: ${e.message || e}`, true);
          }
        });
        itemsWrap.appendChild(row);
      }
    }

    const approvalsWrap = $("#runApprovals");
    if (approvalsWrap) {
      approvalsWrap.innerHTML = "";
      for (const ap of run.approvals || []) {
        const div = document.createElement("div");
        div.className = "ap-card";
        div.textContent = `step ${ap.step_no} / ${ap.approver_name || ap.approver_login || "-"} / ${ap.decision} / ${ap.decided_at || "-"}`;
        approvalsWrap.appendChild(div);
      }
    }
  }

  async function openRun(runId) {
    if (runId <= 0) return;
    const data = await apiGet(`/api/inspection/runs/${runId}`);
    state.selectedRunId = runId;
    state.selectedRun = data;
    verifyMsg("");
    renderRunDetail();
  }

  async function saveItems() {
    const runId = Number(state.selectedRunId || 0);
    if (runId <= 0) return;
    const payload = { items: collectItemPayloads() };
    await apiPatch(`/api/inspection/runs/${runId}/items`, payload);
    msg("점검 항목 저장 완료");
    await openRun(runId);
    await loadRuns();
  }

  async function submitRun() {
    const runId = Number(state.selectedRunId || 0);
    if (runId <= 0) return;
    await apiPost(`/api/inspection/runs/${runId}/submit`, {});
    msg("점검 제출 완료");
    await openRun(runId);
    await loadRuns();
  }

  async function approveRun() {
    const runId = Number(state.selectedRunId || 0);
    if (runId <= 0) return;
    const comment = window.prompt("승인 코멘트(선택)", "") || "";
    await apiPost(`/api/inspection/runs/${runId}/approve`, { comment });
    msg("승인 처리 완료");
    await openRun(runId);
    await loadRuns();
  }

  async function rejectRun() {
    const runId = Number(state.selectedRunId || 0);
    if (runId <= 0) return;
    const comment = window.prompt("반려 사유", "") || "";
    await apiPost(`/api/inspection/runs/${runId}/reject`, { comment });
    msg("반려 처리 완료");
    await openRun(runId);
    await loadRuns();
  }

  async function verifyArchive() {
    const runId = Number(state.selectedRunId || 0);
    if (runId <= 0) return;
    try {
      const out = await apiGet(`/api/inspection/archives/${runId}/verify`);
      verifyMsg(out.valid ? "무결성 검증 OK" : "무결성 불일치", !out.valid);
    } catch (e) {
      verifyMsg(`검증 실패: ${e.message || e}`, true);
    }
  }

  function openArchivePdf() {
    const runId = Number(state.selectedRunId || 0);
    if (runId <= 0) return;
    window.open(`/api/inspection/archives/${runId}/pdf`, "_blank");
  }

  async function init() {
    try {
      if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
      state.user = await window.KAAuth.requireAuth();
      $("#runDate").value = todayYmd();
      $("#btnReload")?.addEventListener("click", () => {
        loadBootstrap().then(loadRuns).catch((e) => msg(e.message || e, true));
      });
      $("#btnCreateRun")?.addEventListener("click", () => createRun().catch((e) => msg(e.message || e, true)));
      $("#btnLoadRuns")?.addEventListener("click", () => loadRuns().catch((e) => msg(e.message || e, true)));
      $("#btnSaveItems")?.addEventListener("click", () => saveItems().catch((e) => msg(e.message || e, true)));
      $("#btnSubmitRun")?.addEventListener("click", () => submitRun().catch((e) => msg(e.message || e, true)));
      $("#btnApproveRun")?.addEventListener("click", () => approveRun().catch((e) => msg(e.message || e, true)));
      $("#btnRejectRun")?.addEventListener("click", () => rejectRun().catch((e) => msg(e.message || e, true)));
      $("#btnVerifyArchive")?.addEventListener("click", () => verifyArchive().catch((e) => verifyMsg(e.message || e, true)));
      $("#btnOpenPdf")?.addEventListener("click", openArchivePdf);
      $("#btnQuickSetup")?.addEventListener("click", () => quickSetup().catch((e) => msg(e.message || e, true)));
      $("#btnBuildDetailItems")?.addEventListener("click", () => {
        buildDetailItemInputs(Number($("#detailItemCount")?.value || 5));
      });

      await loadBootstrap();
      await loadRuns();
    } catch (e) {
      msg(e.message || String(e), true);
    }
  }

  init();
})();
