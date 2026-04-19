(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const STATUS_VALUES = ["접수", "처리중", "완료", "이월"];
  const MAX_CHAT_DIGEST_IMAGES = 30;
  const MAX_WORK_REPORT_IMAGES = 200;
  const MAX_WORK_REPORT_SOURCE_FILES = 20;
  const MAX_FACILITY_ASSET_IMAGES = 3;
  const DEFAULT_DOCUMENT_CATEGORY_VALUES = [
    "기안지(10만원 이상)",
    "구매요청서(10만원 이하)",
    "견적서와 발주서",
    "월업무보고(작업 보고서)",
    "계약서관리",
    "배상보험",
    "주요업무일정관리",
    "전기수도검침",
    "전기수도부과",
    "직무고시",
    "안전관리대장관리",
    "법정 정기점검",
    "수질검사",
    "소방정기점검",
    "기계설비유지관리",
    "기계설비성능점검",
    "승강기안전점검",
    "안전점검하자보수완료보고",
    "기타",
  ];
  const NOTICE_STATUS_LABELS = {
    draft: "임시저장",
    published: "게시중",
    archived: "보관",
  };
  const USER_ROLE_OPTIONS = [
    { value: "desk", label: "민원접수" },
    { value: "manager", label: "운영담당" },
    { value: "staff", label: "일반직원" },
    { value: "vendor", label: "외주업체" },
    { value: "reader", label: "읽기전용" },
    { value: "integration", label: "연동계정" },
  ];
  const WORK_REPORT_LEARNING_CHECK_LABELS = {
    choice_feedback_rows: "선택 피드백 부족",
    top1_accuracy: "top-1 부족",
    top3_hit_rate: "top-3 부족",
    human_intervention_rate: "사람 개입률 높음",
    unmatched_false_positive_rate: "미매칭 오판 높음",
  };

  let me = null;
  let tenants = [];
  let adminWorkReportLearning = { items: [], meta: {} };
  let buildInfo = null;
  let selectedComplaintId = 0;
  let selectedComplaint = null;
  let lastAiResult = null;
  let users = [];
  let selectedUserId = 0;
  let selectedUser = null;
  let opsNotices = [];
  let opsDocuments = [];
  let opsSchedules = [];
  let opsVendors = [];
  let infoBuildings = [];
  let infoRegistrations = [];
  let selectedInfoBuildingId = 0;
  let selectedInfoRegistrationId = 0;
  let facilityAssets = [];
  let facilityAssetCatalog = [];
  let facilityChecklists = [];
  let facilityInspections = [];
  let facilityWorkOrders = [];
  let selectedNoticeId = 0;
  let selectedDocumentId = 0;
  let selectedScheduleId = 0;
  let selectedVendorId = 0;
  let selectedFacilityAssetId = 0;
  let selectedFacilityChecklistId = 0;
  let selectedFacilityInspectionId = 0;
  let selectedFacilityWorkOrderId = 0;
  let pendingFacilityAssetImages = [];
  let facilityAssetPreviewUrls = [];
  let facilityAssetPendingImageSequence = 0;
  let documentNumberingConfig = null;
  let documentCatalog = {
    categories: [...DEFAULT_DOCUMENT_CATEGORY_VALUES],
    profiles: [],
    common_fields: [],
    preview_examples: {},
  };
  let chatSourcePreviewUrls = [];
  let chatDigestPreviewUrls = [];
  let lastDigestResult = null;
  let lastDigestImported = false;
  let lastDigestSelectedKeys = new Set();
  let lastWorkReportResult = null;
  let lastWorkReportBaseline = null;
  let lastWorkReportJobId = "";
  let lastWorkReportFeedbackSavedSignature = "";
  let activeWorkReportPreviewIndex = 0;
  let currentIntakeStep = 1;
  let currentMobileWorkspace = "intake";

  const MOBILE_INTAKE_STEPS = [
    { step: 1, title: "1단계 / 위치와 연락처" },
    { step: 2, title: "2단계 / 민원 내용과 AI 분류" },
    { step: 3, title: "3단계 / 사진 첨부" },
    { step: 4, title: "4단계 / 검토 후 저장" },
  ];
  const MOBILE_PANEL_DEFAULTS = {
    complaintOverview: "urgent",
    opsOverview: "notice",
    infoWorkspace: "overview",
    facilityOverview: "due",
    adminWorkspaceSections: "dashboard",
    facilityWorkspaceSections: "dashboard",
    infoWorkspaceSections: "info",
    adminManagePanels: "notice",
    facilityManagePanels: "asset",
  };
  const mobilePanelState = { ...MOBILE_PANEL_DEFAULTS };

  function setMessage(selector, message, isError = false) {
    const el = $(selector);
    if (!el) return;
    el.textContent = String(message || "");
    el.classList.toggle("error", !!isError);
  }

  function formatElapsedMinSec(value) {
    const totalSeconds = Math.max(0, Math.floor(Number(value || 0)));
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}분 ${String(seconds).padStart(2, "0")}초`;
  }

  const WORK_REPORT_REQUEST_TIMEOUT_MS = 1200000;
  const WORK_REPORT_IMAGE_STAGE_OPTIONS = [
    { value: "general", label: "현장 이미지" },
    { value: "before", label: "작업 전" },
    { value: "during", label: "작업 중" },
    { value: "after", label: "작업 후" },
  ];

  function renderWorkReportProgress(state) {
    const modeLabel = state?.mode === "pdf" ? "주요업무보고 PDF" : "주요업무보고 미리보기";
    const elapsed = Math.max(0, Number(state?.elapsedSec || 0));
    const currentStep = Math.max(0, Number(state?.currentStep || 0));
    const steps = Array.isArray(state?.steps) ? state.steps : [];
    const hint = String(state?.hint || "").trim();
    return [
      '<div class="work-report-progress">',
      `<div class="work-report-progress-head"><strong>${escapeHtml(modeLabel)}</strong><span>${escapeHtml(`${formatElapsedMinSec(elapsed)} 경과`)}</span></div>`,
      '<div class="work-report-progress-visual" aria-hidden="true"><div class="work-report-progress-ring"></div><div class="work-report-progress-core"></div><div class="work-report-progress-dot dot-a"></div><div class="work-report-progress-dot dot-b"></div><div class="work-report-progress-dot dot-c"></div></div>',
      `<div class="work-report-progress-note">${escapeHtml(state?.summary || "처리 중입니다.")}</div>`,
      `<div class="work-report-progress-steps">${steps.map((step, index) => {
        const status = index < currentStep ? "done" : index === currentStep ? "active" : "pending";
        const label = status === "done" ? "완료" : status === "active" ? "진행 중" : "대기";
        return `<div class="work-report-progress-step ${status}"><strong>${escapeHtml(`${index + 1}. ${String(step || "")}`)}</strong><span>${escapeHtml(label)}</span></div>`;
      }).join("")}</div>`,
      hint ? `<div class="work-report-progress-hint">${escapeHtml(hint)}</div>` : "",
      "</div>",
    ].join("");
  }

  function renderWorkReportProgressTerminal(mode, elapsedSec, message, status = "done") {
    const modeLabel = mode === "pdf" ? "주요업무보고 PDF" : "주요업무보고 미리보기";
    const safeMessage = String(message || "").trim() || (status === "error" ? "처리 중 오류가 발생했습니다." : "처리가 완료되었습니다.");
    const statusLabel = status === "error" ? "실패" : "완료";
    const statusClass = status === "error" ? " is-error" : " is-done";
    return [
      `<div class="work-report-progress${statusClass}">`,
      `<div class="work-report-progress-head"><strong>${escapeHtml(modeLabel)}</strong><span>${escapeHtml(`${formatElapsedMinSec(elapsedSec)} 경과 · ${statusLabel}`)}</span></div>`,
      `<div class="work-report-progress-note">${escapeHtml(safeMessage)}</div>`,
      status === "error" ? '<div class="work-report-progress-hint">잠시 후 다시 시도해 주세요. 같은 지점에서 멈추면 서버 처리 시간을 추가로 줄이겠습니다.</div>' : "",
      "</div>",
    ].join("");
  }

  function focusWorkReportBox() {
    const target = $("#workReportBox");
    if (!target) return;
    target.classList.remove("hidden");
    window.requestAnimationFrame(() => {
      target.scrollIntoView({ behavior: "smooth", block: "start" });
      if (typeof target.focus === "function") {
        target.focus({ preventScroll: true });
      }
    });
  }

  function startWorkReportProgress(mode = "preview") {
    const target = $("#workReportBox");
    const previewSteps = [
      "입력 내용 확인",
      "원문과 사진 정리",
      "서버 분석 요청",
      "작업 항목과 이미지 매칭",
      "결과 정리",
    ];
    const pdfSteps = [
      "입력 내용 확인",
      "원문과 사진 정리",
      "서버 분석 요청",
      "작업 항목과 이미지 매칭",
      "PDF 렌더링",
    ];
    const steps = mode === "pdf" ? pdfSteps : previewSteps;
    const state = {
      mode,
      steps,
      currentStep: 0,
      summary: mode === "pdf" ? "PDF 생성을 준비하고 있습니다." : "배치 작업을 등록하고 있습니다.",
      hint: mode === "pdf" ? "미리보기에서 고른 항목을 PDF로 정리합니다." : "원문과 현장 사진을 서버 작업으로 넘기는 중입니다.",
    };
    let timerId = 0;
    let baseElapsedSec = 0;
    let lastSyncedAt = Date.now();

    const effectiveElapsedSec = () => {
      const extraSeconds = Math.max(0, Math.floor((Date.now() - lastSyncedAt) / 1000));
      return Math.max(0, baseElapsedSec + extraSeconds);
    };

    const paint = () => {
      if (!target) return;
      target.classList.remove("hidden");
      target.innerHTML = renderWorkReportProgress({
        mode: state.mode,
        elapsedSec: effectiveElapsedSec(),
        currentStep: state.currentStep,
        steps: state.steps,
        summary: state.summary,
        hint: state.hint,
      });
    };

    paint();
    focusWorkReportBox();
    timerId = window.setInterval(paint, 1000);
    const stop = () => {
      if (timerId) {
        window.clearInterval(timerId);
        timerId = 0;
      }
    };
    return {
      stop,
      elapsedSec() {
        return effectiveElapsedSec();
      },
      sync(serverState = {}) {
        baseElapsedSec = Number.isFinite(Number(serverState.elapsedSec))
          ? Math.max(0, Math.floor(Number(serverState.elapsedSec)))
          : effectiveElapsedSec();
        lastSyncedAt = Date.now();
        if (Number.isFinite(Number(serverState.currentStep))) {
          state.currentStep = Math.max(0, Math.min(steps.length - 1, Math.floor(Number(serverState.currentStep))));
        }
        if (serverState.summary !== undefined) {
          const summary = String(serverState.summary || "").trim();
          if (summary) state.summary = summary;
        }
        if (serverState.hint !== undefined) {
          state.hint = String(serverState.hint || "").trim();
        }
        paint();
      },
      complete(message) {
        stop();
        if (!target) return;
        target.classList.remove("hidden");
        target.innerHTML = renderWorkReportProgressTerminal(mode, effectiveElapsedSec(), message, "done");
      },
      fail(error) {
        stop();
        if (!target) return;
        const message = String(error?.message || error || "처리 중 오류가 발생했습니다.").trim();
        target.classList.remove("hidden");
        target.innerHTML = renderWorkReportProgressTerminal(mode, effectiveElapsedSec(), message, "error");
      },
    };
  }

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function formatDateTime(value) {
    const raw = String(value || "").trim();
    if (!raw) return "-";
    const dt = new Date(raw.replace(" ", "T"));
    if (Number.isNaN(dt.getTime())) return raw;
    return dt.toLocaleString("ko-KR", { hour12: false });
  }

  function formatDate(value) {
    const raw = String(value || "").trim();
    if (!raw) return "-";
    const dt = new Date(`${raw}T00:00:00`);
    if (Number.isNaN(dt.getTime())) return raw;
    return dt.toLocaleDateString("ko-KR");
  }

  function formatFileSize(value) {
    const size = Number(value || 0);
    if (!Number.isFinite(size) || size <= 0) return "";
    if (size >= 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(size >= 10 * 1024 * 1024 ? 0 : 1)}MB`;
    if (size >= 1024) return `${Math.round(size / 1024)}KB`;
    return `${size}B`;
  }

  function formatNumber(value) {
    const number = Number(value || 0);
    if (!Number.isFinite(number)) return "0";
    return new Intl.NumberFormat("ko-KR").format(number);
  }

  function formatPercent(value) {
    const number = Number(value || 0);
    if (!Number.isFinite(number)) return "-";
    return `${(number * 100).toFixed(1)}%`;
  }

  function isMobileViewport() {
    return window.matchMedia("(max-width: 760px)").matches;
  }

  function isAdmin() {
    return !!(me && me.user && me.user.is_admin);
  }

  function canManageUsers() {
    return !!(me && me.user && (me.user.is_admin || me.user.is_site_admin));
  }

  function canEditOps() {
    if (!me || !me.user) return false;
    if (me.user.is_admin || me.user.is_site_admin) return true;
    return ["manager", "desk", "staff"].includes(String(me.user.role || ""));
  }

  function canManageDocNumberingConfig() {
    return !!(me && me.user && (me.user.is_admin || me.user.is_site_admin));
  }

  function canEditFacility() {
    if (!me || !me.user) return false;
    if (me.user.is_admin || me.user.is_site_admin) return true;
    return ["manager", "desk", "staff"].includes(String(me.user.role || ""));
  }

  function canDeleteComplaints() {
    return !!(me && me.user && (me.user.is_admin || me.user.is_site_admin));
  }

  function currentTenantId() {
    if (isAdmin()) {
      return String($("#tenantSelect")?.value || "").trim();
    }
    return String((me && (me.tenant?.id || me.user?.tenant_id)) || "").trim();
  }

  function currentTenantLabel() {
    const tenantId = currentTenantId();
    if (isAdmin()) {
      const tenant = tenants.find((item) => String(item.id) === tenantId);
      return tenant ? `${tenant.name} (${tenant.id})` : (tenantId || "전체");
    }
    return String(me?.tenant?.name || me?.user?.tenant_id || "-");
  }

  function documentCategoryValues() {
    const categories = Array.isArray(documentCatalog?.categories) ? documentCatalog.categories.filter(Boolean) : [];
    return categories.length ? categories : [...DEFAULT_DOCUMENT_CATEGORY_VALUES];
  }

  function documentProfileByCategory(category) {
    const raw = String(category || "").trim();
    const profiles = Array.isArray(documentCatalog?.profiles) ? documentCatalog.profiles : [];
    return profiles.find((item) => String(item.category || "") === raw) || null;
  }

  function defaultDocumentCategory() {
    return documentCategoryValues()[0] || "기타";
  }

  async function api(url, opts = {}) {
    return window.KAAuth.requestJson(url, opts);
  }

  async function authFetchJson(url, opts = {}) {
    const headers = { ...(opts.headers || {}) };
    const token = window.KAAuth.getToken();
    if (token && !headers.Authorization) {
      headers.Authorization = `Bearer ${token}`;
    }
    const timeoutMs = Math.max(0, Number(opts.timeoutMs || 0));
    const controller = timeoutMs > 0 ? new AbortController() : null;
    const { timeoutMs: _timeoutMs, signal: externalSignal, ...fetchOpts } = opts;
    let timeoutId = 0;
    if (controller) {
      if (externalSignal) {
        if (externalSignal.aborted) controller.abort();
        else externalSignal.addEventListener("abort", () => controller.abort(), { once: true });
      }
      timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
    }
    let response;
    try {
      response = await fetch(url, {
        ...fetchOpts,
        signal: controller?.signal || externalSignal,
        headers,
        credentials: "same-origin",
      });
    } catch (error) {
      if (controller?.signal?.aborted) {
        throw new Error(`요청 시간이 ${formatElapsedMinSec(Math.ceil(timeoutMs / 1000))}을 넘어 중단되었습니다.`);
      }
      throw error;
    } finally {
      if (timeoutId) window.clearTimeout(timeoutId);
    }
    const contentType = response.headers.get("content-type") || "";
    const body = contentType.includes("application/json") ? await response.json() : await response.text();
    if (response.status === 401) {
      window.KAAuth.clearSession({ includeSensitive: true, broadcast: true });
      window.KAAuth.redirectLogin();
      throw new Error(typeof body === "string" ? body : String(body.detail || "401"));
    }
    if (!response.ok) {
      throw new Error(typeof body === "string" ? body : String(body.detail || body.message || response.status));
    }
    return body;
  }

  async function authFetchBlob(url, opts = {}) {
    const headers = { ...(opts.headers || {}) };
    const token = window.KAAuth.getToken();
    if (token && !headers.Authorization) {
      headers.Authorization = `Bearer ${token}`;
    }
    const timeoutMs = Math.max(0, Number(opts.timeoutMs || 0));
    const controller = timeoutMs > 0 ? new AbortController() : null;
    const { timeoutMs: _timeoutMs, signal: externalSignal, ...fetchOpts } = opts;
    let timeoutId = 0;
    if (controller) {
      if (externalSignal) {
        if (externalSignal.aborted) controller.abort();
        else externalSignal.addEventListener("abort", () => controller.abort(), { once: true });
      }
      timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
    }
    let response;
    try {
      response = await fetch(url, {
        ...fetchOpts,
        signal: controller?.signal || externalSignal,
        headers,
        credentials: "same-origin",
      });
    } catch (error) {
      if (controller?.signal?.aborted) {
        throw new Error(`요청 시간이 ${formatElapsedMinSec(Math.ceil(timeoutMs / 1000))}을 넘어 중단되었습니다.`);
      }
      throw error;
    } finally {
      if (timeoutId) window.clearTimeout(timeoutId);
    }
    if (response.status === 401) {
      window.KAAuth.clearSession({ includeSensitive: true, broadcast: true });
      window.KAAuth.redirectLogin();
      throw new Error("401");
    }
    if (!response.ok) {
      const contentType = response.headers.get("content-type") || "";
      const body = contentType.includes("application/json") ? await response.json() : await response.text();
      throw new Error(typeof body === "string" ? body : String(body.detail || body.message || response.status));
    }
    const disposition = String(response.headers.get("content-disposition") || "");
    const matched = disposition.match(/filename=\"?([^\";]+)\"?/i);
    return {
      blob: await response.blob(),
      filename: matched ? matched[1] : "",
    };
  }

  function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = String(filename || "report.pdf").trim() || "report.pdf";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function selectedSingleFile(inputSelector) {
    return $(inputSelector)?.files?.[0] || null;
  }

  function complaintPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      building: String($("#buildingInput").value || "").trim(),
      unit: String($("#unitInput").value || "").trim(),
      complainant_phone: String($("#phoneInput").value || "").trim(),
      channel: String($("#channelInput").value || "기타").trim() || "기타",
      content: String($("#contentInput").value || "").trim(),
      manager: String($("#managerInput").value || "").trim(),
      auto_classify: true,
      summary: String((lastAiResult || {}).summary || "").trim(),
      type: String((lastAiResult || {}).type || "").trim(),
      urgency: String((lastAiResult || {}).urgency || "").trim(),
    };
  }

  function selectedFiles(inputSelector) {
    const files = $(inputSelector)?.files;
    return files ? Array.from(files) : [];
  }

  function setInputFiles(inputSelector, files) {
    const input = $(inputSelector);
    if (!input) return;
    if (typeof DataTransfer === "undefined") {
      input.value = "";
      return;
    }
    const dt = new DataTransfer();
    for (const file of Array.isArray(files) ? files : []) {
      if (file instanceof File) {
        dt.items.add(file);
      }
    }
    input.files = dt.files;
  }

  function removeInputFileAt(inputSelector, index) {
    const files = selectedFiles(inputSelector);
    if (index < 0 || index >= files.length) return;
    files.splice(index, 1);
    setInputFiles(inputSelector, files);
  }

  function clearInputFiles(inputSelector) {
    setInputFiles(inputSelector, []);
    const input = $(inputSelector);
    if (input) input.value = "";
  }

  function formatInlineFileSize(file) {
    const size = Number(file?.size || 0);
    if (!Number.isFinite(size) || size <= 0) return "";
    if (size >= 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)}MB`;
    if (size >= 1024) return `${Math.round(size / 1024)}KB`;
    return `${size}B`;
  }

  function renderSelectableFileList(inputSelector, targetSelector, emptyMessage, kindLabel = "파일") {
    const box = $(targetSelector);
    if (!box) return;
    const files = selectedFiles(inputSelector);
    if (!files.length) {
      box.innerHTML = "";
      box.classList.add("hidden");
      return;
    }
    box.innerHTML = files.map((file, index) => [
      '<article class="work-report-file-card">',
      '<div>',
      `<strong>${escapeHtml(`${kindLabel} ${index + 1}`)}</strong>`,
      `<div class="meta">${escapeHtml(file.name || `${kindLabel}-${index + 1}`)}${formatInlineFileSize(file) ? ` · ${escapeHtml(formatInlineFileSize(file))}` : ""}</div>`,
      '</div>',
      `<button class="file-remove-btn" type="button" data-input-selector="${escapeHtml(inputSelector)}" data-file-index="${index}">삭제</button>`,
      '</article>',
    ].join("")).join("");
    box.classList.remove("hidden");
  }

  function updatePhotoHint(inputSelector, targetSelector, limit = 6) {
    const files = selectedFiles(inputSelector);
    const el = $(targetSelector);
    if (!el) return;
    if (!files.length) {
      el.textContent = "선택된 사진이 없습니다.";
      return;
    }
    el.textContent = `선택 ${files.length}장 / 최대 ${limit}장`;
  }

  function updateGenericFileHint(inputSelector, targetSelector, emptyMessage = "선택된 파일이 없습니다.") {
    const files = selectedFiles(inputSelector);
    const el = $(targetSelector);
    if (!el) return;
    if (!files.length) {
      el.textContent = emptyMessage;
      return;
    }
    el.textContent = `선택 ${files.length}건`;
  }

  function updateSingleFileHint(inputSelector, targetSelector, emptyMessage) {
    const file = selectedSingleFile(inputSelector);
    const el = $(targetSelector);
    if (!el) return;
    el.textContent = file ? `선택된 파일 1건` : emptyMessage;
  }

  function syncWorkReportSourceSelection() {
    updateGenericFileHint(
      "#workReportSourceInput",
      "#workReportSourceHint",
      "텍스트/HWP와 카톡 캡처 이미지를 함께 넣을 수 있습니다. 텍스트는 원문으로 합치고, 이미지는 분석 참고용으로만 사용합니다."
    );
    renderSelectableFileList("#workReportSourceInput", "#workReportSourceList", "선택된 원문 파일이 없습니다.", "원문 자료");
  }

  function syncWorkReportSampleSelection() {
    updateSingleFileHint("#workReportSampleInput", "#workReportSampleHint", "양식을 맞출 때만 사용합니다. 비워두면 기본 양식으로 생성합니다.");
    renderSelectableFileList("#workReportSampleInput", "#workReportSampleList", "선택된 샘플 파일이 없습니다.", "샘플 파일");
  }

  function syncWorkReportAttachmentSelection() {
    updateGenericFileHint("#workReportFileInput", "#workReportFileHint", "선택된 첨부파일이 없습니다.");
    renderSelectableFileList("#workReportFileInput", "#workReportFileList", "선택된 첨부파일이 없습니다.", "첨부파일");
  }

  function fileSignature(file) {
    return [file.name || "", file.size || 0, file.lastModified || 0, file.type || ""].join("::");
  }

  function mergeFilesIntoInput(inputSelector, incomingFiles, limit = MAX_CHAT_DIGEST_IMAGES) {
    const input = $(inputSelector);
    if (!input || !incomingFiles || !incomingFiles.length || typeof DataTransfer === "undefined") {
      return { added: 0, total: selectedFiles(inputSelector).length };
    }
    const existingFiles = selectedFiles(inputSelector);
    const dt = new DataTransfer();
    const seen = new Set();
    let added = 0;

    for (const file of [...existingFiles, ...incomingFiles]) {
      if (!(file instanceof File)) continue;
      const signature = fileSignature(file);
      if (seen.has(signature)) continue;
      if (dt.items.length >= limit) break;
      seen.add(signature);
      dt.items.add(file);
      if (!existingFiles.some((item) => fileSignature(item) === signature)) {
        added += 1;
      }
    }
    input.files = dt.files;
    return { added, total: dt.files.length };
  }

  function textMeasureContext(font) {
    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    if (!ctx) throw new Error("브라우저에서 이미지 생성을 지원하지 않습니다.");
    ctx.font = font;
    return ctx;
  }

  function splitTextToCanvasLines(ctx, text, maxWidth) {
    const source = String(text || "").replace(/\r\n?/g, "\n");
    const paragraphs = source.split("\n");
    const lines = [];

    for (const paragraph of paragraphs) {
      const raw = String(paragraph || "").trimEnd();
      if (!raw.trim()) {
        lines.push("");
        continue;
      }
      let rest = raw;
      while (rest.length) {
        let low = 1;
        let high = rest.length;
        let best = 1;
        while (low <= high) {
          const mid = Math.floor((low + high) / 2);
          const candidate = rest.slice(0, mid);
          if (ctx.measureText(candidate).width <= maxWidth) {
            best = mid;
            low = mid + 1;
          } else {
            high = mid - 1;
          }
        }
        let cut = best;
        if (cut < rest.length) {
          const candidate = rest.slice(0, cut);
          const lastSpace = Math.max(candidate.lastIndexOf(" "), candidate.lastIndexOf("\t"));
          if (lastSpace >= Math.max(10, cut - 12)) {
            cut = lastSpace;
          }
        }
        const line = rest.slice(0, cut).trimEnd();
        lines.push(line || rest.slice(0, best).trimEnd());
        rest = rest.slice(cut || best).trimStart();
      }
    }
    return lines;
  }

  function chatTextImagePlan(text, maxImages = MAX_CHAT_DIGEST_IMAGES) {
    const normalized = String(text || "").trim();
    if (!normalized) {
      return { pageCount: 0, pages: [], truncated: false };
    }
    const width = 1280;
    const height = 1760;
    const paddingX = 88;
    const paddingTop = 128;
    const paddingBottom = 104;
    const bodyTop = 244;
    const lineHeight = 40;
    const ctx = textMeasureContext('28px "Malgun Gothic", "Apple SD Gothic Neo", sans-serif');
    const lines = splitTextToCanvasLines(ctx, normalized, width - (paddingX * 2));
    const linesPerPage = Math.max(1, Math.floor((height - bodyTop - paddingBottom) / lineHeight));
    const totalPages = Math.max(1, Math.ceil(lines.length / linesPerPage));
    const limitedPages = Math.min(totalPages, maxImages);
    const truncated = totalPages > maxImages;
    const pages = [];
    for (let index = 0; index < limitedPages; index += 1) {
      const start = index * linesPerPage;
      const end = start + linesPerPage;
      pages.push(lines.slice(start, end));
    }
    if (truncated && pages.length) {
      const lastPage = pages[pages.length - 1].slice(0, Math.max(0, linesPerPage - 2));
      lastPage.push("...");
      lastPage.push("[이후 내용은 자동 저장 한도를 넘어 생략되었습니다.]");
      pages[pages.length - 1] = lastPage;
    }
    return {
      width,
      height,
      paddingX,
      paddingTop,
      bodyTop,
      lineHeight,
      pageCount: limitedPages,
      pages,
      truncated,
    };
  }

  async function renderChatTextAsImageFiles(text, maxImages = MAX_CHAT_DIGEST_IMAGES) {
    const plan = chatTextImagePlan(text, maxImages);
    if (!plan.pageCount) return [];
    const createdAt = new Date().toLocaleString("ko-KR", { hour12: false });
    const output = [];

    for (let index = 0; index < plan.pages.length; index += 1) {
      const canvas = document.createElement("canvas");
      canvas.width = plan.width;
      canvas.height = plan.height;
      const ctx = canvas.getContext("2d");
      if (!ctx) throw new Error("브라우저에서 원문 이미지 생성을 지원하지 않습니다.");

      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.strokeStyle = "#d8d0c4";
      ctx.lineWidth = 2;
      ctx.strokeRect(18, 18, canvas.width - 36, canvas.height - 36);

      ctx.fillStyle = "#143735";
      ctx.font = '700 36px "Malgun Gothic", "Apple SD Gothic Neo", sans-serif';
      ctx.fillText("카카오톡 대화 원문 자동저장", plan.paddingX, plan.paddingTop);

      ctx.fillStyle = "#6b736e";
      ctx.font = '20px "Malgun Gothic", "Apple SD Gothic Neo", sans-serif';
      ctx.fillText(`${createdAt} · ${index + 1}/${plan.pageCount}`, plan.paddingX, plan.paddingTop + 40);

      ctx.fillStyle = "#1f2a28";
      ctx.font = '28px "Malgun Gothic", "Apple SD Gothic Neo", sans-serif';
      let y = plan.bodyTop;
      for (const line of plan.pages[index]) {
        ctx.fillText(line || " ", plan.paddingX, y);
        y += plan.lineHeight;
      }

      const blob = await new Promise((resolve, reject) => {
        canvas.toBlob((value) => {
          if (value) {
            resolve(value);
            return;
          }
          reject(new Error("카톡 원문 이미지를 생성하지 못했습니다."));
        }, "image/png");
      });
      output.push(new File([blob], `kakao-chat-text-${String(index + 1).padStart(2, "0")}.png`, { type: "image/png", lastModified: Date.now() }));
    }
    return output;
  }

  function revokeChatSourcePreviewUrls() {
    for (const url of chatSourcePreviewUrls) {
      try {
        URL.revokeObjectURL(url);
      } catch (_) {
        // ignore revoke errors for already released object URLs
      }
    }
    chatSourcePreviewUrls = [];
  }

  function revokeChatDigestPreviewUrls() {
    for (const url of chatDigestPreviewUrls) {
      try {
        URL.revokeObjectURL(url);
      } catch (_) {
        // ignore revoke errors for already released object URLs
      }
    }
    chatDigestPreviewUrls = [];
  }

  function clearChatDigestImagePreview() {
    revokeChatDigestPreviewUrls();
    const box = $("#chatImagePreview");
    if (box) {
      box.innerHTML = "";
      box.classList.add("hidden");
    }
  }

  function renderSelectedChatImages(files) {
    const box = $("#chatImagePreview");
    if (!box) return;
    revokeChatDigestPreviewUrls();
    if (!files.length) {
      box.innerHTML = "";
      box.classList.add("hidden");
      return;
    }
    box.innerHTML = files
      .map((file, index) => {
        const url = URL.createObjectURL(file);
        chatDigestPreviewUrls.push(url);
        return [
          '<article class="chat-source-card">',
          '<div class="chat-source-card-head">',
          `<strong>선택 이미지 ${index + 1}</strong>`,
          `<button class="file-remove-btn" type="button" data-input-selector="#chatImageInput" data-file-index="${index}">삭제</button>`,
          '</div>',
          `<img src="${escapeHtml(url)}" alt="카톡 분석 선택 이미지 ${index + 1}" loading="lazy" />`,
          `<span class="meta">${escapeHtml(file.name || `image-${index + 1}`)}${formatInlineFileSize(file) ? ` · ${escapeHtml(formatInlineFileSize(file))}` : ""}</span>`,
          "</article>",
        ].join("");
      })
      .join("");
    box.classList.remove("hidden");
  }

  function clearChatSourcePreview() {
    revokeChatSourcePreviewUrls();
    const box = $("#chatSourcePreview");
    if (box) {
      box.innerHTML = "";
      box.classList.add("hidden");
    }
    const meta = $("#chatSourcePreviewMeta");
    if (meta) {
      meta.textContent = "원문 이미지 미리보기를 열면 자동 저장될 PNG를 여기서 확인할 수 있습니다.";
    }
  }

  function renderChatSourcePreview(files) {
    const box = $("#chatSourcePreview");
    const meta = $("#chatSourcePreviewMeta");
    if (!box) return;
    revokeChatSourcePreviewUrls();
    if (!files.length) {
      box.innerHTML = "";
      box.classList.add("hidden");
      if (meta) {
        meta.textContent = "원문 이미지가 없습니다.";
      }
      return;
    }
    box.innerHTML = files
      .map((file, index) => {
        const url = URL.createObjectURL(file);
        chatSourcePreviewUrls.push(url);
        return [
          '<article class="chat-source-card">',
          `<strong>원문 PNG ${index + 1}</strong>`,
          `<img src="${escapeHtml(url)}" alt="카톡 원문 자동 저장 이미지 ${index + 1}" loading="lazy" />`,
          `<span>${escapeHtml(file.name)}</span>`,
          "</article>",
        ].join("");
      })
      .join("");
    box.classList.remove("hidden");
    if (meta) {
      meta.textContent = `자동 저장된 원문 이미지 ${files.length}장을 미리보고 있습니다. 다운로드 버튼으로 PNG를 별도 저장할 수 있습니다.`;
    }
  }

  function setChatPasteZoneState(active = false, dragover = false) {
    const zone = $("#chatPasteZone");
    if (!zone) return;
    zone.classList.toggle("active", !!active);
    zone.classList.toggle("dragover", !!dragover);
  }

  function updateChatDigestHint() {
    const text = String($("#chatInput")?.value || "").trim();
    const files = selectedFiles("#chatImageInput");
    const imageHint = $("#chatImageHint");
    const modeHint = $("#chatDigestModeHint");
    renderSelectedChatImages(files);
    if (imageHint) {
      if (files.length) {
        imageHint.textContent = `선택 ${files.length}장 / 최대 ${MAX_WORK_REPORT_IMAGES}장`;
      } else if (text || selectedFiles("#workReportSourceInput").length) {
        imageHint.textContent = "선택된 사진이 없습니다. 사진이 없으면 텍스트 전용 작업 목록으로만 정리됩니다.";
      } else {
        imageHint.textContent = "선택된 사진이 없습니다.";
      }
    }
    if (modeHint) {
      if (files.length) {
        modeHint.textContent = "현재 입력한 원문과 선택한 현장 사진으로 주요업무보고를 생성합니다.";
      } else {
        modeHint.textContent = "카톡 원문이나 원문 파일만으로도 보고서는 만들 수 있지만, 사진이 있으면 작업 전/후 구분이 더 정확해집니다.";
      }
    }
  }

  async function resolveChatDigestFiles(text) {
    const explicitFiles = selectedFiles("#chatImageInput");
    if (explicitFiles.length > MAX_WORK_REPORT_IMAGES) {
      throw new Error(`현장 사진은 최대 ${MAX_WORK_REPORT_IMAGES}장까지 업로드할 수 있습니다.`);
    }
    if (explicitFiles.length) {
      return { files: explicitFiles, autogenerated: false };
    }
    const generated = await renderChatTextAsImageFiles(text, MAX_CHAT_DIGEST_IMAGES);
    return { files: generated, autogenerated: generated.length > 0 };
  }

  async function previewChatSourceImages() {
    const text = String($("#chatInput").value || "").trim();
    if (!text) throw new Error("원문 이미지 미리보기는 카톡 대화 원문을 먼저 입력해야 합니다.");
    const files = await renderChatTextAsImageFiles(text, MAX_CHAT_DIGEST_IMAGES);
    if (!files.length) throw new Error("원문 이미지가 생성되지 않았습니다.");
    renderChatSourcePreview(files);
    setMessage("#intakeMsg", `카톡 원문 PNG ${files.length}장 미리보기를 준비했습니다.`);
  }

  function downloadFiles(files) {
    files.forEach((file, index) => {
      window.setTimeout(() => downloadBlob(file, file.name || `download-${index + 1}`), index * 180);
    });
  }

  async function downloadChatSourceImages() {
    const text = String($("#chatInput").value || "").trim();
    if (!text) throw new Error("원문 이미지를 내려받으려면 카톡 대화 원문을 먼저 입력해야 합니다.");
    const files = await renderChatTextAsImageFiles(text, MAX_CHAT_DIGEST_IMAGES);
    if (!files.length) throw new Error("원문 이미지가 생성되지 않았습니다.");
    renderChatSourcePreview(files);
    downloadFiles(files);
    setMessage("#intakeMsg", `카톡 원문 PNG ${files.length}장을 별도 다운로드했습니다.`);
  }

  function extractClipboardImageFiles(event) {
    const clipboardData = event?.clipboardData || event;
    const items = Array.from(clipboardData?.items || []);
    const files = [];
    for (const item of items) {
      if (!String(item.type || "").startsWith("image/")) continue;
      const blob = item.getAsFile();
      if (!blob) continue;
      const ext = String(blob.type || "image/png").split("/")[1] || "png";
      files.push(new File([blob], `kakao-paste-${Date.now()}-${files.length + 1}.${ext}`, { type: blob.type || "image/png", lastModified: Date.now() }));
    }
    if (files.length) {
      return files;
    }
    for (const file of Array.from(clipboardData?.files || [])) {
      if (!(file instanceof File)) continue;
      if (!String(file.type || "").startsWith("image/")) continue;
      files.push(file);
    }
    return files;
  }

  function addChatDigestImages(files, sourceLabel = "카톡 캡처 이미지") {
    if (!files.length) return false;
    const merged = mergeFilesIntoInput("#chatImageInput", files, MAX_WORK_REPORT_IMAGES);
    clearChatSourcePreview();
    updateChatDigestHint();
    if (merged.added < files.length) {
      setMessage("#intakeMsg", `${sourceLabel}는 최대 ${MAX_WORK_REPORT_IMAGES}장까지 저장됩니다.`, true);
      return true;
    }
    setMessage("#intakeMsg", `${sourceLabel} ${merged.added}장을 불러왔습니다.`);
    return true;
  }

  function handleChatInputPaste(event) {
    const pastedImages = extractClipboardImageFiles(event);
    if (!pastedImages.length) return false;
    event.__kaChatPasteHandled = true;
    event.preventDefault();
    return addChatDigestImages(pastedImages, "카톡 캡처 이미지");
  }

  async function readClipboardImageFiles() {
    if (!navigator.clipboard || typeof navigator.clipboard.read !== "function") {
      throw new Error("이 브라우저는 클립보드 이미지 읽기를 지원하지 않습니다. 붙여넣기 영역을 클릭한 뒤 Ctrl+V를 사용해 주세요.");
    }
    const items = await navigator.clipboard.read();
    const files = [];
    for (const item of items) {
      const imageType = item.types.find((type) => String(type || "").startsWith("image/"));
      if (!imageType) continue;
      const blob = await item.getType(imageType);
      const ext = String(blob.type || "image/png").split("/")[1] || "png";
      files.push(new File([blob], `kakao-clipboard-${Date.now()}-${files.length + 1}.${ext}`, { type: blob.type || "image/png", lastModified: Date.now() }));
    }
    return files;
  }

  async function importClipboardImages() {
    const files = await readClipboardImageFiles();
    if (!files.length) {
      throw new Error("클립보드에서 이미지가 발견되지 않았습니다.");
    }
    addChatDigestImages(files, "클립보드 이미지");
  }

  function handleChatImageDrop(event) {
    event.preventDefault();
    setChatPasteZoneState(false, false);
    const files = Array.from(event?.dataTransfer?.files || []).filter((file) => String(file.type || "").startsWith("image/"));
    if (!files.length) {
      setMessage("#intakeMsg", "드롭한 파일 중 이미지가 없습니다.", true);
      return false;
    }
    return addChatDigestImages(files, "드롭 이미지");
  }

  function renderRoleOptions(selector, selected = "") {
    const el = $(selector);
    if (!el) return;
    el.innerHTML = USER_ROLE_OPTIONS
      .map((item) => `<option value="${escapeHtml(item.value)}"${item.value === selected ? " selected" : ""}>${escapeHtml(item.label)}</option>`)
      .join("");
  }

  function roleLabel(user) {
    if (!user) return "-";
    if (user.is_admin) return "최고관리자";
    const matched = USER_ROLE_OPTIONS.find((item) => item.value === String(user.role || ""));
    const base = matched ? matched.label : String(user.role || "일반직원");
    if (user.is_site_admin) return `현장관리자 / ${base}`;
    return base;
  }

  function isPendingApproval(user) {
    return !!user && !user.is_active && String(user.note || "").includes("[self-register]");
  }

  function userStatusLabel(user) {
    if (isPendingApproval(user)) return "승인대기";
    return user && user.is_active ? "활성" : "비활성";
  }

  function renderTenantBadge() {
    const wrap = $("#tenantBadge");
    if (!wrap || !me) return;
    const chips = [];
    chips.push(`<span class="badge">사용자: ${escapeHtml(me.user.name || me.user.login_id)}</span>`);
    if (me.tenant?.name) {
      chips.push(`<span class="badge">현재 테넌트: ${escapeHtml(me.tenant.name)}</span>`);
    } else if (isAdmin()) {
      chips.push('<span class="badge">최고관리자</span>');
    }
    if (canManageUsers() && !isAdmin()) {
      chips.push('<span class="badge">현장관리자 권한</span>');
    }
    wrap.innerHTML = chips.join("");
  }

  function renderBuildInfoStrip() {
    const el = $("#buildInfoStrip");
    if (!el) return;
    if (!buildInfo) {
      el.classList.remove("is-error");
      el.textContent = "서버 빌드 정보를 확인하는 중입니다.";
      return;
    }
    if (buildInfo.error) {
      el.classList.add("is-error");
      el.innerHTML = [
        "<strong>빌드 확인 실패</strong>",
        `<span>${escapeHtml(buildInfo.error)}</span>`,
        '<a href="/diag/build" target="_blank" rel="noopener">진단 페이지</a>',
      ].join("");
      return;
    }
    const releaseId = String(buildInfo.release_id || "-").trim() || "-";
    const commit = String(buildInfo.git_commit_short || buildInfo.git_commit || "-").trim() || "-";
    const assetVersion = String(buildInfo?.static_assets?.pwa_asset_version || "-").trim() || "-";
    const startedAt = String(buildInfo.started_at_utc || "").trim();
    el.classList.remove("is-error");
    el.innerHTML = [
      "<strong>현재 서버 빌드</strong>",
      `<span>release ${escapeHtml(releaseId)}</span>`,
      `<span>commit ${escapeHtml(commit)}</span>`,
      `<span>asset ${escapeHtml(assetVersion)}</span>`,
      startedAt ? `<span>started ${escapeHtml(formatDateTime(startedAt))}</span>` : "",
      '<a href="/diag/build" target="_blank" rel="noopener">진단 페이지</a>',
    ].join("");
  }

  async function loadBuildInfo() {
    try {
      buildInfo = await api("/api/build_info", { noAuth: true });
    } catch (error) {
      buildInfo = { error: error?.message || String(error) };
    }
    renderBuildInfoStrip();
    return buildInfo;
  }

  function applyHero() {
    renderTenantBadge();
    const role = isAdmin() ? "최고관리자" : (me?.user?.is_site_admin ? "현장관리자" : (me?.user?.role || "staff"));
    const tenantLabel = me?.tenant?.name || me?.user?.tenant_id || "선택 필요";
    $("#heroLine").textContent = `${role} 계정으로 접속 중입니다. 현재 작업 테넌트는 ${tenantLabel}입니다. 민원 접수와 함께 공지, 문서, 일정, 업체 관리까지 한 화면에서 운영할 수 있습니다.`;
    renderBuildInfoStrip();
  }

  function renderAiSuggestion(result) {
    const box = $("#aiSuggestion");
    if (!box) return;
    if (!result) {
      box.textContent = "민원내용을 입력하고 AI 자동분류를 실행하세요.";
      updateIntakeReview();
      return;
    }
    box.innerHTML = [
      `<strong>유형:</strong> ${escapeHtml(result.type)}`,
      `<strong>긴급도:</strong> ${escapeHtml(result.urgency)}`,
      `<strong>요약:</strong> ${escapeHtml(result.summary)}`,
      `<strong>모델:</strong> ${escapeHtml(result.model || "-")}`,
    ].join("<br>");
    updateIntakeReview();
  }

  function digestRowKey(row) {
    return [
      String(row?.building || "").trim(),
      String(row?.unit || "").trim(),
      String(row?.type || "").trim(),
      String(row?.summary || "").trim(),
      String(row?.content || "").trim(),
    ].join("||");
  }

  function selectedDigestRows() {
    const rows = Array.isArray(lastDigestResult?.excel_rows) ? lastDigestResult.excel_rows : [];
    if (!rows.length) return [];
    return rows.filter((row) => lastDigestSelectedKeys.has(digestRowKey(row)));
  }

  function resetDigestImportState() {
    lastDigestResult = null;
    lastDigestImported = false;
    lastDigestSelectedKeys = new Set();
    const button = $("#btnImportDigestComplaints");
    if (button) {
      button.disabled = true;
      button.textContent = "분석 결과 민원 등록";
    }
    const hint = $("#chatDigestImportHint");
    if (hint) {
      hint.textContent = "카톡 분석 후 첫 항목을 민원 입력폼에 자동 채우고, 추출된 전체 항목은 버튼 한 번으로 민원 등록할 수 있습니다.";
    }
  }

  function updateDigestImportState() {
    const rows = Array.isArray(lastDigestResult?.excel_rows) ? lastDigestResult.excel_rows : [];
    const count = rows.length;
    const selectedCount = rows.filter((row) => lastDigestSelectedKeys.has(digestRowKey(row))).length;
    const button = $("#btnImportDigestComplaints");
    if (button) {
      button.disabled = count === 0 || selectedCount === 0 || lastDigestImported;
      button.textContent = count ? `선택 민원 등록 (${selectedCount}/${count}건)` : "분석 결과 민원 등록";
    }
    const hint = $("#chatDigestImportHint");
    if (!hint) return;
    if (!count) {
      hint.textContent = "분석 결과에서 민원 항목이 추출되면 입력폼 자동채우기와 일괄 등록이 활성화됩니다.";
      return;
    }
    if (lastDigestImported) {
      hint.textContent = `방금 분석 결과 ${count}건을 민원으로 등록했습니다. 다시 분석하면 새 결과로 갱신됩니다.`;
      return;
    }
    if (count === 1) {
      hint.textContent = "분석 결과 1건을 찾았습니다. 민원 입력폼에 자동 채웠고, 바로 저장하거나 선택 민원 등록 버튼을 사용할 수 있습니다.";
      return;
    }
    hint.textContent = `분석 결과 ${count}건을 찾았습니다. 체크된 민원만 등록되며, 여러 건이면 자동 저장하지 않고 선택 등록으로 처리하는 것이 안전합니다.`;
  }

  function consumeDigestRow(createdItem) {
    const rows = Array.isArray(lastDigestResult?.excel_rows) ? lastDigestResult.excel_rows : [];
    if (!rows.length || !createdItem) return;
    const createdBuilding = String(createdItem.building || "").trim();
    const createdUnit = String(createdItem.unit || "").trim();
    const createdType = String(createdItem.type || "").trim();
    const createdSummary = String(createdItem.summary || "").trim();
    const matchIndex = rows.findIndex((row) => (
      String(row.building || "").trim() === createdBuilding &&
      String(row.unit || "").trim() === createdUnit &&
      String(row.type || "").trim() === createdType &&
      String(row.summary || "").trim() === createdSummary
    ));
    if (matchIndex < 0) return;
    const removed = rows.splice(matchIndex, 1)[0];
    if (removed) {
      lastDigestSelectedKeys.delete(digestRowKey(removed));
    }
    lastDigestResult = { ...(lastDigestResult || {}), excel_rows: rows, total: rows.length };
    updateDigestImportState();
  }

  function applyDigestLeadToIntake(item) {
    const rows = Array.isArray(item?.excel_rows) ? item.excel_rows : [];
    if (rows.length !== 1) {
      lastAiResult = null;
      renderAiSuggestion(null);
      return;
    }
    const lead = rows[0] || {};
    $("#buildingInput").value = String(lead.building || "").trim();
    $("#unitInput").value = String(lead.unit || "").trim();
    $("#channelInput").value = "카톡";
    $("#managerInput").value = String(lead.manager || "").trim();
    $("#contentInput").value = String(lead.content || lead.summary || "").trim();
    lastAiResult = {
      type: String(lead.type || "기타").trim() || "기타",
      urgency: String(lead.urgency || "일반").trim() || "일반",
      summary: String(lead.summary || lead.content || "").trim(),
      model: String(item?.image_analysis_model || "kakao-digest").trim() || "kakao-digest",
    };
    renderAiSuggestion(lastAiResult);
    if (isMobileViewport()) {
      setCurrentIntakeStep(4);
    }
  }

  function syncUserTenantDisplay() {
    const el = $("#userTenantDisplay");
    if (el) {
      el.value = currentTenantLabel();
    }
  }

  async function loadTenants() {
    if (!isAdmin()) return [];
    const data = await api("/api/admin/tenants");
    let learningError = "";
    try {
      const learningData = await api("/api/admin/work_report_learning?limit=300");
      adminWorkReportLearning = {
        items: Array.isArray(learningData.items) ? learningData.items : [],
        meta: learningData?.meta && typeof learningData.meta === "object" ? learningData.meta : {},
      };
    } catch (error) {
      learningError = error?.message || String(error);
      adminWorkReportLearning = { items: [], meta: {} };
    }
    tenants = Array.isArray(data.items) ? data.items : [];
    const select = $("#tenantSelect");
    if (select) {
      select.innerHTML = tenants
        .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)} (${escapeHtml(item.id)})</option>`)
        .join("");
      if (tenants.length && !select.value) {
        select.value = String(tenants[0].id);
      }
    }
    syncUserTenantDisplay();
    renderTenantsTable();
    renderAdminWorkReportLearning();
    setMessage("#adminLearningMsg", learningError, !!learningError);
    return tenants;
  }

  function renderTenantsTable() {
    const body = $("#tenantsTableBody");
    if (!body) return;
    body.innerHTML = tenants.map((item) => `
      <tr>
        <td class="mono">${escapeHtml(item.id)}</td>
        <td>${escapeHtml(item.name)}</td>
        <td>${escapeHtml(item.site_code || "")}</td>
        <td>${escapeHtml(item.status)}</td>
        <td>${escapeHtml(formatDateTime(item.last_used_at))}</td>
        <td><button class="ghost-btn tenant-rotate" data-tenant="${escapeHtml(item.id)}" type="button">키 재발급</button></td>
      </tr>
    `).join("");
    body.querySelectorAll(".tenant-rotate").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const tenantId = String(btn.getAttribute("data-tenant") || "").trim();
        const res = await api(`/api/admin/tenants/${encodeURIComponent(tenantId)}/rotate_key`, { method: "POST" });
        $("#apiKeyBox").textContent = `새 API Key (${tenantId}): ${res.item.api_key}`;
        setMessage("#adminMsg", "API Key를 재발급했습니다.");
        await loadTenants();
      });
    });
  }

  function renderAdminWorkReportLearning() {
    const summaryEl = $("#adminLearningSummary");
    const tableBody = $("#adminLearningTableBody");
    const metaEl = $("#adminLearningMeta");
    const snapshot = adminWorkReportLearning && typeof adminWorkReportLearning === "object" ? adminWorkReportLearning : { items: [], meta: {} };
    const items = Array.isArray(snapshot.items) ? snapshot.items : [];
    const meta = snapshot?.meta && typeof snapshot.meta === "object" ? snapshot.meta : {};
    const tenantCount = Number(meta.tenant_count || items.length || 0);
    const limitPerTenant = Number(meta.limit_per_tenant || 300);
    const latestFeedbackAt = String(meta.latest_feedback_at || "").trim();

    if (metaEl) {
      const parts = [`최근 ${formatNumber(limitPerTenant)}건 기준`];
      parts.push(`누적 피드백 ${formatNumber(meta.total_feedback_rows || 0)}건`);
      parts.push(`ready ${formatNumber(meta.deploy_ready_tenants || 0)}/${formatNumber(tenantCount)}`);
      if (latestFeedbackAt) {
        parts.push(`최근 ${formatDateTime(latestFeedbackAt)}`);
      }
      metaEl.textContent = parts.join(" · ");
    }

    if (summaryEl) {
      summaryEl.innerHTML = [
        { label: "누적 피드백", value: formatNumber(meta.total_feedback_rows || 0), note: "전체 저장 건수" },
        { label: "평가 학습셋", value: formatNumber(meta.inspected_learning_dataset_rows || 0), note: `최근 ${formatNumber(limitPerTenant)}건 기준` },
        { label: "피드백 보유 단지", value: formatNumber(meta.tenants_with_feedback || 0), note: `전체 ${formatNumber(tenantCount)}개 단지` },
        { label: "배포 가능 단지", value: formatNumber(meta.deploy_ready_tenants || 0), note: "평가 기준 통과" },
      ].map((card) => (
        `<article class="metric-card"><span>${escapeHtml(card.label)}</span><strong>${escapeHtml(card.value)}</strong><span>${escapeHtml(card.note)}</span></article>`
      )).join("");
    }

    if (!tableBody) return;
    if (!items.length) {
      tableBody.innerHTML = '<tr><td colspan="7">업무보고 학습 데이터가 아직 없습니다.</td></tr>';
      return;
    }
    tableBody.innerHTML = items.map((item) => {
      const summary = item?.summary && typeof item.summary === "object" ? item.summary : {};
      const readiness = item?.readiness && typeof item.readiness === "object" ? item.readiness : {};
      const failedChecks = Array.isArray(readiness.checks)
        ? readiness.checks.filter((check) => !check?.ok).map((check) => WORK_REPORT_LEARNING_CHECK_LABELS[String(check?.name || "")] || String(check?.name || ""))
        : [];
      const ready = !!readiness.ready;
      return `
        <tr>
          <td class="mono">
            ${escapeHtml(item.tenant_id || "-")}
            <span class="learning-table-note">${escapeHtml(item.tenant_name || "-")}${item.site_code ? ` / ${escapeHtml(item.site_code)}` : ""}</span>
          </td>
          <td>
            전체 ${escapeHtml(formatNumber(item.total_feedback_rows || 0))}건
            <span class="learning-table-note">최근 평가 ${escapeHtml(formatNumber(item.inspected_feedback_rows || 0))}건</span>
          </td>
          <td>
            ${escapeHtml(formatNumber(item.inspected_learning_dataset_rows || 0))}행 / few-shot ${escapeHtml(formatNumber(item.few_shot_example_count || 0))}개
          </td>
          <td>
            <span class="learning-status-pill ${ready ? "ready" : "pending"}">${escapeHtml(ready ? "READY" : "대기")}</span>
            <span class="learning-table-note">${escapeHtml(failedChecks.length ? `미달: ${failedChecks.join(", ")}` : "기준 충족")}</span>
          </td>
          <td>
            ${escapeHtml(formatPercent(summary.top1_accuracy || 0))}
            <span class="learning-table-note">top-3 ${escapeHtml(formatPercent(summary.top3_hit_rate || 0))}</span>
          </td>
          <td>
            ${escapeHtml(formatPercent(summary.human_intervention_rate || 0))}
            <span class="learning-table-note">미매칭 오판 ${escapeHtml(formatPercent(summary.unmatched_false_positive_rate || 0))}</span>
          </td>
          <td>${escapeHtml(formatDateTime(item.latest_feedback_at || ""))}</td>
        </tr>
      `;
    }).join("");
  }

  function syncOpsWriteState() {
    const editable = canEditOps();
    document.querySelectorAll("[data-ops-write='1']").forEach((el) => {
      el.disabled = !editable;
    });
    const configurable = canManageDocNumberingConfig();
    document.querySelectorAll("[data-ops-config='1']").forEach((el) => {
      el.disabled = !configurable;
    });
    const hint = $("#opsReadOnlyHint");
    if (!hint) return;
    hint.textContent = editable
      ? "행정업무 편집 권한이 있습니다. 공지, 문서, 일정, 업체를 여기서 직접 관리할 수 있습니다."
      : "현재 계정은 행정업무 조회만 가능합니다. 수정이 필요하면 현장관리자 이상 권한으로 로그인하세요.";
  }

  function syncFacilityWriteState() {
    const editable = canEditFacility();
    document.querySelectorAll("[data-facility-write='1']").forEach((el) => {
      el.disabled = !editable;
    });
    const hint = $("#facilityReadOnlyHint");
    if (!hint) return;
    hint.textContent = editable
      ? "시설운영 편집 권한이 있습니다. 자산, 점검표, 점검기록, 작업지시를 여기서 직접 관리할 수 있습니다."
      : "현재 계정은 시설운영 조회만 가능합니다. 수정이 필요하면 현장관리자 이상 권한으로 로그인하세요.";
  }

  function syncComplaintDeleteOption() {
    const select = $("#detailStatus");
    if (!select) return;
    const option = select.querySelector('option[value="__delete__"]');
    if (!option) return;
    const allowed = canDeleteComplaints();
    option.hidden = !allowed;
    option.disabled = !allowed;
    if (!allowed && select.value === "__delete__") {
      select.value = String((selectedComplaint || {}).status || "접수");
    }
  }

  function renderOpsVendorOptions(selected = "") {
    const el = $("#scheduleVendorId");
    if (!el) return;
    const current = String(selected || "");
    el.innerHTML = [
      '<option value="">업체 미지정</option>',
      ...opsVendors.map((item) => `<option value="${Number(item.id || 0)}"${String(item.id) === current ? " selected" : ""}>${escapeHtml(item.company_name || "-")} / ${escapeHtml(item.service_type || "-")}</option>`),
    ].join("");
  }

  function formatDateTimeLocalInput(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    return raw.replace(" ", "T").slice(0, 16);
  }

  function renderFacilityAssetOptions(selected = "") {
    ["#facilityInspectionAssetId", "#facilityWorkOrderAssetId"].forEach((selector) => {
      const el = $(selector);
      if (!el) return;
      const current = String(selected || el.value || "");
      el.innerHTML = [
        '<option value="">자산 미지정</option>',
        ...facilityAssetCatalog.map((item) => `<option value="${Number(item.id || 0)}"${String(item.id) === current ? " selected" : ""}>${escapeHtml(item.asset_code || "-")} / ${escapeHtml(item.asset_name || "-")}</option>`),
      ].join("");
    });
  }

  function renderFacilityInspectionOptions(selected = "") {
    const el = $("#facilityWorkOrderInspectionId");
    if (!el) return;
    const current = String(selected || el.value || "");
    el.innerHTML = [
      '<option value="">점검 미지정</option>',
      ...facilityInspections.map((item) => `<option value="${Number(item.id || 0)}"${String(item.id) === current ? " selected" : ""}>${escapeHtml(item.title || "-")} / ${escapeHtml(formatDateTime(item.inspected_at))}</option>`),
    ].join("");
  }

  function revokeFacilityAssetPreviewUrls() {
    for (const url of facilityAssetPreviewUrls) {
      try {
        URL.revokeObjectURL(url);
      } catch (_) {
        // ignore revoke errors for already released object URLs
      }
    }
    facilityAssetPreviewUrls = [];
  }

  function clearFacilityAssetImageInputs() {
    if ($("#facilityAssetCameraInput")) $("#facilityAssetCameraInput").value = "";
    if ($("#facilityAssetFileInput")) $("#facilityAssetFileInput").value = "";
  }

  function selectedFacilityAssetRecord() {
    if (!selectedFacilityAssetId) return null;
    return facilityAssets.find((item) => Number(item.id || 0) === selectedFacilityAssetId) || null;
  }

  function nextFacilityAssetPendingImageKey() {
    facilityAssetPendingImageSequence += 1;
    return `asset-pending-${Date.now()}-${facilityAssetPendingImageSequence}`;
  }

  function normalizeFacilityAssetImages(item = null) {
    const images = Array.isArray(item?.images) ? item.images : [];
    if (!images.length) {
      const legacyUrl = String(item?.image_url || "").trim();
      if (!legacyUrl) return [];
      return [{
        id: 0,
        image_url: legacyUrl,
        image_mime_type: String(item?.image_mime_type || "").trim(),
        image_size_bytes: Number(item?.image_size_bytes || 0) || 0,
        is_primary: true,
        sort_order: 0,
      }];
    }
    const hasPrimary = images.some((image) => !!image?.is_primary);
    return [...images]
      .map((image, index) => ({
        id: Number(image?.id || 0),
        image_url: String(image?.image_url || image?.file_url || "").trim(),
        image_mime_type: String(image?.image_mime_type || image?.mime_type || "").trim(),
        image_size_bytes: Number(image?.image_size_bytes || image?.size_bytes || 0) || 0,
        is_primary: hasPrimary ? !!image?.is_primary : index === 0,
        sort_order: Number(image?.sort_order || index) || 0,
      }))
      .filter((image) => !!image.image_url)
      .sort((left, right) => {
        if (!!left.is_primary !== !!right.is_primary) return left.is_primary ? -1 : 1;
        return Number(left.sort_order || 0) - Number(right.sort_order || 0);
      });
  }

  function ensurePendingFacilityAssetPrimary(item = null) {
    if (!pendingFacilityAssetImages.length) return;
    if (pendingFacilityAssetImages.some((image) => image.is_primary)) return;
    if (!normalizeFacilityAssetImages(item || selectedFacilityAssetRecord()).length) {
      pendingFacilityAssetImages[0].is_primary = true;
    }
  }

  function facilityAssetCombinedPreviewItems(item = null) {
    const savedImages = normalizeFacilityAssetImages(item);
    ensurePendingFacilityAssetPrimary(item);
    return [
      ...savedImages.map((image, index) => ({
        ...image,
        source: "saved",
        sort_index: index,
      })),
      ...pendingFacilityAssetImages.map((image, index) => ({
        key: image.key,
        image_url: "",
        image_mime_type: String(image.file?.type || "").trim(),
        image_size_bytes: Number(image.file?.size || 0) || 0,
        is_primary: !!image.is_primary,
        sort_order: index + savedImages.length,
        source: "pending",
        sort_index: index,
        file: image.file,
        file_name: String(image.file?.name || `image-${index + 1}`),
        source_label: String(image.source_label || ""),
      })),
    ].sort((left, right) => {
      if (!!left.is_primary !== !!right.is_primary) return left.is_primary ? -1 : 1;
      if (left.source !== right.source) return left.source === "saved" ? -1 : 1;
      return Number(left.sort_order || left.sort_index || 0) - Number(right.sort_order || right.sort_index || 0);
    });
  }

  function renderFacilityAssetImagePreview(item = null) {
    const preview = $("#facilityAssetImagePreview");
    const hint = $("#facilityAssetImageHint");
    const clearButton = $("#btnFacilityAssetClearImageSelection");
    const cameraButton = $("#btnFacilityAssetCamera");
    const fileButton = $("#btnFacilityAssetFile");
    if (!preview || !hint) return;

    const savedImages = normalizeFacilityAssetImages(item);
    const previewItems = facilityAssetCombinedPreviewItems(item);
    const hasPendingPrimary = pendingFacilityAssetImages.some((image) => image.is_primary);
    const totalCount = savedImages.length + pendingFacilityAssetImages.length;
    const remainingCount = Math.max(0, MAX_FACILITY_ASSET_IMAGES - totalCount);

    revokeFacilityAssetPreviewUrls();
    preview.classList.remove("empty-state");

    if (!previewItems.length) {
      preview.classList.add("empty-state");
      preview.innerHTML = `대표 이미지를 포함해 최대 ${MAX_FACILITY_ASSET_IMAGES}장까지 등록할 수 있습니다.`;
      hint.textContent = "카메라 촬영과 파일 이미지 선택을 섞어서 등록할 수 있습니다.";
      if (clearButton) clearButton.classList.add("hidden");
      if (cameraButton) cameraButton.disabled = false;
      if (fileButton) fileButton.disabled = false;
      return;
    }

    preview.innerHTML = previewItems.map((image, index) => {
      const isPending = image.source === "pending";
      const highlightPrimary = !!image.is_primary && (!hasPendingPrimary || isPending);
      const imageUrl = isPending ? URL.createObjectURL(image.file) : String(image.image_url || "").trim();
      if (isPending) facilityAssetPreviewUrls.push(imageUrl);
      const badgeLabel = image.is_primary
        ? (isPending ? (savedImages.length ? "대표 예정" : "대표 이미지") : (hasPendingPrimary ? "현재 대표" : "대표 이미지"))
        : `이미지 ${index + 1}`;
      const statusLabel = isPending ? "저장 전" : "저장됨";
      const meta = [statusLabel, formatFileSize(image.image_size_bytes || image.file?.size || 0), isPending ? image.source_label : ""]
        .filter(Boolean)
        .join(" · ");
      const actions = isPending
        ? [
            !image.is_primary
              ? `<button class="ghost-btn asset-image-card-btn" type="button" data-pending-primary="${escapeHtml(image.key || "")}">대표로 지정</button>`
              : '<button class="action-btn action-secondary asset-image-card-btn" type="button" disabled>대표 예정</button>',
            `<button class="ghost-btn asset-image-card-btn" type="button" data-pending-remove="${escapeHtml(image.key || "")}">선택 취소</button>`,
          ].join("")
        : [
            !image.is_primary && image.id
              ? `<button class="ghost-btn asset-image-card-btn" type="button" data-image-primary="${Number(image.id || 0)}">대표로 지정</button>`
              : '<button class="action-btn action-secondary asset-image-card-btn" type="button" disabled>대표 이미지</button>',
            image.id
              ? `<button class="ghost-btn asset-image-card-btn" type="button" data-image-delete="${Number(image.id || 0)}">삭제</button>`
              : "",
          ].join("");
      return [
        `<article class="asset-image-card${highlightPrimary ? " is-primary" : ""}">`,
        '<div class="asset-image-card-head">',
        `<span class="asset-image-badge${highlightPrimary ? " is-primary" : ""}">${escapeHtml(badgeLabel)}</span>`,
        `<span class="asset-image-state">${escapeHtml(statusLabel)}</span>`,
        "</div>",
        `<img src="${escapeHtml(imageUrl)}" alt="${escapeHtml(isPending ? image.file_name || "자산 이미지" : item?.asset_name || "자산 이미지")}" loading="lazy" />`,
        `<div class="asset-image-meta">${escapeHtml(meta || "-")}</div>`,
        `<div class="asset-image-card-actions">${actions}</div>`,
        "</article>",
      ].join("");
    }).join("");

    if (pendingFacilityAssetImages.length) {
      hint.textContent = `총 ${totalCount}/${MAX_FACILITY_ASSET_IMAGES}장입니다. 저장 전 이미지 ${pendingFacilityAssetImages.length}장은 자산 등록 또는 선택 수정 시 함께 저장됩니다.`;
    } else {
      hint.textContent = `총 ${totalCount}/${MAX_FACILITY_ASSET_IMAGES}장이 저장되어 있습니다. 카드에서 대표 이미지 지정과 개별 삭제가 가능합니다.`;
    }
    if (remainingCount > 0) {
      hint.textContent += ` ${remainingCount}장 더 추가할 수 있습니다.`;
    }
    if (clearButton) clearButton.classList.toggle("hidden", !pendingFacilityAssetImages.length);
    if (cameraButton) cameraButton.disabled = remainingCount <= 0;
    if (fileButton) fileButton.disabled = remainingCount <= 0;
  }

  function clearPendingFacilityAssetImages(item = null) {
    pendingFacilityAssetImages = [];
    clearFacilityAssetImageInputs();
    renderFacilityAssetImagePreview(item || selectedFacilityAssetRecord());
  }

  function queueFacilityAssetImages(files, sourceLabel = "") {
    const incomingFiles = Array.from(files || []).filter((file) => file instanceof File);
    if (!incomingFiles.length) {
      return { added: 0, duplicates: 0, overflow: 0, total: normalizeFacilityAssetImages(selectedFacilityAssetRecord()).length + pendingFacilityAssetImages.length };
    }
    const savedImages = normalizeFacilityAssetImages(selectedFacilityAssetRecord());
    const availableSlots = Math.max(0, MAX_FACILITY_ASSET_IMAGES - savedImages.length - pendingFacilityAssetImages.length);
    if (availableSlots <= 0) {
      throw new Error(`자산 이미지는 대표 이미지를 포함해 최대 ${MAX_FACILITY_ASSET_IMAGES}장까지 등록할 수 있습니다.`);
    }
    const seen = new Set(pendingFacilityAssetImages.map((image) => image.signature));
    const addedImages = [];
    let duplicates = 0;
    let overflow = 0;

    for (const file of incomingFiles) {
      if (!String(file.type || "").startsWith("image/")) {
        throw new Error("이미지 파일만 선택할 수 있습니다.");
      }
      const signature = fileSignature(file);
      if (seen.has(signature)) {
        duplicates += 1;
        continue;
      }
      if (addedImages.length >= availableSlots) {
        overflow += 1;
        continue;
      }
      seen.add(signature);
      addedImages.push({
        key: nextFacilityAssetPendingImageKey(),
        file,
        signature,
        source_label: sourceLabel || "선택 이미지",
        is_primary: false,
      });
    }

    if (!addedImages.length) {
      throw new Error(duplicates ? "같은 이미지는 한 번만 선택할 수 있습니다." : `자산 이미지는 대표 이미지를 포함해 최대 ${MAX_FACILITY_ASSET_IMAGES}장까지 등록할 수 있습니다.`);
    }

    pendingFacilityAssetImages = [...pendingFacilityAssetImages, ...addedImages];
    ensurePendingFacilityAssetPrimary(selectedFacilityAssetRecord());
    clearFacilityAssetImageInputs();
    renderFacilityAssetImagePreview(selectedFacilityAssetRecord());
    return {
      added: addedImages.length,
      duplicates,
      overflow,
      total: savedImages.length + pendingFacilityAssetImages.length,
    };
  }

  function markPendingFacilityAssetImagePrimary(key) {
    const targetKey = String(key || "");
    if (!targetKey) return;
    pendingFacilityAssetImages = pendingFacilityAssetImages.map((image) => ({
      ...image,
      is_primary: image.key === targetKey,
    }));
    renderFacilityAssetImagePreview(selectedFacilityAssetRecord());
  }

  function removePendingFacilityAssetImage(key) {
    const targetKey = String(key || "");
    if (!targetKey) return;
    pendingFacilityAssetImages = pendingFacilityAssetImages.filter((image) => image.key !== targetKey);
    ensurePendingFacilityAssetPrimary(selectedFacilityAssetRecord());
    renderFacilityAssetImagePreview(selectedFacilityAssetRecord());
  }

  function clearFacilityAssetForm() {
    selectedFacilityAssetId = 0;
    $("#facilityAssetCode").value = "";
    $("#facilityAssetName").value = "";
    $("#facilityAssetCategory").value = "승강기";
    $("#facilityAssetState").value = "운영중";
    $("#facilityAssetLocation").value = "";
    $("#facilityAssetVendor").value = "";
    $("#facilityAssetInstalledOn").value = "";
    $("#facilityAssetCycleDays").value = "30";
    $("#facilityAssetQrId").value = "";
    $("#facilityAssetChecklistKey").value = "";
    $("#facilityAssetNextDate").value = "";
    $("#facilityAssetNote").value = "";
    $("#facilityAssetDetail").textContent = "자산을 선택하거나 새로 등록하세요.";
    clearPendingFacilityAssetImages(null);
  }

  function clearFacilityChecklistForm() {
    selectedFacilityChecklistId = 0;
    $("#facilityChecklistKey").value = "";
    $("#facilityChecklistTitle").value = "";
    $("#facilityChecklistTaskType").value = "";
    $("#facilityChecklistVersion").value = "";
    $("#facilityChecklistState").value = "운영중";
    $("#facilityChecklistSource").value = "manual";
    $("#facilityChecklistItems").value = "";
    $("#facilityChecklistNote").value = "";
    $("#facilityChecklistDetail").textContent = "점검표를 선택하거나 새로 등록하세요.";
  }

  function clearFacilityInspectionForm() {
    selectedFacilityInspectionId = 0;
    $("#facilityInspectionTitle").value = "";
    $("#facilityInspectionAssetId").value = "";
    $("#facilityInspectionChecklistKey").value = "";
    $("#facilityInspectionInspector").value = "";
    $("#facilityInspectionAt").value = "";
    $("#facilityInspectionStatus").value = "정상";
    $("#facilityInspectionNotes").value = "";
    $("#facilityInspectionDetail").textContent = "점검 기록을 선택하거나 새로 등록하세요.";
  }

  function clearFacilityWorkOrderForm() {
    selectedFacilityWorkOrderId = 0;
    $("#facilityWorkOrderTitle").value = "";
    $("#facilityWorkOrderAssetId").value = "";
    $("#facilityWorkOrderInspectionId").value = "";
    $("#facilityWorkOrderCategory").value = "점검후속";
    $("#facilityWorkOrderPriority").value = "보통";
    $("#facilityWorkOrderStatus").value = "접수";
    $("#facilityWorkOrderAssignee").value = "";
    $("#facilityWorkOrderReporter").value = "";
    $("#facilityWorkOrderDueDate").value = "";
    $("#facilityWorkOrderEscalated").checked = false;
    $("#facilityWorkOrderDescription").value = "";
    $("#facilityWorkOrderResolution").value = "";
    $("#facilityWorkOrderDetail").textContent = "작업지시를 선택하거나 새로 등록하세요.";
  }

  function renderFacilityAssetDetail(item, options = {}) {
    const preservePending = !!options.preservePending;
    const nextAssetId = Number(item.id || 0);
    const previousAssetId = Number(selectedFacilityAssetId || 0);
    if (!preservePending && (previousAssetId !== nextAssetId || previousAssetId === 0)) {
      pendingFacilityAssetImages = [];
      clearFacilityAssetImageInputs();
    }
    selectedFacilityAssetId = nextAssetId;
    $("#facilityAssetCode").value = String(item.asset_code || "");
    $("#facilityAssetName").value = String(item.asset_name || "");
    $("#facilityAssetCategory").value = String(item.category || "기타");
    $("#facilityAssetState").value = String(item.lifecycle_state || "운영중");
    $("#facilityAssetLocation").value = String(item.location_name || "");
    $("#facilityAssetVendor").value = String(item.vendor_name || "");
    $("#facilityAssetInstalledOn").value = String(item.installed_on || "");
    $("#facilityAssetCycleDays").value = String(item.inspection_cycle_days || 30);
    $("#facilityAssetQrId").value = String(item.qr_id || "");
    $("#facilityAssetChecklistKey").value = String(item.checklist_key || "");
    $("#facilityAssetNextDate").value = String(item.next_inspection_date || "");
    $("#facilityAssetNote").value = String(item.note || "");
    const images = normalizeFacilityAssetImages(item);
    renderFacilityAssetImagePreview(item);
    $("#facilityAssetDetail").innerHTML = [
      images.length
        ? [
            '<div class="detail-media asset-detail-gallery">',
            ...images.map((image, index) => [
              `<article class="asset-detail-tile${image.is_primary ? " is-primary" : ""}">`,
              `<img class="detail-inline-image" src="${escapeHtml(image.image_url)}" alt="${escapeHtml(item.asset_name || "자산 이미지")}" loading="lazy" />`,
              `<div class="asset-image-meta">${escapeHtml(image.is_primary ? "대표 이미지" : `이미지 ${index + 1}`)}${image.image_size_bytes ? ` · ${escapeHtml(formatFileSize(image.image_size_bytes))}` : ""}</div>`,
              "</article>",
            ].join("")),
            "</div>",
          ].join("")
        : "",
      `<strong>${escapeHtml(item.asset_name || "-")}</strong>`,
      `코드: ${escapeHtml(item.asset_code || "-")}`,
      `분류: ${escapeHtml(item.category || "-")}`,
      `위치: ${escapeHtml(item.location_name || "-")}`,
      `관리업체: ${escapeHtml(item.vendor_name || "-")}`,
      `설치일: ${escapeHtml(formatDate(item.installed_on))}`,
      `점검주기: ${escapeHtml(String(item.inspection_cycle_days || 30))}일`,
      `상태: ${escapeHtml(item.lifecycle_state || "-")}`,
      `최근 점검결과: ${escapeHtml(item.last_result_status || "-")}`,
      `다음 점검일: ${escapeHtml(formatDate(item.next_inspection_date))}`,
    ].filter(Boolean).join("<br>");
  }

  function renderFacilityChecklistDetail(item) {
    selectedFacilityChecklistId = Number(item.id || 0);
    $("#facilityChecklistKey").value = String(item.checklist_key || "");
    $("#facilityChecklistTitle").value = String(item.title || "");
    $("#facilityChecklistTaskType").value = String(item.task_type || "");
    $("#facilityChecklistVersion").value = String(item.version_no || "");
    $("#facilityChecklistState").value = String(item.lifecycle_state || "운영중");
    $("#facilityChecklistSource").value = String(item.source || "manual");
    $("#facilityChecklistItems").value = Array.isArray(item.items) ? item.items.join("\n") : "";
    $("#facilityChecklistNote").value = String(item.note || "");
    $("#facilityChecklistDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `Key: ${escapeHtml(item.checklist_key || "-")}`,
      `유형: ${escapeHtml(item.task_type || "-")}`,
      `상태: ${escapeHtml(item.lifecycle_state || "-")}`,
      `항목수: ${escapeHtml(String((item.items || []).length || 0))}`,
    ].join("<br>");
  }

  function renderFacilityInspectionDetail(item) {
    selectedFacilityInspectionId = Number(item.id || 0);
    $("#facilityInspectionTitle").value = String(item.title || "");
    $("#facilityInspectionAssetId").value = String(item.asset_id || "");
    $("#facilityInspectionChecklistKey").value = String(item.checklist_key || "");
    $("#facilityInspectionInspector").value = String(item.inspector || "");
    $("#facilityInspectionAt").value = formatDateTimeLocalInput(item.inspected_at);
    $("#facilityInspectionStatus").value = String(item.result_status || "정상");
    $("#facilityInspectionNotes").value = String(item.notes || "");
    $("#facilityInspectionDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `자산: ${escapeHtml(item.asset_name || "-")}`,
      `결과: ${escapeHtml(item.result_status || "-")}`,
      `점검일시: ${escapeHtml(formatDateTime(item.inspected_at))}`,
      `점검자: ${escapeHtml(item.inspector || "-")}`,
      item.result_status && item.result_status !== "정상" ? "안내: 후속 작업지시 생성을 권장합니다." : "",
    ].join("<br>");
  }

  function renderFacilityWorkOrderDetail(item) {
    selectedFacilityWorkOrderId = Number(item.id || 0);
    $("#facilityWorkOrderTitle").value = String(item.title || "");
    $("#facilityWorkOrderAssetId").value = String(item.asset_id || "");
    $("#facilityWorkOrderInspectionId").value = String(item.inspection_id || "");
    $("#facilityWorkOrderCategory").value = String(item.category || "기타");
    $("#facilityWorkOrderPriority").value = String(item.priority || "보통");
    $("#facilityWorkOrderStatus").value = String(item.status || "접수");
    $("#facilityWorkOrderAssignee").value = String(item.assignee || "");
    $("#facilityWorkOrderReporter").value = String(item.reporter || "");
    $("#facilityWorkOrderDueDate").value = String(item.due_date || "");
    $("#facilityWorkOrderEscalated").checked = !!item.is_escalated;
    $("#facilityWorkOrderDescription").value = String(item.description || "");
    $("#facilityWorkOrderResolution").value = String(item.resolution_notes || "");
    $("#facilityWorkOrderDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `자산: ${escapeHtml(item.asset_name || "-")}`,
      `우선순위: ${escapeHtml(item.priority || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `기한: ${escapeHtml(formatDate(item.due_date))}`,
      `담당: ${escapeHtml(item.assignee || "-")}`,
      `연결 민원: ${item.complaint_id ? `#${escapeHtml(String(item.complaint_id))} / ${escapeHtml(item.complaint_status || "-")} / ${escapeHtml(item.complaint_summary || "-")}` : "-"}`,
    ].join("<br>");
  }

  function facilityAssetPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      asset_code: String($("#facilityAssetCode").value || "").trim(),
      asset_name: String($("#facilityAssetName").value || "").trim(),
      category: String($("#facilityAssetCategory").value || "기타").trim(),
      lifecycle_state: String($("#facilityAssetState").value || "운영중").trim(),
      location_name: String($("#facilityAssetLocation").value || "").trim(),
      vendor_name: String($("#facilityAssetVendor").value || "").trim(),
      installed_on: String($("#facilityAssetInstalledOn").value || "").trim(),
      inspection_cycle_days: Number($("#facilityAssetCycleDays").value || 30),
      qr_id: String($("#facilityAssetQrId").value || "").trim(),
      checklist_key: String($("#facilityAssetChecklistKey").value || "").trim(),
      next_inspection_date: String($("#facilityAssetNextDate").value || "").trim(),
      note: String($("#facilityAssetNote").value || "").trim(),
    };
  }

  async function uploadPendingFacilityAssetImages(assetId) {
    if (!pendingFacilityAssetImages.length) return null;
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    let latestItem = null;
    const uploadQueue = [...pendingFacilityAssetImages].sort((left, right) => {
      if (!!left.is_primary !== !!right.is_primary) return left.is_primary ? -1 : 1;
      return 0;
    });
    for (const image of uploadQueue) {
      const formData = new FormData();
      formData.append("file", image.file, image.file?.name || "asset-image");
      const data = await authFetchJson(
        `/api/facility/assets/${assetId}/images?tenant_id=${encodeURIComponent(tenantId)}&is_primary=${image.is_primary ? "true" : "false"}`,
        {
          method: "POST",
          body: formData,
        },
      );
      latestItem = data.item || latestItem;
      pendingFacilityAssetImages = pendingFacilityAssetImages.filter((pending) => pending.key !== image.key);
      renderFacilityAssetImagePreview(latestItem || selectedFacilityAssetRecord());
    }
    clearFacilityAssetImageInputs();
    return latestItem;
  }

  async function setFacilityAssetPrimaryImage(imageId) {
    if (!selectedFacilityAssetId) throw new Error("대표 이미지를 변경할 자산을 선택하세요.");
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const data = await api(`/api/facility/assets/${selectedFacilityAssetId}/images/${Number(imageId || 0)}/primary?tenant_id=${encodeURIComponent(tenantId)}`, {
      method: "PATCH",
    });
    renderFacilityAssetDetail(data.item || {}, { preservePending: true });
    setMessage("#facilityAssetMsg", "대표 이미지를 변경했습니다.");
    await loadFacilityAssets();
    await loadFacilityDashboard();
  }

  async function deleteFacilityAssetImage(imageId) {
    if (!selectedFacilityAssetId) throw new Error("이미지를 삭제할 자산을 선택하세요.");
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const data = await api(`/api/facility/assets/${selectedFacilityAssetId}/images/${Number(imageId || 0)}?tenant_id=${encodeURIComponent(tenantId)}`, {
      method: "DELETE",
    });
    renderFacilityAssetDetail(data.item || {}, { preservePending: true });
    setMessage("#facilityAssetMsg", "저장된 이미지를 삭제했습니다.");
    await loadFacilityAssets();
    await loadFacilityDashboard();
  }

  async function handleFacilityAssetPreviewAction(event) {
    const button = event.target.closest("button");
    if (!button) return;
    const pendingPrimaryKey = String(button.getAttribute("data-pending-primary") || "").trim();
    if (pendingPrimaryKey) {
      markPendingFacilityAssetImagePrimary(pendingPrimaryKey);
      setMessage("#facilityAssetMsg", "선택한 이미지를 대표 이미지로 지정했습니다. 저장 시 반영됩니다.");
      return;
    }
    const pendingRemoveKey = String(button.getAttribute("data-pending-remove") || "").trim();
    if (pendingRemoveKey) {
      removePendingFacilityAssetImage(pendingRemoveKey);
      setMessage("#facilityAssetMsg", "선택한 저장 전 이미지를 제거했습니다.");
      return;
    }
    const imagePrimaryId = Number(button.getAttribute("data-image-primary") || 0);
    if (imagePrimaryId) {
      await setFacilityAssetPrimaryImage(imagePrimaryId);
      return;
    }
    const imageDeleteId = Number(button.getAttribute("data-image-delete") || 0);
    if (imageDeleteId) {
      await deleteFacilityAssetImage(imageDeleteId);
    }
  }

  function facilityChecklistPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      checklist_key: String($("#facilityChecklistKey").value || "").trim(),
      title: String($("#facilityChecklistTitle").value || "").trim(),
      task_type: String($("#facilityChecklistTaskType").value || "").trim(),
      version_no: String($("#facilityChecklistVersion").value || "").trim(),
      lifecycle_state: String($("#facilityChecklistState").value || "운영중").trim(),
      source: String($("#facilityChecklistSource").value || "manual").trim(),
      items: String($("#facilityChecklistItems").value || "").trim(),
      note: String($("#facilityChecklistNote").value || "").trim(),
    };
  }

  function facilityInspectionPayloadFromForm() {
    const inspectedAt = String($("#facilityInspectionAt").value || "").trim();
    return {
      tenant_id: currentTenantId(),
      title: String($("#facilityInspectionTitle").value || "").trim(),
      asset_id: String($("#facilityInspectionAssetId").value || "").trim(),
      checklist_key: String($("#facilityInspectionChecklistKey").value || "").trim(),
      inspector: String($("#facilityInspectionInspector").value || "").trim(),
      inspected_at: inspectedAt ? inspectedAt.replace("T", " ") : "",
      result_status: String($("#facilityInspectionStatus").value || "정상").trim(),
      notes: String($("#facilityInspectionNotes").value || "").trim(),
    };
  }

  function facilityWorkOrderPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      title: String($("#facilityWorkOrderTitle").value || "").trim(),
      asset_id: String($("#facilityWorkOrderAssetId").value || "").trim(),
      inspection_id: String($("#facilityWorkOrderInspectionId").value || "").trim(),
      category: String($("#facilityWorkOrderCategory").value || "기타").trim(),
      priority: String($("#facilityWorkOrderPriority").value || "보통").trim(),
      status: String($("#facilityWorkOrderStatus").value || "접수").trim(),
      assignee: String($("#facilityWorkOrderAssignee").value || "").trim(),
      reporter: String($("#facilityWorkOrderReporter").value || "").trim(),
      due_date: String($("#facilityWorkOrderDueDate").value || "").trim(),
      is_escalated: !!$("#facilityWorkOrderEscalated").checked,
      description: String($("#facilityWorkOrderDescription").value || "").trim(),
      resolution_notes: String($("#facilityWorkOrderResolution").value || "").trim(),
    };
  }

  function facilityAssetListFilters() {
    return {
      query: String($("#facilityAssetSearchQuery")?.value || "").trim(),
      category: String($("#facilityAssetFilterCategory")?.value || "").trim(),
      lifecycle_state: String($("#facilityAssetFilterState")?.value || "").trim(),
    };
  }

  function updateFacilityAssetListSummary(total) {
    const target = $("#facilityAssetListSummary");
    if (!target) return;
    const filters = facilityAssetListFilters();
    const labels = [];
    if (filters.query) labels.push(`검색어: ${filters.query}`);
    if (filters.category) labels.push(`분류: ${filters.category}`);
    if (filters.lifecycle_state) labels.push(`상태: ${filters.lifecycle_state}`);
    if (!labels.length) {
      target.textContent = total ? `전체 자산 ${total}건` : "등록된 자산이 없습니다.";
      return;
    }
    target.textContent = total
      ? `${labels.join(" / ")} · ${total}건`
      : `${labels.join(" / ")} · 검색 결과가 없습니다.`;
  }

  async function resetFacilityAssetFilters() {
    if ($("#facilityAssetSearchQuery")) $("#facilityAssetSearchQuery").value = "";
    if ($("#facilityAssetFilterCategory")) $("#facilityAssetFilterCategory").value = "";
    if ($("#facilityAssetFilterState")) $("#facilityAssetFilterState").value = "";
    return loadFacilityAssets();
  }

  async function loadFacilityDashboard() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/facility/dashboard?tenant_id=${encodeURIComponent(tenantId)}`);
    const item = data.item || {};
    $("#facilityMetricAssets").textContent = String(item.active_assets || 0);
    $("#facilityMetricQr").textContent = String(item.active_qr_assets || 0);
    $("#facilityMetricWorkOrders").textContent = String(item.open_work_orders || 0);
    $("#facilityMetricInspections").textContent = String(item.month_inspections || 0);
    $("#facilityDueAssets").innerHTML = (item.due_assets || []).length
      ? item.due_assets.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.asset_name || "-")}</strong><p>${escapeHtml(row.category || "-")} / ${escapeHtml(row.location_name || "-")} / ${escapeHtml(formatDate(row.next_inspection_date))}</p></article>`).join("")
      : '<div class="empty-state">다가오는 점검 자산이 없습니다.</div>';
    $("#facilityUrgentWorkOrders").innerHTML = (item.urgent_work_orders || []).length
      ? item.urgent_work_orders.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.priority || "-")} / ${escapeHtml(row.status || "-")} / ${escapeHtml(row.asset_name || "-")}</p></article>`).join("")
      : '<div class="empty-state">긴급 작업지시가 없습니다.</div>';
    $("#facilityRecentInspections").innerHTML = (item.recent_inspections || []).length
      ? item.recent_inspections.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.result_status || "-")} / ${escapeHtml(formatDateTime(row.inspected_at))} / ${escapeHtml(row.inspector || "-")}</p></article>`).join("")
      : '<div class="empty-state">최근 점검 기록이 없습니다.</div>';
    $("#facilityRecentChecklists").innerHTML = (item.recent_checklists || []).length
      ? item.recent_checklists.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.checklist_key || "-")} / ${escapeHtml(row.task_type || "-")} / ${escapeHtml(row.lifecycle_state || "-")}</p></article>`).join("")
      : '<div class="empty-state">등록된 점검표가 없습니다.</div>';
  }

  async function loadFacilityAssets() {
    const tenantId = currentTenantId();
    if (!tenantId) {
      facilityAssets = [];
      facilityAssetCatalog = [];
      renderFacilityAssetOptions($("#facilityInspectionAssetId")?.value || $("#facilityWorkOrderAssetId")?.value || "");
      updateFacilityAssetListSummary(0);
      return [];
    }
    const filters = facilityAssetListFilters();
    const hasFilters = !!(filters.query || filters.category || filters.lifecycle_state);
    const params = new URLSearchParams({ tenant_id: tenantId });
    if (filters.query) params.set("query", filters.query);
    if (filters.category) params.set("category", filters.category);
    if (filters.lifecycle_state) params.set("lifecycle_state", filters.lifecycle_state);
    const [data, catalogData] = await Promise.all([
      api(`/api/facility/assets?${params.toString()}`),
      hasFilters ? api(`/api/facility/assets?tenant_id=${encodeURIComponent(tenantId)}`) : Promise.resolve(null),
    ]);
    facilityAssets = Array.isArray(data.items) ? data.items : [];
    facilityAssetCatalog = hasFilters
      ? (Array.isArray(catalogData?.items) ? catalogData.items : [])
      : [...facilityAssets];
    renderFacilityAssetOptions($("#facilityInspectionAssetId")?.value || $("#facilityWorkOrderAssetId")?.value || "");
    updateFacilityAssetListSummary(facilityAssets.length);
    const body = $("#facilityAssetsTableBody");
    body.innerHTML = facilityAssets.length
      ? facilityAssets.map((item) => `<tr class="facility-asset-row" data-id="${Number(item.id || 0)}"><td>${escapeHtml(item.asset_code || "-")}</td><td>${escapeHtml(item.asset_name || "-")}</td><td>${escapeHtml(item.category || "-")}</td><td>${escapeHtml(item.location_name || "-")}</td><td>${escapeHtml(item.lifecycle_state || "-")}</td></tr>`).join("")
      : `<tr><td colspan="5" class="empty-state">${hasFilters ? "검색 결과가 없습니다." : "등록된 자산이 없습니다."}</td></tr>`;
    body.querySelectorAll(".facility-asset-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = facilityAssets.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderFacilityAssetDetail(item);
      });
    });
    if (selectedFacilityAssetId) {
      const found = facilityAssets.find((item) => Number(item.id || 0) === selectedFacilityAssetId);
      if (found) renderFacilityAssetDetail(found); else clearFacilityAssetForm();
    }
    return facilityAssets;
  }

  async function loadFacilityChecklists() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/facility/checklists?tenant_id=${encodeURIComponent(tenantId)}`);
    facilityChecklists = Array.isArray(data.items) ? data.items : [];
    const body = $("#facilityChecklistsTableBody");
    body.innerHTML = facilityChecklists.length
      ? facilityChecklists.map((item) => `<tr class="facility-checklist-row" data-id="${Number(item.id || 0)}"><td>${escapeHtml(item.checklist_key || "-")}</td><td>${escapeHtml(item.title || "-")}</td><td>${escapeHtml(item.task_type || "-")}</td><td>${escapeHtml(item.lifecycle_state || "-")}</td><td>${escapeHtml(String(item.item_count || 0))}</td></tr>`).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 점검표가 없습니다.</td></tr>';
    body.querySelectorAll(".facility-checklist-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = facilityChecklists.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderFacilityChecklistDetail(item);
      });
    });
    if (selectedFacilityChecklistId) {
      const found = facilityChecklists.find((item) => Number(item.id || 0) === selectedFacilityChecklistId);
      if (found) renderFacilityChecklistDetail(found); else clearFacilityChecklistForm();
    }
    return facilityChecklists;
  }

  async function loadFacilityInspections() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/facility/inspections?tenant_id=${encodeURIComponent(tenantId)}`);
    facilityInspections = Array.isArray(data.items) ? data.items : [];
    renderFacilityInspectionOptions($("#facilityWorkOrderInspectionId")?.value || "");
    const body = $("#facilityInspectionsTableBody");
    body.innerHTML = facilityInspections.length
      ? facilityInspections.map((item) => `<tr class="facility-inspection-row" data-id="${Number(item.id || 0)}"><td>${escapeHtml(item.title || "-")}</td><td>${escapeHtml(item.asset_name || "-")}</td><td>${escapeHtml(item.result_status || "-")}</td><td>${escapeHtml(formatDateTime(item.inspected_at))}</td><td>${escapeHtml(item.inspector || "-")}</td></tr>`).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 점검 기록이 없습니다.</td></tr>';
    body.querySelectorAll(".facility-inspection-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = facilityInspections.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderFacilityInspectionDetail(item);
      });
    });
    if (selectedFacilityInspectionId) {
      const found = facilityInspections.find((item) => Number(item.id || 0) === selectedFacilityInspectionId);
      if (found) renderFacilityInspectionDetail(found); else clearFacilityInspectionForm();
    }
    return facilityInspections;
  }

  async function loadFacilityWorkOrders() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/facility/work_orders?tenant_id=${encodeURIComponent(tenantId)}`);
    facilityWorkOrders = Array.isArray(data.items) ? data.items : [];
    const body = $("#facilityWorkOrdersTableBody");
    body.innerHTML = facilityWorkOrders.length
      ? facilityWorkOrders.map((item) => `<tr class="facility-work-order-row" data-id="${Number(item.id || 0)}"><td>${escapeHtml(item.title || "-")}</td><td>${escapeHtml(item.asset_name || "-")}</td><td>${escapeHtml(item.priority || "-")}</td><td>${escapeHtml(item.status || "-")}</td><td>${escapeHtml(formatDate(item.due_date))}</td></tr>`).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 작업지시가 없습니다.</td></tr>';
    body.querySelectorAll(".facility-work-order-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = facilityWorkOrders.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderFacilityWorkOrderDetail(item);
      });
    });
    if (selectedFacilityWorkOrderId) {
      const found = facilityWorkOrders.find((item) => Number(item.id || 0) === selectedFacilityWorkOrderId);
      if (found) renderFacilityWorkOrderDetail(found); else clearFacilityWorkOrderForm();
    }
    return facilityWorkOrders;
  }

  async function createFacilityAsset() {
    const data = await api("/api/facility/assets", { method: "POST", body: JSON.stringify(facilityAssetPayloadFromForm()) });
    let item = data.item || {};
    const hadPendingImages = pendingFacilityAssetImages.length > 0;
    if (item.id && hadPendingImages) {
      try {
        item = (await uploadPendingFacilityAssetImages(item.id)) || item;
      } catch (error) {
        await loadFacilityAssets();
        renderFacilityAssetDetail(selectedFacilityAssetRecord() || item, { preservePending: true });
        await loadFacilityDashboard();
        setMessage("#facilityAssetMsg", `자산은 등록했지만 일부 이미지 저장은 실패했습니다: ${error.message || String(error)}`, true);
        return;
      }
    }
    renderFacilityAssetDetail(item);
    setMessage("#facilityAssetMsg", hadPendingImages ? "자산과 이미지를 등록했습니다." : "자산을 등록했습니다.");
    await loadFacilityAssets();
    await loadFacilityDashboard();
  }

  async function updateFacilityAsset() {
    if (!selectedFacilityAssetId) throw new Error("수정할 자산을 선택하세요.");
    const data = await api(`/api/facility/assets/${selectedFacilityAssetId}`, { method: "PATCH", body: JSON.stringify(facilityAssetPayloadFromForm()) });
    let item = data.item || {};
    const hadPendingImages = pendingFacilityAssetImages.length > 0;
    if (item.id && hadPendingImages) {
      try {
        item = (await uploadPendingFacilityAssetImages(item.id)) || item;
      } catch (error) {
        await loadFacilityAssets();
        renderFacilityAssetDetail(selectedFacilityAssetRecord() || item, { preservePending: true });
        await loadFacilityDashboard();
        setMessage("#facilityAssetMsg", `자산은 수정했지만 일부 이미지 저장은 실패했습니다: ${error.message || String(error)}`, true);
        return;
      }
    }
    renderFacilityAssetDetail(item);
    setMessage("#facilityAssetMsg", hadPendingImages ? "자산과 이미지를 수정했습니다." : "자산을 수정했습니다.");
    await loadFacilityAssets();
    await loadFacilityDashboard();
  }

  async function deleteFacilityAsset() {
    if (!selectedFacilityAssetId) throw new Error("삭제할 자산을 선택하세요.");
    await api(`/api/facility/assets/${selectedFacilityAssetId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearFacilityAssetForm();
    setMessage("#facilityAssetMsg", "자산을 삭제했습니다.");
    await loadFacilityAssets();
    await loadFacilityDashboard();
  }

  async function createFacilityChecklist() {
    const data = await api("/api/facility/checklists", { method: "POST", body: JSON.stringify(facilityChecklistPayloadFromForm()) });
    renderFacilityChecklistDetail(data.item || {});
    setMessage("#facilityChecklistMsg", "점검표를 등록했습니다.");
    await loadFacilityChecklists();
    await loadFacilityDashboard();
  }

  async function updateFacilityChecklist() {
    if (!selectedFacilityChecklistId) throw new Error("수정할 점검표를 선택하세요.");
    const data = await api(`/api/facility/checklists/${selectedFacilityChecklistId}`, { method: "PATCH", body: JSON.stringify(facilityChecklistPayloadFromForm()) });
    renderFacilityChecklistDetail(data.item || {});
    setMessage("#facilityChecklistMsg", "점검표를 수정했습니다.");
    await loadFacilityChecklists();
    await loadFacilityDashboard();
  }

  async function deleteFacilityChecklist() {
    if (!selectedFacilityChecklistId) throw new Error("삭제할 점검표를 선택하세요.");
    await api(`/api/facility/checklists/${selectedFacilityChecklistId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearFacilityChecklistForm();
    setMessage("#facilityChecklistMsg", "점검표를 삭제했습니다.");
    await loadFacilityChecklists();
    await loadFacilityDashboard();
  }

  async function createFacilityInspection() {
    const data = await api("/api/facility/inspections", { method: "POST", body: JSON.stringify(facilityInspectionPayloadFromForm()) });
    renderFacilityInspectionDetail(data.item || {});
    const inspection = data.item || {};
    const hint = inspection.result_status && inspection.result_status !== "정상" ? " 후속 작업지시 생성을 검토하세요." : "";
    setMessage("#facilityInspectionMsg", `점검 기록을 등록했습니다.${hint}`);
    await loadFacilityAssets();
    await loadFacilityInspections();
    await loadFacilityDashboard();
  }

  async function updateFacilityInspection() {
    if (!selectedFacilityInspectionId) throw new Error("수정할 점검 기록을 선택하세요.");
    const data = await api(`/api/facility/inspections/${selectedFacilityInspectionId}`, { method: "PATCH", body: JSON.stringify(facilityInspectionPayloadFromForm()) });
    renderFacilityInspectionDetail(data.item || {});
    setMessage("#facilityInspectionMsg", "점검 기록을 수정했습니다.");
    await loadFacilityAssets();
    await loadFacilityInspections();
    await loadFacilityDashboard();
  }

  async function deleteFacilityInspection() {
    if (!selectedFacilityInspectionId) throw new Error("삭제할 점검 기록을 선택하세요.");
    await api(`/api/facility/inspections/${selectedFacilityInspectionId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearFacilityInspectionForm();
    setMessage("#facilityInspectionMsg", "점검 기록을 삭제했습니다.");
    await loadFacilityInspections();
    await loadFacilityDashboard();
  }

  async function createFacilityWorkOrder() {
    const data = await api("/api/facility/work_orders", { method: "POST", body: JSON.stringify(facilityWorkOrderPayloadFromForm()) });
    renderFacilityWorkOrderDetail(data.item || {});
    setMessage("#facilityWorkOrderMsg", "작업지시를 등록했습니다.");
    await loadFacilityWorkOrders();
    await loadFacilityDashboard();
  }

  async function issueFacilityWorkOrder() {
    if (!selectedFacilityInspectionId) throw new Error("후속 작업지시를 만들 점검 기록을 선택하세요.");
    const data = await api(`/api/facility/inspections/${selectedFacilityInspectionId}/issue_work_order`, {
      method: "POST",
      body: JSON.stringify({ tenant_id: currentTenantId() }),
    });
    renderFacilityWorkOrderDetail(data.item || {});
    setMessage("#facilityWorkOrderMsg", data.created ? "점검 후속 작업지시를 생성했습니다." : "이미 연결된 열린 작업지시가 있어 그 항목을 불러왔습니다.");
    await loadFacilityWorkOrders();
    await loadFacilityDashboard();
  }

  async function updateFacilityWorkOrder() {
    if (!selectedFacilityWorkOrderId) throw new Error("수정할 작업지시를 선택하세요.");
    const data = await api(`/api/facility/work_orders/${selectedFacilityWorkOrderId}`, { method: "PATCH", body: JSON.stringify(facilityWorkOrderPayloadFromForm()) });
    renderFacilityWorkOrderDetail(data.item || {});
    setMessage("#facilityWorkOrderMsg", "작업지시를 수정했습니다.");
    await loadFacilityWorkOrders();
    await loadFacilityDashboard();
  }

  async function deleteFacilityWorkOrder() {
    if (!selectedFacilityWorkOrderId) throw new Error("삭제할 작업지시를 선택하세요.");
    await api(`/api/facility/work_orders/${selectedFacilityWorkOrderId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearFacilityWorkOrderForm();
    setMessage("#facilityWorkOrderMsg", "작업지시를 삭제했습니다.");
    await loadFacilityWorkOrders();
    await loadFacilityDashboard();
  }

  async function createComplaintFromFacilityWorkOrder() {
    if (!selectedFacilityWorkOrderId) throw new Error("민원으로 전환할 작업지시를 선택하세요.");
    const data = await api(`/api/facility/work_orders/${selectedFacilityWorkOrderId}/create_complaint`, {
      method: "POST",
      body: JSON.stringify({ tenant_id: currentTenantId() }),
    });
    renderFacilityWorkOrderDetail(data.work_order || {});
    selectedComplaintId = Number((data.item || {}).id || 0);
    setMessage("#intakeMsg", data.created ? `작업지시 기반 민원 #${selectedComplaintId}를 생성했습니다.` : `이미 연결된 민원 #${selectedComplaintId}를 불러왔습니다.`);
    await loadComplaints();
    if (selectedComplaintId) {
      await loadComplaintDetail();
    }
    await loadDashboard();
  }

  function clearNoticeForm() {
    selectedNoticeId = 0;
    $("#noticeTitle").value = "";
    $("#noticeCategory").value = "공지";
    $("#noticeStatus").value = "published";
    $("#noticePinned").checked = false;
    $("#noticeBody").value = "";
    $("#opsNoticeDetail").textContent = "공지를 선택하거나 새로 등록하세요.";
  }

  function clearDocumentForm() {
    selectedDocumentId = 0;
    $("#documentTitle").value = "";
    $("#documentCategory").value = defaultDocumentCategory();
    $("#documentStatus").value = "작성중";
    $("#documentOwner").value = "";
    $("#documentDueDate").value = "";
    $("#documentRefNo").value = "";
    $("#documentTargetLabel").value = "";
    $("#documentVendorName").value = "";
    $("#documentAmountTotal").value = "";
    $("#documentBasisDate").value = "";
    $("#documentPeriodStart").value = "";
    $("#documentPeriodEnd").value = "";
    $("#documentSummary").value = "";
    if ($("#documentSampleFile")) $("#documentSampleFile").value = "";
    $("#opsDocumentDetail").textContent = "문서를 선택하거나 새로 등록하세요.";
    renderDocumentCategoryGuide($("#documentCategory").value);
  }

  function clearComplaintDetail() {
    selectedComplaintId = 0;
    selectedComplaint = null;
    $("#complaintDetail").textContent = "목록에서 민원을 선택하세요.";
    $("#detailStatus").value = "접수";
    $("#detailManager").value = "";
    $("#detailNote").value = "";
    $("#detailPhotoInput").value = "";
    $("#attachmentSelectAll").checked = false;
    $("#detailAttachments").innerHTML = '<div class="empty-state">첨부 사진이 없습니다.</div>';
    $("#detailHistory").innerHTML = '<div class="empty-state">이력이 없습니다.</div>';
    if ($("#btnUpdateComplaint")) {
      $("#btnUpdateComplaint").textContent = "상태 저장";
    }
    syncComplaintDeleteOption();
  }

  function updateIntakeReview() {
    const review = $("#intakeReview");
    if (!review) return;
    const payload = complaintPayloadFromForm();
    const photoCount = selectedFiles("#photoInput").length;
    const location = [payload.building ? `${payload.building}동` : "", payload.unit ? `${payload.unit}호` : ""].filter(Boolean).join(" ");
    review.innerHTML = [
      `<strong>${escapeHtml((lastAiResult || {}).summary || payload.content || "민원 요약 대기")}</strong>`,
      `위치: ${escapeHtml(location || "미입력")}`,
      `접수채널: ${escapeHtml(payload.channel || "-")}`,
      `연락처: ${escapeHtml(payload.complainant_phone || "미입력")}`,
      `담당자: ${escapeHtml(payload.manager || "미지정")}`,
      `AI 분류: ${escapeHtml((lastAiResult || {}).type || "미분류")} / ${escapeHtml((lastAiResult || {}).urgency || "미분류")}`,
      `사진: ${escapeHtml(String(photoCount))}장`,
      `내용: ${escapeHtml(payload.content || "미입력")}`,
    ].join("<br>");
  }

  function syncMobileIntakeStep() {
    updateIntakeReview();
    const hint = $("#mobileIntakeStepHint");
    const prevButton = $("#btnIntakePrev");
    const nextButton = $("#btnIntakeNext");
    const stepItems = document.querySelectorAll("[data-intake-step]");
    const stepButtons = document.querySelectorAll(".mobile-intake-step");

    if (!isMobileViewport()) {
      stepItems.forEach((el) => el.classList.remove("intake-step-hidden", "is-active-step"));
      stepButtons.forEach((el) => el.classList.remove("is-active"));
      if (hint) hint.textContent = "모바일에서 단계형 입력이 활성화됩니다.";
      if (prevButton) prevButton.disabled = false;
      if (nextButton) {
        nextButton.disabled = false;
        nextButton.textContent = "다음";
      }
      return;
    }

    stepItems.forEach((el) => {
      const step = Number(el.getAttribute("data-intake-step") || 0);
      const active = step === currentIntakeStep;
      el.classList.toggle("is-active-step", active);
      el.classList.toggle("intake-step-hidden", !active);
    });
    stepButtons.forEach((el) => {
      el.classList.toggle("is-active", Number(el.getAttribute("data-step") || 0) === currentIntakeStep);
    });
    if (hint) {
      hint.textContent = MOBILE_INTAKE_STEPS.find((item) => item.step === currentIntakeStep)?.title || "";
    }
    if (prevButton) prevButton.disabled = currentIntakeStep <= 1;
    if (nextButton) {
      nextButton.disabled = currentIntakeStep >= MOBILE_INTAKE_STEPS.length;
      nextButton.textContent = currentIntakeStep >= MOBILE_INTAKE_STEPS.length ? "저장 단계" : "다음";
    }
  }

  function setCurrentIntakeStep(step) {
    currentIntakeStep = Math.max(1, Math.min(MOBILE_INTAKE_STEPS.length, Number(step || 1)));
    syncMobileIntakeStep();
  }

  function moveIntakeStep(delta) {
    const next = currentIntakeStep + Number(delta || 0);
    if (currentIntakeStep === 2 && delta > 0 && !String($("#contentInput")?.value || "").trim()) {
      throw new Error("민원내용을 입력한 뒤 다음 단계로 이동하세요.");
    }
    setCurrentIntakeStep(next);
  }

  function mobileCompactRootSections() {
    return Array.from(document.querySelectorAll("[data-mobile-compact='accordion']"));
  }

  function mobileCompactStacks(root = document) {
    return Array.from(root.querySelectorAll(".detail-stack"))
      .filter((stack) => stack.dataset.mobileStackReady === "1");
  }

  function setMobileCompactStackExpanded(stack, expanded) {
    if (!(stack instanceof HTMLElement)) return;
    const toggle = stack.querySelector(":scope > .mobile-stack-head .mobile-stack-toggle");
    const content = stack.querySelector(":scope > .mobile-stack-content");
    const state = stack.querySelector(":scope > .mobile-stack-head .mobile-stack-toggle-state");
    const shouldCollapse = isMobileViewport() ? !expanded : false;
    stack.classList.toggle("is-collapsed", shouldCollapse);
    stack.dataset.mobileExpanded = shouldCollapse ? "0" : "1";
    if (toggle) toggle.setAttribute("aria-expanded", shouldCollapse ? "false" : "true");
    if (content) content.hidden = shouldCollapse;
    if (state) state.textContent = shouldCollapse ? "펼치기" : "접기";
  }

  function syncMobileCompactStacks() {
    mobileCompactRootSections().forEach((section) => {
      const stacks = mobileCompactStacks(section);
      if (!stacks.length) return;
      if (!isMobileViewport()) {
        stacks.forEach((stack) => setMobileCompactStackExpanded(stack, true));
        return;
      }
      const visibleStacks = stacks.filter((stack) => stack.getClientRects().length > 0);
      const candidateStacks = visibleStacks.length ? visibleStacks : stacks;
      const openStacks = candidateStacks.filter((stack) => stack.dataset.mobileExpanded === "1");
      const activeStack = openStacks[0] || candidateStacks.find((stack) => stack.dataset.mobileDefaultOpen === "1") || candidateStacks[0];
      stacks.forEach((stack) => setMobileCompactStackExpanded(stack, candidateStacks.includes(stack) && stack === activeStack));
    });
  }

  function toggleMobileCompactStack(stack) {
    if (!(stack instanceof HTMLElement) || !isMobileViewport()) return;
    const section = stack.closest("[data-mobile-compact='accordion']");
    if (!section) return;
    const willExpand = stack.dataset.mobileExpanded !== "1";
    mobileCompactStacks(section).forEach((candidate) => setMobileCompactStackExpanded(candidate, willExpand && candidate === stack));
    if (willExpand) {
      const top = stack.getBoundingClientRect().top + window.scrollY - 18;
      window.scrollTo({ top: Math.max(0, top), behavior: "smooth" });
    }
  }

  function setupMobileCompactStacks() {
    mobileCompactRootSections().forEach((section) => {
      const stacks = Array.from(section.querySelectorAll(".detail-stack"))
        .filter((stack) => stack.querySelector(":scope > .subhead") && stack.querySelector(".form-grid"));
      stacks.forEach((stack, index) => {
        if (stack.dataset.mobileStackReady === "1") return;
        const subhead = stack.querySelector(":scope > .subhead");
        if (!(subhead instanceof HTMLElement)) return;
        const title = String(subhead.textContent || "").trim() || `항목 ${index + 1}`;
        const head = document.createElement("div");
        head.className = "mobile-stack-head";
        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "mobile-stack-toggle";
        toggle.setAttribute("aria-expanded", "false");
        toggle.innerHTML = [
          `<span class="subhead">${escapeHtml(title)}</span>`,
          '<span class="mobile-stack-toggle-state">펼치기</span>',
        ].join("");
        head.appendChild(toggle);

        const content = document.createElement("div");
        content.className = "mobile-stack-content";
        let next = subhead.nextSibling;
        while (next) {
          const current = next;
          next = next.nextSibling;
          content.appendChild(current);
        }
        subhead.replaceWith(head);
        stack.appendChild(content);
        stack.dataset.mobileStackReady = "1";
        stack.dataset.mobileDefaultOpen = index === 0 ? "1" : "0";
        Array.from(content.children)
          .filter((child) => child instanceof HTMLElement && child.classList.contains("cta-row"))
          .forEach((row) => row.classList.add("mobile-form-actions"));
        toggle.addEventListener("click", () => toggleMobileCompactStack(stack));
      });
    });
    syncMobileCompactStacks();
  }

  function normalizeMobilePanelGroup(value) {
    return String(value || "").trim();
  }

  function normalizeMobilePanelValue(value) {
    return String(value || "").trim().toLowerCase();
  }

  function mobilePanelGroups() {
    return Array.from(new Set(Array.from(document.querySelectorAll("[data-mobile-panel-group]"))
      .map((el) => normalizeMobilePanelGroup(el.getAttribute("data-mobile-panel-group")))
      .filter(Boolean)));
  }

  function mobilePanelButtons(group) {
    const cleanGroup = normalizeMobilePanelGroup(group);
    if (!cleanGroup) return [];
    return Array.from(document.querySelectorAll(`[data-mobile-panel-group="${cleanGroup}"] [data-mobile-panel-target]`));
  }

  function mobilePanelItems(group) {
    const cleanGroup = normalizeMobilePanelGroup(group);
    if (!cleanGroup) return [];
    return Array.from(document.querySelectorAll(`[data-mobile-panel-item="${cleanGroup}"]`));
  }

  function isAvailableMobilePanelItem(item) {
    return item instanceof HTMLElement && !item.classList.contains("hidden") && !item.hidden;
  }

  function resolveMobilePanelValue(group) {
    const cleanGroup = normalizeMobilePanelGroup(group);
    const preferred = normalizeMobilePanelValue(mobilePanelState[cleanGroup] || MOBILE_PANEL_DEFAULTS[cleanGroup] || "");
    const available = [
      ...mobilePanelItems(cleanGroup)
        .filter((item) => isAvailableMobilePanelItem(item))
        .map((item) => normalizeMobilePanelValue(item.getAttribute("data-mobile-panel-name"))),
      ...mobilePanelButtons(cleanGroup).map((button) => normalizeMobilePanelValue(button.getAttribute("data-mobile-panel-target"))),
    ].filter(Boolean);
    if (available.includes(preferred)) return preferred;
    return available[0] || preferred;
  }

  function applyMobilePanelGroup(group) {
    const cleanGroup = normalizeMobilePanelGroup(group);
    if (!cleanGroup) return;
    const activeValue = resolveMobilePanelValue(cleanGroup);
    mobilePanelState[cleanGroup] = activeValue;
    const buttons = mobilePanelButtons(cleanGroup);
    const items = mobilePanelItems(cleanGroup);
    const availableValues = new Set(items
      .filter((item) => isAvailableMobilePanelItem(item))
      .map((item) => normalizeMobilePanelValue(item.getAttribute("data-mobile-panel-name")))
      .filter(Boolean));
    buttons.forEach((button) => {
      const buttonValue = normalizeMobilePanelValue(button.getAttribute("data-mobile-panel-target"));
      const isAvailable = !availableValues.size || availableValues.has(buttonValue);
      const isActive = isAvailable && buttonValue === activeValue;
      button.classList.toggle("hidden", !isAvailable);
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
    if (!isMobileViewport()) {
      items.forEach((item) => item.classList.remove("mobile-panel-hidden"));
      return;
    }
    items.forEach((item) => {
      if (!isAvailableMobilePanelItem(item)) {
        item.classList.add("mobile-panel-hidden");
        return;
      }
      const itemValue = normalizeMobilePanelValue(item.getAttribute("data-mobile-panel-name"));
      item.classList.toggle("mobile-panel-hidden", itemValue !== activeValue);
    });
  }

  function applyAllMobilePanelGroups() {
    mobilePanelGroups().forEach((group) => applyMobilePanelGroup(group));
  }

  function setMobilePanel(group, value) {
    const cleanGroup = normalizeMobilePanelGroup(group);
    if (!cleanGroup) return;
    mobilePanelState[cleanGroup] = normalizeMobilePanelValue(value);
    applyMobilePanelGroup(cleanGroup);
    syncMobileCompactStacks();
  }

  function wireMobilePanels() {
    document.querySelectorAll("[data-mobile-panel-group] [data-mobile-panel-target]").forEach((button) => {
      if (button.dataset.mobilePanelBound === "1") return;
      button.dataset.mobilePanelBound = "1";
      button.addEventListener("click", () => {
        const group = button.closest("[data-mobile-panel-group]")?.getAttribute("data-mobile-panel-group") || "";
        const value = button.getAttribute("data-mobile-panel-target") || "";
        setMobilePanel(group, value);
      });
    });
    applyAllMobilePanelGroups();
  }

  function mobileWorkspaceSections() {
    return Array.from(document.querySelectorAll("[data-mobile-workspace]"));
  }

  function normalizeMobileWorkspace(value) {
    return String(value || "").trim().toLowerCase();
  }

  function mobileWorkspaceButtons() {
    return Array.from(document.querySelectorAll(".mobile-dock-btn[data-workspace]"));
  }

  function visibleSectionForWorkspace(workspace) {
    const cleanWorkspace = normalizeMobileWorkspace(workspace);
    return mobileWorkspaceSections().find((section) => {
      if (normalizeMobileWorkspace(section.getAttribute("data-mobile-workspace")) !== cleanWorkspace) return false;
      if (section.classList.contains("hidden")) return false;
      return true;
    }) || null;
  }

  function applyMobileWorkspace(scrollToTarget = false) {
    const cleanWorkspace = normalizeMobileWorkspace(currentMobileWorkspace) || "intake";
    if (!isMobileViewport()) {
      document.body.classList.remove("mobile-workspace-mode");
      mobileWorkspaceSections().forEach((section) => section.classList.remove("mobile-workspace-hidden"));
      return;
    }
    document.body.classList.add("mobile-workspace-mode");
    mobileWorkspaceSections().forEach((section) => {
      const sectionWorkspace = normalizeMobileWorkspace(section.getAttribute("data-mobile-workspace"));
      const shouldHide = sectionWorkspace && sectionWorkspace !== cleanWorkspace;
      section.classList.toggle("mobile-workspace-hidden", shouldHide);
    });
    const target = visibleSectionForWorkspace(cleanWorkspace);
    if (scrollToTarget && target) {
      const top = target.getBoundingClientRect().top + window.scrollY - 12;
      window.scrollTo({ top: Math.max(0, top), behavior: "smooth" });
    }
  }

  function setMobileWorkspace(workspace, scrollToTarget = false) {
    const cleanWorkspace = normalizeMobileWorkspace(workspace) || "intake";
    currentMobileWorkspace = cleanWorkspace;
    applyMobileWorkspace(scrollToTarget);
    syncMobileDockState();
    syncMobileCompactStacks();
  }

  function mobileDockSections() {
    return Array.from(document.querySelectorAll(".mobile-dock-btn"))
      .map((button) => ({
        button,
        target: document.querySelector(String(button.getAttribute("data-target") || "")),
        workspace: normalizeMobileWorkspace(button.getAttribute("data-workspace")),
      }))
      .filter((item) => item.target);
  }

  function syncMobileDockState() {
    const sections = mobileDockSections();
    if (!sections.length) return;
    if (isMobileViewport()) {
      const cleanWorkspace = normalizeMobileWorkspace(currentMobileWorkspace) || "intake";
      const activeButton = sections.find((item) => item.workspace === cleanWorkspace) || sections[0];
      sections.forEach((item) => item.button.classList.toggle("is-active", item === activeButton));
      const current = $("#mobileDockCurrent");
      if (current) {
        current.textContent = `현재: ${String(activeButton?.button?.getAttribute("data-label") || activeButton?.button?.textContent || "").trim() || "접수"}`;
      }
      return;
    }
    let activeTarget = sections[0].target;
    const threshold = window.innerHeight * 0.28;
    for (const item of sections) {
      const rect = item.target.getBoundingClientRect();
      if (rect.top <= threshold) {
        activeTarget = item.target;
      }
    }
    let activeButton = sections[0].button;
    sections.forEach((item) => {
      const isActive = item.target === activeTarget;
      item.button.classList.toggle("is-active", isActive);
      if (isActive) activeButton = item.button;
    });
    const current = $("#mobileDockCurrent");
    if (current) {
      current.textContent = `현재: ${String(activeButton?.getAttribute("data-label") || activeButton?.textContent || "").trim() || "접수"}`;
    }
  }

  function renderDocumentCategoryOptions() {
    const categories = documentCategoryValues();
    const select = $("#documentCategory");
    const filter = $("#documentCategoryFilter");
    const currentCategory = String(select?.value || "").trim();
    const currentFilter = String(filter?.value || "").trim();
    if (select) {
      select.innerHTML = categories.map((category) => `<option value="${escapeHtml(category)}">${escapeHtml(category)}</option>`).join("");
      select.value = categories.includes(currentCategory) ? currentCategory : defaultDocumentCategory();
    }
    if (filter) {
      filter.innerHTML = ['<option value="">전체</option>', ...categories.map((category) => `<option value="${escapeHtml(category)}">${escapeHtml(category)}</option>`)].join("");
      filter.value = currentFilter && categories.includes(currentFilter) ? currentFilter : "";
    }
  }

  function renderDocumentCodeInputs() {
    const container = $("#docCategoryCodeGrid");
    if (!container) return;
    container.innerHTML = documentCategoryValues().map((category) => `
      <label class="field">
        <span>${escapeHtml(category)}</span>
        <input type="text" maxlength="8" data-category-code="${escapeHtml(category)}" />
      </label>
    `).join("");
  }

  function renderDocumentCategoryGuide(category) {
    const box = $("#documentCategoryGuide");
    if (!box) return;
    const profile = documentProfileByCategory(category) || {};
    const focusFields = Array.isArray(profile.focus_fields) ? profile.focus_fields : [];
    const commonFields = Array.isArray(documentCatalog?.common_fields) ? documentCatalog.common_fields : [];
    const focusLabels = focusFields
      .map((key) => commonFields.find((item) => String(item.key || "") === key))
      .filter(Boolean)
      .map((item) => String(item.label || "").trim())
      .filter(Boolean);
    box.innerHTML = [
      `<strong>${escapeHtml(profile.category || category || "문서")}</strong>`,
      escapeHtml(profile.description || "문서 유형 안내가 없습니다."),
      profile.amount_policy ? `금액 기준: ${escapeHtml(profile.amount_policy)}` : "",
      `권장 입력: ${escapeHtml((focusLabels.length ? focusLabels : ["제목", "요약/메모"]).join(", "))}`,
      `PDF 제목: ${escapeHtml(profile.pdf_heading || "행정 문서")}`,
    ].filter(Boolean).join("<br>");

    if ($("#documentTitle")) {
      $("#documentTitle").placeholder = String(profile.default_title || "예: 승강기 부품 교체의 건");
    }
    if ($("#documentSummary")) {
      $("#documentSummary").placeholder = String(profile.summary_placeholder || "문서 목적과 진행 메모를 입력하세요.");
    }
  }

  function numberingConfigFromForm() {
    const categoryCodes = {};
    document.querySelectorAll("[data-category-code]").forEach((input) => {
      const category = String(input.getAttribute("data-category-code") || "").trim();
      if (!category) return;
      categoryCodes[category] = String(input.value || "").trim();
    });
    return {
      separator: String($("#docNumberSeparator")?.value || "").trim(),
      date_mode: String($("#docNumberDateMode")?.value || "yyyymmdd").trim(),
      sequence_digits: Number($("#docNumberDigits")?.value || 3),
      category_codes: categoryCodes,
    };
  }

  function applyDocumentNumberingConfig(config) {
    const item = config || {};
    const codes = item.category_codes || {};
    $("#docNumberSeparator").value = String(item.separator || "-");
    $("#docNumberDateMode").value = String(item.date_mode || "yyyymmdd");
    $("#docNumberDigits").value = String(item.sequence_digits || 3);
    document.querySelectorAll("[data-category-code]").forEach((input) => {
      const category = String(input.getAttribute("data-category-code") || "").trim();
      input.value = String(codes[category] || "");
    });
  }

  function renderDocumentNumberingPreview(item) {
    const box = $("#opsDocNumberingPreview");
    if (!box) return;
    const config = item?.config || {};
    const previews = item?.preview_examples || {};
    const dateLabelMap = {
      yyyymmdd: "YYYYMMDD",
      yyyymm: "YYYYMM",
      none: "날짜 없음",
    };
    box.innerHTML = [
      `<strong>현재 규칙</strong>`,
      `구분자: ${escapeHtml(config.separator || "-") || "(없음)"}`,
      `날짜 형식: ${escapeHtml(dateLabelMap[config.date_mode] || config.date_mode || "-")}`,
      `일련번호 자리수: ${escapeHtml(String(config.sequence_digits || 3))}`,
      ``,
      `<strong>다음 번호 미리보기</strong>`,
      ...documentCategoryValues().map((category) => `${escapeHtml(category)}: ${escapeHtml(previews[category] || "-")}`),
    ].join("<br>");
  }

  function clearScheduleForm() {
    selectedScheduleId = 0;
    $("#scheduleTitle").value = "";
    $("#scheduleType").value = "행정";
    $("#scheduleStatus").value = "예정";
    $("#scheduleOwner").value = "";
    $("#scheduleDueDate").value = "";
    $("#scheduleNote").value = "";
    renderOpsVendorOptions("");
    $("#opsScheduleDetail").textContent = "일정을 선택하거나 새로 등록하세요.";
  }

  function clearVendorForm() {
    selectedVendorId = 0;
    $("#vendorCompanyName").value = "";
    $("#vendorServiceType").value = "";
    $("#vendorContactName").value = "";
    $("#vendorPhone").value = "";
    $("#vendorEmail").value = "";
    $("#vendorStatus").value = "활성";
    $("#vendorNote").value = "";
    $("#opsVendorDetail").textContent = "협력업체를 선택하거나 새로 등록하세요.";
  }

  function clearInfoBuildingForm() {
    selectedInfoBuildingId = 0;
    $("#infoBuildingCode").value = "";
    $("#infoBuildingName").value = "";
    $("#infoBuildingUsageType").value = "아파트동";
    $("#infoBuildingStatus").value = "운영중";
    $("#infoBuildingFloorsAbove").value = "";
    $("#infoBuildingFloorsBelow").value = "";
    $("#infoBuildingHouseholdCount").value = "";
    $("#infoBuildingNote").value = "";
    $("#infoBuildingDetail").textContent = "건물정보를 선택하거나 새로 등록하세요.";
    syncSelectableCollection(".info-building-row, .info-building-card", selectedInfoBuildingId);
  }

  function clearInfoRegistrationForm() {
    selectedInfoRegistrationId = 0;
    $("#infoRegistrationType").value = "사업자등록";
    $("#infoRegistrationTitle").value = "";
    $("#infoRegistrationRefNo").value = "";
    $("#infoRegistrationStatus").value = "유효";
    $("#infoRegistrationIssuer").value = "";
    $("#infoRegistrationIssuedOn").value = "";
    $("#infoRegistrationExpiresOn").value = "";
    $("#infoRegistrationNote").value = "";
    $("#infoRegistrationDetail").textContent = "등록정보를 선택하거나 새로 등록하세요.";
    syncSelectableCollection(".info-registration-row, .info-registration-card", selectedInfoRegistrationId);
  }

  function syncSelectableCollection(selector, selectedId) {
    document.querySelectorAll(selector).forEach((el) => {
      const currentId = Number(el.getAttribute("data-id") || 0);
      el.classList.toggle("active", currentId > 0 && currentId === Number(selectedId || 0));
    });
  }

  function renderInfoBuildingCards() {
    const list = $("#infoBuildingsCardList");
    if (!list) return;
    list.innerHTML = infoBuildings.length
      ? infoBuildings.map((item) => `
        <article class="record-card info-building-card" data-id="${Number(item.id || 0)}">
          <div>
            <strong>${escapeHtml(item.building_name || "-")}</strong>
            <p>${escapeHtml(item.building_code || "-")} / ${escapeHtml(item.usage_type || "-")}</p>
          </div>
          <div class="record-card-meta">
            <span>${escapeHtml(item.status || "-")}</span>
            <span>세대 ${escapeHtml(item.household_count == null ? "-" : String(item.household_count))}</span>
          </div>
        </article>
      `).join("")
      : '<div class="empty-state">등록된 건물정보가 없습니다.</div>';
    list.querySelectorAll(".info-building-card").forEach((card) => {
      card.addEventListener("click", () => {
        const item = infoBuildings.find((row) => Number(row.id || 0) === Number(card.getAttribute("data-id") || 0));
        if (item) renderInfoBuildingDetail(item);
      });
    });
    syncSelectableCollection(".info-building-row, .info-building-card", selectedInfoBuildingId);
  }

  function renderInfoRegistrationCards() {
    const list = $("#infoRegistrationsCardList");
    if (!list) return;
    list.innerHTML = infoRegistrations.length
      ? infoRegistrations.map((item) => `
        <article class="record-card info-registration-card" data-id="${Number(item.id || 0)}">
          <div>
            <strong>${escapeHtml(item.title || "-")}</strong>
            <p>${escapeHtml(item.record_type || "-")} / ${escapeHtml(item.reference_no || "-")}</p>
          </div>
          <div class="record-card-meta">
            <span>${escapeHtml(item.status || "-")}</span>
            <span>${escapeHtml(formatDate(item.expires_on))}</span>
          </div>
        </article>
      `).join("")
      : '<div class="empty-state">등록된 등록정보가 없습니다.</div>';
    list.querySelectorAll(".info-registration-card").forEach((card) => {
      card.addEventListener("click", () => {
        const item = infoRegistrations.find((row) => Number(row.id || 0) === Number(card.getAttribute("data-id") || 0));
        if (item) renderInfoRegistrationDetail(item);
      });
    });
    syncSelectableCollection(".info-registration-row, .info-registration-card", selectedInfoRegistrationId);
  }

  function renderInfoBuildingDetail(item) {
    selectedInfoBuildingId = Number(item.id || 0);
    $("#infoBuildingCode").value = String(item.building_code || "");
    $("#infoBuildingName").value = String(item.building_name || "");
    $("#infoBuildingUsageType").value = String(item.usage_type || "아파트동");
    $("#infoBuildingStatus").value = String(item.status || "운영중");
    $("#infoBuildingFloorsAbove").value = item.floors_above == null ? "" : String(item.floors_above);
    $("#infoBuildingFloorsBelow").value = item.floors_below == null ? "" : String(item.floors_below);
    $("#infoBuildingHouseholdCount").value = item.household_count == null ? "" : String(item.household_count);
    $("#infoBuildingNote").value = String(item.note || "");
    $("#infoBuildingDetail").innerHTML = [
      `<strong>${escapeHtml(item.building_name || "-")}</strong>`,
      `건물코드: ${escapeHtml(item.building_code || "-")}`,
      `용도: ${escapeHtml(item.usage_type || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `층수: 지상 ${escapeHtml(item.floors_above == null ? "-" : String(item.floors_above))} / 지하 ${escapeHtml(item.floors_below == null ? "-" : String(item.floors_below))}`,
      `세대/호실 수: ${escapeHtml(item.household_count == null ? "-" : String(item.household_count))}`,
    ].join("<br>");
    syncSelectableCollection(".info-building-row, .info-building-card", selectedInfoBuildingId);
  }

  function renderInfoRegistrationDetail(item) {
    selectedInfoRegistrationId = Number(item.id || 0);
    $("#infoRegistrationType").value = String(item.record_type || "사업자등록");
    $("#infoRegistrationTitle").value = String(item.title || "");
    $("#infoRegistrationRefNo").value = String(item.reference_no || "");
    $("#infoRegistrationStatus").value = String(item.status || "유효");
    $("#infoRegistrationIssuer").value = String(item.issuer_name || "");
    $("#infoRegistrationIssuedOn").value = String(item.issued_on || "");
    $("#infoRegistrationExpiresOn").value = String(item.expires_on || "");
    $("#infoRegistrationNote").value = String(item.note || "");
    $("#infoRegistrationDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `구분: ${escapeHtml(item.record_type || "-")}`,
      `번호: ${escapeHtml(item.reference_no || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `발행처: ${escapeHtml(item.issuer_name || "-")}`,
      `유효기간: ${escapeHtml([formatDate(item.issued_on), formatDate(item.expires_on)].filter((value) => value && value !== "-").join(" ~ ") || "-")}`,
    ].join("<br>");
    syncSelectableCollection(".info-registration-row, .info-registration-card", selectedInfoRegistrationId);
  }

  function infoBuildingPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      building_code: String($("#infoBuildingCode").value || "").trim(),
      building_name: String($("#infoBuildingName").value || "").trim(),
      usage_type: String($("#infoBuildingUsageType").value || "아파트동").trim(),
      status: String($("#infoBuildingStatus").value || "운영중").trim(),
      floors_above: String($("#infoBuildingFloorsAbove").value || "").trim(),
      floors_below: String($("#infoBuildingFloorsBelow").value || "").trim(),
      household_count: String($("#infoBuildingHouseholdCount").value || "").trim(),
      note: String($("#infoBuildingNote").value || "").trim(),
    };
  }

  function infoRegistrationPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      record_type: String($("#infoRegistrationType").value || "사업자등록").trim(),
      title: String($("#infoRegistrationTitle").value || "").trim(),
      reference_no: String($("#infoRegistrationRefNo").value || "").trim(),
      status: String($("#infoRegistrationStatus").value || "유효").trim(),
      issuer_name: String($("#infoRegistrationIssuer").value || "").trim(),
      issued_on: String($("#infoRegistrationIssuedOn").value || "").trim(),
      expires_on: String($("#infoRegistrationExpiresOn").value || "").trim(),
      note: String($("#infoRegistrationNote").value || "").trim(),
    };
  }

  function renderInfoPreviewList(targetSelector, rows, formatRow) {
    const el = $(targetSelector);
    if (!el) return;
    const items = Array.isArray(rows) ? rows : [];
    el.innerHTML = items.length
      ? items.map((row) => formatRow(row)).join("")
      : '<div class="empty-state">등록된 정보가 없습니다.</div>';
  }

  function renderInfoDashboard(item) {
    $("#infoMetricVendors").textContent = String(item.vendor_count || 0);
    $("#infoMetricStaff").textContent = String(item.staff_count || 0);
    $("#infoMetricAssets").textContent = String(item.asset_count || 0);
    $("#infoMetricBuildings").textContent = String(item.building_count || 0);
    $("#infoMetricRegistrations").textContent = String(item.registration_count || 0);
    renderInfoPreviewList("#infoVendorList", item.recent_vendors || [], (row) => `<article class="timeline-item"><strong>${escapeHtml(row.company_name || "-")}</strong><p>${escapeHtml(row.service_type || "-")} / ${escapeHtml(row.contact_name || "-")} / ${escapeHtml(row.phone || "-")}</p></article>`);
    renderInfoPreviewList("#infoStaffList", item.recent_staff || [], (row) => `<article class="timeline-item"><strong>${escapeHtml(row.name || "-")}</strong><p>${escapeHtml(row.role || "-")} / ${escapeHtml(row.login_id || "-")} / ${escapeHtml(row.phone || "-")}</p></article>`);
    renderInfoPreviewList("#infoAssetList", item.recent_assets || [], (row) => `<article class="timeline-item"><strong>${escapeHtml(row.asset_name || "-")}</strong><p>${escapeHtml(row.category || "-")} / ${escapeHtml(row.location_name || "-")} / ${escapeHtml(row.lifecycle_state || "-")}</p></article>`);
  }

  function renderNoticeDetail(item) {
    selectedNoticeId = Number(item.id || 0);
    $("#noticeTitle").value = String(item.title || "");
    $("#noticeCategory").value = String(item.category || "공지");
    $("#noticeStatus").value = String(item.status || "published");
    $("#noticePinned").checked = !!item.pinned;
    $("#noticeBody").value = String(item.body || "");
    $("#opsNoticeDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `분류: ${escapeHtml(item.category || "-")}`,
      `상태: ${escapeHtml(NOTICE_STATUS_LABELS[item.status] || item.status || "-")}`,
      `고정: ${item.pinned ? "예" : "아니오"}`,
      `수정일: ${escapeHtml(formatDateTime(item.updated_at))}`,
      `작성자: ${escapeHtml(item.created_by_label || "-")}`,
    ].join("<br>");
  }

  function renderDocumentDetail(item) {
    selectedDocumentId = Number(item.id || 0);
    $("#documentTitle").value = String(item.title || "");
    $("#documentCategory").value = String(item.category || defaultDocumentCategory());
    $("#documentStatus").value = String(item.status || "작성중");
    $("#documentOwner").value = String(item.owner || "");
    $("#documentDueDate").value = String(item.due_date || "");
    $("#documentRefNo").value = String(item.reference_no || "");
    $("#documentTargetLabel").value = String(item.target_label || "");
    $("#documentVendorName").value = String(item.vendor_name || "");
    $("#documentAmountTotal").value = item.amount_total == null ? "" : String(item.amount_total);
    $("#documentBasisDate").value = String(item.basis_date || "");
    $("#documentPeriodStart").value = String(item.period_start || "");
    $("#documentPeriodEnd").value = String(item.period_end || "");
    $("#documentSummary").value = String(item.summary || "");
    $("#opsDocumentDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `문서번호: ${escapeHtml(item.reference_no || "-")}`,
      `분류: ${escapeHtml(item.category || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `담당: ${escapeHtml(item.owner || "-")}`,
      `기한: ${escapeHtml(formatDate(item.due_date))}`,
      `대상: ${escapeHtml(item.target_label || "-")}`,
      `업체/상대처: ${escapeHtml(item.vendor_name || "-")}`,
      `금액: ${escapeHtml(item.amount_total == null ? "-" : `${new Intl.NumberFormat("ko-KR").format(Number(item.amount_total || 0))}원`)}`,
      `기준일: ${escapeHtml(formatDate(item.basis_date))}`,
      `기간: ${escapeHtml([formatDate(item.period_start), formatDate(item.period_end)].filter((value) => value && value !== "-").join(" ~ ") || "-")}`,
    ].join("<br>");
    renderDocumentCategoryGuide(item.category);
  }

  function selectedDocumentCategoryFilter() {
    return String($("#documentCategoryFilter")?.value || "").trim();
  }

  function documentCategoryOrder(category) {
    const values = documentCategoryValues();
    const index = values.indexOf(String(category || ""));
    return index === -1 ? values.length : index;
  }

  function renderDocumentCategorySummary(counts, selectedCategory = "") {
    const summary = $("#opsDocumentCategorySummary");
    if (!summary) return;
    const rows = Array.isArray(counts) ? counts : [];
    const total = rows.reduce((acc, row) => acc + Number(row.total_count || 0), 0);
    summary.innerHTML = [
      `<button class="summary-chip summary-chip-btn${selectedCategory ? "" : " active"}" type="button" data-category="">전체 ${escapeHtml(String(total))}건</button>`,
      ...documentCategoryValues().map((category) => {
        const matched = rows.find((row) => String(row.category || "") === category);
        const totalCount = Number((matched || {}).total_count || 0);
        const openCount = Number((matched || {}).open_count || 0);
        return `<button class="summary-chip summary-chip-btn${selectedCategory === category ? " active" : ""}" type="button" data-category="${escapeHtml(category)}">${escapeHtml(category)} ${escapeHtml(String(totalCount))}건 / 진행 ${escapeHtml(String(openCount))}</button>`;
      }),
    ].join("");
    summary.querySelectorAll("[data-category]").forEach((button) => {
      button.addEventListener("click", () => {
        const value = String(button.getAttribute("data-category") || "");
        if ($("#documentCategoryFilter")) $("#documentCategoryFilter").value = value;
        loadOpsDocuments().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true));
      });
    });
  }

  function documentLedgerRowHtml(item) {
    return `
      <tr class="ops-document-row" data-id="${Number(item.id || 0)}">
        <td>${escapeHtml(item.title || "-")}</td>
        <td class="mono">${escapeHtml(item.reference_no || "-")}</td>
        <td>${escapeHtml(item.category || "-")}</td>
        <td>${escapeHtml(item.status || "-")}</td>
        <td>${escapeHtml(item.owner || "-")}</td>
        <td>${escapeHtml(formatDate(item.due_date))}</td>
      </tr>
    `;
  }

  function buildDocumentLedgerHtml(items) {
    const rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      return '<tr><td colspan="6" class="empty-state">등록된 문서가 없습니다.</td></tr>';
    }
    const groups = new Map();
    for (const row of rows) {
      const category = String(row.category || "기타");
      if (!groups.has(category)) groups.set(category, []);
      groups.get(category).push(row);
    }
    return Array.from(groups.keys())
      .sort((left, right) => documentCategoryOrder(left) - documentCategoryOrder(right) || left.localeCompare(right, "ko"))
      .map((category) => {
        const groupedRows = groups.get(category) || [];
        return [
          `<tr class="category-group-row"><td colspan="6">${escapeHtml(category)} · ${escapeHtml(String(groupedRows.length))}건</td></tr>`,
          groupedRows.map((item) => documentLedgerRowHtml(item)).join(""),
        ].join("");
      })
      .join("");
  }

  function renderScheduleDetail(item) {
    selectedScheduleId = Number(item.id || 0);
    $("#scheduleTitle").value = String(item.title || "");
    $("#scheduleType").value = String(item.schedule_type || "행정");
    $("#scheduleStatus").value = String(item.status || "예정");
    $("#scheduleOwner").value = String(item.owner || "");
    $("#scheduleDueDate").value = String(item.due_date || "");
    $("#scheduleNote").value = String(item.note || "");
    renderOpsVendorOptions(String(item.vendor_id || ""));
    $("#opsScheduleDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `분류: ${escapeHtml(item.schedule_type || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `예정일: ${escapeHtml(formatDate(item.due_date))}`,
      `담당: ${escapeHtml(item.owner || "-")}`,
      `업체: ${escapeHtml(item.vendor_name || "-")}`,
    ].join("<br>");
  }

  function renderVendorDetail(item) {
    selectedVendorId = Number(item.id || 0);
    $("#vendorCompanyName").value = String(item.company_name || "");
    $("#vendorServiceType").value = String(item.service_type || "");
    $("#vendorContactName").value = String(item.contact_name || "");
    $("#vendorPhone").value = String(item.phone || "");
    $("#vendorEmail").value = String(item.email || "");
    $("#vendorStatus").value = String(item.status || "활성");
    $("#vendorNote").value = String(item.note || "");
    $("#opsVendorDetail").innerHTML = [
      `<strong>${escapeHtml(item.company_name || "-")}</strong>`,
      `분야: ${escapeHtml(item.service_type || "-")}`,
      `담당자: ${escapeHtml(item.contact_name || "-")}`,
      `전화: ${escapeHtml(item.phone || "-")}`,
      `이메일: ${escapeHtml(item.email || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
    ].join("<br>");
  }

  async function loadOpsDashboard() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/ops/dashboard?tenant_id=${encodeURIComponent(tenantId)}`);
    const item = data.item || {};
    $("#opsMetricNotices").textContent = String(item.published_notices || 0);
    $("#opsMetricDocuments").textContent = String(item.open_documents || 0);
    $("#opsMetricSchedules").textContent = String(item.open_schedules || 0);
    $("#opsMetricVendors").textContent = String(item.active_vendors || 0);
    $("#opsRecentNotices").innerHTML = (item.recent_notices || []).length
      ? item.recent_notices.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.category || "-")} / ${escapeHtml(NOTICE_STATUS_LABELS[row.status] || row.status || "-")} / ${escapeHtml(formatDateTime(row.updated_at))}</p></article>`).join("")
      : '<div class="empty-state">등록된 공지가 없습니다.</div>';
    $("#opsOverdueDocuments").innerHTML = (item.overdue_documents || []).length
      ? item.overdue_documents.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.category || "-")} / ${escapeHtml(row.status || "-")} / 기한 ${escapeHtml(formatDate(row.due_date))}</p></article>`).join("")
      : '<div class="empty-state">기한 지연 문서가 없습니다.</div>';
    $("#opsUpcomingSchedules").innerHTML = (item.upcoming_schedules || []).length
      ? item.upcoming_schedules.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.title || "-")}</strong><p>${escapeHtml(row.schedule_type || "-")} / ${escapeHtml(row.status || "-")} / ${escapeHtml(formatDate(row.due_date))} / ${escapeHtml(row.vendor_name || "내부")}</p></article>`).join("")
      : '<div class="empty-state">등록된 일정이 없습니다.</div>';
  }

  async function loadOpsNotices() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/ops/notices?tenant_id=${encodeURIComponent(tenantId)}`);
    opsNotices = Array.isArray(data.items) ? data.items : [];
    const body = $("#opsNoticesTableBody");
    body.innerHTML = opsNotices.length
      ? opsNotices.map((item) => `
        <tr class="ops-notice-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.title || "-")}</td>
          <td>${escapeHtml(item.category || "-")}</td>
          <td>${escapeHtml(NOTICE_STATUS_LABELS[item.status] || item.status || "-")}</td>
          <td>${item.pinned ? "예" : "-"}</td>
          <td>${escapeHtml(formatDateTime(item.updated_at))}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 공지가 없습니다.</td></tr>';
    body.querySelectorAll(".ops-notice-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsNotices.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderNoticeDetail(item);
      });
    });
    if (selectedNoticeId) {
      const found = opsNotices.find((item) => Number(item.id || 0) === selectedNoticeId);
      if (found) renderNoticeDetail(found); else clearNoticeForm();
    }
    return opsNotices;
  }

  async function loadOpsDocuments() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const category = selectedDocumentCategoryFilter();
    const params = new URLSearchParams({ tenant_id: tenantId });
    if (category) params.set("category", category);
    const data = await api(`/api/ops/documents?${params.toString()}`);
    opsDocuments = Array.isArray(data.items) ? data.items : [];
    renderDocumentCategorySummary(data.category_counts || [], category);
    const body = $("#opsDocumentsTableBody");
    body.innerHTML = buildDocumentLedgerHtml(opsDocuments);
    body.querySelectorAll(".ops-document-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsDocuments.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderDocumentDetail(item);
      });
    });
    if (selectedDocumentId) {
      const found = opsDocuments.find((item) => Number(item.id || 0) === selectedDocumentId);
      if (found) renderDocumentDetail(found); else clearDocumentForm();
    }
    return opsDocuments;
  }

  async function loadOpsVendors() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/ops/vendors?tenant_id=${encodeURIComponent(tenantId)}`);
    opsVendors = Array.isArray(data.items) ? data.items : [];
    renderOpsVendorOptions($("#scheduleVendorId")?.value || "");
    const body = $("#opsVendorsTableBody");
    body.innerHTML = opsVendors.length
      ? opsVendors.map((item) => `
        <tr class="ops-vendor-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.company_name || "-")}</td>
          <td>${escapeHtml(item.service_type || "-")}</td>
          <td>${escapeHtml(item.contact_name || "-")}</td>
          <td>${escapeHtml(item.phone || "-")}</td>
          <td>${escapeHtml(item.status || "-")}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 업체가 없습니다.</td></tr>';
    body.querySelectorAll(".ops-vendor-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsVendors.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderVendorDetail(item);
      });
    });
    if (selectedVendorId) {
      const found = opsVendors.find((item) => Number(item.id || 0) === selectedVendorId);
      if (found) renderVendorDetail(found); else clearVendorForm();
    }
    return opsVendors;
  }

  async function loadOpsSchedules() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/ops/schedules?tenant_id=${encodeURIComponent(tenantId)}`);
    opsSchedules = Array.isArray(data.items) ? data.items : [];
    const body = $("#opsSchedulesTableBody");
    body.innerHTML = opsSchedules.length
      ? opsSchedules.map((item) => `
        <tr class="ops-schedule-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.title || "-")}</td>
          <td>${escapeHtml(item.schedule_type || "-")}</td>
          <td>${escapeHtml(item.status || "-")}</td>
          <td>${escapeHtml(formatDate(item.due_date))}</td>
          <td>${escapeHtml(item.vendor_name || "-")}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 일정이 없습니다.</td></tr>';
    body.querySelectorAll(".ops-schedule-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = opsSchedules.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderScheduleDetail(item);
      });
    });
    if (selectedScheduleId) {
      const found = opsSchedules.find((item) => Number(item.id || 0) === selectedScheduleId);
      if (found) renderScheduleDetail(found); else clearScheduleForm();
    }
    return opsSchedules;
  }

  async function loadInfoDashboard() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/info/dashboard?tenant_id=${encodeURIComponent(tenantId)}`);
    renderInfoDashboard(data.item || {});
  }

  async function loadInfoBuildings() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/info/buildings?tenant_id=${encodeURIComponent(tenantId)}`);
    infoBuildings = Array.isArray(data.items) ? data.items : [];
    const body = $("#infoBuildingsTableBody");
    if (!body) return infoBuildings;
    body.innerHTML = infoBuildings.length
      ? infoBuildings.map((item) => `
        <tr class="info-building-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.building_code || "-")}</td>
          <td>${escapeHtml(item.building_name || "-")}</td>
          <td>${escapeHtml(item.usage_type || "-")}</td>
          <td>${escapeHtml(item.status || "-")}</td>
          <td>${escapeHtml(item.household_count == null ? "-" : String(item.household_count))}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 건물정보가 없습니다.</td></tr>';
    body.querySelectorAll(".info-building-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = infoBuildings.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderInfoBuildingDetail(item);
      });
    });
    renderInfoBuildingCards();
    if (selectedInfoBuildingId) {
      const found = infoBuildings.find((item) => Number(item.id || 0) === selectedInfoBuildingId);
      if (found) renderInfoBuildingDetail(found); else clearInfoBuildingForm();
    } else {
      syncSelectableCollection(".info-building-row, .info-building-card", selectedInfoBuildingId);
    }
    return infoBuildings;
  }

  async function loadInfoRegistrations() {
    const tenantId = currentTenantId();
    if (!tenantId) return [];
    const data = await api(`/api/info/registrations?tenant_id=${encodeURIComponent(tenantId)}`);
    infoRegistrations = Array.isArray(data.items) ? data.items : [];
    const body = $("#infoRegistrationsTableBody");
    if (!body) return infoRegistrations;
    body.innerHTML = infoRegistrations.length
      ? infoRegistrations.map((item) => `
        <tr class="info-registration-row" data-id="${Number(item.id || 0)}">
          <td>${escapeHtml(item.record_type || "-")}</td>
          <td>${escapeHtml(item.title || "-")}</td>
          <td>${escapeHtml(item.reference_no || "-")}</td>
          <td>${escapeHtml(item.status || "-")}</td>
          <td>${escapeHtml(formatDate(item.expires_on))}</td>
        </tr>
      `).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 등록정보가 없습니다.</td></tr>';
    body.querySelectorAll(".info-registration-row").forEach((rowEl) => {
      rowEl.addEventListener("click", () => {
        const item = infoRegistrations.find((row) => Number(row.id || 0) === Number(rowEl.getAttribute("data-id") || 0));
        if (item) renderInfoRegistrationDetail(item);
      });
    });
    renderInfoRegistrationCards();
    if (selectedInfoRegistrationId) {
      const found = infoRegistrations.find((item) => Number(item.id || 0) === selectedInfoRegistrationId);
      if (found) renderInfoRegistrationDetail(found); else clearInfoRegistrationForm();
    } else {
      syncSelectableCollection(".info-registration-row, .info-registration-card", selectedInfoRegistrationId);
    }
    return infoRegistrations;
  }

  function noticePayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      title: String($("#noticeTitle").value || "").trim(),
      body: String($("#noticeBody").value || "").trim(),
      category: String($("#noticeCategory").value || "공지").trim(),
      status: String($("#noticeStatus").value || "published").trim(),
      pinned: !!$("#noticePinned").checked,
    };
  }

  function documentPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      title: String($("#documentTitle").value || "").trim(),
      summary: String($("#documentSummary").value || "").trim(),
      category: String($("#documentCategory").value || defaultDocumentCategory()).trim(),
      status: String($("#documentStatus").value || "작성중").trim(),
      owner: String($("#documentOwner").value || "").trim(),
      due_date: String($("#documentDueDate").value || "").trim(),
      reference_no: String($("#documentRefNo").value || "").trim(),
      target_label: String($("#documentTargetLabel").value || "").trim(),
      vendor_name: String($("#documentVendorName").value || "").trim(),
      amount_total: String($("#documentAmountTotal").value || "").trim(),
      basis_date: String($("#documentBasisDate").value || "").trim(),
      period_start: String($("#documentPeriodStart").value || "").trim(),
      period_end: String($("#documentPeriodEnd").value || "").trim(),
    };
  }

  async function loadDocumentCatalog() {
    const tenantId = currentTenantId();
    if (!tenantId) {
      renderDocumentCategoryOptions();
      renderDocumentCodeInputs();
      renderDocumentCategoryGuide(defaultDocumentCategory());
      return documentCatalog;
    }
    const data = await api(`/api/ops/documents/catalog?tenant_id=${encodeURIComponent(tenantId)}`);
    documentCatalog = {
      categories: Array.isArray(data.item?.categories) && data.item.categories.length ? data.item.categories : [...DEFAULT_DOCUMENT_CATEGORY_VALUES],
      profiles: Array.isArray(data.item?.profiles) ? data.item.profiles : [],
      common_fields: Array.isArray(data.item?.common_fields) ? data.item.common_fields : [],
      preview_examples: data.item?.preview_examples || {},
    };
    renderDocumentCategoryOptions();
    renderDocumentCodeInputs();
    renderDocumentCategoryGuide($("#documentCategory")?.value || defaultDocumentCategory());
    return documentCatalog;
  }

  async function loadDocumentNumberingConfig() {
    const tenantId = currentTenantId();
    if (!tenantId) return null;
    const data = await api(`/api/ops/documents/numbering_config?tenant_id=${encodeURIComponent(tenantId)}`);
    documentNumberingConfig = data.item || null;
    applyDocumentNumberingConfig(documentNumberingConfig?.config || {});
    renderDocumentNumberingPreview(documentNumberingConfig);
    return documentNumberingConfig;
  }

  async function saveDocumentNumberingConfig() {
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const data = await api("/api/ops/documents/numbering_config", {
      method: "PATCH",
      body: JSON.stringify({ tenant_id: tenantId, config: numberingConfigFromForm() }),
    });
    documentNumberingConfig = data.item || null;
    applyDocumentNumberingConfig(documentNumberingConfig?.config || {});
    renderDocumentNumberingPreview(documentNumberingConfig);
    setMessage("#opsDocumentMsg", "문서번호 체계 설정을 저장했습니다.");
  }

  async function resetDocumentNumberingConfig() {
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const data = await api("/api/ops/documents/numbering_config", {
      method: "PATCH",
      body: JSON.stringify({ tenant_id: tenantId, reset: true }),
    });
    documentNumberingConfig = data.item || null;
    applyDocumentNumberingConfig(documentNumberingConfig?.config || {});
    renderDocumentNumberingPreview(documentNumberingConfig);
    setMessage("#opsDocumentMsg", "문서번호 체계를 기본값으로 복원했습니다.");
  }

  async function fillNextDocumentReference() {
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const category = String($("#documentCategory").value || "기타").trim() || "기타";
    const params = new URLSearchParams({ tenant_id: tenantId, category });
    const data = await api(`/api/ops/documents/next_reference?${params.toString()}`);
    $("#documentRefNo").value = String(data.item?.reference_no || "");
    setMessage("#opsDocumentMsg", "문서번호를 자동채번했습니다.");
  }

  async function exportDocumentLedgerExcel() {
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const params = new URLSearchParams({ tenant_id: tenantId });
    const category = selectedDocumentCategoryFilter();
    if (category) params.set("category", category);
    const response = await authFetchBlob(`/api/ops/documents/export.xlsx?${params.toString()}`);
    downloadBlob(response.blob, response.filename || `document-ledger-${tenantId || "all"}.xlsx`);
    setMessage("#opsDocumentMsg", `${category || "전체"} 문서관리대장을 엑셀로 내려받았습니다.`);
  }

  async function renderDocumentPdf() {
    const payload = documentPayloadFromForm();
    if (!payload.title) throw new Error("문서 제목을 입력하세요.");
    if (!payload.summary) throw new Error("문서 내용을 입력하세요.");
    const response = await authFetchBlob("/api/ops/documents/render_pdf", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    downloadBlob(response.blob, response.filename || "document.pdf");
    setMessage("#opsDocumentMsg", "문서 PDF를 생성했습니다.");
  }

  async function renderSampleDocumentPdf() {
    const sampleFile = selectedSingleFile("#documentSampleFile");
    if (!sampleFile) throw new Error("샘플 문서를 선택하세요.");
    const fd = new FormData();
    fd.append("tenant_id", currentTenantId());
    fd.append("title", String($("#documentTitle").value || "").trim());
    fd.append("source_file", sampleFile, sampleFile.name || "sample");
    const response = await authFetchBlob("/api/ops/documents/sample_pdf", {
      method: "POST",
      body: fd,
    });
    downloadBlob(response.blob, response.filename || "sample-document.pdf");
    setMessage("#opsDocumentMsg", "샘플 참조 PDF를 생성했습니다.");
  }

  function schedulePayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      title: String($("#scheduleTitle").value || "").trim(),
      schedule_type: String($("#scheduleType").value || "행정").trim(),
      status: String($("#scheduleStatus").value || "예정").trim(),
      due_date: String($("#scheduleDueDate").value || "").trim(),
      owner: String($("#scheduleOwner").value || "").trim(),
      note: String($("#scheduleNote").value || "").trim(),
      vendor_id: String($("#scheduleVendorId").value || "").trim(),
    };
  }

  function vendorPayloadFromForm() {
    return {
      tenant_id: currentTenantId(),
      company_name: String($("#vendorCompanyName").value || "").trim(),
      service_type: String($("#vendorServiceType").value || "").trim(),
      contact_name: String($("#vendorContactName").value || "").trim(),
      phone: String($("#vendorPhone").value || "").trim(),
      email: String($("#vendorEmail").value || "").trim(),
      status: String($("#vendorStatus").value || "활성").trim(),
      note: String($("#vendorNote").value || "").trim(),
    };
  }

  async function createNotice() {
    const data = await api("/api/ops/notices", { method: "POST", body: JSON.stringify(noticePayloadFromForm()) });
    renderNoticeDetail(data.item || {});
    setMessage("#opsNoticeMsg", "공지를 등록했습니다.");
    await loadOpsNotices();
    await loadOpsDashboard();
  }

  async function updateNotice() {
    if (!selectedNoticeId) throw new Error("수정할 공지를 선택하세요.");
    const data = await api(`/api/ops/notices/${selectedNoticeId}`, { method: "PATCH", body: JSON.stringify(noticePayloadFromForm()) });
    renderNoticeDetail(data.item || {});
    setMessage("#opsNoticeMsg", "공지를 수정했습니다.");
    await loadOpsNotices();
    await loadOpsDashboard();
  }

  async function deleteNotice() {
    if (!selectedNoticeId) throw new Error("삭제할 공지를 선택하세요.");
    await api(`/api/ops/notices/${selectedNoticeId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearNoticeForm();
    setMessage("#opsNoticeMsg", "공지를 삭제했습니다.");
    await loadOpsNotices();
    await loadOpsDashboard();
  }

  async function createDocument() {
    const data = await api("/api/ops/documents", { method: "POST", body: JSON.stringify(documentPayloadFromForm()) });
    renderDocumentDetail(data.item || {});
    setMessage("#opsDocumentMsg", "문서를 등록했습니다.");
    await loadOpsDocuments();
    await loadOpsDashboard();
  }

  async function updateDocument() {
    if (!selectedDocumentId) throw new Error("수정할 문서를 선택하세요.");
    const data = await api(`/api/ops/documents/${selectedDocumentId}`, { method: "PATCH", body: JSON.stringify(documentPayloadFromForm()) });
    renderDocumentDetail(data.item || {});
    setMessage("#opsDocumentMsg", "문서를 수정했습니다.");
    await loadOpsDocuments();
    await loadOpsDashboard();
  }

  async function deleteDocument() {
    if (!selectedDocumentId) throw new Error("삭제할 문서를 선택하세요.");
    await api(`/api/ops/documents/${selectedDocumentId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearDocumentForm();
    setMessage("#opsDocumentMsg", "문서를 삭제했습니다.");
    await loadOpsDocuments();
    await loadOpsDashboard();
  }

  async function createVendor() {
    const data = await api("/api/ops/vendors", { method: "POST", body: JSON.stringify(vendorPayloadFromForm()) });
    renderVendorDetail(data.item || {});
    setMessage("#opsVendorMsg", "업체를 등록했습니다.");
    await loadOpsVendors();
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function updateVendor() {
    if (!selectedVendorId) throw new Error("수정할 업체를 선택하세요.");
    const data = await api(`/api/ops/vendors/${selectedVendorId}`, { method: "PATCH", body: JSON.stringify(vendorPayloadFromForm()) });
    renderVendorDetail(data.item || {});
    setMessage("#opsVendorMsg", "업체 정보를 수정했습니다.");
    await loadOpsVendors();
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function deleteVendor() {
    if (!selectedVendorId) throw new Error("삭제할 업체를 선택하세요.");
    await api(`/api/ops/vendors/${selectedVendorId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearVendorForm();
    setMessage("#opsVendorMsg", "업체를 삭제했습니다.");
    await loadOpsVendors();
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function createSchedule() {
    const data = await api("/api/ops/schedules", { method: "POST", body: JSON.stringify(schedulePayloadFromForm()) });
    renderScheduleDetail(data.item || {});
    setMessage("#opsScheduleMsg", "일정을 등록했습니다.");
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function updateSchedule() {
    if (!selectedScheduleId) throw new Error("수정할 일정을 선택하세요.");
    const data = await api(`/api/ops/schedules/${selectedScheduleId}`, { method: "PATCH", body: JSON.stringify(schedulePayloadFromForm()) });
    renderScheduleDetail(data.item || {});
    setMessage("#opsScheduleMsg", "일정을 수정했습니다.");
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function deleteSchedule() {
    if (!selectedScheduleId) throw new Error("삭제할 일정을 선택하세요.");
    await api(`/api/ops/schedules/${selectedScheduleId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearScheduleForm();
    setMessage("#opsScheduleMsg", "일정을 삭제했습니다.");
    await loadOpsSchedules();
    await loadOpsDashboard();
  }

  async function createInfoBuilding() {
    const data = await api("/api/info/buildings", { method: "POST", body: JSON.stringify(infoBuildingPayloadFromForm()) });
    renderInfoBuildingDetail(data.item || {});
    setMessage("#infoBuildingMsg", "건물정보를 등록했습니다.");
    await loadInfoBuildings();
    await loadInfoDashboard();
  }

  async function updateInfoBuilding() {
    if (!selectedInfoBuildingId) throw new Error("수정할 건물정보를 선택하세요.");
    const data = await api(`/api/info/buildings/${selectedInfoBuildingId}`, { method: "PATCH", body: JSON.stringify(infoBuildingPayloadFromForm()) });
    renderInfoBuildingDetail(data.item || {});
    setMessage("#infoBuildingMsg", "건물정보를 수정했습니다.");
    await loadInfoBuildings();
    await loadInfoDashboard();
  }

  async function deleteInfoBuilding() {
    if (!selectedInfoBuildingId) throw new Error("삭제할 건물정보를 선택하세요.");
    await api(`/api/info/buildings/${selectedInfoBuildingId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearInfoBuildingForm();
    setMessage("#infoBuildingMsg", "건물정보를 삭제했습니다.");
    await loadInfoBuildings();
    await loadInfoDashboard();
  }

  async function createInfoRegistration() {
    const data = await api("/api/info/registrations", { method: "POST", body: JSON.stringify(infoRegistrationPayloadFromForm()) });
    renderInfoRegistrationDetail(data.item || {});
    setMessage("#infoRegistrationMsg", "등록정보를 등록했습니다.");
    await loadInfoRegistrations();
    await loadInfoDashboard();
  }

  async function updateInfoRegistration() {
    if (!selectedInfoRegistrationId) throw new Error("수정할 등록정보를 선택하세요.");
    const data = await api(`/api/info/registrations/${selectedInfoRegistrationId}`, { method: "PATCH", body: JSON.stringify(infoRegistrationPayloadFromForm()) });
    renderInfoRegistrationDetail(data.item || {});
    setMessage("#infoRegistrationMsg", "등록정보를 수정했습니다.");
    await loadInfoRegistrations();
    await loadInfoDashboard();
  }

  async function deleteInfoRegistration() {
    if (!selectedInfoRegistrationId) throw new Error("삭제할 등록정보를 선택하세요.");
    await api(`/api/info/registrations/${selectedInfoRegistrationId}`, { method: "DELETE", body: JSON.stringify({ tenant_id: currentTenantId() }) });
    clearInfoRegistrationForm();
    setMessage("#infoRegistrationMsg", "등록정보를 삭제했습니다.");
    await loadInfoRegistrations();
    await loadInfoDashboard();
  }

  async function classifyCurrentText() {
    const payload = complaintPayloadFromForm();
    if (!payload.content) {
      throw new Error("민원내용을 입력하세요.");
    }
    const text = [payload.building ? `${payload.building}동` : "", payload.unit ? `${payload.unit}호` : "", payload.content].filter(Boolean).join(" ");
    const data = await api("/api/ai/classify", {
      method: "POST",
      body: JSON.stringify({ tenant_id: payload.tenant_id, text }),
    });
    lastAiResult = data.item || null;
    renderAiSuggestion(lastAiResult);
    if (isMobileViewport() && currentIntakeStep === 2) {
      setCurrentIntakeStep(3);
    }
    return lastAiResult;
  }

  async function createComplaint() {
    const payload = complaintPayloadFromForm();
    const files = selectedFiles("#photoInput");
    const hadDigestRows = Array.isArray(lastDigestResult?.excel_rows) && lastDigestResult.excel_rows.length > 0;
    if (!payload.tenant_id) throw new Error("테넌트를 선택하세요.");
    if (!payload.content) throw new Error("민원내용을 입력하세요.");
    if (files.length > 6) throw new Error("사진은 최대 6장까지 업로드할 수 있습니다.");
    if (!lastAiResult) {
      await classifyCurrentText();
      payload.summary = String((lastAiResult || {}).summary || "");
      payload.type = String((lastAiResult || {}).type || "");
      payload.urgency = String((lastAiResult || {}).urgency || "");
    }
    const data = await api("/api/complaints", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const item = data.item;
    for (const file of files) {
      if (!item?.id) break;
      const fd = new FormData();
      fd.append("file", file, file.name || "photo");
      await authFetchJson(`/api/complaints/${item.id}/attachments?tenant_id=${encodeURIComponent(payload.tenant_id)}`, {
        method: "POST",
        body: fd,
      });
    }
    setMessage("#intakeMsg", "민원을 저장했습니다.");
    $("#buildingInput").value = "";
    $("#unitInput").value = "";
    $("#managerInput").value = "";
    $("#contentInput").value = "";
    $("#phoneInput").value = "";
    $("#channelInput").value = "전화";
    $("#photoInput").value = "";
    updatePhotoHint("#photoInput", "#photoHint");
    if (hadDigestRows) {
      consumeDigestRow(item);
      $("#chatDigestBox").innerHTML = renderChatDigestResult(lastDigestResult);
      wireDigestResultControls();
      applyDigestLeadToIntake(lastDigestResult);
    } else {
      lastAiResult = null;
      renderAiSuggestion(null);
    }
    setCurrentIntakeStep(1);
    await reloadAll();
  }

  async function importDigestComplaints() {
    const rows = selectedDigestRows();
    if (!rows.length) throw new Error("먼저 카톡 분석을 실행해 민원 항목을 추출하세요.");
    if (lastDigestImported) throw new Error("현재 분석 결과는 이미 민원 등록을 마쳤습니다. 다시 분석한 뒤 사용하세요.");
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const data = await api("/api/ai/kakao_digest/import", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: tenantId,
        rows,
        source_text: String($("#chatInput").value || "").trim(),
        image_analysis_model: String(lastDigestResult?.image_analysis_model || "").trim(),
        channel: "카톡",
      }),
    });
    lastDigestImported = true;
    updateDigestImportState();
    $("#chatDigestBox").innerHTML = renderChatDigestResult(lastDigestResult);
    wireDigestResultControls();
    const created = Array.isArray(data.items) ? data.items.length : Number(data.created_count || 0);
    setMessage("#intakeMsg", `카톡 분석 결과 ${created}건을 민원으로 등록했습니다.`);
    $("#filterStatus").value = "접수";
    await loadComplaints();
    await loadDashboard();
  }

  async function loadDashboard() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/dashboard/summary?tenant_id=${encodeURIComponent(tenantId)}`);
    const item = data.item || {};
    $("#metricTodayTotal").textContent = String(item.today_total || 0);
    $("#metricTodayDone").textContent = String(item.today_done || 0);
    $("#metricPending").textContent = String(item.pending_total || 0);
    $("#metricCarry").textContent = String(item.carry_total || 0);

    $("#urgentList").innerHTML = (item.urgent_items || []).length
      ? item.urgent_items.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.summary || row.type)}</strong><p>${escapeHtml((row.building || "-") + "동 / " + (row.status || "-"))}</p></article>`).join("")
      : '<div class="empty-state">긴급 민원이 없습니다.</div>';

    $("#repeatList").innerHTML = (item.repeat_items || []).length
      ? item.repeat_items.map((row) => `<article class="timeline-item"><strong>${escapeHtml((row.building || "-") + "동 " + (row.unit || ""))}</strong><p>${escapeHtml(`${row.type} / ${row.count}회`)}</p></article>`).join("")
      : '<div class="empty-state">반복 민원이 없습니다.</div>';

    $("#typeSummary").innerHTML = (item.type_counts || []).length
      ? item.type_counts.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.type)}</strong><p>${escapeHtml(String(row.count))}건</p></article>`).join("")
      : '<div class="empty-state">유형별 데이터가 없습니다.</div>';

    $("#managerSummary").innerHTML = (item.manager_load || []).length
      ? item.manager_load.map((row) => `<article class="timeline-item"><strong>${escapeHtml(row.manager)}</strong><p>${escapeHtml(String(row.count))}건</p></article>`).join("")
      : '<div class="empty-state">담당자 부하 데이터가 없습니다.</div>';
  }

  function complaintRowHtml(row) {
    const location = [row.building ? `${row.building}동` : "", row.unit ? `${row.unit}호` : ""].filter(Boolean).join(" ");
    return `
      <tr class="complaint-row" data-id="${Number(row.id || 0)}">
        <td>${escapeHtml(formatDateTime(row.created_at))}</td>
        <td>${escapeHtml(location || "-")}</td>
        <td>${escapeHtml(row.type || "-")}</td>
        <td>${escapeHtml(row.urgency || "-")}</td>
        <td>${escapeHtml(row.status || "-")}</td>
        <td>${escapeHtml(row.manager || "-")}</td>
        <td>${escapeHtml(row.summary || row.content || "-")}</td>
      </tr>
    `;
  }

  function complaintCardHtml(row) {
    const id = Number(row.id || 0);
    const location = [row.building ? `${row.building}동` : "", row.unit ? `${row.unit}호` : ""].filter(Boolean).join(" ");
    const summary = String(row.summary || row.content || "-");
    const urgencyClass = `urgency-${String(row.urgency || "").trim()}`;
    return [
      `<article class="complaint-card${selectedComplaintId === id ? " active" : ""}" data-id="${id}">`,
      '<div class="complaint-card-top">',
      `<div><h3 class="complaint-card-title">${escapeHtml(summary)}</h3></div>`,
      `<div class="complaint-chip status">${escapeHtml(row.status || "-")}</div>`,
      "</div>",
      '<div class="complaint-card-chips">',
      `<span class="complaint-chip ${escapeHtml(urgencyClass)}">${escapeHtml(row.urgency || "-")}</span>`,
      `<span class="complaint-chip">${escapeHtml(row.type || "-")}</span>`,
      row.manager ? `<span class="complaint-chip">${escapeHtml(row.manager)}</span>` : "",
      "</div>",
      '<div class="complaint-card-meta">',
      `<span>${escapeHtml(formatDateTime(row.created_at))}</span>`,
      `<span>${escapeHtml(location || "위치 미입력")}</span>`,
      "</div>",
      `<div class="complaint-card-summary">${escapeHtml(summary)}</div>`,
      "</article>",
    ].join("");
  }

  async function selectComplaint(id, { scrollDetail = false } = {}) {
    selectedComplaintId = Number(id || 0);
    await loadComplaintDetail();
    if (scrollDetail) {
      $("#complaintDetail")?.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  async function loadComplaints() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const status = String($("#filterStatus").value || "").trim();
    const building = String($("#filterBuilding").value || "").trim().replace(/동$/u, "").trim();
    const params = new URLSearchParams({ tenant_id: tenantId });
    if (status) params.set("status", status);
    if (building) params.set("building", building);
    const data = await api(`/api/complaints?${params.toString()}`);
    const rows = Array.isArray(data.items) ? data.items : [];
    const body = $("#complaintsTableBody");
    const cards = $("#complaintsCardList");
    body.innerHTML = rows.length ? rows.map(complaintRowHtml).join("") : '<tr><td colspan="7" class="empty-state">조회된 민원이 없습니다.</td></tr>';
    if (cards) {
      cards.innerHTML = rows.length ? rows.map(complaintCardHtml).join("") : '<div class="empty-state">조회된 민원이 없습니다.</div>';
    }
    body.querySelectorAll(".complaint-row").forEach((rowEl) => {
      rowEl.addEventListener("click", async () => selectComplaint(rowEl.getAttribute("data-id") || 0));
    });
    cards?.querySelectorAll(".complaint-card").forEach((cardEl) => {
      cardEl.addEventListener("click", async () => selectComplaint(cardEl.getAttribute("data-id") || 0, { scrollDetail: true }));
    });
  }

  async function loadComplaintDetail() {
    if (!selectedComplaintId) return;
    const tenantId = currentTenantId();
    const data = await api(`/api/complaints/${selectedComplaintId}?tenant_id=${encodeURIComponent(tenantId)}`);
    selectedComplaint = data.item || null;
    if (!selectedComplaint) return;
    document.querySelectorAll(".complaint-card").forEach((el) => {
      el.classList.toggle("active", Number(el.getAttribute("data-id") || 0) === selectedComplaintId);
    });
    document.querySelectorAll(".complaint-row").forEach((el) => {
      el.classList.toggle("active", Number(el.getAttribute("data-id") || 0) === selectedComplaintId);
    });
    syncComplaintDeleteOption();
    $("#complaintDetail").innerHTML = [
      `<strong>${escapeHtml(selectedComplaint.summary || "-")}</strong>`,
      `유형: ${escapeHtml(selectedComplaint.type || "-")}`,
      `긴급도: ${escapeHtml(selectedComplaint.urgency || "-")}`,
      `상태: ${escapeHtml(selectedComplaint.status || "-")}`,
      `채널: ${escapeHtml(selectedComplaint.channel || "-")}`,
      `연락처: ${escapeHtml(selectedComplaint.complainant_phone || "-")}`,
      `내용: ${escapeHtml(selectedComplaint.content || "-")}`,
      `반복접수: ${escapeHtml(String(selectedComplaint.repeat_count || 0))}회`,
    ].join("<br>");
    $("#detailStatus").value = String(selectedComplaint.status || "접수");
    $("#detailManager").value = String(selectedComplaint.manager || "");
    $("#attachmentSelectAll").checked = false;
    $("#detailAttachments").innerHTML = (selectedComplaint.attachments || []).length
      ? selectedComplaint.attachments.map((row) => `
        <article class="attachment-card">
          <label class="attachment-top">
            <input class="attachment-check" type="checkbox" value="${Number(row.id || 0)}" />
            <span>${escapeHtml(row.file_url || "")}</span>
          </label>
          <img src="${escapeHtml(row.file_url || "")}" alt="민원 첨부 이미지" loading="lazy" />
          <div class="attachment-meta">${escapeHtml(formatDateTime(row.created_at))}</div>
        </article>
      `).join("")
      : '<div class="empty-state">첨부 사진이 없습니다.</div>';
    $("#detailHistory").innerHTML = (selectedComplaint.history || []).length
      ? selectedComplaint.history.map((row) => `<article class="timeline-item"><strong>${escapeHtml((row.from_status || "초기") + " → " + row.to_status)}</strong><p>${escapeHtml(formatDateTime(row.created_at))} / ${escapeHtml(row.actor_label || "-")}</p>${row.note ? `<p>${escapeHtml(row.note)}</p>` : ""}</article>`).join("")
      : '<div class="empty-state">이력이 없습니다.</div>';
    const stickySave = $("#btnUpdateComplaint");
    if (stickySave) {
      stickySave.textContent = `상태 저장${selectedComplaintId ? ` #${selectedComplaintId}` : ""}`;
    }
  }

  function selectedAttachmentIds() {
    return Array.from(document.querySelectorAll(".attachment-check:checked")).map((el) => Number(el.value || 0)).filter(Boolean);
  }

  async function uploadDetailAttachments() {
    if (!selectedComplaintId || !selectedComplaint) throw new Error("목록에서 민원을 먼저 선택하세요.");
    const tenantId = currentTenantId();
    const currentCount = Array.isArray(selectedComplaint.attachments) ? selectedComplaint.attachments.length : 0;
    const files = selectedFiles("#detailPhotoInput");
    if (!files.length) throw new Error("추가할 사진을 선택하세요.");
    if (currentCount + files.length > 6) throw new Error("첨부 사진은 민원당 최대 6장까지 가능합니다.");
    for (const file of files) {
      const fd = new FormData();
      fd.append("file", file, file.name || "photo");
      await authFetchJson(`/api/complaints/${selectedComplaintId}/attachments?tenant_id=${encodeURIComponent(tenantId)}`, {
        method: "POST",
        body: fd,
      });
    }
    $("#detailPhotoInput").value = "";
    await loadComplaintDetail();
    await loadComplaints();
  }

  async function deleteAttachments(deleteAll = false) {
    if (!selectedComplaintId) throw new Error("목록에서 민원을 먼저 선택하세요.");
    const tenantId = currentTenantId();
    const attachmentIds = deleteAll ? [] : selectedAttachmentIds();
    if (!deleteAll && !attachmentIds.length) throw new Error("삭제할 첨부를 선택하세요.");
    await api(`/api/complaints/${selectedComplaintId}/attachments`, {
      method: "DELETE",
      body: JSON.stringify({ tenant_id: tenantId, delete_all: deleteAll, attachment_ids: attachmentIds }),
    });
    await loadComplaintDetail();
    await loadComplaints();
  }

  async function updateSelectedComplaint() {
    if (!selectedComplaintId) throw new Error("목록에서 민원을 먼저 선택하세요.");
    const tenantId = currentTenantId();
    const nextStatus = String($("#detailStatus").value || "접수").trim();
    if (nextStatus === "__delete__") {
      if (!canDeleteComplaints()) throw new Error("관리자 권한으로만 삭제할 수 있습니다.");
      if (!window.confirm("선택한 민원을 삭제하시겠습니까? 삭제 후 복구할 수 없습니다.")) return;
      await api(`/api/complaints/${selectedComplaintId}`, {
        method: "DELETE",
        body: JSON.stringify({ tenant_id: tenantId }),
      });
      clearComplaintDetail();
      setMessage("#intakeMsg", "민원을 삭제했습니다.");
      await loadComplaints();
      await loadDashboard();
      return;
    }
    const payload = {
      tenant_id: tenantId,
      status: nextStatus,
      manager: String($("#detailManager").value || "").trim(),
      note: String($("#detailNote").value || "").trim(),
    };
    await api(`/api/complaints/${selectedComplaintId}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    $("#detailNote").value = "";
    await loadComplaints();
    await loadComplaintDetail();
    await loadDashboard();
  }

  async function generateReport() {
    const tenantId = currentTenantId();
    if (!tenantId) return;
    const data = await api(`/api/report/daily?tenant_id=${encodeURIComponent(tenantId)}`);
    $("#reportBox").textContent = String(data.item?.report_text || "");
  }

  function wireDigestResultControls() {
    document.querySelectorAll(".digest-row-check").forEach((el) => {
      el.addEventListener("change", () => {
        const key = String(el.getAttribute("data-row-key") || "").trim();
        if (!key) return;
        if (el.checked) {
          lastDigestSelectedKeys.add(key);
        } else {
          lastDigestSelectedKeys.delete(key);
        }
        updateDigestImportState();
      });
    });
  }

  function renderChatDigestResult(item) {
    const digest = item || {};
    const rows = Array.isArray(digest.excel_rows) ? digest.excel_rows : [];
    const sections = [];
    const model = String(digest.image_analysis_model || "").trim();
    const imageCount = Number(digest.input_image_count || 0);
    const total = Number(digest.total || 0);
    if (model || imageCount) {
      sections.push([
        "<div class=\"subhead\">분석 메타</div>",
        "<div class=\"detail-block\">",
        `분석 방식: ${escapeHtml(model || "텍스트/규칙 기반")}<br>`,
        `입력 이미지 수: ${escapeHtml(String(imageCount))}<br>`,
        `추출 민원 수: ${escapeHtml(String(total))}`,
        "</div>",
      ].join(""));
    }
    if (digest.analysis_notice) {
      sections.push([
        "<div class=\"subhead\">안내</div>",
        `<div class="detail-block">${escapeHtml(String(digest.analysis_notice || ""))}</div>`,
      ].join(""));
    }
    if (rows.length) {
      sections.push([
        "<div class=\"subhead\">추출 민원 목록</div>",
        "<div class=\"table-wrap\"><table class=\"data-table\"><thead><tr>",
        "<th>선택</th><th>동/호</th><th>유형</th><th>긴급도</th><th>상태</th><th>내용요약</th>",
        "</tr></thead><tbody>",
        rows.map((row) => {
          const key = digestRowKey(row);
          const checked = lastDigestSelectedKeys.has(key) ? " checked" : "";
          const location = [String(row.building || "").trim() ? `${escapeHtml(String(row.building || "").trim())}동` : "", String(row.unit || "").trim() ? `${escapeHtml(String(row.unit || "").trim())}호` : ""].filter(Boolean).join(" ");
          return [
            "<tr>",
            `<td><input class="digest-row-check" type="checkbox" data-row-key="${escapeHtml(key)}"${checked}></td>`,
            `<td>${location || "-"}</td>`,
            `<td>${escapeHtml(String(row.type || "-"))}</td>`,
            `<td>${escapeHtml(String(row.urgency || "-"))}</td>`,
            `<td>${escapeHtml(String(row.status || "-"))}</td>`,
            `<td>${escapeHtml(String(row.summary || row.content || "-"))}</td>`,
            "</tr>",
          ].join("");
        }).join(""),
        "</tbody></table></div>",
      ].join(""));
    }
    const reportText = String(digest.report_text || "").trim();
    if (reportText) {
      sections.push([
        "<div class=\"subhead\">보고서 본문</div>",
        `<div class="detail-block">${escapeHtml(reportText)}</div>`,
      ].join(""));
    }
    return sections.join("");
  }

  function workReportAnalysisModeLabel(report) {
    const explicit = String(report?.analysis_mode_label || "").trim();
    if (explicit) return explicit;
    const model = String(report?.analysis_model || "").trim();
    if (!model) return "-";
    if (model === "heuristic") return "규칙 기반";
    if (model.startsWith("gpt-")) {
      return String(report?.analysis_reason || "").trim() ? "OpenAI + 보조 분석" : `OpenAI (${model})`;
    }
    return model;
  }

  function workReportAnalysisReasonLabel(report) {
    const explicit = String(report?.analysis_reason_label || "").trim();
    if (explicit) return explicit;
    const reason = String(report?.analysis_reason || "").trim();
    if (reason === "api_timeout") return "응답 시간 초과";
    if (reason === "insufficient_quota") return "할당량 부족";
    if (reason === "rate_limited") return "요청 한도 초과";
    if (reason === "missing_api_key") return "API 키 미설정";
    if (reason === "missing_sdk") return "SDK 미설치";
    if (reason === "auth_error") return "인증 설정 오류";
    if (reason === "invalid_json") return "응답 형식 오류";
    if (reason === "openai_error") return "OpenAI 호출 실패";
    return "";
  }

  function deepCloneJson(value) {
    if (value === undefined) return undefined;
    return JSON.parse(JSON.stringify(value));
  }

  function normalizeWorkReportImageStage(stage) {
    const value = String(stage || "").trim();
    return WORK_REPORT_IMAGE_STAGE_OPTIONS.some((row) => row.value === value) ? value : "general";
  }

  function workReportImageStageLabel(stage) {
    const normalized = normalizeWorkReportImageStage(stage);
    const match = WORK_REPORT_IMAGE_STAGE_OPTIONS.find((row) => row.value === normalized);
    return match ? match.label : "현장 이미지";
  }

  function syncWorkReportImageCollection(images) {
    const list = Array.isArray(images) ? images : [];
    list.sort(
      (left, right) => Number(left?.index || 0) - Number(right?.index || 0)
        || String(left?.filename || "").localeCompare(String(right?.filename || ""), "ko")
    );
    list.forEach((image) => {
      if (!image || typeof image !== "object") return;
      const stage = normalizeWorkReportImageStage(image.stage);
      image.stage = stage;
      image.stage_label = workReportImageStageLabel(stage);
      if (image.include_in_output === undefined) {
        image.include_in_output = true;
      }
    });
    return list;
  }

  function workReportItemByIndex(report, itemIndex) {
    if (!report || !Array.isArray(report.items)) return null;
    return report.items.find((row) => Number(row?.index || 0) === Number(itemIndex || 0)) || null;
  }

  function syncWorkReportItemOutputState(item) {
    if (!item || typeof item !== "object") return;
    const images = Array.isArray(item.images) ? item.images : [];
    if (images.some((image) => image && image.include_in_output !== false)) {
      item.include_in_output = true;
      return;
    }
    if (item.include_in_output === undefined) {
      item.include_in_output = true;
    }
  }

  function syncWorkReportItemOutputStates(report) {
    if (!report || !Array.isArray(report.items)) return;
    report.items.forEach((item) => syncWorkReportItemOutputState(item));
  }

  function workReportImageKey(image, fallbackKey = "") {
    const index = Number(image?.index || 0);
    if (index > 0) return `id:${index}`;
    const filename = String(image?.filename || "").trim();
    if (filename) return `file:${filename}`;
    return `fallback:${fallbackKey}`;
  }

  function workReportImageLocationByKey(report, imageKey) {
    const normalizedKey = String(imageKey || "").trim();
    if (!report || !normalizedKey) return null;
    const items = Array.isArray(report.items) ? report.items : [];
    for (const item of items) {
      const itemIndex = Number(item?.index || 0);
      const images = Array.isArray(item?.images) ? item.images : [];
      for (let imageIndex = 0; imageIndex < images.length; imageIndex += 1) {
        const image = images[imageIndex];
        if (workReportImageKey(image, `item-${itemIndex}-${imageIndex}`) === normalizedKey) {
          return {
            sourceType: "item",
            itemIndex,
            imageIndex,
            unmatchedIndex: -1,
            row: image,
          };
        }
      }
    }
    const unmatchedImages = Array.isArray(report.unmatched_images) ? report.unmatched_images : [];
    for (let unmatchedIndex = 0; unmatchedIndex < unmatchedImages.length; unmatchedIndex += 1) {
      const image = unmatchedImages[unmatchedIndex];
      if (workReportImageKey(image, `unmatched-${unmatchedIndex}`) === normalizedKey) {
        return {
          sourceType: "unmatched",
          itemIndex: 0,
          imageIndex: -1,
          unmatchedIndex,
          row: image,
        };
      }
    }
    return null;
  }

  function collectWorkReportImageAssignments(report) {
    const snapshot = new Map();
    const items = Array.isArray(report?.items) ? report.items : [];
    items.forEach((item) => {
      const itemIndex = Number(item?.index || 0);
      const itemTitle = String(item?.title || "").trim() || "작업 항목";
      (Array.isArray(item?.images) ? item.images : []).forEach((image, order) => {
        const key = workReportImageKey(image, `item-${itemIndex}-${order}`);
        snapshot.set(key, {
          key,
          image_index: Number(image?.index || 0),
          filename: String(image?.filename || "").trim(),
          item_index: itemIndex,
          item_title: itemTitle,
          stage: normalizeWorkReportImageStage(image?.stage),
          manual_override: !!image?.manual_override,
          review_reason: String(image?.review_reason || "").trim(),
          review_confidence: String(image?.review_confidence || "").trim(),
          review_candidates: Array.isArray(image?.review_candidates) ? image.review_candidates.map((candidate) => ({
            item_index: Number(candidate?.item_index || 0),
            title: String(candidate?.title || "").trim(),
            score: Number(candidate?.score || 0),
          })) : [],
        });
      });
    });
    (Array.isArray(report?.unmatched_images) ? report.unmatched_images : []).forEach((image, order) => {
      const key = workReportImageKey(image, `unmatched-${order}`);
      snapshot.set(key, {
        key,
        image_index: Number(image?.index || 0),
        filename: String(image?.filename || "").trim(),
        item_index: 0,
        item_title: "미매칭",
        stage: normalizeWorkReportImageStage(image?.stage),
        manual_override: !!image?.manual_override,
        review_reason: String(image?.review_reason || "").trim(),
        review_confidence: String(image?.review_confidence || "").trim(),
        review_candidates: Array.isArray(image?.review_candidates) ? image.review_candidates.map((candidate) => ({
          item_index: Number(candidate?.item_index || 0),
          title: String(candidate?.title || "").trim(),
          score: Number(candidate?.score || 0),
        })) : [],
      });
    });
    return snapshot;
  }

  function buildWorkReportFeedbackSummary(report, baseline) {
    if (!report || !baseline) {
      return { changes: [], signature: "" };
    }
    const current = collectWorkReportImageAssignments(report);
    const original = collectWorkReportImageAssignments(baseline);
    const changes = [];
    current.forEach((row, key) => {
      const before = original.get(key);
      if (!before) return;
      const assignmentChanged = Number(before.item_index || 0) !== Number(row.item_index || 0);
      const stageChanged = String(before.stage || "general") !== String(row.stage || "general");
      const manualConfirmed = !!row.manual_override && !before.manual_override && !assignmentChanged && !stageChanged;
      if (!assignmentChanged && !stageChanged && !manualConfirmed) return;
      let feedbackType = "confirm_current";
      if (assignmentChanged && Number(row.item_index || 0) <= 0) {
        feedbackType = "mark_unmatched";
      } else if (assignmentChanged) {
        feedbackType = "reassign_item";
      } else if (stageChanged) {
        feedbackType = "change_stage";
      }
      changes.push({
        feedback_type: feedbackType,
        image_index: Number(row.image_index || 0),
        filename: String(row.filename || "").trim(),
        from_item_index: Number(before.item_index || 0),
        from_item_title: String(before.item_title || "미매칭"),
        to_item_index: Number(row.item_index || 0),
        to_item_title: String(row.item_title || "미매칭"),
        from_stage: String(before.stage || "general"),
        from_stage_label: workReportImageStageLabel(before.stage),
        to_stage: String(row.stage || "general"),
        to_stage_label: workReportImageStageLabel(row.stage),
        review_reason: String(row.review_reason || "").trim(),
        review_confidence: String(row.review_confidence || "").trim(),
        candidate_items: Array.isArray(row.review_candidates) ? row.review_candidates.slice(0, 3) : [],
      });
    });
    changes.sort(
      (left, right) => Number(left.image_index || 0) - Number(right.image_index || 0)
        || String(left.filename || "").localeCompare(String(right.filename || ""), "ko")
    );
    return {
      changes,
      signature: changes.length ? JSON.stringify(changes) : "",
    };
  }

  function buildWorkReportFeedbackSnapshot(report) {
    const items = Array.isArray(report?.items) ? report.items : [];
    const unmatchedImages = Array.isArray(report?.unmatched_images) ? report.unmatched_images : [];
    return {
      report_title: String(report?.report_title || "").trim(),
      period_label: String(report?.period_label || "").trim(),
      analysis_model: String(report?.analysis_model || "").trim(),
      analysis_reason: String(report?.analysis_reason || "").trim(),
      item_count: items.length,
      items: items.map((item) => ({
        index: Number(item?.index || 0),
        title: String(item?.title || "").trim(),
        summary: String(item?.summary || "").trim(),
        images: syncWorkReportImageCollection(Array.isArray(item?.images) ? item.images.map((row) => ({ ...row })) : []).map((image) => ({
          index: Number(image?.index || 0),
          filename: String(image?.filename || "").trim(),
          stage: String(image?.stage || "general"),
          stage_label: String(image?.stage_label || workReportImageStageLabel(image?.stage)),
          manual_override: !!image?.manual_override,
          review_reason: String(image?.review_reason || "").trim(),
          review_confidence: String(image?.review_confidence || "").trim(),
        })),
      })),
      unmatched_images: syncWorkReportImageCollection(unmatchedImages.map((row) => ({ ...row }))).map((image) => ({
        index: Number(image?.index || 0),
        filename: String(image?.filename || "").trim(),
        stage: String(image?.stage || "general"),
        stage_label: String(image?.stage_label || workReportImageStageLabel(image?.stage)),
        manual_override: !!image?.manual_override,
        review_reason: String(image?.review_reason || "").trim(),
        review_confidence: String(image?.review_confidence || "").trim(),
      })),
    };
  }

  function renderWorkReportAssignmentOptions(report, selectedValue) {
    const options = [];
    const currentValue = String(selectedValue || "__unmatched__");
    options.push(`<option value="__unmatched__"${currentValue === "__unmatched__" ? " selected" : ""}>미매칭으로 두기</option>`);
    (Array.isArray(report?.items) ? report.items : []).forEach((item) => {
      const itemIndex = Number(item?.index || 0);
      if (itemIndex <= 0) return;
      const label = `${itemIndex}. ${String(item?.title || "-")}`;
      options.push(`<option value="${itemIndex}"${currentValue === String(itemIndex) ? " selected" : ""}>${escapeHtml(label)}</option>`);
    });
    return options.join("");
  }

  function renderWorkReportStageOptions(selectedStage) {
    const currentValue = normalizeWorkReportImageStage(selectedStage);
    return WORK_REPORT_IMAGE_STAGE_OPTIONS.map((option) => (
      `<option value="${option.value}"${currentValue === option.value ? " selected" : ""}>${escapeHtml(option.label)}</option>`
    )).join("");
  }

  function workReportPreviewJobId(report) {
    return String(report?.batch_job_id || lastWorkReportJobId || "").trim();
  }

  function workReportImagePreviewUrl(report, image) {
    if (image && image.preview_available === false) return "";
    const jobId = workReportPreviewJobId(report);
    const imageIndex = Number(image?.index || 0);
    if (!jobId || imageIndex <= 0) return "";
    return `/api/ai/work_report/jobs/${encodeURIComponent(jobId)}/images/${imageIndex}`;
  }

  function renderWorkReportInlineImagePreview(report, image) {
    const previewUrl = workReportImagePreviewUrl(report, image);
    const imageIndex = Number(image?.index || 0);
    if (!previewUrl || activeWorkReportPreviewIndex !== imageIndex) return "";
    return [
      '<div class="work-report-image-preview-panel">',
      `<img src="${escapeHtml(previewUrl)}" alt="${escapeHtml(String(image?.filename || "업무보고 이미지"))}" loading="lazy">`,
      "</div>",
    ].join("");
  }

  function renderWorkReportImageEditor(report, image, config = {}) {
    const row = image && typeof image === "object" ? image : {};
    const stage = normalizeWorkReportImageStage(row.stage);
    const stageLabel = String(row.stage_label || workReportImageStageLabel(stage));
    const checked = row && row.include_in_output !== false ? " checked" : "";
    const sourceType = String(config.sourceType || "item");
    const imageKey = workReportImageKey(
      row,
      sourceType === "unmatched"
        ? `unmatched-${Number(config.unmatchedIndex || 0)}`
        : `item-${Number(config.itemIndex || 0)}-${Number(config.imageIndex || 0)}`
    );
    const sourceAttrs = sourceType === "unmatched"
      ? `data-unmatched-index="${Number(config.unmatchedIndex || 0)}" data-image-record-key="${escapeHtml(imageKey)}"`
      : `data-item-index="${Number(config.itemIndex || 0)}" data-image-index="${Number(config.imageIndex || 0)}" data-image-record-key="${escapeHtml(imageKey)}"`;
    const editorAttrs = sourceType === "unmatched"
      ? `data-source-type="unmatched" data-unmatched-index="${Number(config.unmatchedIndex || 0)}" data-image-record-key="${escapeHtml(imageKey)}"`
      : `data-source-type="item" data-item-index="${Number(config.itemIndex || 0)}" data-image-index="${Number(config.imageIndex || 0)}" data-image-record-key="${escapeHtml(imageKey)}"`;
    const assignmentValue = sourceType === "unmatched" ? "__unmatched__" : String(Number(config.itemIndex || 0));
    const checkboxClass = sourceType === "unmatched" ? "work-report-unmatched-output-check" : "work-report-output-check";
    const previewUrl = workReportImagePreviewUrl(report, row);
    const feedbackDirty = !!config.feedbackDirty;
    const feedbackSaved = !!config.feedbackSaved;
    const previewButton = previewUrl
      ? `<button class="work-report-image-preview-toggle" type="button" data-image-preview-index="${Number(row.index || 0)}">${escapeHtml(activeWorkReportPreviewIndex === Number(row.index || 0) ? "이미지 닫기" : "이미지 보기")}</button>`
      : "";
    const saveButton = `<button class="work-report-image-save" type="button"${feedbackDirty ? "" : " disabled"}>${escapeHtml(feedbackSaved ? "저장 완료" : feedbackDirty ? "수정완료(저장)" : "저장할 변경 없음")}</button>`;
    return [
      '<div class="work-report-image-select">',
      `<input class="${checkboxClass}" type="checkbox" ${sourceAttrs}${checked}>`,
      '<div class="work-report-image-body">',
      '<div class="work-report-image-head">',
      `<span class="work-report-image-title">${escapeHtml(`${stageLabel} · ${String(row.filename || "-")}`)}</span>`,
      '<div class="work-report-image-head-actions">',
      previewButton,
      saveButton,
      '</div>',
      "</div>",
      row.manual_override ? '<span class="work-report-image-note">수동 보정</span>' : "",
      '<div class="work-report-image-controls">',
      `<label class="work-report-control-field"><span>연결 작업</span><select class="work-report-image-assignment" ${editorAttrs}>${renderWorkReportAssignmentOptions(report, assignmentValue)}</select></label>`,
      `<label class="work-report-control-field"><span>단계</span><select class="work-report-image-stage" ${editorAttrs}>${renderWorkReportStageOptions(stage)}</select></label>`,
      "</div>",
      renderWorkReportInlineImagePreview(report, row),
      "</div>",
      "</div>",
    ].join("");
  }

  function workReportReviewConfidenceLabel(value) {
    const normalized = String(value || "").trim();
    if (normalized === "high") return "확신 높음";
    if (normalized === "medium") return "확신 보통";
    if (normalized === "low") return "검토 필요";
    return "";
  }

  function collectWorkReportReviewQueue(report) {
    const queue = [];
    (Array.isArray(report?.items) ? report.items : []).forEach((item) => {
      const itemIndex = Number(item?.index || 0);
      (Array.isArray(item?.images) ? item.images : []).forEach((image, imageIndex) => {
        if (!image || image.manual_override || image.review_needed !== true) return;
        queue.push({
          sourceType: "item",
          itemIndex,
          imageIndex,
          unmatchedIndex: -1,
          currentItemIndex: itemIndex,
          currentItemTitle: String(item?.title || "").trim() || "작업 항목",
          image,
        });
      });
    });
    (Array.isArray(report?.unmatched_images) ? report.unmatched_images : []).forEach((image, unmatchedIndex) => {
      if (!image || image.manual_override || image.review_needed !== true) return;
      queue.push({
        sourceType: "unmatched",
        itemIndex: 0,
        imageIndex: -1,
        unmatchedIndex,
        currentItemIndex: 0,
        currentItemTitle: "미매칭",
        image,
      });
    });
    queue.sort((left, right) => {
      const leftPriority = String(left?.image?.review_confidence || "") === "low" ? 0 : 1;
      const rightPriority = String(right?.image?.review_confidence || "") === "low" ? 0 : 1;
      return leftPriority - rightPriority || Number(left?.image?.index || 0) - Number(right?.image?.index || 0);
    });
    return queue;
  }

  function renderWorkReportReviewCandidate(review, candidate) {
    const row = candidate && typeof candidate === "object" ? candidate : {};
    const reasonText = Array.isArray(row.reason_parts) && row.reason_parts.length
      ? row.reason_parts.join(" / ")
      : String(row.reason_text || "").trim();
    const commonAttrs = [
      `data-source-type="${escapeHtml(String(review.sourceType || "item"))}"`,
      `data-item-index="${Number(review.itemIndex || 0)}"`,
      `data-image-index="${Number(review.imageIndex || 0)}"`,
      `data-unmatched-index="${Number(review.unmatchedIndex || -1)}"`,
    ].join(" ");
    return [
      `<button class="work-report-review-option" type="button" ${commonAttrs} data-destination-item-index="${Number(row.item_index || 0)}">`,
      `<strong>${escapeHtml(`${Number(row.rank || 0)}순위 · ${String(row.title || "-")}`)}</strong>`,
      `<span>${escapeHtml(`점수 ${Number(row.score || 0)}${row.location_name ? ` / 위치 ${String(row.location_name)}` : ""}`)}</span>`,
      reasonText ? `<small>${escapeHtml(reasonText)}</small>` : "",
      "</button>",
    ].join("");
  }

  function renderWorkReportReviewCard(review) {
    const image = review?.image && typeof review.image === "object" ? review.image : {};
    const stageLabel = String(image.stage_label || workReportImageStageLabel(image.stage));
    const confidenceLabel = workReportReviewConfidenceLabel(image.review_confidence);
    const currentLabel = review?.currentItemIndex > 0
      ? `${Number(review.currentItemIndex || 0)}. ${String(review.currentItemTitle || "작업 항목")}`
      : "미매칭";
    const previewUrl = workReportImagePreviewUrl(lastWorkReportResult, image);
    const attrs = [
      `data-source-type="${escapeHtml(String(review?.sourceType || "item"))}"`,
      `data-item-index="${Number(review?.itemIndex || 0)}"`,
      `data-image-index="${Number(review?.imageIndex || 0)}"`,
      `data-unmatched-index="${Number(review?.unmatchedIndex || -1)}"`,
    ].join(" ");
    const candidates = Array.isArray(image.review_candidates) ? image.review_candidates.slice(0, 3) : [];
    return [
      '<article class="work-report-review-card">',
      '<div class="work-report-review-head">',
      `<strong>${escapeHtml(`${stageLabel} · ${String(image.filename || "-")}`)}</strong>`,
      '<div class="work-report-review-head-actions">',
      confidenceLabel ? `<span class="work-report-review-chip">${escapeHtml(confidenceLabel)}</span>` : "",
      previewUrl ? `<button class="work-report-image-preview-toggle" type="button" data-image-preview-index="${Number(image.index || 0)}">${escapeHtml(activeWorkReportPreviewIndex === Number(image.index || 0) ? "이미지 닫기" : "이미지 보기")}</button>` : "",
      '</div>',
      "</div>",
      `<p>${escapeHtml(`현재 연결: ${currentLabel}`)}</p>`,
      image.review_reason ? `<p>${escapeHtml(`검토 사유: ${String(image.review_reason || "")}`)}</p>` : "",
      renderWorkReportInlineImagePreview(lastWorkReportResult, image),
      candidates.length
        ? `<div class="work-report-review-options">${candidates.map((candidate) => renderWorkReportReviewCandidate(review, candidate)).join("")}</div>`
        : '<p>추천 후보를 만들지 못했습니다. 아래 편집기에서 직접 선택해 주세요.</p>',
      '<div class="work-report-review-actions">',
      `<button class="work-report-review-confirm" type="button" ${attrs}>현재 선택 확정</button>`,
      `<button class="work-report-review-unmatched" type="button" ${attrs} data-destination-item-index="__unmatched__">미매칭으로 두기</button>`,
      "</div>",
      "</article>",
    ].join("");
  }

  function renderWorkReportResult(item) {
    const report = item || {};
    syncWorkReportItemOutputStates(report);
    const items = Array.isArray(report.items) ? report.items : [];
    const imageItems = items.filter((row) => Array.isArray(row.images) && row.images.length);
    const textOnlyItems = items.filter((row) => !Array.isArray(row.images) || !row.images.length);
    const unmatchedImages = Array.isArray(report.unmatched_images) ? report.unmatched_images : [];
    const reviewQueue = collectWorkReportReviewQueue(report);
    const modeLabel = workReportAnalysisModeLabel(report);
    const reasonLabel = workReportAnalysisReasonLabel(report);
    const isRuleBasedMode = String(report?.analysis_model || "").trim() === "heuristic" || modeLabel === "규칙 기반";
    const feedback = buildWorkReportFeedbackSummary(report, lastWorkReportBaseline);
    const feedbackSaved = !!feedback.signature && feedback.signature === lastWorkReportFeedbackSavedSignature;
    const feedbackDisabled = !feedback.changes.length || feedbackSaved;
    const feedbackSummary = feedback.changes.length
      ? feedbackSaved
        ? `수동 보정 ${feedback.changes.length}건이 저장되었습니다.`
        : `수동 보정 ${feedback.changes.length}건이 저장 대기 중입니다.`
      : "아직 저장할 이미지 매칭 수정은 없습니다.";
    const sections = [];
    if (isRuleBasedMode) {
      sections.push([
        '<div class="work-report-mode-banner is-fallback">',
        `<strong>${escapeHtml(`모델 ${modeLabel}`)}</strong>`,
        `<span>${escapeHtml(reasonLabel ? `사유 ${reasonLabel}` : "OpenAI 분석 결과를 받지 못해 규칙 기반 결과를 표시 중입니다.")}</span>`,
        report.analysis_notice ? `<p>${escapeHtml(String(report.analysis_notice || ""))}</p>` : "",
        "</div>",
      ].join(""));
    }
    sections.push([
      "<div class=\"subhead\">보고 개요</div>",
      `<div class="work-report-meta">`,
      `<span>${escapeHtml(String(report.report_title || "시설팀 주요 업무 보고"))}</span>`,
      `<span>모델 ${escapeHtml(modeLabel)}</span>`,
      reasonLabel ? `<span>사유 ${escapeHtml(reasonLabel)}</span>` : "",
      `<span>기간 ${escapeHtml(String(report.period_label || "-"))}</span>`,
      `<span>작업 ${escapeHtml(String(items.length || 0))}건</span>`,
      `<span>사진 포함 ${escapeHtml(String(imageItems.length || 0))}건</span>`,
      `<span>텍스트 전용 ${escapeHtml(String(textOnlyItems.length || 0))}건</span>`,
      report.template_source_name ? `<span>양식 ${escapeHtml(String(report.template_source_name || ""))}</span>` : "",
      `</div>`,
    ].join(""));
    if (report.analysis_notice && !isRuleBasedMode) {
      sections.push(`<div class="detail-block">${escapeHtml(String(report.analysis_notice || ""))}</div>`);
    }
    if (imageItems.length) {
      sections.push('<div class="detail-block">미리보기에서 체크된 사진만 PDF에 출력됩니다.</div>');
    }
    if (imageItems.length || unmatchedImages.length) {
      sections.push(
        `<div class="work-report-feedback-bar${feedback.changes.length && !feedbackSaved ? " is-dirty" : ""}${feedbackSaved ? " is-saved" : ""}">`
        + `<div class="work-report-feedback-summary"><strong>이미지 매칭 보정</strong><span>${escapeHtml(feedbackSummary)}</span></div>`
        + `<button class="work-report-feedback-save" type="button"${feedbackDisabled ? " disabled" : ""}>${escapeHtml(feedbackSaved ? "저장 완료" : feedback.changes.length ? "매칭 수정 저장" : "저장할 변경 없음")}</button>`
        + "</div>"
      );
      sections.push('<div class="detail-block">이미지 매칭이 애매하면 사진별로 연결 작업과 단계값을 직접 바꿀 수 있습니다. 저장한 수정 내역은 다음 매칭 개선용 피드백으로 누적됩니다.</div>');
    }
    if (reviewQueue.length) {
      sections.push("<div class=\"subhead\">애매한 이미지 검토 큐</div>");
      sections.push('<div class="detail-block">점수 차이가 작거나 단서가 약한 사진만 따로 모았습니다. 추천 후보를 누르거나 현재 선택을 확정하면 검토 큐에서 빠집니다.</div>');
      sections.push(`<div class="work-report-review-list">${reviewQueue.map((review) => renderWorkReportReviewCard(review)).join("")}</div>`);
    }
    if (imageItems.length) {
      sections.push("<div class=\"subhead\">사진 포함 작업 항목</div>");
      sections.push(
        `<div class="work-report-match-list">${imageItems.map((row, index) => [
          '<article class="work-report-match-card">',
          '<label class="work-report-item-toggle">',
          `<input class="work-report-item-output-check" type="checkbox" data-item-index="${Number(row.index || 0)}"${row && row.include_in_output !== false ? " checked" : ""}>`,
          '<span>이 항목 출력</span>',
          '</label>',
          `<strong>${escapeHtml(`${index + 1}. ${String(row.title || "-")}`)}</strong>`,
          `<p>${escapeHtml(`작업일자: ${String(row.work_date_label || row.work_date || "-")} / 작업자: ${String(row.vendor_name || "-")} / 위치: ${String(row.location_name || "-")}`)}</p>`,
          `<p>${escapeHtml(`내용설명 : ${String(row.summary || row.title || "-")}`)}</p>`,
          Array.isArray(row.images) && row.images.length ? `<div class="work-report-image-select-list">${row.images.map((image, imageIndex) => (
            renderWorkReportImageEditor(report, image, {
              sourceType: "item",
              itemIndex: Number(row.index || 0),
              imageIndex,
              feedbackDirty: feedback.changes.length > 0,
              feedbackSaved,
            })
          )).join("")}</div>` : '<p>매칭된 이미지 없음</p>',
          "</article>",
        ].join("")).join("")}</div>`
      );
    }
    if (textOnlyItems.length) {
      sections.push("<div class=\"subhead\">세대민원 및 기타 작업</div>");
      sections.push(
        `<div class="work-report-match-list">${textOnlyItems.map((row, index) => [
          '<article class="work-report-match-card">',
          '<label class="work-report-item-toggle">',
          `<input class="work-report-item-output-check" type="checkbox" data-item-index="${Number(row.index || 0)}"${row && row.include_in_output !== false ? " checked" : ""}>`,
          '<span>이 항목 출력</span>',
          '</label>',
          `<strong>${escapeHtml(`${index + 1}. ${String(row.title || "-")}`)}</strong>`,
          `<p>${escapeHtml(`작업일자: ${String(row.work_date_label || row.work_date || "-")} / 작업자: ${String(row.vendor_name || "-")} / 위치: ${String(row.location_name || "-")}`)}</p>`,
          `<p>${escapeHtml(`내용설명 : ${String(row.summary || row.title || "-")}`)}</p>`,
          "</article>",
        ].join("")).join("")}</div>`
      );
    }
    if (unmatchedImages.length) {
      sections.push("<div class=\"subhead\">미매칭 자료</div>");
      sections.push(
        `<div class="work-report-image-select-list">${
          unmatchedImages.map((row, index) => (
            renderWorkReportImageEditor(report, row, {
              sourceType: "unmatched",
              unmatchedIndex: index,
              feedbackDirty: feedback.changes.length > 0,
              feedbackSaved,
            })
          )).join("")
        }</div>`
      );
    }
    if (report.report_text) {
      if (feedback.changes.length) {
        sections.push('<div class="detail-block">자동 생성 보고 요약은 최초 분석 기준일 수 있습니다. PDF 출력과 사진 배치는 현재 수동 보정 상태를 우선 적용합니다.</div>');
      }
      sections.push("<div class=\"subhead\">자동 생성 보고 요약</div>");
      sections.push(`<div class="detail-block">${escapeHtml(String(report.report_text || ""))}</div>`);
    }
    return sections.join("");
  }

  function invalidateWorkReportCache() {
    lastWorkReportResult = null;
    lastWorkReportBaseline = null;
    lastWorkReportJobId = "";
    lastWorkReportFeedbackSavedSignature = "";
    activeWorkReportPreviewIndex = 0;
  }

  function cloneWorkReportForPdf(report) {
    const cloned = JSON.parse(JSON.stringify(report || {}));
    const items = Array.isArray(cloned.items) ? cloned.items : [];
    cloned.items = items
      .filter((row) => row && row.include_in_output !== false)
      .map((row) => ({
        ...row,
        images: Array.isArray(row.images) ? row.images.filter((image) => !image || image.include_in_output !== false) : [],
        attachments: [],
      }));
    cloned.image_items = cloned.items.filter((row) => Array.isArray(row.images) && row.images.length);
    cloned.text_only_items = cloned.items.filter((row) => !Array.isArray(row.images) || !row.images.length);
    cloned.image_item_count = cloned.image_items.length;
    cloned.text_only_item_count = cloned.text_only_items.length;
    cloned.item_count = cloned.items.length;
    cloned.unmatched_images = Array.isArray(cloned.unmatched_images)
      ? cloned.unmatched_images.filter((row) => row && row.include_in_output !== false)
      : [];
    return cloned;
  }

  function rerenderWorkReportPreview(message = "", isError = false) {
    if (lastWorkReportResult) {
      syncWorkReportItemOutputStates(lastWorkReportResult);
      $("#workReportBox").innerHTML = renderWorkReportResult(lastWorkReportResult);
    }
    if (message) {
      setMessage("#intakeMsg", message, isError);
    }
  }

  function workReportImageRecord(sourceType, itemIndex, imageIndex, unmatchedIndex) {
    if (!lastWorkReportResult) return null;
    if (String(sourceType || "item") === "unmatched") {
      const unmatched = Array.isArray(lastWorkReportResult.unmatched_images) ? lastWorkReportResult.unmatched_images : [];
      return unmatched[Number(unmatchedIndex || -1)] || null;
    }
    const item = workReportItemByIndex(lastWorkReportResult, itemIndex);
    if (!item || !Array.isArray(item.images)) return null;
    return item.images[Number(imageIndex || -1)] || null;
  }

  function markWorkReportImageManualOverride(image) {
    if (!image || typeof image !== "object") return;
    image.manual_override = true;
    const stage = normalizeWorkReportImageStage(image.stage);
    image.stage = stage;
    image.stage_label = workReportImageStageLabel(stage);
    if (image.include_in_output === undefined) {
      image.include_in_output = true;
    }
  }

  function moveWorkReportImageRecord(sourceType, itemIndex, imageIndex, unmatchedIndex, destinationValue) {
    if (!lastWorkReportResult) return false;
    const nextDestination = String(destinationValue || "__unmatched__");
    const sourceKind = String(sourceType || "item");
    const currentDestination = sourceKind === "unmatched" ? "__unmatched__" : String(Number(itemIndex || 0));
    if (nextDestination === currentDestination) return false;

    let sourceList = [];
    let sourceRow = null;
    let sourcePosition = -1;
    let sourceItem = null;
    if (sourceKind === "unmatched") {
      sourceList = Array.isArray(lastWorkReportResult.unmatched_images) ? lastWorkReportResult.unmatched_images : [];
      sourcePosition = Number(unmatchedIndex || -1);
      sourceRow = sourceList[sourcePosition] || null;
    } else {
      sourceItem = workReportItemByIndex(lastWorkReportResult, itemIndex);
      if (!sourceItem) return false;
      sourceItem.images = Array.isArray(sourceItem.images) ? sourceItem.images : [];
      sourceList = sourceItem.images;
      sourcePosition = Number(imageIndex || -1);
      sourceRow = sourceList[sourcePosition] || null;
    }
    if (!sourceRow) return false;

    let targetList = null;
    let targetItem = null;
    if (nextDestination === "__unmatched__") {
      lastWorkReportResult.unmatched_images = Array.isArray(lastWorkReportResult.unmatched_images) ? lastWorkReportResult.unmatched_images : [];
      targetList = lastWorkReportResult.unmatched_images;
    } else {
      targetItem = workReportItemByIndex(lastWorkReportResult, Number(nextDestination || 0));
      if (!targetItem) return false;
      targetItem.images = Array.isArray(targetItem.images) ? targetItem.images : [];
      targetList = targetItem.images;
    }

    const [moved] = sourceList.splice(sourcePosition, 1);
    if (!moved) return false;
    markWorkReportImageManualOverride(moved);
    targetList.push(moved);
    syncWorkReportImageCollection(sourceList);
    syncWorkReportImageCollection(targetList);
    syncWorkReportItemOutputState(sourceItem);
    syncWorkReportItemOutputState(targetItem);
    if (Array.isArray(lastWorkReportResult.unmatched_images)) {
      syncWorkReportImageCollection(lastWorkReportResult.unmatched_images);
    }
    return true;
  }

  function moveWorkReportImageByKey(imageKey, destinationValue) {
    if (!lastWorkReportResult) return false;
    const location = workReportImageLocationByKey(lastWorkReportResult, imageKey);
    if (!location) return false;
    return moveWorkReportImageRecord(
      location.sourceType,
      location.itemIndex,
      location.imageIndex,
      location.unmatchedIndex,
      destinationValue
    );
  }

  function confirmWorkReportImageReview(sourceType, itemIndex, imageIndex, unmatchedIndex) {
    const row = workReportImageRecord(sourceType, itemIndex, imageIndex, unmatchedIndex);
    if (!row) return false;
    markWorkReportImageManualOverride(row);
    return true;
  }

  function updateWorkReportImageStage(sourceType, itemIndex, imageIndex, unmatchedIndex, stageValue) {
    if (!lastWorkReportResult) return false;
    let row = null;
    if (String(sourceType || "item") === "unmatched") {
      const unmatched = Array.isArray(lastWorkReportResult.unmatched_images) ? lastWorkReportResult.unmatched_images : [];
      row = unmatched[Number(unmatchedIndex || -1)] || null;
      if (!row) return false;
      row.stage = normalizeWorkReportImageStage(stageValue);
      markWorkReportImageManualOverride(row);
      syncWorkReportImageCollection(unmatched);
      return true;
    }
    const item = workReportItemByIndex(lastWorkReportResult, itemIndex);
    if (!item) return false;
    item.images = Array.isArray(item.images) ? item.images : [];
    row = item.images[Number(imageIndex || -1)] || null;
    if (!row) return false;
    row.stage = normalizeWorkReportImageStage(stageValue);
    markWorkReportImageManualOverride(row);
    syncWorkReportImageCollection(item.images);
    return true;
  }

  function updateWorkReportImageStageByKey(imageKey, stageValue) {
    if (!lastWorkReportResult) return false;
    const location = workReportImageLocationByKey(lastWorkReportResult, imageKey);
    if (!location) return false;
    return updateWorkReportImageStage(
      location.sourceType,
      location.itemIndex,
      location.imageIndex,
      location.unmatchedIndex,
      stageValue
    );
  }

  function syncWorkReportPendingEditsFromDom() {
    if (!lastWorkReportResult) return;
    const root = $("#workReportBox");
    if (!(root instanceof HTMLElement)) return false;
    let changed = false;

    root.querySelectorAll(".work-report-image-assignment").forEach((element) => {
      if (!(element instanceof HTMLSelectElement)) return;
      const imageKey = String(element.getAttribute("data-image-record-key") || "").trim();
      if (!imageKey) return;
      const location = workReportImageLocationByKey(lastWorkReportResult, imageKey);
      if (!location) return;
      const currentValue = location.sourceType === "unmatched" ? "__unmatched__" : String(location.itemIndex);
      const nextValue = String(element.value || "__unmatched__");
      if (nextValue === currentValue) return;
      changed = moveWorkReportImageByKey(imageKey, nextValue) || changed;
    });

    root.querySelectorAll(".work-report-image-stage").forEach((element) => {
      if (!(element instanceof HTMLSelectElement)) return;
      const imageKey = String(element.getAttribute("data-image-record-key") || "").trim();
      if (!imageKey) return;
      const location = workReportImageLocationByKey(lastWorkReportResult, imageKey);
      if (!location || !location.row) return;
      const currentStage = normalizeWorkReportImageStage(location.row.stage);
      const nextStage = normalizeWorkReportImageStage(element.value);
      if (currentStage === nextStage) return;
      changed = updateWorkReportImageStageByKey(imageKey, nextStage) || changed;
    });
    syncWorkReportItemOutputStates(lastWorkReportResult);
    return changed;
  }

  async function saveWorkReportFeedback(options = {}) {
    const silent = !!options.silent;
    if (!lastWorkReportResult || !lastWorkReportBaseline) return 0;
    syncWorkReportPendingEditsFromDom();
    const feedback = buildWorkReportFeedbackSummary(lastWorkReportResult, lastWorkReportBaseline);
    if (!feedback.changes.length) {
      if (!silent) {
        setMessage("#intakeMsg", "저장할 이미지 매칭 수정이 없습니다.");
      }
      return 0;
    }
    if (feedback.signature && feedback.signature === lastWorkReportFeedbackSavedSignature) {
      if (!silent) {
        setMessage("#intakeMsg", `이미지 매칭 수정 ${feedback.changes.length}건이 이미 저장되었습니다.`);
      }
      return feedback.changes.length;
    }
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const data = await authFetchJson("/api/ai/work_report/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        tenant_id: tenantId,
        job_id: lastWorkReportJobId,
        corrections: feedback.changes,
        report: buildWorkReportFeedbackSnapshot(lastWorkReportResult),
      }),
    });
    lastWorkReportFeedbackSavedSignature = feedback.signature;
    $("#workReportBox").innerHTML = renderWorkReportResult(lastWorkReportResult);
    const savedCount = Number(data?.item?.correction_count || feedback.changes.length || 0);
    if (!silent) {
      setMessage("#intakeMsg", `이미지 매칭 수정 ${savedCount}건을 다음 매칭 개선용 피드백으로 저장했습니다.`);
    }
    return savedCount;
  }

  function handleWorkReportImageOutputToggle(event) {
    const target = event?.target;
    if (!(target instanceof HTMLInputElement) || !target.classList.contains("work-report-output-check")) return;
    if (!lastWorkReportResult || !Array.isArray(lastWorkReportResult.items)) return;
    const itemIndex = Number(target.getAttribute("data-item-index") || -1);
    const imageIndex = Number(target.getAttribute("data-image-index") || -1);
    const item = lastWorkReportResult.items.find((row) => Number(row?.index || 0) === itemIndex);
    if (!item || !Array.isArray(item.images) || !item.images[imageIndex]) return;
    item.images[imageIndex].include_in_output = !!target.checked;
    syncWorkReportItemOutputState(item);
    rerenderWorkReportPreview("미리보기에서 선택한 사진만 PDF에 출력됩니다.");
  }

  function handleWorkReportItemOutputToggle(event) {
    const target = event?.target;
    if (!(target instanceof HTMLInputElement) || !target.classList.contains("work-report-item-output-check")) return;
    if (!lastWorkReportResult || !Array.isArray(lastWorkReportResult.items)) return;
    const itemIndex = Number(target.getAttribute("data-item-index") || -1);
    const item = lastWorkReportResult.items.find((row) => Number(row?.index || 0) === itemIndex);
    if (!item) return;
    item.include_in_output = !!target.checked;
    rerenderWorkReportPreview("미리보기에서 체크된 항목만 PDF에 출력됩니다.");
  }

  function handleWorkReportUnmatchedOutputToggle(event) {
    const target = event?.target;
    if (!(target instanceof HTMLInputElement) || !target.classList.contains("work-report-unmatched-output-check")) return;
    if (!lastWorkReportResult || !Array.isArray(lastWorkReportResult.unmatched_images)) return;
    const unmatchedIndex = Number(target.getAttribute("data-unmatched-index") || -1);
    const row = lastWorkReportResult.unmatched_images[unmatchedIndex];
    if (!row) return;
    row.include_in_output = !!target.checked;
    rerenderWorkReportPreview("미매칭 이미지도 체크된 항목만 PDF에 출력됩니다.");
  }

  function handleWorkReportImageAssignmentChange(event) {
    const target = event?.target;
    if (!(target instanceof HTMLSelectElement) || !target.classList.contains("work-report-image-assignment")) return;
    if (!lastWorkReportResult) return;
    const changed = syncWorkReportPendingEditsFromDom();
    if (!changed) return;
    rerenderWorkReportPreview("이미지 연결 작업을 수동으로 조정했습니다. 필요하면 '매칭 수정 저장'으로 다음 매칭 개선 자료로 남길 수 있습니다.");
  }

  function handleWorkReportImageStageChange(event) {
    const target = event?.target;
    if (!(target instanceof HTMLSelectElement) || !target.classList.contains("work-report-image-stage")) return;
    if (!lastWorkReportResult) return;
    const changed = syncWorkReportPendingEditsFromDom();
    if (!changed) return;
    rerenderWorkReportPreview("이미지 단계값을 수정했습니다. 현재 보정 결과로 PDF가 생성됩니다.");
  }

  function handleWorkReportImagePreviewToggle(event) {
    const trigger = event?.target instanceof HTMLElement ? event.target.closest(".work-report-image-preview-toggle") : null;
    if (!(trigger instanceof HTMLElement)) return;
    syncWorkReportPendingEditsFromDom();
    const imageIndex = Number(trigger.getAttribute("data-image-preview-index") || 0);
    if (imageIndex <= 0) return;
    activeWorkReportPreviewIndex = activeWorkReportPreviewIndex === imageIndex ? 0 : imageIndex;
    rerenderWorkReportPreview();
  }

  function handleWorkReportReviewApply(event) {
    const trigger = event?.target instanceof HTMLElement ? event.target.closest(".work-report-review-option, .work-report-review-unmatched") : null;
    if (!(trigger instanceof HTMLElement) || !lastWorkReportResult) return;
    const sourceType = String(trigger.getAttribute("data-source-type") || "item");
    const itemIndex = Number(trigger.getAttribute("data-item-index") || -1);
    const imageIndex = Number(trigger.getAttribute("data-image-index") || -1);
    const unmatchedIndex = Number(trigger.getAttribute("data-unmatched-index") || -1);
    const destinationItemIndex = String(trigger.getAttribute("data-destination-item-index") || "__unmatched__");
    const changed = moveWorkReportImageRecord(sourceType, itemIndex, imageIndex, unmatchedIndex, destinationItemIndex);
    if (!changed) return;
    rerenderWorkReportPreview("추천 후보를 적용했습니다. 이 선택도 다음 매칭 개선용 피드백에 포함됩니다.");
  }

  function handleWorkReportReviewConfirm(event) {
    const trigger = event?.target instanceof HTMLElement ? event.target.closest(".work-report-review-confirm") : null;
    if (!(trigger instanceof HTMLElement) || !lastWorkReportResult) return;
    const sourceType = String(trigger.getAttribute("data-source-type") || "item");
    const itemIndex = Number(trigger.getAttribute("data-item-index") || -1);
    const imageIndex = Number(trigger.getAttribute("data-image-index") || -1);
    const unmatchedIndex = Number(trigger.getAttribute("data-unmatched-index") || -1);
    const changed = confirmWorkReportImageReview(sourceType, itemIndex, imageIndex, unmatchedIndex);
    if (!changed) return;
    rerenderWorkReportPreview("현재 선택을 사람 검토 결과로 확정했습니다. 이 판단도 다음 매칭 개선 자료로 저장됩니다.");
  }

  async function handleWorkReportFeedbackSave(event) {
    const target = event?.target;
    if (!(target instanceof HTMLElement) || !target.closest(".work-report-feedback-save, .work-report-image-save")) return;
    event.preventDefault();
    await saveWorkReportFeedback();
  }

  function workReportFormData(options = {}) {
    const includeCachedReport = !!options.includeCachedReport;
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const text = String($("#chatInput")?.value || "").trim();
    const images = selectedFiles("#chatImageInput");
    const attachments = selectedFiles("#workReportFileInput");
    const sourceFiles = selectedFiles("#workReportSourceInput");
    const sampleFile = selectedSingleFile("#workReportSampleInput");
    if (!text && !sourceFiles.length && !images.length && !attachments.length) {
      throw new Error("카톡 대화, 원문 파일, 이미지, 첨부파일 중 하나 이상을 입력하세요.");
    }
    const fd = new FormData();
    fd.append("tenant_id", tenantId);
    fd.append("text", text);
    sourceFiles.forEach((file) => fd.append("source_files", file, file.name || "source"));
    images.forEach((file) => fd.append("images", file, file.name || "work-image"));
    attachments.forEach((file) => fd.append("attachments", file, file.name || "attachment"));
    if (sampleFile) {
      fd.append("sample_file", sampleFile, sampleFile.name || "sample");
    }
    if (includeCachedReport && lastWorkReportResult) {
      fd.append("report_json", JSON.stringify(cloneWorkReportForPdf(lastWorkReportResult)));
    }
    return fd;
  }

  function delay(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, Math.max(0, Number(ms || 0))));
  }

  async function pollWorkReportPreviewJob(jobId, progress) {
    const tenantId = currentTenantId();
    if (!jobId) throw new Error("업무보고 배치 작업 ID를 찾을 수 없습니다.");
    for (;;) {
      const data = await authFetchJson(`/api/ai/work_report/jobs/${encodeURIComponent(jobId)}?tenant_id=${encodeURIComponent(tenantId)}`, {
        method: "GET",
        timeoutMs: 60000,
      });
      const job = data.item || {};
      progress.sync({
        elapsedSec: job.elapsed_sec,
        currentStep: job.current_step,
        summary: job.summary || "배치 작업 진행 중입니다.",
        hint: job.hint || "",
      });
      if (String(job.status || "") === "completed") {
        return job.result || null;
      }
      if (String(job.status || "") === "failed") {
        throw new Error(String(job.error_message || job.summary || "업무보고 배치 작업이 실패했습니다."));
      }
      await delay(Number(job.poll_after_ms || 2000));
    }
  }

  async function digestChat() {
    const tenantId = currentTenantId();
    const text = String($("#chatInput").value || "").trim();
    const { files, autogenerated } = await resolveChatDigestFiles(text);
    if (!text && !files.length) throw new Error("카톡 대화 또는 이미지를 입력하세요.");
    $("#chatDigestBox").textContent = "카톡 분석 중입니다...";

    let data;
    if (files.length) {
      const fd = new FormData();
      fd.append("tenant_id", tenantId);
      fd.append("text", text);
      for (const file of files) {
        fd.append("files", file, file.name || "chat-image");
      }
      data = await authFetchJson("/api/ai/kakao_digest/images", {
        method: "POST",
        body: fd,
      });
    } else {
      data = await api("/api/ai/kakao_digest", {
        method: "POST",
        body: JSON.stringify({ tenant_id: tenantId, text }),
      });
    }
    lastDigestResult = data.item || null;
    lastDigestImported = false;
    lastDigestSelectedKeys = new Set((lastDigestResult?.excel_rows || []).map((row) => digestRowKey(row)));
    updateDigestImportState();
    applyDigestLeadToIntake(lastDigestResult);
    $("#chatDigestBox").innerHTML = renderChatDigestResult(lastDigestResult);
    wireDigestResultControls();
    if (autogenerated && files.length) {
      setMessage("#intakeMsg", `카톡 원문을 PNG ${files.length}장으로 자동 저장해 함께 분석했습니다.`);
      return;
    }
    const extractedCount = Number(data.item?.total || 0);
    if (extractedCount > 1) {
      setMessage("#intakeMsg", `카톡 분석으로 민원 ${extractedCount}건을 찾았습니다. 체크된 항목만 선택 등록하세요.`);
    } else if (extractedCount === 1) {
      setMessage("#intakeMsg", "카톡 분석으로 민원 1건을 찾았습니다. 민원 입력폼에 자동 채웠습니다.");
    } else if (data.item?.analysis_notice) {
      setMessage("#intakeMsg", String(data.item.analysis_notice), true);
    }
  }

  async function downloadDigestPdf() {
    const tenantId = currentTenantId();
    const text = String($("#chatInput").value || "").trim();
    const { files, autogenerated } = await resolveChatDigestFiles(text);
    if (!text && !files.length) throw new Error("카톡 대화 또는 이미지를 입력하세요.");

    const fd = new FormData();
    fd.append("tenant_id", tenantId);
    fd.append("text", text);
    for (const file of files) {
      fd.append("files", file, file.name || "chat-image");
    }
    const response = await authFetchBlob("/api/ai/kakao_digest/pdf", {
      method: "POST",
      body: fd,
    });
    downloadBlob(response.blob, response.filename || `kakao-digest-${tenantId || "report"}.pdf`);
    if (autogenerated && files.length) {
      setMessage("#intakeMsg", `카톡 원문을 PNG ${files.length}장으로 자동 저장한 뒤 PDF를 생성했습니다.`);
    }
  }

  async function analyzeWorkReport() {
    const progress = startWorkReportProgress("preview");
    try {
      const created = await authFetchJson("/api/ai/work_report/jobs", {
        method: "POST",
        body: workReportFormData(),
        timeoutMs: WORK_REPORT_REQUEST_TIMEOUT_MS,
      });
      const job = created.item || {};
      lastWorkReportJobId = String(job.id || "");
      progress.sync({
        elapsedSec: job.elapsed_sec,
        currentStep: job.current_step,
        summary: job.summary || "배치 작업을 시작했습니다.",
        hint: job.hint || "",
      });
      const result = await pollWorkReportPreviewJob(String(job.id || ""), progress);
      lastWorkReportResult = result || null;
      lastWorkReportBaseline = deepCloneJson(result || null);
      lastWorkReportFeedbackSavedSignature = "";
      activeWorkReportPreviewIndex = 0;
      $("#workReportBox").innerHTML = renderWorkReportResult(lastWorkReportResult);
      const previewNotice = String(lastWorkReportResult?.analysis_notice || "").trim();
      const previewMessage = `미리보기에서 작업 ${Number(lastWorkReportResult?.item_count || 0)}건을 정리했습니다.`;
      setMessage("#intakeMsg", previewNotice ? `${previewMessage} ${previewNotice}` : previewMessage, !!previewNotice);
    } catch (error) {
      progress.fail(error);
      throw error;
    } finally {
      progress.stop();
    }
  }

  async function downloadWorkReportPdf() {
    if (!lastWorkReportResult) {
      throw new Error("먼저 미리보기를 생성한 뒤, 미리보기에서 출력할 사진을 선택해 주세요.");
    }
    let savedFeedbackCount = 0;
    try {
      savedFeedbackCount = await saveWorkReportFeedback({ silent: true });
    } catch (error) {
      setMessage("#intakeMsg", `PDF 생성은 계속 진행하지만 이미지 매칭 수정 저장은 실패했습니다. ${error.message || String(error)}`, true);
    }
    const progress = startWorkReportProgress("pdf");
    try {
      const response = await authFetchBlob("/api/ai/work_report/pdf", {
        method: "POST",
        body: workReportFormData({ includeCachedReport: true }),
        timeoutMs: WORK_REPORT_REQUEST_TIMEOUT_MS,
      });
      downloadBlob(response.blob, response.filename || "work-report.pdf");
      progress.complete("PDF 파일 생성을 완료했습니다. 다운로드된 파일을 확인해 주세요.");
      if (savedFeedbackCount > 0) {
        setMessage("#intakeMsg", `이미지 매칭 수정 ${savedFeedbackCount}건을 저장하고 주요업무보고 PDF를 생성했습니다.`);
      } else {
        setMessage("#intakeMsg", "주요업무보고 PDF를 생성했습니다. 이 버튼만 사용하면 됩니다.");
      }
    } catch (error) {
      progress.fail(error);
      throw error;
    } finally {
      progress.stop();
    }
  }

  async function createTenant() {
    const payload = {
      tenant_id: String($("#newTenantId").value || "").trim(),
      name: String($("#newTenantName").value || "").trim(),
      site_code: String($("#newTenantSiteCode").value || "").trim(),
      site_name: String($("#newTenantSiteName").value || "").trim(),
    };
    const data = await api("/api/admin/tenants", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    $("#apiKeyBox").textContent = `신규 API Key (${data.item.id}): ${data.item.api_key}`;
    setMessage("#adminMsg", "테넌트를 생성했습니다.");
    ["#newTenantId", "#newTenantName", "#newTenantSiteCode", "#newTenantSiteName"].forEach((sel) => {
      const el = $(sel);
      if (el) el.value = "";
    });
    await loadTenants();
  }

  function clearUserCreateForm() {
    ["#newUserLoginId", "#newUserName", "#newUserPhone", "#newUserPassword", "#newUserNote"].forEach((sel) => {
      const el = $(sel);
      if (el) el.value = "";
    });
    $("#newUserIsSiteAdmin").checked = false;
    renderRoleOptions("#newUserRole", "desk");
  }

  function clearSelectedUserEditor() {
    selectedUserId = 0;
    selectedUser = null;
    $("#userDetail").textContent = "사용자를 선택하세요.";
    ["#editUserName", "#editUserPhone", "#editUserNote", "#resetUserPassword"].forEach((sel) => {
      const el = $(sel);
      if (el) el.value = "";
    });
    $("#editUserActive").checked = true;
    $("#editUserIsSiteAdmin").checked = false;
    renderRoleOptions("#editUserRole", "desk");
  }

  function renderUserDetail(user) {
    selectedUserId = Number(user.id || 0);
    selectedUser = user;
    $("#userDetail").innerHTML = [
      `<strong>${escapeHtml(user.name || user.login_id || "-")}</strong>`,
      `아이디: ${escapeHtml(user.login_id || "-")}`,
      `권한: ${escapeHtml(roleLabel(user))}`,
      `연락처: ${escapeHtml(user.phone || "-")}`,
      `상태: ${escapeHtml(userStatusLabel(user))}`,
      `최근 로그인: ${escapeHtml(formatDateTime(user.last_login_at))}`,
      `메모: ${escapeHtml(user.note || "-")}`,
    ].join("<br>");
    $("#editUserName").value = String(user.name || "");
    $("#editUserPhone").value = String(user.phone || "");
    $("#editUserNote").value = String(user.note || "");
    $("#editUserActive").checked = !!user.is_active;
    $("#editUserIsSiteAdmin").checked = !!user.is_site_admin;
    renderRoleOptions("#editUserRole", String(user.role || "desk"));
    $("#btnApproveUser")?.toggleAttribute("disabled", !isPendingApproval(user));
  }

  function renderUsersTable() {
    const body = $("#usersTableBody");
    if (!body) return;
    const pendingUsers = users.filter((user) => isPendingApproval(user));
    const hint = $("#pendingUsersHint");
    if (hint) {
      hint.textContent = pendingUsers.length
        ? `승인대기 회원 ${pendingUsers.length}건이 있습니다. 목록에서 바로 승인하거나 선택 후 승인할 수 있습니다.`
        : "승인대기 회원이 없습니다.";
    }
    body.innerHTML = users.length
      ? users.map((user) => `
        <tr class="user-row" data-id="${Number(user.id || 0)}">
          <td class="mono">${escapeHtml(user.login_id || "")}</td>
          <td>${escapeHtml(user.name || "")}</td>
          <td>${escapeHtml(roleLabel(user))}</td>
          <td>${escapeHtml(user.phone || "-")}</td>
          <td>${escapeHtml(userStatusLabel(user))}</td>
          <td>${escapeHtml(formatDateTime(user.last_login_at))}</td>
          <td class="table-actions">
            <button class="ghost-btn user-select" type="button" data-id="${Number(user.id || 0)}">선택</button>
            ${isPendingApproval(user) ? `<button class="action-btn action-secondary user-approve" type="button" data-id="${Number(user.id || 0)}">승인</button>` : ""}
          </td>
        </tr>
      `).join("")
      : '<tr><td colspan="7" class="empty-state">조회된 사용자가 없습니다.</td></tr>';
    body.querySelectorAll(".user-select").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const userId = Number(btn.getAttribute("data-id") || 0);
        await loadUser(userId);
      });
    });
    body.querySelectorAll(".user-approve").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const userId = Number(btn.getAttribute("data-id") || 0);
        await approveUser(userId);
      });
    });
  }

  async function loadUsers() {
    if (!canManageUsers()) return [];
    syncUserTenantDisplay();
    const tenantId = currentTenantId();
    const params = new URLSearchParams();
    params.set("active_only", "false");
    if (tenantId) params.set("tenant_id", tenantId);
    const data = await api(`/api/users?${params.toString()}`);
    users = Array.isArray(data.items) ? data.items : [];
    renderUsersTable();
    if (selectedUserId) {
      const found = users.find((item) => Number(item.id || 0) === selectedUserId);
      if (found) {
        renderUserDetail(found);
      } else {
        clearSelectedUserEditor();
      }
    }
    return users;
  }

  async function loadUser(userId) {
    if (!userId) throw new Error("사용자를 먼저 선택하세요.");
    const data = await api(`/api/users/${userId}`);
    renderUserDetail(data.item || {});
  }

  async function createUser() {
    const tenantId = currentTenantId();
    if (!tenantId && !isAdmin()) throw new Error("작업할 테넌트가 없습니다.");
    const payload = {
      tenant_id: tenantId,
      login_id: String($("#newUserLoginId").value || "").trim().toLowerCase(),
      name: String($("#newUserName").value || "").trim(),
      role: String($("#newUserRole").value || "desk").trim(),
      phone: String($("#newUserPhone").value || "").trim(),
      password: String($("#newUserPassword").value || ""),
      note: String($("#newUserNote").value || "").trim(),
    };
    if (isAdmin()) {
      payload.is_site_admin = !!$("#newUserIsSiteAdmin").checked;
    }
    const data = await api("/api/users", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    clearUserCreateForm();
    setMessage("#usersMsg", `${data.item?.login_id || "새 사용자"} 계정을 등록했습니다.`);
    await loadUsers();
  }

  async function updateUser() {
    if (!selectedUserId) throw new Error("수정할 사용자를 선택하세요.");
    const payload = {
      name: String($("#editUserName").value || "").trim(),
      role: String($("#editUserRole").value || "desk").trim(),
      phone: String($("#editUserPhone").value || "").trim(),
      note: String($("#editUserNote").value || "").trim(),
      is_active: !!$("#editUserActive").checked,
    };
    if (isAdmin()) {
      payload.is_site_admin = !!$("#editUserIsSiteAdmin").checked;
    }
    const data = await api(`/api/users/${selectedUserId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
    setMessage("#usersMsg", `${data.item?.login_id || "사용자"} 정보를 저장했습니다.`);
    await loadUsers();
    await loadUser(selectedUserId);
  }

  async function approveUser(userId = selectedUserId) {
    if (!userId) throw new Error("승인할 사용자를 선택하세요.");
    const data = await api(`/api/users/${userId}/approve`, {
      method: "POST",
    });
    setMessage("#usersMsg", `${data.item?.login_id || "사용자"} 승인을 완료했습니다.`);
    await loadUsers();
    await loadUser(userId);
  }

  async function resetSelectedUserPassword() {
    if (!selectedUserId) throw new Error("사용자를 먼저 선택하세요.");
    const password = String($("#resetUserPassword").value || "");
    if (!password) throw new Error("초기화할 비밀번호를 입력하세요.");
    await api(`/api/users/${selectedUserId}/reset_password`, {
      method: "POST",
      body: JSON.stringify({ password }),
    });
    $("#resetUserPassword").value = "";
    setMessage("#usersMsg", "비밀번호를 초기화했습니다.");
  }

  async function deleteSelectedUser() {
    if (!selectedUserId || !selectedUser) throw new Error("삭제할 사용자를 선택하세요.");
    if (!window.confirm(`${selectedUser.login_id} 계정을 삭제하시겠습니까?`)) return;
    await api(`/api/users/${selectedUserId}`, {
      method: "DELETE",
    });
    setMessage("#usersMsg", `${selectedUser.login_id} 계정을 삭제했습니다.`);
    clearSelectedUserEditor();
    await loadUsers();
  }

  async function reloadAll() {
    await loadBuildInfo();
    await loadDocumentCatalog();
    await loadDashboard();
    await loadComplaints();
    await generateReport();
    await loadInfoDashboard();
    await loadInfoBuildings();
    await loadInfoRegistrations();
    await loadFacilityDashboard();
    await loadFacilityAssets();
    await loadFacilityChecklists();
    await loadFacilityInspections();
    await loadFacilityWorkOrders();
    await loadOpsDashboard();
    await loadOpsNotices();
    await loadDocumentNumberingConfig();
    await loadOpsDocuments();
    await loadOpsVendors();
    await loadOpsSchedules();
    if (canManageUsers()) {
      await loadUsers();
    }
  }

  function wire() {
    wireMobilePanels();
    document.querySelectorAll(".mobile-intake-step").forEach((button) => {
      button.addEventListener("click", () => setCurrentIntakeStep(button.getAttribute("data-step") || 1));
    });
    $("#btnIntakePrev")?.addEventListener("click", () => {
      try {
        moveIntakeStep(-1);
      } catch (error) {
        setMessage("#intakeMsg", error.message || String(error), true);
      }
    });
    $("#btnIntakeNext")?.addEventListener("click", () => {
      try {
        moveIntakeStep(1);
      } catch (error) {
        setMessage("#intakeMsg", error.message || String(error), true);
      }
    });
    document.querySelectorAll(".mobile-dock-btn").forEach((button) => {
      button.addEventListener("click", () => {
        const workspace = button.getAttribute("data-workspace");
        if (isMobileViewport() && workspace && workspace !== "top") {
          setMobileWorkspace(workspace, true);
          return;
        }
        const target = document.querySelector(String(button.getAttribute("data-target") || ""));
        if (!target) return;
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    });
    window.addEventListener("scroll", syncMobileDockState, { passive: true });
    window.addEventListener("resize", () => {
      applyMobileWorkspace(false);
      applyAllMobilePanelGroups();
      syncMobileDockState();
      syncMobileIntakeStep();
      syncMobileCompactStacks();
    }, { passive: true });
    $("#btnLogout")?.addEventListener("click", () => window.KAAuth.logout());
    $("#btnReloadAll")?.addEventListener("click", () => reloadAll().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnClassify")?.addEventListener("click", () => {
      setMessage("#intakeMsg", "");
      classifyCurrentText().catch((error) => setMessage("#intakeMsg", error.message || String(error), true));
    });
    $("#btnCreateComplaint")?.addEventListener("click", () => {
      setMessage("#intakeMsg", "");
      createComplaint().catch((error) => setMessage("#intakeMsg", error.message || String(error), true));
    });
    $("#btnRefreshDashboard")?.addEventListener("click", () => loadDashboard().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnLoadComplaints")?.addEventListener("click", () => loadComplaints().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnUpdateComplaint")?.addEventListener("click", () => updateSelectedComplaint().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnAddAttachments")?.addEventListener("click", () => uploadDetailAttachments().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDeleteSelectedAttachments")?.addEventListener("click", () => deleteAttachments(false).catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDeleteAllAttachments")?.addEventListener("click", () => deleteAttachments(true).catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnGenerateReport")?.addEventListener("click", () => generateReport().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDigestChat")?.addEventListener("click", () => digestChat().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnImportDigestComplaints")?.addEventListener("click", () => importDigestComplaints().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDigestPdf")?.addEventListener("click", () => downloadDigestPdf().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnPreviewDigestSource")?.addEventListener("click", () => previewChatSourceImages().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnDownloadDigestSource")?.addEventListener("click", () => downloadChatSourceImages().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnPasteDigestImage")?.addEventListener("click", () => importClipboardImages().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#workReportBox")?.addEventListener("change", (event) => {
      handleWorkReportItemOutputToggle(event);
      handleWorkReportImageOutputToggle(event);
      handleWorkReportUnmatchedOutputToggle(event);
      handleWorkReportImageAssignmentChange(event);
      handleWorkReportImageStageChange(event);
    });
    $("#workReportBox")?.addEventListener("click", (event) => {
      handleWorkReportImagePreviewToggle(event);
      handleWorkReportReviewApply(event);
      handleWorkReportReviewConfirm(event);
      handleWorkReportFeedbackSave(event).catch((error) => setMessage("#intakeMsg", error.message || String(error), true));
    });
    $("#btnAnalyzeWorkReport")?.addEventListener("click", () => analyzeWorkReport().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnWorkReportPdf")?.addEventListener("click", () => downloadWorkReportPdf().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#btnLoadFacilityDashboard")?.addEventListener("click", () => loadFacilityDashboard().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnLoadFacilityAssets")?.addEventListener("click", () => loadFacilityAssets().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnResetFacilityAssetFilters")?.addEventListener("click", () => resetFacilityAssetFilters().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnLoadInfoDashboard")?.addEventListener("click", () => loadInfoDashboard().catch((error) => setMessage("#infoBuildingMsg", error.message || String(error), true)));
    $("#btnCreateInfoBuilding")?.addEventListener("click", () => createInfoBuilding().catch((error) => setMessage("#infoBuildingMsg", error.message || String(error), true)));
    $("#btnUpdateInfoBuilding")?.addEventListener("click", () => updateInfoBuilding().catch((error) => setMessage("#infoBuildingMsg", error.message || String(error), true)));
    $("#btnDeleteInfoBuilding")?.addEventListener("click", () => deleteInfoBuilding().catch((error) => setMessage("#infoBuildingMsg", error.message || String(error), true)));
    $("#btnClearInfoBuilding")?.addEventListener("click", () => clearInfoBuildingForm());
    $("#btnCreateInfoRegistration")?.addEventListener("click", () => createInfoRegistration().catch((error) => setMessage("#infoRegistrationMsg", error.message || String(error), true)));
    $("#btnUpdateInfoRegistration")?.addEventListener("click", () => updateInfoRegistration().catch((error) => setMessage("#infoRegistrationMsg", error.message || String(error), true)));
    $("#btnDeleteInfoRegistration")?.addEventListener("click", () => deleteInfoRegistration().catch((error) => setMessage("#infoRegistrationMsg", error.message || String(error), true)));
    $("#btnClearInfoRegistration")?.addEventListener("click", () => clearInfoRegistrationForm());
    $("#btnCreateFacilityAsset")?.addEventListener("click", () => createFacilityAsset().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnUpdateFacilityAsset")?.addEventListener("click", () => updateFacilityAsset().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnDeleteFacilityAsset")?.addEventListener("click", () => deleteFacilityAsset().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnClearFacilityAsset")?.addEventListener("click", () => clearFacilityAssetForm());
    $("#btnFacilityAssetCamera")?.addEventListener("click", () => $("#facilityAssetCameraInput")?.click());
    $("#btnFacilityAssetFile")?.addEventListener("click", () => $("#facilityAssetFileInput")?.click());
    $("#btnFacilityAssetClearImageSelection")?.addEventListener("click", () => clearPendingFacilityAssetImages());
    $("#facilityAssetImagePreview")?.addEventListener("click", (event) => handleFacilityAssetPreviewAction(event).catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#facilityAssetFilterCategory")?.addEventListener("change", () => loadFacilityAssets().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#facilityAssetFilterState")?.addEventListener("change", () => loadFacilityAssets().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnCreateFacilityChecklist")?.addEventListener("click", () => createFacilityChecklist().catch((error) => setMessage("#facilityChecklistMsg", error.message || String(error), true)));
    $("#btnUpdateFacilityChecklist")?.addEventListener("click", () => updateFacilityChecklist().catch((error) => setMessage("#facilityChecklistMsg", error.message || String(error), true)));
    $("#btnDeleteFacilityChecklist")?.addEventListener("click", () => deleteFacilityChecklist().catch((error) => setMessage("#facilityChecklistMsg", error.message || String(error), true)));
    $("#btnClearFacilityChecklist")?.addEventListener("click", () => clearFacilityChecklistForm());
    $("#btnCreateFacilityInspection")?.addEventListener("click", () => createFacilityInspection().catch((error) => setMessage("#facilityInspectionMsg", error.message || String(error), true)));
    $("#btnUpdateFacilityInspection")?.addEventListener("click", () => updateFacilityInspection().catch((error) => setMessage("#facilityInspectionMsg", error.message || String(error), true)));
    $("#btnIssueFacilityWorkOrder")?.addEventListener("click", () => issueFacilityWorkOrder().catch((error) => setMessage("#facilityInspectionMsg", error.message || String(error), true)));
    $("#btnDeleteFacilityInspection")?.addEventListener("click", () => deleteFacilityInspection().catch((error) => setMessage("#facilityInspectionMsg", error.message || String(error), true)));
    $("#btnClearFacilityInspection")?.addEventListener("click", () => clearFacilityInspectionForm());
    $("#btnCreateFacilityWorkOrder")?.addEventListener("click", () => createFacilityWorkOrder().catch((error) => setMessage("#facilityWorkOrderMsg", error.message || String(error), true)));
    $("#btnUpdateFacilityWorkOrder")?.addEventListener("click", () => updateFacilityWorkOrder().catch((error) => setMessage("#facilityWorkOrderMsg", error.message || String(error), true)));
    $("#btnCreateComplaintFromWorkOrder")?.addEventListener("click", () => createComplaintFromFacilityWorkOrder().catch((error) => setMessage("#facilityWorkOrderMsg", error.message || String(error), true)));
    $("#btnDeleteFacilityWorkOrder")?.addEventListener("click", () => deleteFacilityWorkOrder().catch((error) => setMessage("#facilityWorkOrderMsg", error.message || String(error), true)));
    $("#btnClearFacilityWorkOrder")?.addEventListener("click", () => clearFacilityWorkOrderForm());
    $("#btnLoadOpsDashboard")?.addEventListener("click", () => loadOpsDashboard().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnCreateNotice")?.addEventListener("click", () => createNotice().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnUpdateNotice")?.addEventListener("click", () => updateNotice().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnDeleteNotice")?.addEventListener("click", () => deleteNotice().catch((error) => setMessage("#opsNoticeMsg", error.message || String(error), true)));
    $("#btnClearNotice")?.addEventListener("click", () => clearNoticeForm());
    $("#btnCreateDocument")?.addEventListener("click", () => createDocument().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnUpdateDocument")?.addEventListener("click", () => updateDocument().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnDeleteDocument")?.addEventListener("click", () => deleteDocument().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnAutoDocumentRefNo")?.addEventListener("click", () => fillNextDocumentReference().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnLoadDocNumberingConfig")?.addEventListener("click", () => loadDocumentNumberingConfig().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnSaveDocNumberingConfig")?.addEventListener("click", () => saveDocumentNumberingConfig().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnResetDocNumberingConfig")?.addEventListener("click", () => resetDocumentNumberingConfig().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnRenderDocumentPdf")?.addEventListener("click", () => renderDocumentPdf().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnSampleDocumentPdf")?.addEventListener("click", () => renderSampleDocumentPdf().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnExportDocumentLedger")?.addEventListener("click", () => exportDocumentLedgerExcel().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#btnClearDocument")?.addEventListener("click", () => clearDocumentForm());
    $("#documentCategoryFilter")?.addEventListener("change", () => loadOpsDocuments().catch((error) => setMessage("#opsDocumentMsg", error.message || String(error), true)));
    $("#documentCategory")?.addEventListener("change", () => renderDocumentCategoryGuide($("#documentCategory")?.value || defaultDocumentCategory()));
    $("#filterBuilding")?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        loadComplaints().catch((error) => setMessage("#intakeMsg", error.message || String(error), true));
      }
    });
    $("#facilityAssetSearchQuery")?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        loadFacilityAssets().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true));
      }
    });
    $("#btnCreateVendor")?.addEventListener("click", () => createVendor().catch((error) => setMessage("#opsVendorMsg", error.message || String(error), true)));
    $("#btnUpdateVendor")?.addEventListener("click", () => updateVendor().catch((error) => setMessage("#opsVendorMsg", error.message || String(error), true)));
    $("#btnDeleteVendor")?.addEventListener("click", () => deleteVendor().catch((error) => setMessage("#opsVendorMsg", error.message || String(error), true)));
    $("#btnClearVendor")?.addEventListener("click", () => clearVendorForm());
    $("#btnCreateSchedule")?.addEventListener("click", () => createSchedule().catch((error) => setMessage("#opsScheduleMsg", error.message || String(error), true)));
    $("#btnUpdateSchedule")?.addEventListener("click", () => updateSchedule().catch((error) => setMessage("#opsScheduleMsg", error.message || String(error), true)));
    $("#btnDeleteSchedule")?.addEventListener("click", () => deleteSchedule().catch((error) => setMessage("#opsScheduleMsg", error.message || String(error), true)));
    $("#btnClearSchedule")?.addEventListener("click", () => clearScheduleForm());
    $("#btnCreateTenant")?.addEventListener("click", () => createTenant().catch((error) => setMessage("#adminMsg", error.message || String(error), true)));
    $("#btnLoadTenants")?.addEventListener("click", () => loadTenants().catch((error) => setMessage("#adminMsg", error.message || String(error), true)));
    $("#btnLoadUsers")?.addEventListener("click", () => loadUsers().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnCreateUser")?.addEventListener("click", () => createUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnClearUserForm")?.addEventListener("click", () => clearUserCreateForm());
    $("#btnApproveUser")?.addEventListener("click", () => approveUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnUpdateUser")?.addEventListener("click", () => updateUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnResetUserPassword")?.addEventListener("click", () => resetSelectedUserPassword().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#btnDeleteUser")?.addEventListener("click", () => deleteSelectedUser().catch((error) => setMessage("#usersMsg", error.message || String(error), true)));
    $("#tenantSelect")?.addEventListener("change", () => reloadAll().catch((error) => setMessage("#intakeMsg", error.message || String(error), true)));
    $("#photoInput")?.addEventListener("change", () => {
      updatePhotoHint("#photoInput", "#photoHint");
      updateIntakeReview();
    });
    $("#facilityAssetCameraInput")?.addEventListener("change", (event) => {
      try {
        const result = queueFacilityAssetImages(event?.target?.files, "촬영한 이미지");
        if (result.added) {
          const notes = [];
          if (result.duplicates) notes.push(`중복 ${result.duplicates}장 제외`);
          if (result.overflow) notes.push(`최대 ${MAX_FACILITY_ASSET_IMAGES}장 제한으로 ${result.overflow}장 제외`);
          setMessage("#facilityAssetMsg", notes.length ? `이미지 ${result.added}장을 추가했습니다. ${notes.join(", ")}.` : `이미지 ${result.added}장을 추가했습니다.`);
        }
      } catch (error) {
        setMessage("#facilityAssetMsg", error.message || String(error), true);
      }
    });
    $("#facilityAssetFileInput")?.addEventListener("change", (event) => {
      try {
        const result = queueFacilityAssetImages(event?.target?.files, "선택한 파일 이미지");
        if (result.added) {
          const notes = [];
          if (result.duplicates) notes.push(`중복 ${result.duplicates}장 제외`);
          if (result.overflow) notes.push(`최대 ${MAX_FACILITY_ASSET_IMAGES}장 제한으로 ${result.overflow}장 제외`);
          setMessage("#facilityAssetMsg", notes.length ? `이미지 ${result.added}장을 추가했습니다. ${notes.join(", ")}.` : `이미지 ${result.added}장을 추가했습니다.`);
        }
      } catch (error) {
        setMessage("#facilityAssetMsg", error.message || String(error), true);
      }
    });
    ["#buildingInput", "#unitInput", "#channelInput", "#managerInput", "#phoneInput", "#contentInput"].forEach((selector) => {
      const el = $(selector);
      if (!el) return;
      el.addEventListener("input", () => updateIntakeReview());
      el.addEventListener("change", () => updateIntakeReview());
    });
    $("#chatImageInput")?.addEventListener("change", () => {
      const files = selectedFiles("#chatImageInput");
      if (files.length > MAX_WORK_REPORT_IMAGES) {
        setInputFiles("#chatImageInput", files.slice(0, MAX_WORK_REPORT_IMAGES));
        setMessage("#intakeMsg", `현장 사진은 최대 ${MAX_WORK_REPORT_IMAGES}장까지 선택할 수 있습니다. 초과분은 제외했습니다.`, true);
      }
      invalidateWorkReportCache();
      clearChatSourcePreview();
      resetDigestImportState();
      updateChatDigestHint();
    });
    $("#workReportFileInput")?.addEventListener("change", () => {
      invalidateWorkReportCache();
      syncWorkReportAttachmentSelection();
    });
    $("#workReportSourceInput")?.addEventListener("change", () => {
      const files = selectedFiles("#workReportSourceInput");
      if (files.length > MAX_WORK_REPORT_SOURCE_FILES) {
        setInputFiles("#workReportSourceInput", files.slice(0, MAX_WORK_REPORT_SOURCE_FILES));
        setMessage("#intakeMsg", `카톡 원문 파일은 최대 ${MAX_WORK_REPORT_SOURCE_FILES}개까지 선택할 수 있습니다. 초과분은 제외했습니다.`, true);
      }
      invalidateWorkReportCache();
      syncWorkReportSourceSelection();
      updateChatDigestHint();
    });
    $("#workReportSampleInput")?.addEventListener("change", () => {
      invalidateWorkReportCache();
      syncWorkReportSampleSelection();
    });
    $("#chatInput")?.addEventListener("input", () => {
      invalidateWorkReportCache();
      clearChatSourcePreview();
      resetDigestImportState();
      updateChatDigestHint();
    });
    $("#chatInput")?.addEventListener("paste", (event) => handleChatInputPaste(event));
    $("#chatPasteZone")?.addEventListener("click", () => {
      setChatPasteZoneState(true, false);
      $("#chatPasteZone")?.focus();
    });
    $("#chatPasteZone")?.addEventListener("focus", () => setChatPasteZoneState(true, false));
    $("#chatPasteZone")?.addEventListener("blur", () => setChatPasteZoneState(false, false));
    $("#chatPasteZone")?.addEventListener("paste", (event) => handleChatInputPaste(event));
    $("#chatPasteZone")?.addEventListener("dragenter", (event) => {
      event.preventDefault();
      setChatPasteZoneState(true, true);
    });
    $("#chatPasteZone")?.addEventListener("dragover", (event) => {
      event.preventDefault();
      setChatPasteZoneState(true, true);
    });
    $("#chatPasteZone")?.addEventListener("dragleave", () => setChatPasteZoneState(false, false));
    $("#chatPasteZone")?.addEventListener("drop", (event) => handleChatImageDrop(event));
    document.addEventListener("paste", (event) => {
      if (event.defaultPrevented || event.__kaChatPasteHandled) return;
      const active = document.activeElement;
      const zone = $("#chatPasteZone");
      const target = event.target;
      const insideChatDigest = [zone, $("#chatInput"), $("#chatImageInput")].some((el) => el && (target === el || active === el || (target instanceof Node && el.contains(target)) || (active instanceof Node && el.contains(active))));
      if (!insideChatDigest) return;
      handleChatInputPaste(event);
    });
    $("#attachmentSelectAll")?.addEventListener("change", (event) => {
      const checked = !!event.target.checked;
      document.querySelectorAll(".attachment-check").forEach((el) => {
        el.checked = checked;
      });
    });
    document.addEventListener("click", (event) => {
      const button = event.target.closest("[data-input-selector][data-file-index]");
      if (!button) return;
      event.preventDefault();
      const inputSelector = String(button.getAttribute("data-input-selector") || "").trim();
      const index = Number(button.getAttribute("data-file-index") || -1);
      if (!inputSelector || index < 0) return;
      removeInputFileAt(inputSelector, index);
      if (inputSelector === "#chatImageInput") {
        invalidateWorkReportCache();
        clearChatSourcePreview();
        resetDigestImportState();
        updateChatDigestHint();
        return;
      }
      if (inputSelector === "#workReportSourceInput") {
        invalidateWorkReportCache();
        syncWorkReportSourceSelection();
        updateChatDigestHint();
        return;
      }
      if (inputSelector === "#workReportSampleInput") {
        invalidateWorkReportCache();
        syncWorkReportSampleSelection();
        return;
      }
      if (inputSelector === "#workReportFileInput") {
        invalidateWorkReportCache();
        syncWorkReportAttachmentSelection();
      }
    });
  }

  async function init() {
    renderBuildInfoStrip();
    loadBuildInfo().catch(() => {});
    me = await api("/api/auth/me");
    renderRoleOptions("#newUserRole", "desk");
    renderRoleOptions("#editUserRole", "desk");
    renderDocumentCategoryOptions();
    renderDocumentCodeInputs();
    $("#btnApproveUser")?.toggleAttribute("disabled", true);
    applyHero();
    syncUserTenantDisplay();
    syncOpsWriteState();
    syncFacilityWriteState();
    syncComplaintDeleteOption();
    $("#filterStatus").value = "접수";
    clearChatDigestImagePreview();
    clearChatSourcePreview();
    resetDigestImportState();
    invalidateWorkReportCache();
    updateChatDigestHint();
    syncWorkReportAttachmentSelection();
    syncWorkReportSourceSelection();
    syncWorkReportSampleSelection();
    clearNoticeForm();
    clearComplaintDetail();
    updateIntakeReview();
    setCurrentIntakeStep(1);
    clearFacilityAssetForm();
    clearFacilityChecklistForm();
    clearFacilityInspectionForm();
    clearFacilityWorkOrderForm();
    clearDocumentForm();
    clearScheduleForm();
    clearVendorForm();
    clearInfoBuildingForm();
    clearInfoRegistrationForm();
    if (isAdmin()) {
      $("#tenantSelectWrap")?.classList.remove("hidden");
      $("#adminPanel")?.classList.remove("hidden");
      $("#newUserSiteAdminWrap")?.classList.remove("hidden");
      $("#editUserSiteAdminWrap")?.classList.remove("hidden");
      await loadTenants();
    }
    if (canManageUsers()) {
      $("#userPanel")?.classList.remove("hidden");
    }
    await reloadAll();
    setMobileWorkspace(currentMobileWorkspace, false);
    applyAllMobilePanelGroups();
    syncMobileDockState();
  }

  setupMobileCompactStacks();
  wire();
  init().catch((error) => {
    setMessage("#intakeMsg", error.message || String(error), true);
  });
})();
