(() => {
  "use strict";

  const $ = (selector) => document.querySelector(selector);
  const MAX_WORK_REPORT_IMAGES = 200;
  const MAX_WORK_REPORT_SOURCE_FILES = 20;
  const WORK_REPORT_TIMEOUT_MS = 1200000;
  const IMAGE_STAGE_OPTIONS = [
    { value: "general", label: "현장 이미지" },
    { value: "before", label: "작업 전" },
    { value: "during", label: "작업 중" },
    { value: "after", label: "작업 후" },
  ];

  let sessionMe = null;
  let tenants = [];
  let currentReport = null;
  let currentJobId = "";
  let sourceFilterBlocks = [];
  let sourceFilterSelectedKeys = new Set();
  let selectedSourceImageIndexes = new Set();
  let selectedSourceImagesInitialized = false;
  let localSourceImageUrls = [];
  let lastMatchedImageFiles = [];

  function setStatus(message, isError = false) {
    const el = $("#moduleStatus");
    if (!el) return;
    el.textContent = String(message || "");
    el.classList.toggle("error", !!isError);
  }

  function showProgress(message, visible = true) {
    const box = $("#moduleProgress");
    if (!box) return;
    box.textContent = String(message || "");
    box.classList.toggle("hidden", !visible);
  }

  function clearResult() {
    $("#moduleResult").textContent = "카톡 대화와 사진을 넣고 작업 항목 추출을 실행하세요.";
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatInlineFileSize(file) {
    const size = Number(file?.size || 0);
    if (!Number.isFinite(size) || size <= 0) return "";
    if (size >= 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)}MB`;
    if (size >= 1024) return `${Math.round(size / 1024)}KB`;
    return `${size}B`;
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
      if (file instanceof File) dt.items.add(file);
    }
    input.files = dt.files;
  }

  function removeInputFileAt(inputSelector, index) {
    const files = selectedFiles(inputSelector);
    if (index < 0 || index >= files.length) return;
    files.splice(index, 1);
    setInputFiles(inputSelector, files);
  }

  function renderFileList(inputSelector, targetSelector, kindLabel) {
    const box = $(targetSelector);
    if (!box) return;
    const files = selectedFiles(inputSelector);
    if (!files.length) {
      box.classList.add("hidden");
      box.innerHTML = "";
      return;
    }
    box.classList.remove("hidden");
    box.innerHTML = files.map((file, index) => [
      '<article class="work-report-file-card">',
      '<div>',
      `<strong>${escapeHtml(`${kindLabel} ${index + 1}`)}</strong>`,
      `<div class="meta">${escapeHtml(file.name || `${kindLabel}-${index + 1}`)}${formatInlineFileSize(file) ? ` · ${escapeHtml(formatInlineFileSize(file))}` : ""}</div>`,
      '</div>',
      `<button class="file-remove-btn" type="button" data-input-selector="${escapeHtml(inputSelector)}" data-file-index="${index}">삭제</button>`,
      '</article>',
    ].join("")).join("");
  }

  function syncSelectedFiles() {
    renderFileList("#moduleSourceInput", "#moduleSourceList", "원문 자료");
    renderFileList("#moduleImageInput", "#moduleImageList", "현장 사진");
    renderFileList("#moduleSampleInput", "#moduleSampleList", "샘플 파일");
    renderFileList("#moduleAttachmentInput", "#moduleAttachmentList", "첨부파일");
    const imageHint = $("#moduleImageHint");
    if (imageHint) {
      const count = selectedFiles("#moduleImageInput").length;
      imageHint.textContent = count ? `선택된 현장 사진 ${count}장` : "선택된 현장 사진이 없습니다.";
    }
  }

  function deepCloneJson(value) {
    return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
  }

  function revokeLocalSourceImageUrls() {
    for (const url of localSourceImageUrls) {
      try {
        URL.revokeObjectURL(url);
      } catch (_) {
        // ignore revoke errors
      }
    }
    localSourceImageUrls = [];
  }

  function resetSourceImageSelection() {
    selectedSourceImageIndexes = new Set();
    selectedSourceImagesInitialized = false;
    revokeLocalSourceImageUrls();
  }

  function ensureSelectedSourceImages() {
    const files = selectedFiles("#moduleImageInput");
    const next = new Set(
      Array.from(selectedSourceImageIndexes)
        .map((value) => Number(value))
        .filter((value) => Number.isInteger(value) && value >= 0 && value < files.length)
    );
    if (!selectedSourceImagesInitialized) {
      files.forEach((_file, index) => next.add(index));
      selectedSourceImagesInitialized = true;
    }
    selectedSourceImageIndexes = next;
    return files;
  }

  function currentSelectedSourceImages() {
    const files = ensureSelectedSourceImages();
    return files.filter((_file, index) => selectedSourceImageIndexes.has(index));
  }

  function setSelectedSourceImage(index, checked) {
    const cleanIndex = Number(index);
    if (!Number.isInteger(cleanIndex) || cleanIndex < 0) return;
    selectedSourceImagesInitialized = true;
    if (checked) selectedSourceImageIndexes.add(cleanIndex);
    else selectedSourceImageIndexes.delete(cleanIndex);
  }

  function resetSourceFilter() {
    sourceFilterBlocks = [];
    sourceFilterSelectedKeys = new Set();
    const box = $("#moduleSourceFilterBox");
    if (box) {
      box.classList.add("hidden");
      box.innerHTML = "";
    }
    const note = $("#moduleSourceFilterNote");
    if (note) {
      note.textContent = "불필요한 대화를 먼저 제외하면, 남긴 내용만으로 작업 항목을 추출합니다.";
    }
  }

  function buildSourceFilterBlocks(text) {
    return String(text || "")
      .split(/\r?\n/)
      .map((line, index) => ({ line: String(line || ""), lineNumber: index + 1 }))
      .filter((row) => row.line.trim())
      .map((row, index) => ({
        key: `line-${index + 1}`,
        text: row.line,
        lineNumber: row.lineNumber,
      }));
  }

  function currentSourceText() {
    const raw = String($("#moduleChatInput")?.value || "").trim();
    if (!sourceFilterBlocks.length || sourceFilterSelectedKeys.size >= sourceFilterBlocks.length) {
      return raw;
    }
    return sourceFilterBlocks
      .filter((row) => sourceFilterSelectedKeys.has(String(row.key || "")))
      .map((row) => String(row.text || ""))
      .join("\n")
      .trim();
  }

  function renderSourceFilter() {
    const box = $("#moduleSourceFilterBox");
    if (!(box instanceof HTMLElement)) return;
    if (!sourceFilterBlocks.length) {
      box.classList.add("hidden");
      box.innerHTML = "";
      return;
    }
    const selectedCount = sourceFilterBlocks.filter((row) => sourceFilterSelectedKeys.has(String(row.key || ""))).length;
    const filtered = selectedCount > 0 && selectedCount < sourceFilterBlocks.length;
    const note = $("#moduleSourceFilterNote");
    if (note) {
      note.textContent = filtered
        ? `대화 ${sourceFilterBlocks.length}줄 중 ${selectedCount}줄만 작업 항목 추출에 사용합니다.`
        : `대화 ${sourceFilterBlocks.length}줄이 모두 포함됩니다. 불필요한 줄만 해제하세요.`;
    }
    box.classList.remove("hidden");
    box.innerHTML = [
      '<div class="work-report-source-filter">',
      '<div class="work-report-source-filter-head">',
      `<div><strong>불필요한 대화 제외</strong><p>${escapeHtml(filtered ? "체크된 줄만 작업 항목 추출에 사용합니다." : "필요 없는 대화 줄만 해제하면 자동으로 제외됩니다.")}</p></div>`,
      '<div class="work-report-source-filter-actions">',
      '<button class="ghost-btn source-filter-select-all" type="button">모두 포함</button>',
      '<button class="ghost-btn source-filter-reset" type="button">초기화</button>',
      '</div>',
      '</div>',
      `<div class="work-report-module-source-grid">${sourceFilterBlocks.map((row) => {
        const checked = sourceFilterSelectedKeys.has(String(row.key || "")) ? " checked" : "";
        return [
          '<label class="work-report-module-source-row">',
          `<input class="source-filter-check" type="checkbox" data-source-key="${escapeHtml(String(row.key || ""))}"${checked}>`,
          '<div class="work-report-module-source-meta">',
          `<strong>${escapeHtml(row.text)}</strong>`,
          `<small>${escapeHtml(`${row.lineNumber}번째 줄`)}</small>`,
          '</div>',
          '</label>',
        ].join("");
      }).join("")}</div>`,
      '</div>',
    ].join("");
  }

  function prepareSourceFilter() {
    const blocks = buildSourceFilterBlocks($("#moduleChatInput")?.value || "");
    if (!blocks.length) {
      throw new Error("불필요한 대화 제외는 카톡 대화 원문을 먼저 붙여 넣은 뒤 사용할 수 있습니다.");
    }
    sourceFilterBlocks = blocks;
    sourceFilterSelectedKeys = new Set(blocks.map((row) => String(row.key || "")));
    renderSourceFilter();
    $("#moduleSourceFilterBox")?.scrollIntoView({ block: "nearest", behavior: "smooth" });
    setStatus(`대화 ${blocks.length}줄을 불러왔습니다. 불필요한 줄만 해제한 뒤 작업 항목 추출을 실행하세요.`);
  }

  function normalizeImageStage(value) {
    const stage = String(value || "").trim();
    return IMAGE_STAGE_OPTIONS.some((item) => item.value === stage) ? stage : "general";
  }

  function imageStageLabel(value) {
    const stage = normalizeImageStage(value);
    return IMAGE_STAGE_OPTIONS.find((item) => item.value === stage)?.label || "현장 이미지";
  }

  function selectedItemIndexSet(report) {
    return new Set(
      (Array.isArray(report?.selected_image_item_indexes) ? report.selected_image_item_indexes : [])
        .map((value) => Number(value || 0))
        .filter((value) => value > 0)
    );
  }

  function setSelectedItem(report, itemIndex, checked) {
    if (!report) return;
    const next = selectedItemIndexSet(report);
    const cleanIndex = Number(itemIndex || 0);
    if (cleanIndex <= 0) return;
    if (checked) next.add(cleanIndex);
    else next.delete(cleanIndex);
    report.selected_image_item_indexes = Array.from(next).sort((left, right) => left - right);
  }

  function workReportImageKey(image, fallbackKey = "") {
    const index = Number(image?.index || 0);
    if (index > 0) return `id:${index}`;
    const filename = String(image?.filename || "").trim();
    if (filename) return `file:${filename}`;
    return `fallback:${fallbackKey}`;
  }

  function workReportPreviewUrl(report, image) {
    const jobId = String(report?.batch_job_id || currentJobId || "").trim();
    const imageIndex = Number(image?.index || 0);
    if (!jobId || imageIndex <= 0 || image?.preview_available === false) return "";
    return `/api/ai/work_report/jobs/${encodeURIComponent(jobId)}/images/${imageIndex}`;
  }

  function locateImageRecord(report, imageKey) {
    const normalizedKey = String(imageKey || "").trim();
    if (!report || !normalizedKey) return null;
    for (const item of Array.isArray(report.items) ? report.items : []) {
      const images = Array.isArray(item.images) ? item.images : [];
      for (let index = 0; index < images.length; index += 1) {
        if (workReportImageKey(images[index], `item-${item.index}-${index}`) === normalizedKey) {
          return { sourceType: "item", item, image: images[index], itemIndex: Number(item.index || 0), arrayIndex: index };
        }
      }
    }
    const unmatched = Array.isArray(report.unmatched_images) ? report.unmatched_images : [];
    for (let index = 0; index < unmatched.length; index += 1) {
      if (workReportImageKey(unmatched[index], `unmatched-${index}`) === normalizedKey) {
        return { sourceType: "unmatched", item: null, image: unmatched[index], itemIndex: 0, arrayIndex: index };
      }
    }
    return null;
  }

  function sortImageList(list) {
    list.sort(
      (left, right) => Number(left?.index || 0) - Number(right?.index || 0)
        || String(left?.filename || "").localeCompare(String(right?.filename || ""), "ko")
    );
  }

  function moveImageRecord(report, imageKey, destinationItemIndex) {
    const located = locateImageRecord(report, imageKey);
    if (!located) return false;
    const targetValue = String(destinationItemIndex || "__unmatched__");
    const currentValue = located.sourceType === "unmatched" ? "__unmatched__" : String(located.itemIndex);
    if (currentValue === targetValue) return false;
    const destination = targetValue === "__unmatched__"
      ? null
      : (Array.isArray(report.items) ? report.items : []).find((item) => Number(item?.index || 0) === Number(targetValue || 0));
    if (targetValue !== "__unmatched__" && !destination) return false;
    const image = located.image;
    image.manual_override = true;
    image.review_needed = false;
    if (located.sourceType === "item") {
      located.item.images.splice(located.arrayIndex, 1);
    } else if (Array.isArray(report.unmatched_images)) {
      report.unmatched_images.splice(located.arrayIndex, 1);
    }
    if (targetValue === "__unmatched__") {
      report.unmatched_images = Array.isArray(report.unmatched_images) ? report.unmatched_images : [];
      report.unmatched_images.push(image);
      sortImageList(report.unmatched_images);
      return true;
    }
    destination.images = Array.isArray(destination.images) ? destination.images : [];
    destination.images.push(image);
    sortImageList(destination.images);
    return true;
  }

  function updateImageStage(report, imageKey, stage) {
    const located = locateImageRecord(report, imageKey);
    if (!located) return false;
    const nextStage = normalizeImageStage(stage);
    if (String(located.image.stage || "general") === nextStage) return false;
    located.image.stage = nextStage;
    located.image.stage_label = imageStageLabel(nextStage);
    located.image.manual_override = true;
    located.image.review_needed = false;
    return true;
  }

  function renderAssignmentOptions(report, selectedValue) {
    const currentValue = String(selectedValue || "__unmatched__");
    const options = [`<option value="__unmatched__"${currentValue === "__unmatched__" ? " selected" : ""}>미매칭으로 두기</option>`];
    for (const item of Array.isArray(report?.items) ? report.items : []) {
      const itemIndex = Number(item?.index || 0);
      if (itemIndex <= 0) continue;
      options.push(`<option value="${itemIndex}"${currentValue === String(itemIndex) ? " selected" : ""}>${escapeHtml(`${itemIndex}. ${String(item?.title || "-")}`)}</option>`);
    }
    return options.join("");
  }

  function renderStageOptions(selectedStage) {
    const currentValue = normalizeImageStage(selectedStage);
    return IMAGE_STAGE_OPTIONS.map((item) => (
      `<option value="${item.value}"${currentValue === item.value ? " selected" : ""}>${escapeHtml(item.label)}</option>`
    )).join("");
  }

  function renderSelectedSourceImagePanel() {
    const files = ensureSelectedSourceImages();
    revokeLocalSourceImageUrls();
    if (!files.length) return "";
    const selectedCount = currentSelectedSourceImages().length;
    return [
      '<div class="work-report-module-section">',
      '<div class="work-report-module-selection-bar">',
      `<div><strong>매칭에 사용할 이미지</strong><p>${escapeHtml(selectedCount ? `${selectedCount}장만 선택 매칭에 사용합니다.` : "아직 선택된 이미지가 없습니다.")}</p></div>`,
      '<div class="work-report-source-filter-actions">',
      '<button class="ghost-btn image-select-all" type="button">이미지 모두 포함</button>',
      '<button class="ghost-btn image-clear-all" type="button">이미지 모두 제외</button>',
      '</div>',
      '</div>',
      `<div class="work-report-module-image-grid">${files.map((file, index) => {
        const checked = selectedSourceImageIndexes.has(index) ? " checked" : "";
        const url = URL.createObjectURL(file);
        localSourceImageUrls.push(url);
        return [
          '<label class="work-report-module-image-card">',
          `<img class="work-report-module-image-thumb" src="${escapeHtml(url)}" alt="${escapeHtml(file.name || `image-${index + 1}`)}" loading="lazy">`,
          '<div class="work-report-module-image-meta">',
          `<strong>${escapeHtml(`이미지 ${index + 1}`)}</strong>`,
          `<span>${escapeHtml(file.name || `image-${index + 1}`)}</span>`,
          formatInlineFileSize(file) ? `<small>${escapeHtml(formatInlineFileSize(file))}</small>` : "",
          `</div><label class="work-report-module-choice"><input class="source-image-check" type="checkbox" data-image-index="${index}"${checked}><span>이 이미지를 사용</span></label>`,
          '</label>',
        ].join("");
      }).join("")}</div>`,
      '</div>',
    ].join("");
  }

  function renderExtractStage(report) {
    const selectedItems = selectedItemIndexSet(report).size;
    const selectedImages = currentSelectedSourceImages().length;
    return [
      '<div class="work-report-module-section">',
      '<div class="work-report-module-stage-card">',
      '<strong>준비 단계</strong>',
      '<p>불필요한 대화를 제외한 결과로 작업 항목을 정리했습니다. 이제 실제로 사진이 붙는 작업만 체크하고 사용할 이미지만 남긴 뒤 선택 매칭을 실행하세요.</p>',
      '</div>',
      '<div class="work-report-module-selection-bar">',
      `<div><strong>선택 매칭 준비</strong><p>${escapeHtml(`사진 항목 ${selectedItems}건 / 사용할 이미지 ${selectedImages}장`)}</p></div>`,
      `<button id="btnModuleMatchSelected" class="action-btn" type="button"${selectedItems && selectedImages ? "" : " disabled"}>선택 항목만 이미지 매칭</button>`,
      '</div>',
      `<div class="work-report-module-card-list">${(Array.isArray(report.items) ? report.items : []).map((item) => [
        '<article class="work-report-module-item">',
        '<div class="work-report-module-item-head">',
        `<strong>${escapeHtml(`${Number(item?.index || 0)}. ${String(item?.title || "-")}`)}</strong>`,
        `<label class="work-report-module-choice"><input class="photo-target-check" type="checkbox" data-item-index="${Number(item?.index || 0)}"${selectedItemIndexSet(report).has(Number(item?.index || 0)) ? " checked" : ""}><span>사진 포함 작업</span></label>`,
        '</div>',
        `<p>${escapeHtml(`작업일자: ${String(item?.work_date_label || item?.work_date || "-")} / 작업자: ${String(item?.vendor_name || "-")} / 위치: ${String(item?.location_name || "-")}`)}</p>`,
        `<p>${escapeHtml(String(item?.summary || item?.title || "-"))}</p>`,
        '</article>',
      ].join("")).join("")}</div>`,
      renderSelectedSourceImagePanel(),
      '</div>',
    ].join("");
  }

  function renderImageEditor(report, image, currentAssignment) {
    const key = workReportImageKey(image, `row-${currentAssignment}-${Number(image?.index || 0)}`);
    const previewUrl = workReportPreviewUrl(report, image);
    return [
      '<article class="work-report-module-image-card">',
      previewUrl
        ? `<img class="work-report-module-image-thumb" src="${escapeHtml(previewUrl)}" alt="${escapeHtml(String(image?.filename || "업무보고 이미지"))}" loading="lazy">`
        : '<div class="work-report-module-image-thumb"></div>',
      '<div class="work-report-module-image-meta">',
      `<strong>${escapeHtml(String(image?.filename || "-"))}</strong>`,
      `<span>${escapeHtml(String(image?.stage_label || imageStageLabel(image?.stage)))}</span>`,
      '</div>',
      '<div class="work-report-module-image-controls">',
      `<select class="image-assignment-select" data-record-key="${escapeHtml(key)}">${renderAssignmentOptions(report, currentAssignment)}</select>`,
      `<select class="image-stage-select" data-record-key="${escapeHtml(key)}">${renderStageOptions(String(image?.stage || "general"))}</select>`,
      '</div>',
      '</article>',
    ].join("");
  }

  function renderMatchedStage(report) {
    const items = Array.isArray(report.items) ? report.items : [];
    const unmatched = Array.isArray(report.unmatched_images) ? report.unmatched_images : [];
    return [
      '<div class="work-report-module-section">',
      items.length
        ? `<h3 class="work-report-module-section-title">작업 항목</h3><div class="work-report-module-card-list">${items.map((item) => [
          '<article class="work-report-module-item">',
          `<strong>${escapeHtml(`${Number(item?.index || 0)}. ${String(item?.title || "-")}`)}</strong>`,
          `<p>${escapeHtml(`작업일자: ${String(item?.work_date_label || item?.work_date || "-")} / 작업자: ${String(item?.vendor_name || "-")} / 위치: ${String(item?.location_name || "-")}`)}</p>`,
          `<p>${escapeHtml(String(item?.summary || item?.title || "-"))}</p>`,
          Array.isArray(item?.images) && item.images.length
            ? `<div class="work-report-module-image-grid">${item.images.map((image) => renderImageEditor(report, image, String(item.index || ""))).join("")}</div>`
            : '<p>연결된 이미지가 없습니다.</p>',
          '</article>',
        ].join("")).join("")}</div>`
        : "",
      unmatched.length
        ? `<div class="work-report-module-section"><h3 class="work-report-module-section-title">미매칭 이미지</h3><div class="work-report-module-image-grid">${unmatched.map((image) => renderImageEditor(report, image, "__unmatched__")).join("")}</div></div>`
        : "",
      report.report_text
        ? `<div class="work-report-module-section"><h3 class="work-report-module-section-title">자동 생성 보고 요약</h3><div class="detail-block work-report-module-summary">${escapeHtml(String(report.report_text || ""))}</div></div>`
        : "",
      '</div>',
    ].join("");
  }

  function renderReport() {
    if (!currentReport) {
      clearResult();
      return;
    }
    const report = currentReport;
    const extractOnly = String(report?.analysis_stage || "") === "extract_only";
    const meta = [
      `<span>${escapeHtml(String(report?.report_title || "시설팀 주요 업무 보고"))}</span>`,
      `<span>기간 ${escapeHtml(String(report?.period_label || "-"))}</span>`,
      `<span>작업 ${escapeHtml(String(report?.item_count || 0))}건</span>`,
      extractOnly
        ? `<span>입력 이미지 ${escapeHtml(String(report?.image_input_count || 0))}장</span>`
        : `<span>사진 포함 ${escapeHtml(String(report?.image_item_count || 0))}건</span>`,
    ].join("");
    $("#moduleResult").innerHTML = [
      `<div class="work-report-module-meta">${meta}</div>`,
      report.analysis_notice ? `<div class="detail-block">${escapeHtml(String(report.analysis_notice || ""))}</div>` : "",
      extractOnly ? renderExtractStage(report) : renderMatchedStage(report),
    ].join("");
  }

  function resetReportState(clearPreview = true) {
    currentReport = null;
    currentJobId = "";
    lastMatchedImageFiles = [];
    if (clearPreview) clearResult();
  }

  function currentTenantId() {
    if (sessionMe?.user?.is_admin) {
      return String($("#moduleTenantSelect")?.value || "").trim();
    }
    return String((sessionMe && (sessionMe.tenant?.id || sessionMe.user?.tenant_id)) || "").trim();
  }

  async function requestJson(url, options = {}) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(new Error("timeout")), Number(options.timeoutMs || WORK_REPORT_TIMEOUT_MS));
    try {
      const headers = new Headers(options.headers || {});
      const token = window.KAAuth?.getToken?.();
      if (token && !headers.has("Authorization")) headers.set("Authorization", `Bearer ${token}`);
      const response = await fetch(url, {
        method: options.method || "GET",
        body: options.body,
        credentials: "same-origin",
        headers,
        signal: controller.signal,
      });
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json") ? await response.json() : await response.text();
      if (response.status === 401) {
        await window.KAAuth.logout();
        throw new Error("로그인이 필요합니다.");
      }
      if (!response.ok) {
        throw new Error(typeof payload === "string" ? payload || String(response.status) : String(payload?.detail || payload?.message || response.status));
      }
      return payload;
    } finally {
      window.clearTimeout(timeout);
    }
  }

  async function requestBlob(url, options = {}) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(new Error("timeout")), Number(options.timeoutMs || WORK_REPORT_TIMEOUT_MS));
    try {
      const headers = new Headers(options.headers || {});
      const token = window.KAAuth?.getToken?.();
      if (token && !headers.has("Authorization")) headers.set("Authorization", `Bearer ${token}`);
      const response = await fetch(url, {
        method: options.method || "GET",
        body: options.body,
        credentials: "same-origin",
        headers,
        signal: controller.signal,
      });
      if (response.status === 401) {
        await window.KAAuth.logout();
        throw new Error("로그인이 필요합니다.");
      }
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || String(response.status));
      }
      return {
        blob: await response.blob(),
        filename: /filename=\"?([^"]+)\"?/i.exec(response.headers.get("content-disposition") || "")?.[1] || "",
      };
    } finally {
      window.clearTimeout(timeout);
    }
  }

  function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename || "work-report.pdf";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  async function loadContext() {
    sessionMe = await requestJson("/api/auth/me");
    if (!sessionMe?.user) {
      throw new Error("로그인이 필요합니다.");
    }
    const tenantWrap = $("#moduleTenantWrap");
    const tenantSelect = $("#moduleTenantSelect");
    if (sessionMe?.user?.is_admin && tenantWrap && tenantSelect) {
      const data = await requestJson("/api/admin/tenants");
      tenants = Array.isArray(data.items) ? data.items : [];
      tenantSelect.innerHTML = tenants.map((item) => (
        `<option value="${escapeHtml(String(item.id || ""))}">${escapeHtml(`${String(item.name || item.id || "")} (${String(item.id || "")})`)}</option>`
      )).join("");
      if (tenants.length && !tenantSelect.value) {
        tenantSelect.value = String(tenants[0].id || "");
      }
      tenantWrap.classList.remove("hidden");
    }
    const tenantLabel = sessionMe?.tenant?.name || sessionMe?.user?.tenant_id || "선택 필요";
    $("#moduleHeroLine").textContent = `현재 작업 테넌트는 ${tenantLabel}입니다. 카톡 원문에서 업무보고만 분리해 다루는 전용 화면입니다.`;
  }

  function startProgress(modeLabel) {
    const startedAt = Date.now();
    let timer = 0;
    const sync = (message) => {
      const elapsed = Math.max(0, Math.floor((Date.now() - startedAt) / 1000));
      showProgress(`${modeLabel} · ${elapsed}초 경과 · ${message}`, true);
    };
    sync("작업을 시작했습니다.");
    timer = window.setInterval(() => sync("작업 진행 중입니다."), 1000);
    return {
      sync,
      stop() {
        window.clearInterval(timer);
        showProgress("", false);
      },
      fail(error) {
        window.clearInterval(timer);
        showProgress("", false);
        setStatus(error?.message || String(error), true);
      },
    };
  }

  async function pollJob(jobId, progress) {
    const tenantId = currentTenantId();
    for (;;) {
      const data = await requestJson(`/api/ai/work_report/jobs/${encodeURIComponent(jobId)}?tenant_id=${encodeURIComponent(tenantId)}`, {
        method: "GET",
        timeoutMs: 60000,
      });
      const job = data.item || {};
      progress.sync(job.summary || "배치 작업 진행 중입니다.");
      if (String(job.status || "") === "completed") return job.result || null;
      if (String(job.status || "") === "failed") {
        throw new Error(String(job.error_message || job.summary || "업무보고 작업이 실패했습니다."));
      }
      await new Promise((resolve) => window.setTimeout(resolve, Number(job.poll_after_ms || 2000)));
    }
  }

  function pdfReportPayload(report) {
    const cloned = deepCloneJson(report || {}) || {};
    const items = Array.isArray(cloned.items) ? cloned.items : [];
    cloned.items = items.map((item) => ({
      ...item,
      images: Array.isArray(item.images) ? item.images : [],
      attachments: [],
    }));
    cloned.image_items = cloned.items.filter((item) => Array.isArray(item.images) && item.images.length);
    cloned.text_only_items = cloned.items.filter((item) => !Array.isArray(item.images) || !item.images.length);
    cloned.item_count = cloned.items.length;
    cloned.image_item_count = cloned.image_items.length;
    cloned.text_only_item_count = cloned.text_only_items.length;
    cloned.review_queue = [];
    cloned.review_queue_count = 0;
    return cloned;
  }

  function buildWorkReportFormData(options = {}) {
    const tenantId = currentTenantId();
    if (!tenantId) throw new Error("테넌트를 선택하세요.");
    const text = currentSourceText();
    const allImages = selectedFiles("#moduleImageInput");
    let images = allImages;
    if (options.imageMode === "selected") {
      images = currentSelectedSourceImages();
    } else if (options.imageMode === "matched") {
      images = lastMatchedImageFiles.length ? lastMatchedImageFiles.slice() : currentSelectedSourceImages();
    }
    const sourceFiles = selectedFiles("#moduleSourceInput");
    const attachments = selectedFiles("#moduleAttachmentInput");
    const sampleFile = selectedFiles("#moduleSampleInput")[0];
    if (!text && !sourceFiles.length && !allImages.length && !attachments.length) {
      throw new Error("카톡 대화, 원문 파일, 이미지, 첨부파일 중 하나 이상을 입력하세요.");
    }
    if ((options.imageMode === "selected" || options.imageMode === "matched") && allImages.length && !images.length) {
      throw new Error("이미지 매칭에 사용할 현장 사진을 먼저 하나 이상 선택해 주세요.");
    }
    const fd = new FormData();
    fd.append("tenant_id", tenantId);
    fd.append("text", text);
    sourceFiles.forEach((file) => fd.append("source_files", file, file.name || "source"));
    images.forEach((file) => fd.append("images", file, file.name || "work-image"));
    attachments.forEach((file) => fd.append("attachments", file, file.name || "attachment"));
    if (sampleFile) fd.append("sample_file", sampleFile, sampleFile.name || "sample");
    if (options.deferImageMatching) {
      fd.append("defer_image_matching", "1");
    }
    if (options.includeSelection && currentReport) {
      fd.append("report_json", JSON.stringify(deepCloneJson(currentReport)));
      fd.append("selected_image_item_indexes", Array.from(selectedItemIndexSet(currentReport)).join(","));
    }
    if (options.includeCachedReport && currentReport) {
      fd.append("report_json", JSON.stringify(pdfReportPayload(currentReport)));
    }
    return fd;
  }

  async function analyzeWorkReport() {
    resetReportState(false);
    lastMatchedImageFiles = [];
    const progress = startProgress("작업 항목 추출");
    try {
      const created = await requestJson("/api/ai/work_report/jobs", {
        method: "POST",
        body: buildWorkReportFormData({ deferImageMatching: true }),
        timeoutMs: WORK_REPORT_TIMEOUT_MS,
      });
      const job = created.item || {};
      currentJobId = String(job.id || "");
      const result = await pollJob(currentJobId, progress);
      currentReport = result || null;
      renderReport();
      const previewMessage = Number(currentReport?.image_input_count || 0) > 0
        ? `작업 ${Number(currentReport?.item_count || 0)}건을 추출했습니다. 사진 포함 항목과 사용할 이미지를 고른 뒤 선택 매칭을 실행하세요.`
        : `작업 ${Number(currentReport?.item_count || 0)}건을 추출했습니다.`;
      setStatus(previewMessage);
    } catch (error) {
      progress.fail(error);
      throw error;
    } finally {
      progress.stop();
    }
  }

  async function matchSelectedItems() {
    if (!currentReport) throw new Error("먼저 작업 항목 추출을 실행해 주세요.");
    const selectedItems = Array.from(selectedItemIndexSet(currentReport));
    if (!selectedItems.length) throw new Error("사진이 실제로 포함된 작업 항목을 먼저 하나 이상 선택해 주세요.");
    const selectedImages = currentSelectedSourceImages();
    if (selectedFiles("#moduleImageInput").length && !selectedImages.length) {
      throw new Error("매칭에 사용할 현장 사진을 먼저 하나 이상 선택해 주세요.");
    }
    const progress = startProgress("선택 이미지 매칭");
    try {
      const created = await requestJson("/api/ai/work_report/jobs", {
        method: "POST",
        body: buildWorkReportFormData({ includeSelection: true, imageMode: "selected" }),
        timeoutMs: WORK_REPORT_TIMEOUT_MS,
      });
      const job = created.item || {};
      currentJobId = String(job.id || "");
      lastMatchedImageFiles = selectedImages.slice();
      const result = await pollJob(currentJobId, progress);
      currentReport = result || null;
      renderReport();
      setStatus(`선택한 ${selectedItems.length}개 항목과 ${selectedImages.length}장 이미지만 매칭했습니다.`);
    } catch (error) {
      progress.fail(error);
      throw error;
    } finally {
      progress.stop();
    }
  }

  async function downloadPdf() {
    if (!currentReport) throw new Error("먼저 작업 항목 추출 또는 선택 매칭을 실행해 주세요.");
    if (String(currentReport?.analysis_stage || "") === "extract_only" && Number(currentReport?.image_input_count || 0) > 0) {
      throw new Error("먼저 사진 포함 항목과 사용할 이미지를 고른 뒤 선택 매칭을 실행하고 PDF를 생성해 주세요.");
    }
    const response = await requestBlob("/api/ai/work_report/pdf", {
      method: "POST",
      body: buildWorkReportFormData({ includeCachedReport: true, imageMode: "matched" }),
      timeoutMs: WORK_REPORT_TIMEOUT_MS,
    });
    downloadBlob(response.blob, response.filename || "work-report.pdf");
    setStatus("PDF를 생성했습니다.");
  }

  function handleResultChange(event) {
    const target = event.target;
    if (!(target instanceof HTMLElement) || !currentReport) return;
    if (target instanceof HTMLInputElement && target.classList.contains("photo-target-check")) {
      setSelectedItem(currentReport, Number(target.getAttribute("data-item-index") || 0), !!target.checked);
      renderReport();
      setStatus("사진 포함 작업 항목 선택을 반영했습니다.");
      return;
    }
    if (target instanceof HTMLInputElement && target.classList.contains("source-image-check")) {
      setSelectedSourceImage(Number(target.getAttribute("data-image-index") || -1), !!target.checked);
      renderReport();
      setStatus("선택 매칭에 사용할 이미지를 반영했습니다.");
      return;
    }
    if (target instanceof HTMLSelectElement && target.classList.contains("image-assignment-select")) {
      if (moveImageRecord(currentReport, target.getAttribute("data-record-key") || "", target.value)) {
        renderReport();
        setStatus("이미지 연결 작업을 반영했습니다.");
      }
      return;
    }
    if (target instanceof HTMLSelectElement && target.classList.contains("image-stage-select")) {
      if (updateImageStage(currentReport, target.getAttribute("data-record-key") || "", target.value)) {
        renderReport();
        setStatus("이미지 단계값을 반영했습니다.");
      }
    }
  }

  function handleResultClick(event) {
    const trigger = event.target instanceof HTMLElement ? event.target.closest("#btnModuleMatchSelected, .image-select-all, .image-clear-all") : null;
    if (!(trigger instanceof HTMLElement)) return;
    if (trigger.id === "btnModuleMatchSelected") {
      matchSelectedItems().catch((error) => setStatus(error.message || String(error), true));
      return;
    }
    if (trigger.classList.contains("image-select-all")) {
      const files = ensureSelectedSourceImages();
      selectedSourceImagesInitialized = true;
      selectedSourceImageIndexes = new Set(files.map((_file, index) => index));
      renderReport();
      return;
    }
    if (trigger.classList.contains("image-clear-all")) {
      selectedSourceImagesInitialized = true;
      selectedSourceImageIndexes = new Set();
      renderReport();
    }
  }

  function wireFileRemoval() {
    document.addEventListener("click", (event) => {
      const button = event.target instanceof HTMLElement ? event.target.closest("[data-input-selector][data-file-index]") : null;
      if (!(button instanceof HTMLElement)) return;
      const selector = String(button.getAttribute("data-input-selector") || "").trim();
      const index = Number(button.getAttribute("data-file-index") || -1);
      if (!selector || index < 0) return;
      removeInputFileAt(selector, index);
      if (selector === "#moduleImageInput") {
        resetSourceImageSelection();
      }
      if (selector === "#moduleChatInput") {
        resetSourceFilter();
      }
      resetReportState();
      syncSelectedFiles();
    });
  }

  function wire() {
    $("#btnModuleLogout")?.addEventListener("click", () => window.KAAuth.logout("/pwa/work_report.html"));
    $("#btnModulePrepareSource")?.addEventListener("click", () => {
      try {
        prepareSourceFilter();
      } catch (error) {
        setStatus(error.message || String(error), true);
      }
    });
    $("#btnModuleAnalyze")?.addEventListener("click", () => analyzeWorkReport().catch((error) => setStatus(error.message || String(error), true)));
    $("#btnModulePdf")?.addEventListener("click", () => downloadPdf().catch((error) => setStatus(error.message || String(error), true)));
    $("#moduleResult")?.addEventListener("change", handleResultChange);
    $("#moduleResult")?.addEventListener("click", handleResultClick);
    $("#moduleSourceFilterBox")?.addEventListener("change", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement) || !target.classList.contains("source-filter-check")) return;
      const key = String(target.getAttribute("data-source-key") || "").trim();
      if (!key) return;
      if (target.checked) sourceFilterSelectedKeys.add(key);
      else sourceFilterSelectedKeys.delete(key);
      renderSourceFilter();
    });
    $("#moduleSourceFilterBox")?.addEventListener("click", (event) => {
      const trigger = event.target instanceof HTMLElement ? event.target.closest(".source-filter-select-all, .source-filter-reset") : null;
      if (!(trigger instanceof HTMLElement)) return;
      event.preventDefault();
      if (trigger.classList.contains("source-filter-select-all")) {
        sourceFilterSelectedKeys = new Set(sourceFilterBlocks.map((row) => String(row.key || "")));
      } else {
        sourceFilterSelectedKeys = new Set(sourceFilterBlocks.map((row) => String(row.key || "")));
        resetSourceFilter();
        return;
      }
      renderSourceFilter();
    });
    $("#moduleChatInput")?.addEventListener("input", () => {
      resetReportState();
      resetSourceFilter();
    });
    $("#moduleSourceInput")?.addEventListener("change", () => {
      const files = selectedFiles("#moduleSourceInput");
      if (files.length > MAX_WORK_REPORT_SOURCE_FILES) {
        setInputFiles("#moduleSourceInput", files.slice(0, MAX_WORK_REPORT_SOURCE_FILES));
        setStatus(`카톡 원문 파일은 최대 ${MAX_WORK_REPORT_SOURCE_FILES}개까지 선택할 수 있습니다.`, true);
      }
      resetReportState();
      syncSelectedFiles();
    });
    $("#moduleImageInput")?.addEventListener("change", () => {
      const files = selectedFiles("#moduleImageInput");
      if (files.length > MAX_WORK_REPORT_IMAGES) {
        setInputFiles("#moduleImageInput", files.slice(0, MAX_WORK_REPORT_IMAGES));
        setStatus(`현장 사진은 최대 ${MAX_WORK_REPORT_IMAGES}장까지 선택할 수 있습니다.`, true);
      }
      resetSourceImageSelection();
      resetReportState();
      syncSelectedFiles();
    });
    $("#moduleSampleInput")?.addEventListener("change", () => {
      resetReportState();
      syncSelectedFiles();
    });
    $("#moduleAttachmentInput")?.addEventListener("change", () => {
      resetReportState();
      syncSelectedFiles();
    });
    $("#moduleTenantSelect")?.addEventListener("change", () => {
      resetReportState();
      setStatus("");
    });
    wireFileRemoval();
  }

  async function init() {
    try {
      await loadContext();
      syncSelectedFiles();
      wire();
    } catch (error) {
      setStatus(error.message || String(error), true);
    }
  }

  init();
})();
