(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const STATUS_VALUES = ["접수", "처리중", "완료", "이월"];
  const MAX_CHAT_DIGEST_IMAGES = 30;
  const DOCUMENT_CATEGORY_VALUES = ["계약", "공문", "보고", "예산", "입주", "점검", "기타"];
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

  let me = null;
  let tenants = [];
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
  let facilityAssets = [];
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
  let documentNumberingConfig = null;
  let chatSourcePreviewUrls = [];
  let chatDigestPreviewUrls = [];
  let lastDigestResult = null;
  let lastDigestImported = false;
  let lastDigestSelectedKeys = new Set();
  let currentIntakeStep = 1;

  const MOBILE_INTAKE_STEPS = [
    { step: 1, title: "1단계 / 위치와 연락처" },
    { step: 2, title: "2단계 / 민원 내용과 AI 분류" },
    { step: 3, title: "3단계 / 사진 첨부" },
    { step: 4, title: "4단계 / 검토 후 저장" },
  ];

  function setMessage(selector, message, isError = false) {
    const el = $(selector);
    if (!el) return;
    el.textContent = String(message || "");
    el.classList.toggle("error", !!isError);
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

  async function api(url, opts = {}) {
    return window.KAAuth.requestJson(url, opts);
  }

  async function authFetchJson(url, opts = {}) {
    const headers = { ...(opts.headers || {}) };
    const token = window.KAAuth.getToken();
    if (token && !headers.Authorization) {
      headers.Authorization = `Bearer ${token}`;
    }
    const response = await fetch(url, {
      ...opts,
      headers,
      credentials: "same-origin",
    });
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
    const response = await fetch(url, {
      ...opts,
      headers,
      credentials: "same-origin",
    });
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

  function updatePhotoHint(inputSelector, targetSelector, limit = 6) {
    const files = selectedFiles(inputSelector);
    const el = $(targetSelector);
    if (!el) return;
    if (!files.length) {
      el.textContent = "선택된 사진이 없습니다.";
      return;
    }
    el.textContent = `선택 ${files.length}장 / 최대 ${limit}장: ${files.map((file) => file.name).join(", ")}`;
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
          `<strong>선택 이미지 ${index + 1}</strong>`,
          `<img src="${escapeHtml(url)}" alt="카톡 분석 선택 이미지 ${index + 1}" loading="lazy" />`,
          `<span class="meta">${escapeHtml(file.name || `image-${index + 1}`)}</span>`,
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
        imageHint.textContent = `선택 ${files.length}장 / 최대 ${MAX_CHAT_DIGEST_IMAGES}장: ${files.map((file) => file.name).join(", ")}`;
      } else if (text) {
        const plan = chatTextImagePlan(text, MAX_CHAT_DIGEST_IMAGES);
        imageHint.textContent = `선택된 이미지는 없지만, 분석 시 카톡 원문을 PNG ${Math.max(plan.pageCount, 1)}장으로 자동 저장합니다.`;
      } else {
        imageHint.textContent = "선택된 이미지가 없습니다.";
      }
    }
    if (modeHint) {
      if (files.length) {
        modeHint.textContent = "현재 선택한 이미지와 입력한 원문을 함께 분석합니다. 위 붙여넣기 영역, 클립보드 불러오기 버튼, 파일 선택을 모두 사용할 수 있습니다.";
      } else {
        modeHint.textContent = "텍스트만 입력한 경우 원문을 PNG 이미지로 자동 저장해 함께 분석합니다. 위 붙여넣기 영역이나 클립보드 불러오기 버튼으로 카톡 캡처 이미지를 추가할 수 있습니다.";
      }
    }
  }

  async function resolveChatDigestFiles(text) {
    const explicitFiles = selectedFiles("#chatImageInput");
    if (explicitFiles.length > MAX_CHAT_DIGEST_IMAGES) {
      throw new Error(`카톡 이미지는 최대 ${MAX_CHAT_DIGEST_IMAGES}장까지 업로드할 수 있습니다.`);
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
    const merged = mergeFilesIntoInput("#chatImageInput", files, MAX_CHAT_DIGEST_IMAGES);
    clearChatSourcePreview();
    updateChatDigestHint();
    if (merged.added < files.length) {
      setMessage("#intakeMsg", `${sourceLabel}는 최대 ${MAX_CHAT_DIGEST_IMAGES}장까지 저장됩니다.`, true);
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

  function applyHero() {
    renderTenantBadge();
    const role = isAdmin() ? "최고관리자" : (me?.user?.is_site_admin ? "현장관리자" : (me?.user?.role || "staff"));
    const tenantLabel = me?.tenant?.name || me?.user?.tenant_id || "선택 필요";
    $("#heroLine").textContent = `${role} 계정으로 접속 중입니다. 현재 작업 테넌트는 ${tenantLabel}입니다. 민원 접수와 함께 공지, 문서, 일정, 업체 관리까지 한 화면에서 운영할 수 있습니다.`;
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
        ...facilityAssets.map((item) => `<option value="${Number(item.id || 0)}"${String(item.id) === current ? " selected" : ""}>${escapeHtml(item.asset_code || "-")} / ${escapeHtml(item.asset_name || "-")}</option>`),
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

  function renderFacilityAssetDetail(item) {
    selectedFacilityAssetId = Number(item.id || 0);
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
    $("#facilityAssetDetail").innerHTML = [
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
    ].join("<br>");
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
    if (!tenantId) return [];
    const data = await api(`/api/facility/assets?tenant_id=${encodeURIComponent(tenantId)}`);
    facilityAssets = Array.isArray(data.items) ? data.items : [];
    renderFacilityAssetOptions($("#facilityInspectionAssetId")?.value || $("#facilityWorkOrderAssetId")?.value || "");
    const body = $("#facilityAssetsTableBody");
    body.innerHTML = facilityAssets.length
      ? facilityAssets.map((item) => `<tr class="facility-asset-row" data-id="${Number(item.id || 0)}"><td>${escapeHtml(item.asset_code || "-")}</td><td>${escapeHtml(item.asset_name || "-")}</td><td>${escapeHtml(item.category || "-")}</td><td>${escapeHtml(item.location_name || "-")}</td><td>${escapeHtml(item.lifecycle_state || "-")}</td></tr>`).join("")
      : '<tr><td colspan="5" class="empty-state">등록된 자산이 없습니다.</td></tr>';
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
    renderFacilityAssetDetail(data.item || {});
    setMessage("#facilityAssetMsg", "자산을 등록했습니다.");
    await loadFacilityAssets();
    await loadFacilityDashboard();
  }

  async function updateFacilityAsset() {
    if (!selectedFacilityAssetId) throw new Error("수정할 자산을 선택하세요.");
    const data = await api(`/api/facility/assets/${selectedFacilityAssetId}`, { method: "PATCH", body: JSON.stringify(facilityAssetPayloadFromForm()) });
    renderFacilityAssetDetail(data.item || {});
    setMessage("#facilityAssetMsg", "자산을 수정했습니다.");
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
    $("#documentCategory").value = "계약";
    $("#documentStatus").value = "작성중";
    $("#documentOwner").value = "";
    $("#documentDueDate").value = "";
    $("#documentRefNo").value = "";
    $("#documentSummary").value = "";
    if ($("#documentSampleFile")) $("#documentSampleFile").value = "";
    $("#opsDocumentDetail").textContent = "문서를 선택하거나 새로 등록하세요.";
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

  function mobileDockSections() {
    return Array.from(document.querySelectorAll(".mobile-dock-btn"))
      .map((button) => ({ button, target: document.querySelector(String(button.getAttribute("data-target") || "")) }))
      .filter((item) => item.target);
  }

  function syncMobileDockState() {
    const sections = mobileDockSections();
    if (!sections.length) return;
    let activeTarget = sections[0].target;
    const threshold = window.innerHeight * 0.28;
    for (const item of sections) {
      const rect = item.target.getBoundingClientRect();
      if (rect.top <= threshold) {
        activeTarget = item.target;
      }
    }
    sections.forEach((item) => item.button.classList.toggle("is-active", item.target === activeTarget));
  }

  function numberingConfigFromForm() {
    return {
      separator: String($("#docNumberSeparator")?.value || "").trim(),
      date_mode: String($("#docNumberDateMode")?.value || "yyyymmdd").trim(),
      sequence_digits: Number($("#docNumberDigits")?.value || 3),
      category_codes: {
        계약: String($("#docCodeContract")?.value || "").trim(),
        공문: String($("#docCodeLetter")?.value || "").trim(),
        보고: String($("#docCodeReport")?.value || "").trim(),
        예산: String($("#docCodeBudget")?.value || "").trim(),
        입주: String($("#docCodeMoveIn")?.value || "").trim(),
        점검: String($("#docCodeInspection")?.value || "").trim(),
        기타: String($("#docCodeOther")?.value || "").trim(),
      },
    };
  }

  function applyDocumentNumberingConfig(config) {
    const item = config || {};
    const codes = item.category_codes || {};
    $("#docNumberSeparator").value = String(item.separator || "-");
    $("#docNumberDateMode").value = String(item.date_mode || "yyyymmdd");
    $("#docNumberDigits").value = String(item.sequence_digits || 3);
    $("#docCodeContract").value = String(codes["계약"] || "");
    $("#docCodeLetter").value = String(codes["공문"] || "");
    $("#docCodeReport").value = String(codes["보고"] || "");
    $("#docCodeBudget").value = String(codes["예산"] || "");
    $("#docCodeMoveIn").value = String(codes["입주"] || "");
    $("#docCodeInspection").value = String(codes["점검"] || "");
    $("#docCodeOther").value = String(codes["기타"] || "");
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
      ...DOCUMENT_CATEGORY_VALUES.map((category) => `${escapeHtml(category)}: ${escapeHtml(previews[category] || "-")}`),
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
    $("#documentCategory").value = String(item.category || "기타");
    $("#documentStatus").value = String(item.status || "작성중");
    $("#documentOwner").value = String(item.owner || "");
    $("#documentDueDate").value = String(item.due_date || "");
    $("#documentRefNo").value = String(item.reference_no || "");
    $("#documentSummary").value = String(item.summary || "");
    $("#opsDocumentDetail").innerHTML = [
      `<strong>${escapeHtml(item.title || "-")}</strong>`,
      `분류: ${escapeHtml(item.category || "-")}`,
      `상태: ${escapeHtml(item.status || "-")}`,
      `담당: ${escapeHtml(item.owner || "-")}`,
      `기한: ${escapeHtml(formatDate(item.due_date))}`,
      `문서번호: ${escapeHtml(item.reference_no || "-")}`,
    ].join("<br>");
  }

  function selectedDocumentCategoryFilter() {
    return String($("#documentCategoryFilter")?.value || "").trim();
  }

  function documentCategoryOrder(category) {
    const index = DOCUMENT_CATEGORY_VALUES.indexOf(String(category || ""));
    return index === -1 ? DOCUMENT_CATEGORY_VALUES.length : index;
  }

  function renderDocumentCategorySummary(counts, selectedCategory = "") {
    const summary = $("#opsDocumentCategorySummary");
    if (!summary) return;
    const rows = Array.isArray(counts) ? counts : [];
    const total = rows.reduce((acc, row) => acc + Number(row.total_count || 0), 0);
    summary.innerHTML = [
      `<button class="summary-chip summary-chip-btn${selectedCategory ? "" : " active"}" type="button" data-category="">전체 ${escapeHtml(String(total))}건</button>`,
      ...DOCUMENT_CATEGORY_VALUES.map((category) => {
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
      return '<tr><td colspan="5" class="empty-state">등록된 문서가 없습니다.</td></tr>';
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
          `<tr class="category-group-row"><td colspan="5">${escapeHtml(category)} · ${escapeHtml(String(groupedRows.length))}건</td></tr>`,
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
      category: String($("#documentCategory").value || "기타").trim(),
      status: String($("#documentStatus").value || "작성중").trim(),
      owner: String($("#documentOwner").value || "").trim(),
      due_date: String($("#documentDueDate").value || "").trim(),
      reference_no: String($("#documentRefNo").value || "").trim(),
    };
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
    setMessage("#opsDocumentMsg", "기안서 PDF를 생성했습니다.");
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
    await loadDashboard();
    await loadComplaints();
    await generateReport();
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
        const target = document.querySelector(String(button.getAttribute("data-target") || ""));
        if (!target) return;
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    });
    window.addEventListener("scroll", syncMobileDockState, { passive: true });
    window.addEventListener("resize", () => {
      syncMobileDockState();
      syncMobileIntakeStep();
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
    $("#btnLoadFacilityDashboard")?.addEventListener("click", () => loadFacilityDashboard().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnCreateFacilityAsset")?.addEventListener("click", () => createFacilityAsset().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnUpdateFacilityAsset")?.addEventListener("click", () => updateFacilityAsset().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnDeleteFacilityAsset")?.addEventListener("click", () => deleteFacilityAsset().catch((error) => setMessage("#facilityAssetMsg", error.message || String(error), true)));
    $("#btnClearFacilityAsset")?.addEventListener("click", () => clearFacilityAssetForm());
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
    $("#filterBuilding")?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        loadComplaints().catch((error) => setMessage("#intakeMsg", error.message || String(error), true));
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
    ["#buildingInput", "#unitInput", "#channelInput", "#managerInput", "#phoneInput", "#contentInput"].forEach((selector) => {
      const el = $(selector);
      if (!el) return;
      el.addEventListener("input", () => updateIntakeReview());
      el.addEventListener("change", () => updateIntakeReview());
    });
    $("#chatImageInput")?.addEventListener("change", () => {
      clearChatSourcePreview();
      resetDigestImportState();
      updateChatDigestHint();
    });
    $("#chatInput")?.addEventListener("input", () => {
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
  }

  async function init() {
    me = await api("/api/auth/me");
    renderRoleOptions("#newUserRole", "desk");
    renderRoleOptions("#editUserRole", "desk");
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
    updateChatDigestHint();
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
    syncMobileDockState();
  }

  wire();
  init().catch((error) => {
    setMessage("#intakeMsg", error.message || String(error), true);
  });
})();
