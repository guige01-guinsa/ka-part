(function () {
  const $ = (s) => document.querySelector(s);

  const body = document.body;
  const ctxLine = $("#ctxLine");
  const ttlLine = $("#ttlLine");
  const ocrLine = $("#ocrLine");
  const resultCard = $("#resultCard");
  const resultText = $("#resultText");

  const video = $("#video");
  const canvas = $("#canvas");
  const plateInput = $("#plateInput");

  const illegalPlate = $("#illegalPlate");
  const illegalReason = $("#illegalReason");
  const illegalMemo = $("#illegalMemo");
  const illegalList = $("#illegalList");
  const scanList = $("#scanList");

  const btnCam = $("#btnCam");
  const btnCapture = $("#btnCapture");
  const btnCheck = $("#btnCheck");
  const btnRegisterIllegal = $("#btnRegisterIllegal");
  const btnReloadIllegal = $("#btnReloadIllegal");

  let token = (body.dataset.parkingToken || "").trim() || (localStorage.getItem("parking_token") || "");
  let context = null;
  let stream = null;
  let lastNormalized = "";

  function esc(v) {
    return String(v ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function normalizePlate(raw) {
    if (!raw) return "";
    return String(raw).replace(/[^0-9A-Za-z가-힣]/g, "").toUpperCase().trim();
  }

  function parseBootstrapContext() {
    const el = document.getElementById("parkingContextJson");
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "null");
    } catch (_) {
      return null;
    }
  }

  function setResult(kind, text) {
    resultCard.className = `result ${kind}`;
    resultText.textContent = text;
  }

  function setContextLine(msg) {
    ctxLine.textContent = msg;
  }

  function setHint(msg) {
    ocrLine.textContent = msg;
  }

  async function apiGet(path, params) {
    const u = new URL(path, location.origin);
    Object.entries(params || {}).forEach(([k, v]) => {
      if (v !== undefined && v !== null && String(v) !== "") u.searchParams.set(k, String(v));
    });
    const res = await fetch(u.toString());
    const txt = await res.text();
    let data = {};
    try {
      data = JSON.parse(txt);
    } catch (_) {}
    if (!res.ok) {
      throw new Error(data.detail || txt || `HTTP ${res.status}`);
    }
    return data;
  }

  async function apiPost(path, bodyObj) {
    const res = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(bodyObj || {}),
    });
    const txt = await res.text();
    let data = {};
    try {
      data = JSON.parse(txt);
    } catch (_) {}
    if (!res.ok) {
      throw new Error(data.detail || txt || `HTTP ${res.status}`);
    }
    return data;
  }

  async function ensureContext() {
    if (!token) {
      const bootErr = (body.dataset.bootstrapError || "").trim();
      setContextLine(bootErr || "토큰 없음: 시설관리 시스템에서 '주차관리(접속/실행)' 버튼으로 접속하세요.");
      setResult("neutral", "접속 토큰이 없어 조회를 수행할 수 없습니다.");
      return false;
    }

    try {
      const res = await apiGet("/api/parking/context", { token });
      context = res.context || null;
      localStorage.setItem("parking_token", token);
      if (context) {
        setContextLine(`${context.complex.name} (${context.complex.code}) · ${context.user.name} (${context.user.login})`);
        if (context.expires_at) {
          const dt = new Date(context.expires_at * 1000);
          ttlLine.textContent = `토큰 만료: ${dt.toLocaleString()}`;
        }
      }
      return true;
    } catch (e) {
      localStorage.removeItem("parking_token");
      token = "";
      setContextLine(`인증 실패: ${e.message || e}`);
      setResult("neutral", "토큰이 만료되었거나 유효하지 않습니다. 시설관리 시스템에서 다시 실행하세요.");
      return false;
    }
  }

  async function startCamera() {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      setHint("이 브라우저는 카메라 API를 지원하지 않습니다.");
      return;
    }
    if (stream) return;
    try {
      stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: "environment" }, width: { ideal: 1280 }, height: { ideal: 720 } },
        audio: false,
      });
      video.srcObject = stream;
      setHint("카메라 활성화 완료");
    } catch (e) {
      setHint(`카메라 접근 실패: ${e.message || e}`);
    }
  }

  function captureFrame() {
    const w = video.videoWidth || 1280;
    const h = video.videoHeight || 720;
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext("2d");
    ctx.drawImage(video, 0, 0, w, h);
    return canvas;
  }

  function extractPlate(text) {
    const normalizedText = normalizePlate(text || "");
    const matches = normalizedText.match(/\d{2,3}[가-힣]\d{4}/g);
    if (matches && matches.length) return matches[0];
    return normalizedText;
  }

  async function runOcr() {
    if (!window.Tesseract || !window.Tesseract.recognize) {
      throw new Error("OCR 엔진 로딩 중입니다. 잠시 후 다시 시도하세요.");
    }
    setHint("OCR 처리 중...");
    const snapped = captureFrame();
    const result = await window.Tesseract.recognize(snapped, "kor+eng");
    const text = result?.data?.text || "";
    const plate = extractPlate(text);
    setHint(plate ? `OCR 추출: ${plate}` : "OCR 결과에서 번호판을 찾지 못했습니다.");
    return plate;
  }

  function renderIllegalList(items) {
    if (!items || !items.length) {
      illegalList.innerHTML = `<div class="empty">등록된 불법주차 차량이 없습니다.</div>`;
      return;
    }
    illegalList.innerHTML = items
      .map(
        (it) => `
        <div class="item">
          <strong>${esc(it.plate_number)}</strong>
          <div class="line">${esc(it.reason || "-")}</div>
          <div class="meta">정규화: ${esc(it.plate_normalized)} · 갱신: ${esc(it.updated_at)}</div>
          <div class="row">
            <button class="btn" data-clear-id="${esc(it.id)}">해제</button>
          </div>
        </div>
      `
      )
      .join("");
  }

  function renderScans(items) {
    if (!items || !items.length) {
      scanList.innerHTML = `<div class="empty">최근 조회 이력이 없습니다.</div>`;
      return;
    }
    scanList.innerHTML = items
      .map(
        (it) => `
        <div class="item">
          <strong>${esc(it.plate_normalized || "-")}</strong>
          <div class="line">판정: ${esc(it.verdict)}${it.illegal_reason ? ` · 사유: ${esc(it.illegal_reason)}` : ""}</div>
          <div class="meta">${esc(it.source)} · ${esc(it.scanned_at)}</div>
        </div>
      `
      )
      .join("");
  }

  async function loadIllegalList() {
    if (!token) return;
    const res = await apiGet("/api/parking/illegal-vehicles", { token, status: "ACTIVE", limit: 50 });
    renderIllegalList(res.items || []);
  }

  async function loadScans() {
    if (!token) return;
    const res = await apiGet("/api/parking/scans/recent", { token, limit: 20 });
    renderScans(res.items || []);
  }

  async function checkCurrentPlate(source) {
    if (!token) {
      setResult("neutral", "토큰이 없어 조회할 수 없습니다.");
      return;
    }
    const raw = (plateInput.value || "").trim();
    const normalized = normalizePlate(raw);
    if (normalized.length < 7) {
      setResult("neutral", "번호판 형식이 너무 짧습니다.");
      return;
    }

    const res = await apiPost("/api/parking/check", { token, plate: raw, source: source || "MANUAL" });
    lastNormalized = res.plate_normalized || normalized;
    illegalPlate.value = illegalPlate.value.trim() || lastNormalized;

    if (res.verdict === "ILLEGAL") {
      const reason = res.illegal_vehicle?.reason || "불법 주차 차량";
      setResult("bad", `불법 주차 차량입니다. (${lastNormalized}) ${reason}`);
    } else if (res.verdict === "CLEAR") {
      setResult("ok", `정상 차량으로 확인되었습니다. (${lastNormalized})`);
    } else {
      setResult("neutral", `판정 불가: 번호판 재확인이 필요합니다. (${lastNormalized || raw})`);
    }
    await loadScans();
  }

  async function registerIllegal() {
    if (!token) {
      setResult("neutral", "토큰이 없어 등록할 수 없습니다.");
      return;
    }
    const plate = illegalPlate.value.trim() || plateInput.value.trim() || lastNormalized;
    const normalized = normalizePlate(plate);
    if (normalized.length < 7) {
      setResult("neutral", "등록할 번호판이 올바르지 않습니다.");
      return;
    }
    const reason = illegalReason.value.trim() || "미등록/불법 주차 차량";
    const memo = illegalMemo.value.trim();
    await apiPost("/api/parking/illegal-vehicles", {
      token,
      plate_number: plate,
      reason,
      memo: memo || null,
    });
    setResult("bad", `불법주차 차량으로 등록했습니다. (${normalized})`);
    illegalPlate.value = normalized;
    await loadIllegalList();
  }

  async function clearIllegal(vehicleId) {
    await apiPost(`/api/parking/illegal-vehicles/${vehicleId}/clear`, { token });
    setResult("ok", `불법주차 등록을 해제했습니다. (ID: ${vehicleId})`);
    await loadIllegalList();
  }

  function bindEvents() {
    btnCam.addEventListener("click", () => {
      startCamera();
    });

    btnCapture.addEventListener("click", async () => {
      try {
        await startCamera();
        const plate = await runOcr();
        if (plate) {
          plateInput.value = plate;
          illegalPlate.value = illegalPlate.value.trim() || plate;
          await checkCurrentPlate("OCR");
        } else {
          setResult("neutral", "OCR이 번호판을 찾지 못했습니다. 수동 입력 후 조회하세요.");
        }
      } catch (e) {
        setHint(`OCR 오류: ${e.message || e}`);
      }
    });

    btnCheck.addEventListener("click", async () => {
      try {
        await checkCurrentPlate("MANUAL");
      } catch (e) {
        setResult("neutral", `조회 실패: ${e.message || e}`);
      }
    });

    btnRegisterIllegal.addEventListener("click", async () => {
      try {
        await registerIllegal();
      } catch (e) {
        setResult("neutral", `등록 실패: ${e.message || e}`);
      }
    });

    btnReloadIllegal.addEventListener("click", async () => {
      try {
        await loadIllegalList();
        await loadScans();
      } catch (e) {
        setResult("neutral", `목록 갱신 실패: ${e.message || e}`);
      }
    });

    illegalList.addEventListener("click", async (e) => {
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      const id = target.getAttribute("data-clear-id");
      if (!id) return;
      try {
        await clearIllegal(Number(id));
      } catch (err) {
        setResult("neutral", `해제 실패: ${err.message || err}`);
      }
    });
  }

  async function registerServiceWorker() {
    if (!("serviceWorker" in navigator)) return;
    try {
      await navigator.serviceWorker.register("/parking/sw.js", { scope: "/parking/" });
    } catch (_) {}
  }

  async function init() {
    bindEvents();
    registerServiceWorker();

    context = parseBootstrapContext();
    if (!token && context && context.token) {
      token = String(context.token);
    }

    const ok = await ensureContext();
    if (!ok) return;

    await loadIllegalList();
    await loadScans();
    await startCamera();
  }

  init();
})();
