(() => {
  const login = new URL(location.href).searchParams.get("login") || "vendor1";
  const headers = { "X-User-Login": login };

  const qEl = document.getElementById("q");
  const modeEl = document.getElementById("mode");
  const cardsEl = document.getElementById("vendorCards");
  const emptyEl = document.getElementById("empty");
  const commentBox = document.getElementById("commentBox");
  const commentTitle = document.getElementById("commentTitle");
  const commentText = document.getElementById("commentText");
  const commentMsg = document.getElementById("commentMsg");
  const btnComment = document.getElementById("btnComment");
  const btnReload = document.getElementById("btnReload");
  const detailBox = document.getElementById("detailBox");
  const detailTitle = document.getElementById("detailTitle");
  const detailBody = document.getElementById("detailBody");
  const attachmentList = document.getElementById("attachmentList");

  let selectedWorkId = null;

  const api = (path, opts = {}) =>
    fetch(path, { ...opts, headers: { ...headers, ...(opts.headers || {}) } })
      .then((r) => r.json());

  function render(items) {
    cardsEl.innerHTML = "";
    if (!items.length) {
      emptyEl.style.display = "block";
      return;
    }
    emptyEl.style.display = "none";
    items.forEach((w) => {
      const div = document.createElement("div");
      div.style.border = "1px solid #25324a";
      div.style.borderRadius = "12px";
      div.style.padding = "10px";
      div.style.background = "rgba(0,0,0,.08)";
      div.innerHTML = `
        <div style="font-weight:800;">${w.work_code || ""} <span class="small">[${w.status}]</span></div>
        <div style="margin-top:6px;">${w.title || "-"}</div>
        <div class="small" style="margin-top:6px;">${w.location_name || "-"} / ${w.category_name || "-"}</div>
        <button class="pill" style="margin-top:8px;">문의/답변</button>
      `;
      div.querySelector("button").onclick = (e) => {
        e.stopPropagation();
        openComment(w);
      };
      div.onclick = () => openDetail(w);
      cardsEl.appendChild(div);
    });
  }

  function openDetail(w) {
    selectedWorkId = w.id;
    loadDetail(w.id);
  }

  function openComment(w) {
    selectedWorkId = w.id;
    commentTitle.textContent = `${w.work_code || ""} ${w.title || ""}`;
    commentText.value = "";
    commentMsg.textContent = "";
    commentBox.style.display = "block";
    loadDetail(w.id);
  }

  async function loadDetail(workId) {
    detailBox.style.display = "block";
    detailTitle.textContent = "";
    detailBody.innerHTML = "";
    attachmentList.innerHTML = "로딩 중...";
    const work = await api(`/api/works/${workId}`);
    if (work.ok) {
      const w = work.work;
      detailTitle.textContent = `${w.work_code || ""} [${w.status}]`;
      detailBody.innerHTML = `
        <div>${w.title || "-"}</div>
        <div class="small" style="margin-top:6px;">위치: ${w.location_name || "-"}</div>
        <div class="small">분류: ${w.category_name || "-"}</div>
        <div class="small">결과: ${w.result_note || "-"}</div>
      `;
    }

    const atts = await api(`/api/attachments?entity_type=WORK_ORDER&entity_id=${workId}`);
    if (!atts.ok || !atts.items) {
      attachmentList.textContent = "첨부파일 없음";
      return;
    }
    if (!atts.items.length) {
      attachmentList.textContent = "첨부파일 없음";
      return;
    }
    attachmentList.innerHTML = "";
    atts.items.forEach((a) => {
      const link = document.createElement("a");
      link.href = `/api/attachments/file/${a.id}?login=${encodeURIComponent(login)}`;
      link.textContent = a.file_name || `attachment_${a.id}`;
      link.style.display = "block";
      link.style.marginBottom = "4px";
      attachmentList.appendChild(link);
    });
  }

  async function load() {
    const mode = modeEl.value || "open";
    const q = encodeURIComponent(qEl.value || "");
    const res = await api(`/api/works?mode=${mode}&q=${q}&limit=200`);
    if (!res.ok) {
      emptyEl.style.display = "block";
      return;
    }
    render(res.items || []);
  }

  btnReload.onclick = load;
  btnComment.onclick = async () => {
    if (!selectedWorkId) return;
    const note = commentText.value.trim();
    if (!note) return;
    const res = await api(`/api/works/${selectedWorkId}/comment`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({ note }),
    });
    if (res.ok) {
      commentMsg.textContent = "전송 완료";
      commentText.value = "";
    } else {
      commentMsg.textContent = res.detail || "전송 실패";
    }
  };

  qEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter") load();
  });

  load();
})();
