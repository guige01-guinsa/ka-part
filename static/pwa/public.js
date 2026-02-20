(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);

  function escapeHtml(v) {
    return String(v)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function formatDateTime(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    const normalized = raw.replace(" ", "T");
    const dt = new Date(normalized);
    if (Number.isNaN(dt.getTime())) return raw;
    return dt.toLocaleString("ko-KR", { hour12: false });
  }

  async function fetchJson(url) {
    const res = await fetch(url, { method: "GET", credentials: "same-origin" });
    const ct = res.headers.get("content-type") || "";
    const body = ct.includes("application/json") ? await res.json() : await res.text();
    if (!res.ok) {
      const detail = body && typeof body === "object" ? (body.detail || body.message) : "";
      throw new Error(String(detail || body || `HTTP ${res.status}`));
    }
    return body;
  }

  function setMsg(id, text, isErr) {
    const el = $(id);
    if (!el) return;
    el.textContent = String(text || "");
    el.classList.toggle("err", !!isErr);
  }

  function renderNotices(items) {
    const wrap = $("#noticesList");
    if (!wrap) return;
    const rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      wrap.innerHTML = '<div class="feed-empty">공지 항목이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = rows
      .map((it) => {
        const title = escapeHtml(it.title || "-");
        const content = escapeHtml(it.content || "");
        const pinned = !!it.is_pinned;
        const publishedAt = formatDateTime(it.published_at || it.created_at);
        const badge = pinned ? '<span class="pin-badge">PIN</span>' : "";
        const metaRight = publishedAt ? `<span>${escapeHtml(publishedAt)}</span>` : "";
        return `
          <article class="feed-card">
            <div class="feed-meta">
              <div>${badge}</div>
              <div>${metaRight}</div>
            </div>
            <h4 class="feed-title">${title}</h4>
            <p class="feed-body">${content}</p>
          </article>
        `;
      })
      .join("");
  }

  function renderFaqs(items) {
    const wrap = $("#faqsList");
    if (!wrap) return;
    const rows = Array.isArray(items) ? items : [];
    if (!rows.length) {
      wrap.innerHTML = '<div class="feed-empty">FAQ 항목이 없습니다.</div>';
      return;
    }
    wrap.innerHTML = rows
      .map((it) => {
        const q = escapeHtml(it.question || "-");
        const a = escapeHtml(it.answer || "");
        return `
          <details class="faq-entry">
            <summary>
              <div class="faq-row">
                <div class="faq-question">${q}</div>
                <div class="faq-toggle">열기</div>
              </div>
            </summary>
            <p class="faq-answer">${a}</p>
          </details>
        `;
      })
      .join("");
  }

  async function init() {
    setMsg("#noticesMsg", "불러오는 중...", false);
    setMsg("#faqsMsg", "불러오는 중...", false);

    const [notices, faqs] = await Promise.all([
      fetchJson("/api/v1/notices?limit=50"),
      fetchJson("/api/v1/faqs?limit=120"),
    ]);

    renderNotices(notices && notices.items ? notices.items : []);
    renderFaqs(faqs && faqs.items ? faqs.items : []);
    setMsg("#noticesMsg", "", false);
    setMsg("#faqsMsg", "", false);
  }

  try {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("/pwa/sw.js?v=20260208a").catch(() => {});
    }
  } catch (_e) {}

  init().catch((e) => {
    const msg = e && e.message ? e.message : String(e);
    setMsg("#noticesMsg", `공지 로딩 오류: ${msg}`, true);
    setMsg("#faqsMsg", `FAQ 로딩 오류: ${msg}`, true);
  });
})();
