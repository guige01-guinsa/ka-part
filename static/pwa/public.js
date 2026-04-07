(() => {
  "use strict";

  const notices = [
    {
      title: "운영 방식",
      content: "전화, 카톡, 방문 민원은 관리사무소 직원이 직접 입력하고 AI 자동분류 후 저장합니다.",
    },
    {
      title: "일일보고",
      content: "일일보고 생성 버튼으로 당일 민원 요약, 긴급 민원, 내일 처리 항목을 자동으로 만듭니다.",
    },
  ];

  const faqs = [
    {
      question: "이 시스템은 기존 시설운영관리 시스템에 붙일 수 있나요?",
      answer: "가능합니다. 테넌트별 API Key로 민원 등록, 목록 조회, 일일보고 생성 기능을 모듈처럼 연동할 수 있습니다.",
    },
    {
      question: "카카오톡 단체방 정리는 어떻게 하나요?",
      answer: "카톡 대화 원문을 붙여 넣으면 AI가 중복 제거, 중요도 판단, 엑셀 입력용 리스트를 자동으로 정리합니다.",
    },
  ];

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function renderList(selector, items, type) {
    const wrap = document.querySelector(selector);
    if (!wrap) return;
    wrap.innerHTML = items
      .map((item) => {
        if (type === "notice") {
          return `<article class="feed-item"><h3>${escapeHtml(item.title)}</h3><p>${escapeHtml(item.content)}</p></article>`;
        }
        return `<details class="feed-item"><summary><strong>${escapeHtml(item.question)}</strong></summary><p>${escapeHtml(item.answer)}</p></details>`;
      })
      .join("");
  }

  renderList("#noticesList", notices, "notice");
  renderList("#faqsList", faqs, "faq");
})();
