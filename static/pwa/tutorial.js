(() => {
  "use strict";

  const $ = (sel) => document.querySelector(sel);
  const STORAGE_KEY = "ka_tutorial_progress_v1";

  const ROLES = [
    { key: "resident", label: "입주민", desc: "민원/문의 중심으로 가장 자주 쓰는 기능만 빠르게 익힙니다." },
    { key: "security", label: "보안·경비", desc: "주차관리 진입과 현장 처리 흐름을 우선 익힙니다." },
    { key: "site_admin", label: "단지대표자", desc: "단지 설정, 공지/FAQ 확인, 운영 관리 흐름을 익힙니다." },
    { key: "admin", label: "최고/운영관리자", desc: "초기 설정과 운영 관리 전체 흐름을 빠르게 점검합니다." },
  ];

  const ROLE_SET = new Set(ROLES.map((x) => x.key));
  const STEPS = {
    resident: [
      {
        id: "signup",
        title: "신규가입 작성",
        desc: "이름, 휴대폰, 희망 아이디, 단지 정보를 입력합니다.",
        action: "가입하기",
        href: "/pwa/login.html?force=1&mode=signup#signupCard",
      },
      {
        id: "verify",
        title: "휴대폰 인증 + 비밀번호 설정",
        desc: "인증번호 확인 후 비밀번호까지 설정하면 가입이 완료됩니다.",
        action: "인증진행",
        href: "/pwa/login.html?force=1&mode=signup#signupCard",
      },
      {
        id: "login",
        title: "로그인",
        desc: "가입한 아이디/비밀번호로 로그인합니다.",
        action: "로그인",
        href: "/pwa/login.html?force=1&mode=login",
      },
      {
        id: "complaint",
        title: "민원 작성 연습",
        desc: "민원관리 화면에서 첫 민원을 등록해 봅니다.",
        action: "민원화면",
        href: "/pwa/complaints.html",
      },
      {
        id: "faq",
        title: "공지/FAQ 확인",
        desc: "운영 안내와 자주 묻는 질문을 확인합니다.",
        action: "FAQ 보기",
        href: "/pwa/public.html#faqs",
      },
    ],
    security: [
      {
        id: "signup",
        title: "신규가입 작성",
        desc: "보안/경비 분류로 가입 요청을 작성합니다.",
        action: "가입하기",
        href: "/pwa/login.html?force=1&mode=signup#signupCard",
      },
      {
        id: "verify",
        title: "휴대폰 인증 + 비밀번호 설정",
        desc: "인증번호 확인 후 비밀번호를 설정합니다.",
        action: "인증진행",
        href: "/pwa/login.html?force=1&mode=signup#signupCard",
      },
      {
        id: "login",
        title: "로그인",
        desc: "로그인 후 보안/경비 권한으로 진입합니다.",
        action: "로그인",
        href: "/pwa/login.html?force=1&mode=login",
      },
      {
        id: "parking",
        title: "주차관리 진입",
        desc: "차량 조회/입출차 처리 기본 동선을 확인합니다.",
        action: "주차관리",
        href: "/parking/admin2",
      },
      {
        id: "notice",
        title: "공지 확인",
        desc: "근무 중 필요한 공지/긴급연락 정보를 확인합니다.",
        action: "공지 확인",
        href: "/pwa/public.html#notices",
      },
    ],
    site_admin: [
      {
        id: "signup",
        title: "신규가입 작성",
        desc: "단지대표자 분류로 가입 요청을 작성합니다.",
        action: "가입하기",
        href: "/pwa/login.html?force=1&mode=signup#signupCard",
      },
      {
        id: "verify",
        title: "휴대폰 인증 + 비밀번호 설정",
        desc: "인증번호 확인 후 비밀번호를 설정합니다.",
        action: "인증진행",
        href: "/pwa/login.html?force=1&mode=signup#signupCard",
      },
      {
        id: "login",
        title: "로그인",
        desc: "단지대표자 계정으로 로그인합니다.",
        action: "로그인",
        href: "/pwa/login.html?force=1&mode=login",
      },
      {
        id: "spec_env",
        title: "단지 제원 기본 설정",
        desc: "제원설정에서 탭/항목을 확인하고 저장합니다.",
        action: "제원설정",
        href: "/pwa/spec_env.html",
      },
      {
        id: "notice",
        title: "공지/FAQ 운영 확인",
        desc: "입주민 안내 문구와 공지를 점검합니다.",
        action: "공개안내",
        href: "/pwa/public.html",
      },
    ],
    admin: [
      {
        id: "signup",
        title: "신규가입 또는 초기계정 준비",
        desc: "최초 계정이 없으면 초기 관리자 설정을 진행합니다.",
        action: "로그인페이지",
        href: "/pwa/login.html?force=1&mode=login",
      },
      {
        id: "login",
        title: "관리자 로그인",
        desc: "운영관리자 계정으로 로그인합니다.",
        action: "로그인",
        href: "/pwa/login.html?force=1&mode=login",
      },
      {
        id: "spec_env",
        title: "제원 템플릿/항목 기본값 점검",
        desc: "단지 제원 환경변수 설정에서 기본값을 점검합니다.",
        action: "제원설정",
        href: "/pwa/spec_env.html",
      },
      {
        id: "users",
        title: "사용자/권한 운영 점검",
        desc: "신규가입 승인, 권한 분류, 단지코드 매핑을 확인합니다.",
        action: "사용자관리",
        href: "/pwa/users.html",
      },
      {
        id: "public",
        title: "공개 첫 화면 점검",
        desc: "공지/FAQ가 신규가입자 관점에서 이해되게 보이는지 점검합니다.",
        action: "공개화면",
        href: "/pwa/public.html",
      },
    ],
  };

  const state = {
    role: "resident",
    checkedByRole: {},
  };

  function safeJsonParse(raw, fallback) {
    try {
      const v = JSON.parse(String(raw || ""));
      return v && typeof v === "object" ? v : fallback;
    } catch (_e) {
      return fallback;
    }
  }

  function normalizeRole(raw) {
    const v = String(raw || "").trim().toLowerCase();
    if (!v) return "";
    if (ROLE_SET.has(v)) return v;
    if (v === "resident" || v === "입주민" || v === "입대의") return "resident";
    if (v === "security" || v === "security_guard" || v === "보안" || v === "경비") return "security";
    if (v === "site_admin" || v === "단지대표자") return "site_admin";
    if (v === "admin" || v === "최고관리자" || v === "운영관리자") return "admin";
    return "";
  }

  function readState() {
    const saved = safeJsonParse(localStorage.getItem(STORAGE_KEY), {});
    const checkedByRole = saved && typeof saved.checkedByRole === "object" ? saved.checkedByRole : {};
    state.checkedByRole = checkedByRole;

    const u = new URL(window.location.href);
    const roleFromQuery = normalizeRole(u.searchParams.get("role"));
    const roleFromSaved = normalizeRole(saved.role);
    state.role = roleFromQuery || roleFromSaved || "resident";
  }

  function writeState() {
    const payload = {
      role: state.role,
      checkedByRole: state.checkedByRole,
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  }

  function roleMeta(roleKey) {
    return ROLES.find((x) => x.key === roleKey) || ROLES[0];
  }

  function roleSteps(roleKey) {
    return Array.isArray(STEPS[roleKey]) ? STEPS[roleKey] : [];
  }

  function checkedMap(roleKey) {
    if (!state.checkedByRole[roleKey] || typeof state.checkedByRole[roleKey] !== "object") {
      state.checkedByRole[roleKey] = {};
    }
    return state.checkedByRole[roleKey];
  }

  function renderRoleTabs() {
    const wrap = $("#roleTabs");
    if (!wrap) return;
    wrap.innerHTML = "";
    for (const role of ROLES) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = `role-tab${state.role === role.key ? " active" : ""}`;
      btn.dataset.role = role.key;
      btn.textContent = role.label;
      wrap.appendChild(btn);
    }
    const desc = $("#roleDesc");
    if (desc) desc.textContent = roleMeta(state.role).desc;
  }

  function progressFor(roleKey) {
    const steps = roleSteps(roleKey);
    const map = checkedMap(roleKey);
    const total = steps.length;
    const done = steps.filter((s) => !!map[s.id]).length;
    return { total, done, pct: total ? Math.round((done / total) * 100) : 0 };
  }

  function renderProgress() {
    const info = progressFor(state.role);
    const text = $("#progressText");
    const fill = $("#progressFill");
    if (text) text.textContent = `${info.done}/${info.total} 완료 (${info.pct}%)`;
    if (fill) fill.style.width = `${info.pct}%`;
  }

  function renderSteps() {
    const wrap = $("#stepList");
    if (!wrap) return;
    const steps = roleSteps(state.role);
    const map = checkedMap(state.role);

    wrap.innerHTML = steps
      .map((s, idx) => {
        const done = !!map[s.id];
        const checked = done ? "checked" : "";
        return `
          <article class="step-item${done ? " done" : ""}" data-step-id="${s.id}">
            <input class="step-check" type="checkbox" data-step-id="${s.id}" ${checked} aria-label="${idx + 1}단계 완료" />
            <div>
              <h3 class="step-title">${idx + 1}. ${s.title}</h3>
              <p class="step-desc">${s.desc}</p>
            </div>
            <a class="btn step-go" href="${s.href}">${s.action}</a>
          </article>
        `;
      })
      .join("");

    renderProgress();
  }

  function setRole(roleKey) {
    const next = normalizeRole(roleKey);
    if (!next) return;
    state.role = next;
    writeState();
    renderRoleTabs();
    renderSteps();
  }

  function setStepChecked(stepId, checked) {
    const id = String(stepId || "").trim();
    if (!id) return;
    const map = checkedMap(state.role);
    map[id] = !!checked;
    writeState();
    renderSteps();
  }

  function setAllCurrentRole(checked) {
    const map = checkedMap(state.role);
    for (const step of roleSteps(state.role)) {
      map[step.id] = !!checked;
    }
    writeState();
    renderSteps();
  }

  function wire() {
    $("#roleTabs")?.addEventListener("click", (e) => {
      const btn = e.target.closest("button.role-tab[data-role]");
      if (!btn) return;
      setRole(btn.dataset.role || "");
    });

    $("#stepList")?.addEventListener("change", (e) => {
      const input = e.target.closest("input.step-check[data-step-id]");
      if (!input) return;
      setStepChecked(input.dataset.stepId || "", !!input.checked);
    });

    $("#btnCheckAll")?.addEventListener("click", () => setAllCurrentRole(true));
    $("#btnResetRole")?.addEventListener("click", () => setAllCurrentRole(false));
  }

  function init() {
    readState();
    wire();
    renderRoleTabs();
    renderSteps();
  }

  init();
})();
