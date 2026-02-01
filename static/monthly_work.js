(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);

  let currentUser = { roles: [], is_admin: false, login: "" };

  function toast(msg) {
    const t = $("toast");
    if (!t) return alert(msg);
    t.textContent = msg;
    t.style.display = "block";
    setTimeout(() => (t.style.display = "none"), 1800);
  }

  function getLogin() {
    const inp = $("login");
    const v = inp ? (inp.value || "").trim() : "";
    if (v) localStorage.setItem("KA_LOGIN", v);
    return v || (localStorage.getItem("KA_LOGIN") || "admin");
  }

  function setLoginFromStorage() {
    const saved = localStorage.getItem("KA_LOGIN");
    if ($("login") && saved) $("login").value = saved;
  }

  function yyyymmValue() {
    return ($("yyyymm").value || "").trim();
  }

  async function fetchJSON(url, opts = {}) {
    const login = getLogin();
    const res = await fetch(url, {
      ...opts,
      headers: {
        ...(opts.headers || {}),
        "X-User-Login": login,
      },
    });

    const ct = res.headers.get("content-type") || "";
    const data = ct.includes("application/json")
      ? await res.json().catch(() => ({}))
      : await res.text().catch(() => "");

    if (!res.ok) {
      const msg = (data && data.detail) ? data.detail : (typeof data === "string" ? data : `HTTP ${res.status}`);
      throw new Error(msg);
    }
    return data;
  }

  async function loadMe() {
    // ✅ /api/me 기반 role-driven
    const login = getLogin();
    const data = await fetchJSON(`/api/me?login=${encodeURIComponent(login)}`);
    currentUser = {
      login: data.login,
      roles: data.roles || [],
      is_admin: !!data.is_admin,
    };
    return currentUser;
  }

  function setPdfLinks(yyyymm) {
    const login = getLogin();

    $("btnPdfLive").href =
      `/api/reports/monthly-work.pdf?yyyymm=${encodeURIComponent(yyyymm)}&login=${encodeURIComponent(login)}`;

    const rid = $("selReports").value;
    $("btnPdfSnap").href = rid
      ? `/api/reports/monthly-work/report.pdf?report_id=${encodeURIComponent(rid)}&login=${encodeURIComponent(login)}`
      : "#";
  }

  function applyReportButtonLock(status) {
    const st = (status || "").toUpperCase();

    const btnSubmit = $("btnSubmit");
    const btnApprove = $("btnApprove");
    if (!btnSubmit || !btnApprove) return;

    // ✅ 진짜 권한: /api/me 결과
    const isAdminUI = !!currentUser.is_admin;

    // admin 아닌 경우 승인 버튼 숨김(UX 잠금)
    btnApprove.style.display = isAdminUI ? "" : "none";

    // 기본 잠금
    btnSubmit.disabled = true;
    btnApprove.disabled = true;

    // 상태별 활성
    if (st === "DRAFT") {
      // 제출: 기본은 admin(관리소장/시설과장)만 허용으로 두는 게 안전
      // 필요시 staff 제출 허용로 확장 가능
      btnSubmit.disabled = !isAdminUI;
    } else if (st === "SUBMITTED") {
      // 승인: admin만
      btnApprove.disabled = !isAdminUI;
    } else if (st === "APPROVED") {
      // 둘 다 잠금 유지
    }
  }

  function fmt(n) {
    return (n === null || n === undefined) ? "-" : String(n);
  }

  function render(data) {
    const s = data.summary;
    const sb = data.status_breakdown;

    $("summary").innerHTML = `
      <div style="font-weight:900;">요약</div>
      <div class="muted" style="margin-top:6px;">기간: ${data.range.start} ~ ${data.range.next_start}(미포함)</div>
      <div style="margin-top:8px; line-height:1.8;">
        <div>생성: <b>${fmt(s.total_created)}</b> / 완료(DONE): <b>${fmt(s.done_count)}</b> / 미완료: <b>${fmt(s.not_done_count)}</b> / 완료율: <b>${fmt(s.done_rate_pct)}%</b></div>
        <div>긴급 생성: <b>${fmt(s.urgent_created)}</b> / 긴급 미완료: <b>${fmt(s.urgent_open)}</b></div>
        <div class="muted">상태: NEW ${fmt(sb.NEW)} · ASSIGNED ${fmt(sb.ASSIGNED)} · IN_PROGRESS ${fmt(sb.IN_PROGRESS)} · REVIEW ${fmt(sb.REVIEW)} · APPROVED ${fmt(sb.APPROVED)} · DONE ${fmt(sb.DONE)}</div>
      </div>
    `;

    const loc = data.by_location || [];
    $("byLocation").innerHTML = loc.length ? `
      <table class="tbl">
        <thead><tr><th>위치</th><th>생성</th><th>미완료</th><th>완료</th><th>긴급</th></tr></thead>
        <tbody>
          ${loc.map(r => `
            <tr>
              <td>${r.location_name ?? "-"}</td>
              <td>${fmt(r.created)}</td>
              <td><b>${fmt(r.open)}</b></td>
              <td>${fmt(r.done)}</td>
              <td>${fmt(r.urgent)}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    ` : `<div class="muted">데이터 없음</div>`;

    const top = data.top_open || [];
    $("topOpen").innerHTML = top.length ? top.map(r => `
      <div class="box" style="margin-top:8px;">
        <div style="font-weight:900;">${r.work_code || ("#" + r.id)} <span class="muted">[${r.status}]</span></div>
        <div class="meta">${r.title || "-"}</div>
        <div style="margin-top:6px;">
          <a class="btn" href="/ui/works/${r.id}">상세</a>
        </div>
      </div>
    `).join("") : `<div class="muted">미완료 없음</div>`;
  }

  async function loadMonthly() {
    const yyyymm = yyyymmValue();
    if (!yyyymm) throw new Error("yyyymm이 비었습니다.");

    const login = getLogin();
    const url = `/api/reports/monthly-work?yyyymm=${encodeURIComponent(yyyymm)}&login=${encodeURIComponent(login)}`;
    const data = await fetchJSON(url);

    render(data);
    setPdfLinks(yyyymm);
  }

  async function loadReportsList() {
    const yyyymm = yyyymmValue();
    const login = getLogin();

    const url = `/api/reports/monthly-work/reports?yyyymm=${encodeURIComponent(yyyymm)}&login=${encodeURIComponent(login)}`;
    const data = await fetchJSON(url);

    const sel = $("selReports");
    sel.innerHTML = "";

    if (!data.items || data.items.length === 0) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "(없음)";
      sel.appendChild(opt);
      applyReportButtonLock("");
      setPdfLinks(yyyymm);
      return;
    }

    for (const it of data.items) {
      const opt = document.createElement("option");
      opt.value = it.id;
      opt.textContent = `${it.report_code} [${it.status}]`;
      sel.appendChild(opt);
    }

    sel.value = data.items[0].id;
    setPdfLinks(yyyymm);
    applyReportButtonLock(data.items[0].status);
  }

  async function generateReport() {
    const yyyymm = yyyymmValue();
    const login = getLogin();
    const url = `/api/reports/monthly-work/generate?yyyymm=${encodeURIComponent(yyyymm)}&login=${encodeURIComponent(login)}`;
    const r = await fetchJSON(url, { method: "POST" });
    toast(`생성: ${r.report_code}`);
    await loadReportsList();
  }

  async function transitionReport(toStatus) {
    const rid = $("selReports").value;
    if (!rid) throw new Error("선택된 보고서가 없습니다.");

    const login = getLogin();
    const url = `/api/reports/monthly-work/report/status?report_id=${encodeURIComponent(rid)}&login=${encodeURIComponent(login)}`;

    const body = JSON.stringify({ to_status: toStatus });
    const r = await fetchJSON(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });

    toast(`전이: ${r.from} → ${r.to}`);
    await loadReportsList();
  }

  document.addEventListener("DOMContentLoaded", () => {
    setLoginFromStorage();

    // ✅ login 변경 시 /api/me 다시 로드 + UI 잠금 즉시 반영
    $("login")?.addEventListener("change", async () => {
      try {
        await loadMe();
        await loadReportsList();
        setPdfLinks(yyyymmValue());
        toast(`사용자: ${currentUser.login} (${(currentUser.roles||[]).join(",") || "역할없음"})`);
      } catch (e) {
        toast(e.message);
      }
    });

    $("btnLoad").addEventListener("click", async () => {
      try {
        await loadMe();
        await loadMonthly();
        await loadReportsList();
        toast("조회 완료");
      } catch (e) {
        toast(e.message);
      }
    });

    $("btnGen").addEventListener("click", async () => {
      try {
        await loadMe();
        await generateReport();
      } catch (e) {
        toast(e.message);
      }
    });

    $("selReports").addEventListener("change", () => {
      setPdfLinks(yyyymmValue());
      const txt = $("selReports").selectedOptions?.[0]?.textContent || "";
      const m = txt.match(/\[(.+)\]/);
      applyReportButtonLock(m ? m[1] : "");
    });

    $("btnSubmit").addEventListener("click", () => {
      transitionReport("SUBMITTED").catch(e => toast(e.message));
    });

    $("btnApprove").addEventListener("click", () => {
      transitionReport("APPROVED").catch(e => toast(e.message));
    });

    // 최초 자동 로드
    (async () => {
      try {
        await loadMe();
        await loadMonthly();
        await loadReportsList();
      } catch (e) {
        toast(e.message);
      }
    })();
  });
})();
