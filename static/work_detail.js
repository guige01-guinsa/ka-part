// static/work_detail.js
(() => {
  "use strict";

  // --------------------
  // utils
  // --------------------
  const qs = (sel) => document.querySelector(sel);

  function getWorkIdFromPath() {
    // /ui/works/{id}
    let m = location.pathname.match(/\/ui\/works\/(\d+)/);
    if (!m) m = location.pathname.match(/\/ui\/work\/(\d+)/);
    return m ? parseInt(m[1], 10) : null;
  }

  function getLogin() {
    // 1) input#login 우선
    const el = qs("#login");
    const v = el && el.value ? el.value.trim() : "";
    if (v) return v;

    // 2) querystring ?login=
    const p = new URLSearchParams(location.search);
    return (p.get("login") || "").trim();
  }

  async function apiFetch(url, opts = {}) {
    const login = getLogin();
    const headers = new Headers(opts.headers || {});
    if (login) headers.set("X-User-Login", login);

    // Also pass login via query to survive cross-origin redirects.
    let finalUrl = url;
    if (login) {
      try {
        const u = new URL(url, location.origin);
        if (!u.searchParams.get("login")) {
          u.searchParams.set("login", login);
        }
        finalUrl = u.pathname + u.search + u.hash;
      } catch (_) {}
    }

    // JSON default accept
    if (!headers.has("Accept")) headers.set("Accept", "application/json");

    const res = await fetch(finalUrl, { ...opts, headers });
    let data = null;

    const ct = res.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      try { data = await res.json(); } catch (_) {}
    } else {
      try { data = await res.text(); } catch (_) {}
    }

    if (!res.ok) {
      const msg = (data && data.detail) ? data.detail : `HTTP ${res.status}`;
      throw new Error(msg);
    }
    return data;
  }

  function fmt(s) {
    return s == null ? "" : String(s);
  }

  function escapeHtml(s) {
    return fmt(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function setStatusBadge(el, status) {
    if (!el) return;
    el.textContent = status || "-";
    el.setAttribute("data-status", status || "");
  }

  function roleHas(me, roleName) {
    const roles = me && me.roles ? me.roles : [];
    return roles.includes(roleName);
  }

  function roleHasAny(me, list) {
    const roles = me && me.roles ? me.roles : [];
    return list.some(r => roles.includes(r));
  }

  // --------------------
  // permission model (UI side)
  // --------------------
  function canUploadAttachment(me, work) {
    if (!me || !work) return false;
    if (work.status === "DONE") return !!me.is_admin;
    return !!me.is_admin || roleHasAny(me, ["TECH","STAFF","LEAD","FACILITY_MANAGER","CHIEF","MANAGER","ADMIN"]);
  }

  function canDeleteAttachment(me, work) {
    if (!me || !work) return false;
    if (work.status === "DONE") return !!me.is_admin;
    return !!me.is_admin;
  }

  function isReadOnlyMode(me, work) {
    if (!me || !work) return true;
    // DONE면 현장 사용자는 read-only
    if (work.status === "DONE" && !me.is_admin) return true;
    return false;
  }

  function allowedTransitions(me, work) {
    const cur = work.status;

    const map = {
      "NEW": ["ASSIGNED", "IN_PROGRESS", "HOLD", "CANCELED"],
      "ASSIGNED": ["IN_PROGRESS", "REVIEW", "HOLD", "CANCELED"],
      "IN_PROGRESS": ["REVIEW", "HOLD", "CANCELED"],
      "REVIEW": ["APPROVED", "REJECTED", "IN_PROGRESS"],
      "APPROVED": ["DONE", "REJECTED"],
      "REJECTED": ["IN_PROGRESS", "CANCELED"],
      "HOLD": ["IN_PROGRESS", "CANCELED"],
      "DONE": [],
      "CANCELED": [],
    };

    const nexts = map[cur] || [];

    if (me.is_admin || roleHasAny(me, ["CHIEF","MANAGER","ADMIN"])) return nexts;

    if (roleHasAny(me, ["TECH","STAFF","LEAD","FACILITY_MANAGER"])) {
      return nexts.filter(s => s === "IN_PROGRESS" || s === "REVIEW" || s === "HOLD" || s === "CANCELED");
    }

    return [];
  }

  // --------------------
  // rendering
  // --------------------
  function renderWork(work) {
    setStatusBadge(qs("#workStatus"), work.status);

    const titleEl = qs("#workTitle");
    if (titleEl) titleEl.textContent = fmt(work.title || work.work_code || `WORK #${work.id}`);

    const codeEl = qs("#workCode");
    if (codeEl) codeEl.textContent = fmt(work.work_code || "");

    const metaEl = qs("#workMeta");
    if (metaEl) {
      metaEl.textContent = [
        `ID: ${work.id}`,
        work.location_id ? `location_id=${work.location_id}` : "",
        work.category_id ? `category_id=${work.category_id}` : "",
      ].filter(Boolean).join(" · ");
    }

    const noteEl = qs("#resultNote");
    if (noteEl) noteEl.value = fmt(work.result_note || "");

    const urgentEl = qs("#urgent");
    if (urgentEl) urgentEl.checked = !!work.urgent;

    const completedEl = qs("#completedAt");
    if (completedEl) completedEl.textContent = fmt(work.completed_at || "");
  }

  function renderAttachments(me, work) {
    const box = qs("#attachments");
    if (!box) return;

    const items = (work.attachments || []);
    if (!items.length) {
      box.innerHTML = `<div class="muted">첨부가 없습니다.</div>`;
      return;
    }

    const canDel = canDeleteAttachment(me, work);

    box.innerHTML = items.map(a => {
      const id = a.id;
      const fn = escapeHtml(a.file_name || `attachment_${id}`);
      const mime = escapeHtml(a.mime_type || "");
      const path = escapeHtml(a.file_path || "");
      const createdAt = escapeHtml(a.created_at || "");

      const delBtn = canDel
        ? `<button class="btn danger" data-act="att-del" data-id="${id}">삭제</button>`
        : "";

      const fileLink = `/api/attachments/file/${id}`;
      return `
        <div class="card" style="display:flex; gap:10px; align-items:center; justify-content:space-between; margin:8px 0;">
          <div style="min-width:0;">
            <div style="font-weight:800; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${fn}</div>
            <div class="muted" style="font-size:12px;">
              ${createdAt ? `등록: ${createdAt} · ` : ""}${mime ? `type=${mime} · ` : ""}${path ? `path=${path}` : ""}
            </div>
            <div style="margin-top:6px;">
              <a class="btn" href="${fileLink}" target="_blank" rel="noopener">열기</a>
            </div>
          </div>
          <div style="flex:0 0 auto; display:flex; gap:6px;">
            ${delBtn}
          </div>
        </div>
      `;
    }).join("");
  }

  function renderEvents(work) {
    const box = qs("#events");
    if (!box) return;

    const evs = (work.events || []);
    if (!evs.length) {
      box.innerHTML = `<div class="muted">이력이 없습니다.</div>`;
      return;
    }

    box.innerHTML = evs.map(e => {
      const t = escapeHtml(e.event_type || "");
      const note = escapeHtml(e.note || "");
      const fromS = escapeHtml(e.from_status || "");
      const toS = escapeHtml(e.to_status || "");
      const at = escapeHtml(e.created_at || "");
      return `
        <div class="card" style="margin:8px 0;">
          <div style="font-weight:900;">
            ${t}
            ${fromS || toS ? `<span class="muted"> [${fromS} → ${toS}]</span>` : ""}
          </div>
          ${note ? `<div style="margin-top:4px;">${note}</div>` : ""}
          ${at ? `<div class="muted" style="margin-top:4px; font-size:12px;">${at}</div>` : ""}
        </div>
      `;
    }).join("");
  }

  function renderTransitionButtons(me, work) {
    const box = qs("#transitions");
    if (!box) return;

    const nexts = allowedTransitions(me, work);
    if (!nexts.length) {
      box.innerHTML = `<div class="muted">전이 가능한 단계가 없습니다.</div>`;
      return;
    }

    box.innerHTML = nexts.map(st => {
      return `<button class="btn" data-act="transition" data-to="${st}">${st}</button>`;
    }).join(" ");
  }

  function applyReadOnly(me, work) {
    const ro = isReadOnlyMode(me, work);

    const noteEl = qs("#resultNote");
    if (noteEl) noteEl.disabled = ro;

    const urgentEl = qs("#urgent");
    if (urgentEl) urgentEl.disabled = ro;

    const saveBtn = qs("#btnSave");
    if (saveBtn) saveBtn.disabled = ro;
    const delBtn = qs("#btnDelete");
    if (delBtn) delBtn.disabled = !(me && (me.is_admin));

    const up = qs("#attFile");
    const upBtn = qs("#btnUpload");
    const upAllowed = canUploadAttachment(me, work) && !ro;
    if (up) up.disabled = !upAllowed;
    if (upBtn) upBtn.disabled = !upAllowed;

    const trans = qs("#transitions");
    if (trans && ro) {
      trans.querySelectorAll("button").forEach(b => (b.disabled = true));
    }

    const roMsg = qs("#readOnlyMsg");
    if (roMsg) {
      roMsg.style.display = ro ? "block" : "none";
      if (ro) roMsg.textContent = "DONE 상태입니다. 현장 사용자는 읽기 전용입니다.";
    }
  }

  // --------------------
  // actions
  // --------------------
  async function doPatchWork(workId) {
    const body = {};
    const noteEl = qs("#resultNote");
    const urgentEl = qs("#urgent");

    if (noteEl) body.result_note = noteEl.value || "";
    if (urgentEl) body.urgent = !!urgentEl.checked;

    return await apiFetch(`/api/works/${workId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async function doTransition(workId, toStatus) {
    const note = (qs("#transitionNote") && qs("#transitionNote").value) ? qs("#transitionNote").value : "";
    const body = { to_status: toStatus, note };

    return await apiFetch(`/api/works/${workId}/status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async function doUploadAttachment(workId) {
    const f = qs("#attFile");
    if (!f || !f.files || !f.files[0]) throw new Error("첨부 파일을 선택하세요.");

    const fd = new FormData();
    fd.append("entity_type", "WORK_ORDER");
    fd.append("entity_id", String(workId));
    fd.append("file", f.files[0]);

    return await apiFetch(`/api/attachments`, {
      method: "POST",
      body: fd,
    });
  }

  async function doDeleteAttachment(attId) {
    return await apiFetch(`/api/attachments/${attId}`, { method: "DELETE" });
  }

  async function doDeleteWork(workId) {
    return await apiFetch(`/api/works/${workId}`, { method: "DELETE" });
  }

  // --------------------
  // main load
  // --------------------
  async function load() {
    const workId = getWorkIdFromPath();
    if (!workId) {
      const err = qs("#error");
      if (err) err.textContent = "잘못된 접근입니다. (work id 없음)";
      return;
    }

    const me = await apiFetch("/api/me");
    const loginEl = qs("#login");
    if (loginEl && (!loginEl.value || !loginEl.value.trim()) && me?.login) {
      loginEl.value = me.login;
    }
    const meEl = qs("#me");
    if (meEl) {
      const r = (me.roles || []).join(", ");
      meEl.textContent = `${fmt(me.name)} (${fmt(me.login)}) · roles=[${r}] · is_admin=${!!me.is_admin}`;
    }

    const w = await apiFetch(`/api/works/${workId}`);
    const work = w.work ? w.work : w;

    renderWork(work);
    renderTransitionButtons(me, work);
    renderAttachments(me, work);
    renderEvents(work);
    applyReadOnly(me, work);

    const saveBtn = qs("#btnSave");
    if (saveBtn) {
      saveBtn.onclick = async () => {
        try {
          await doPatchWork(workId);
          await load();
        } catch (e) {
          alert(e.message || String(e));
        }
      };
    }

    const delBtn = qs("#btnDelete");
    if (delBtn) {
      delBtn.onclick = async () => {
        if (!confirm("삭제(취소) 하시겠습니까?")) return;
        try {
          await doDeleteWork(workId);
          window.location.href = `/ui/works?login=${encodeURIComponent(getLogin() || "admin")}`;
        } catch (e) {
          alert(e.message || String(e));
        }
      };
    }

    const upBtn = qs("#btnUpload");
    if (upBtn) {
      upBtn.onclick = async () => {
        try {
          await doUploadAttachment(workId);
          const f = qs("#attFile");
          if (f) f.value = "";
          await load();
        } catch (e) {
          alert(e.message || String(e));
        }
      };
    }

    const transBox = qs("#transitions");
    if (transBox) {
      transBox.onclick = async (ev) => {
        const btn = ev.target.closest("button[data-act='transition']");
        if (!btn) return;
        const to = btn.getAttribute("data-to");
        if (!to) return;
        try {
          await doTransition(workId, to);
          await load();
        } catch (e) {
          alert(e.message || String(e));
        }
      };
    }

    const attBox = qs("#attachments");
    if (attBox) {
      attBox.onclick = async (ev) => {
        const btn = ev.target.closest("button[data-act='att-del']");
        if (!btn) return;
        const id = parseInt(btn.getAttribute("data-id"), 10);
        if (!id) return;
        if (!confirm("첨부를 삭제(소프트 삭제)할까요?")) return;

        try {
          await doDeleteAttachment(id);
          await load();
        } catch (e) {
          alert(e.message || String(e));
        }
      };
    }

    const loginEl = qs("#login");
    if (loginEl) {
      loginEl.addEventListener("change", async () => {
        try {
          await load();
        } catch (e) {
          alert(e.message || String(e));
        }
      });
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    load().catch((e) => {
      const err = qs("#error");
      if (err) err.textContent = e.message || String(e);
    });
  });
})();

