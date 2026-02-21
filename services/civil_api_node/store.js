const SCOPE = new Set(["COMMON", "PRIVATE", "EMERGENCY"]);
const STATUS = new Set(["RECEIVED", "TRIAGED", "GUIDANCE_SENT", "ASSIGNED", "IN_PROGRESS", "COMPLETED", "CLOSED"]);
const PRIORITY = new Set(["LOW", "NORMAL", "HIGH", "URGENT"]);
const RESOLUTION = new Set(["REPAIR", "GUIDANCE_ONLY", "EXTERNAL_VENDOR"]);
const VISIT_REASON = new Set(["FIRE_INSPECTION", "NEIGHBOR_DAMAGE", "EMERGENCY_INFRA"]);
const WORK_STATUS = new Set(["OPEN", "DISPATCHED", "DONE", "CANCELED"]);

const state = {
  seq: { complaint: 0, workOrder: 0, visit: 0, notice: 0, comment: 0 },
  categories: [
    { id: 1, name: "Electric / Lighting (Common)", scope: "COMMON", is_active: true },
    { id: 2, name: "Water / Drainage (Common)", scope: "COMMON", is_active: true },
    { id: 3, name: "Fire Safety (Common)", scope: "COMMON", is_active: true },
    { id: 4, name: "Inside Unit Fixture", scope: "PRIVATE", is_active: true },
    { id: 5, name: "Emergency Leak / Blackout / Fire", scope: "EMERGENCY", is_active: true }
  ],
  complaints: new Map(),
  workOrders: new Map(),
  visits: new Map(),
  notices: new Map(),
  comments: [],
  faqs: [
    {
      id: 1,
      question: "실내 조명 교체를 관리사무소가 해주나요?",
      answer: "아니요. 실내 설비는 세대 내부(개인) 영역입니다. 관리사무소는 사용 안내를 도와드릴 수 있습니다.",
      display_order: 10,
      is_active: true
    },
    {
      id: 2,
      question: "직원이 세대에 방문할 수 있는 경우는 언제인가요?",
      answer: "소방시설 점검, 이웃 피해 예방, 정전·누수 등 긴급 시설 조치가 필요한 경우에 한해 가능합니다.",
      display_order: 20,
      is_active: true
    }
  ]
};

function nowISO() {
  return new Date().toISOString();
}

function ticketNo(id) {
  const dt = new Date();
  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dt.getUTCDate()).padStart(2, "0");
  return `C-${y}${m}${d}-${String(id).padStart(5, "0")}`;
}

function nextId(key) {
  state.seq[key] += 1;
  return state.seq[key];
}

function listCategories() {
  return state.categories.filter((x) => x.is_active);
}

function listNotices(limit) {
  const arr = Array.from(state.notices.values());
  arr.sort((a, b) => {
    if (a.is_pinned !== b.is_pinned) return a.is_pinned ? -1 : 1;
    return String(b.created_at).localeCompare(String(a.created_at));
  });
  return arr.slice(0, Math.max(1, Math.min(Number(limit || 50), 200)));
}

function listFaqs(limit) {
  const arr = state.faqs.filter((x) => x.is_active);
  arr.sort((a, b) => (a.display_order - b.display_order) || (a.id - b.id));
  return arr.slice(0, Math.max(1, Math.min(Number(limit || 100), 300)));
}

function createComplaint({ reporterUserId, payload, forceEmergency }) {
  const id = nextId("complaint");
  let scope = String(payload.scope || "").toUpperCase();
  let priority = String(payload.priority || "NORMAL").toUpperCase();
  if (!SCOPE.has(scope)) throw new Error("scope invalid");
  if (!PRIORITY.has(priority)) throw new Error("priority invalid");
  if (forceEmergency) {
    scope = "EMERGENCY";
    priority = "URGENT";
  }
  if (scope === "EMERGENCY") priority = "URGENT";
  let status = "RECEIVED";
  let resolutionType = null;
  let closedAt = null;
  if (scope === "PRIVATE") {
    status = "GUIDANCE_SENT";
    resolutionType = "GUIDANCE_ONLY";
    closedAt = nowISO();
  }
  const row = {
    id,
    ticket_no: ticketNo(id),
    category_id: Number(payload.category_id),
    scope,
    status,
    priority,
    title: String(payload.title || "").trim(),
    description: String(payload.description || "").trim(),
    location_detail: String(payload.location_detail || "").trim(),
    site_code: String(payload.site_code || "").trim().toUpperCase(),
    site_name: String(payload.site_name || "").trim(),
    unit_label: String(payload.unit_label || "").trim(),
    reporter_user_id: Number(reporterUserId),
    assigned_to_user_id: null,
    resolution_type: resolutionType,
    created_at: nowISO(),
    updated_at: nowISO(),
    closed_at: closedAt,
    attachments: Array.isArray(payload.attachments)
      ? payload.attachments.map((x) => String(x || "").trim()).filter(Boolean)
      : []
  };
  state.complaints.set(id, row);
  return { ...row };
}

function listComplaintsForUser({ reporterUserId, status, limit, offset }) {
  const cleanStatus = String(status || "").toUpperCase().trim();
  if (cleanStatus && !STATUS.has(cleanStatus)) throw new Error("status invalid");
  let arr = Array.from(state.complaints.values()).filter((x) => Number(x.reporter_user_id) === Number(reporterUserId));
  if (cleanStatus) arr = arr.filter((x) => x.status === cleanStatus);
  arr.sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
  const lim = Math.max(1, Math.min(Number(limit || 50), 200));
  const off = Math.max(0, Number(offset || 0));
  return arr.slice(off, off + lim);
}

function getComplaint(id) {
  const row = state.complaints.get(Number(id));
  return row ? { ...row } : null;
}

function addComment({ complaintId, userId, comment, isInternal }) {
  if (!state.complaints.has(Number(complaintId))) throw new Error("complaint not found");
  const id = nextId("comment");
  const row = {
    id,
    complaint_id: Number(complaintId),
    user_id: Number(userId),
    comment: String(comment || "").trim(),
    is_internal: Boolean(isInternal),
    created_at: nowISO()
  };
  state.comments.push(row);
  return { ...row };
}

function listAdminComplaints({ scope, status, siteCode, limit, offset }) {
  const cleanScope = String(scope || "").toUpperCase().trim();
  const cleanStatus = String(status || "").toUpperCase().trim();
  const cleanSite = String(siteCode || "").toUpperCase().trim();
  if (cleanScope && !SCOPE.has(cleanScope)) throw new Error("scope invalid");
  if (cleanStatus && !STATUS.has(cleanStatus)) throw new Error("status invalid");
  let arr = Array.from(state.complaints.values());
  if (cleanScope) arr = arr.filter((x) => x.scope === cleanScope);
  if (cleanStatus) arr = arr.filter((x) => x.status === cleanStatus);
  if (cleanSite) arr = arr.filter((x) => String(x.site_code || "").toUpperCase() === cleanSite);
  arr.sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
  const lim = Math.max(1, Math.min(Number(limit || 100), 500));
  const off = Math.max(0, Number(offset || 0));
  return arr.slice(off, off + lim);
}

function triageComplaint({ complaintId, payload }) {
  const row = state.complaints.get(Number(complaintId));
  if (!row) throw new Error("complaint not found");
  const scope = String(payload.scope || "").toUpperCase().trim();
  const priority = String(payload.priority || "NORMAL").toUpperCase().trim();
  let resolutionType = String(payload.resolution_type || "REPAIR").toUpperCase().trim();
  if (!SCOPE.has(scope)) throw new Error("scope invalid");
  if (!PRIORITY.has(priority)) throw new Error("priority invalid");
  if (!RESOLUTION.has(resolutionType)) throw new Error("resolution_type invalid");
  row.scope = scope;
  row.priority = priority;
  row.status = "TRIAGED";
  row.resolution_type = resolutionType;
  if (scope === "PRIVATE") {
    row.status = "GUIDANCE_SENT";
    row.resolution_type = "GUIDANCE_ONLY";
    row.assigned_to_user_id = null;
    row.closed_at = row.closed_at || nowISO();
  }
  row.updated_at = nowISO();
  state.complaints.set(Number(complaintId), row);
  return { ...row };
}

function assignComplaint({ complaintId, assigneeUserId, scheduledAt, note }) {
  const row = state.complaints.get(Number(complaintId));
  if (!row) throw new Error("complaint not found");
  if (row.scope === "PRIVATE") throw new Error("cannot assign private complaint to work order");
  row.assigned_to_user_id = Number(assigneeUserId);
  row.status = "ASSIGNED";
  row.updated_at = nowISO();
  state.complaints.set(Number(complaintId), row);
  const wid = nextId("workOrder");
  const wo = {
    id: wid,
    complaint_id: Number(complaintId),
    assignee_user_id: Number(assigneeUserId),
    status: "OPEN",
    scheduled_at: scheduledAt || null,
    completed_at: null,
    result_note: String(note || "").trim() || null,
    created_at: nowISO()
  };
  state.workOrders.set(wid, wo);
  return { ...row, new_work_order_id: wid };
}

function patchWorkOrder({ workOrderId, status, resultNote }) {
  const row = state.workOrders.get(Number(workOrderId));
  if (!row) throw new Error("work_order not found");
  const cleanStatus = String(status || "").toUpperCase().trim();
  if (!WORK_STATUS.has(cleanStatus)) throw new Error("status invalid");
  row.status = cleanStatus;
  if (cleanStatus === "DONE") {
    row.completed_at = nowISO();
    const c = state.complaints.get(Number(row.complaint_id));
    if (c) {
      c.status = "COMPLETED";
      c.closed_at = c.closed_at || nowISO();
      c.updated_at = nowISO();
      state.complaints.set(Number(row.complaint_id), c);
    }
  }
  if (resultNote) row.result_note = String(resultNote).trim();
  state.workOrders.set(Number(workOrderId), row);
  return { ...row };
}

function createVisit({ complaintId, visitorUserId, visitReason, resultNote }) {
  const row = state.complaints.get(Number(complaintId));
  if (!row) throw new Error("complaint not found");
  const reason = String(visitReason || "").toUpperCase().trim();
  if (!VISIT_REASON.has(reason)) throw new Error("visit_reason invalid");
  const id = nextId("visit");
  const v = {
    id,
    complaint_id: Number(complaintId),
    unit_label: row.unit_label || "",
    visitor_user_id: Number(visitorUserId),
    visit_reason: reason,
    check_in_at: nowISO(),
    check_out_at: null,
    result_note: String(resultNote || "").trim() || null,
    created_at: nowISO()
  };
  state.visits.set(id, v);
  return { ...v };
}

function checkoutVisit({ visitId, resultNote }) {
  const row = state.visits.get(Number(visitId));
  if (!row) throw new Error("visit not found");
  row.check_out_at = row.check_out_at || nowISO();
  if (resultNote) row.result_note = String(resultNote).trim();
  state.visits.set(Number(visitId), row);
  return { ...row };
}

function createNotice({ authorUserId, title, content, isPinned, publishNow }) {
  const id = nextId("notice");
  const n = {
    id,
    title: String(title || "").trim(),
    content: String(content || "").trim(),
    is_pinned: Boolean(isPinned),
    published_at: publishNow ? nowISO() : null,
    author_user_id: Number(authorUserId),
    created_at: nowISO(),
    updated_at: nowISO()
  };
  state.notices.set(id, n);
  return { ...n };
}

function patchNotice({ noticeId, title, content, isPinned, publishNow }) {
  const row = state.notices.get(Number(noticeId));
  if (!row) throw new Error("notice not found");
  if (title !== undefined && title !== null) row.title = String(title).trim();
  if (content !== undefined && content !== null) row.content = String(content).trim();
  if (isPinned !== undefined && isPinned !== null) row.is_pinned = Boolean(isPinned);
  if (publishNow) row.published_at = nowISO();
  row.updated_at = nowISO();
  state.notices.set(Number(noticeId), row);
  return { ...row };
}

function stats({ siteCode }) {
  const cleanSite = String(siteCode || "").toUpperCase().trim();
  let arr = Array.from(state.complaints.values());
  if (cleanSite) arr = arr.filter((x) => String(x.site_code || "").toUpperCase() === cleanSite);
  const byStatus = {};
  const byScope = {};
  let delayed = 0;
  let totalHours = 0;
  let closedCount = 0;
  const now = Date.now();
  arr.forEach((row) => {
    byStatus[row.status] = (byStatus[row.status] || 0) + 1;
    byScope[row.scope] = (byScope[row.scope] || 0) + 1;
    const created = Date.parse(row.created_at);
    const ageHours = (now - created) / 3600000.0;
    if (!["COMPLETED", "CLOSED"].includes(row.status) && ageHours > 48) delayed += 1;
    if (row.closed_at) {
      totalHours += (Date.parse(row.closed_at) - created) / 3600000.0;
      closedCount += 1;
    }
  });
  return {
    total_count: arr.length,
    emergency_count: byScope.EMERGENCY || 0,
    delayed_count: delayed,
    avg_resolution_hours: closedCount ? (totalHours / closedCount) : null,
    by_status: Object.keys(byStatus).sort().map((k) => ({ status: k, count: byStatus[k] })),
    by_scope: Object.keys(byScope).sort().map((k) => ({ scope: k, count: byScope[k] }))
  };
}

module.exports = {
  listCategories,
  listNotices,
  listFaqs,
  createComplaint,
  listComplaintsForUser,
  getComplaint,
  addComment,
  listAdminComplaints,
  triageComplaint,
  assignComplaint,
  patchWorkOrder,
  createVisit,
  checkoutVisit,
  createNotice,
  patchNotice,
  stats
};
