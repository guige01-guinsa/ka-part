const express = require("express");
const store = require("./store");

const app = express();
const PORT = Number(process.env.PORT || 8200);

app.use(express.json({ limit: "2mb" }));

function requireAuth(req, res, next) {
  const userId = String(req.header("X-User-Id") || "").trim();
  if (!userId) return res.status(401).json({ ok: false, detail: "missing X-User-Id header" });
  const parsed = Number(userId);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return res.status(400).json({ ok: false, detail: "X-User-Id must be a positive integer" });
  }
  req.user = {
    id: parsed,
    role: String(req.header("X-Role") || "resident").trim().toLowerCase()
  };
  next();
}

function requireAdmin(req, res, next) {
  requireAuth(req, res, () => {
    if (!["admin", "staff"].includes(req.user.role)) {
      return res.status(403).json({ ok: false, detail: "admin only" });
    }
    next();
  });
}

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "civil_api_node", now: new Date().toISOString() });
});

app.get("/api/v1/codes/complaint-categories", (_req, res) => {
  res.json({ ok: true, items: store.listCategories() });
});

app.get("/api/v1/notices", (req, res) => {
  const limit = Number(req.query.limit || 50);
  res.json({ ok: true, items: store.listNotices(limit) });
});

app.get("/api/v1/faqs", (req, res) => {
  const limit = Number(req.query.limit || 100);
  res.json({ ok: true, items: store.listFaqs(limit) });
});

app.post("/api/v1/complaints", requireAuth, (req, res) => {
  try {
    const item = store.createComplaint({ reporterUserId: req.user.id, payload: req.body, forceEmergency: false });
    const message = item.scope === "PRIVATE"
      ? "Private-unit issue: guidance provided, no direct repair dispatch."
      : "Complaint received.";
    res.json({ ok: true, message, item });
  } catch (e) {
    res.status(400).json({ ok: false, detail: String(e.message || e) });
  }
});

app.post("/api/v1/emergencies", requireAuth, (req, res) => {
  try {
    const item = store.createComplaint({ reporterUserId: req.user.id, payload: req.body, forceEmergency: true });
    res.json({ ok: true, message: "Emergency complaint received with urgent priority.", item });
  } catch (e) {
    res.status(400).json({ ok: false, detail: String(e.message || e) });
  }
});

app.get("/api/v1/complaints", requireAuth, (req, res) => {
  try {
    const status = String(req.query.status || "");
    const limit = Number(req.query.limit || 50);
    const offset = Number(req.query.offset || 0);
    const items = store.listComplaintsForUser({
      reporterUserId: req.user.id,
      status,
      limit,
      offset
    });
    res.json({ ok: true, items });
  } catch (e) {
    res.status(400).json({ ok: false, detail: String(e.message || e) });
  }
});

app.get("/api/v1/complaints/:id", requireAuth, (req, res) => {
  const item = store.getComplaint(Number(req.params.id));
  if (!item || (req.user.role === "resident" && Number(item.reporter_user_id) !== req.user.id)) {
    return res.status(404).json({ ok: false, detail: "complaint not found" });
  }
  res.json({ ok: true, item });
});

app.post("/api/v1/complaints/:id/comments", requireAuth, (req, res) => {
  const item = store.getComplaint(Number(req.params.id));
  if (!item || (req.user.role === "resident" && Number(item.reporter_user_id) !== req.user.id)) {
    return res.status(404).json({ ok: false, detail: "complaint not found" });
  }
  try {
    const out = store.addComment({
      complaintId: Number(req.params.id),
      userId: req.user.id,
      comment: String(req.body.comment || ""),
      isInternal: false
    });
    res.json({ ok: true, item: out });
  } catch (e) {
    res.status(400).json({ ok: false, detail: String(e.message || e) });
  }
});

app.get("/api/v1/admin/complaints", requireAdmin, (req, res) => {
  try {
    const items = store.listAdminComplaints({
      scope: String(req.query.scope || ""),
      status: String(req.query.status || ""),
      siteCode: String(req.query.site_code || ""),
      limit: Number(req.query.limit || 100),
      offset: Number(req.query.offset || 0)
    });
    res.json({ ok: true, items });
  } catch (e) {
    res.status(400).json({ ok: false, detail: String(e.message || e) });
  }
});

app.get("/api/v1/admin/complaints/:id", requireAdmin, (req, res) => {
  const item = store.getComplaint(Number(req.params.id));
  if (!item) return res.status(404).json({ ok: false, detail: "complaint not found" });
  res.json({ ok: true, item });
});

app.patch("/api/v1/admin/complaints/:id/triage", requireAdmin, (req, res) => {
  try {
    const out = store.triageComplaint({ complaintId: Number(req.params.id), payload: req.body || {} });
    res.json({ ok: true, item: out });
  } catch (e) {
    const status = String(e.message || "").includes("not found") ? 404 : 400;
    res.status(status).json({ ok: false, detail: String(e.message || e) });
  }
});

app.post("/api/v1/admin/complaints/:id/assign", requireAdmin, (req, res) => {
  try {
    const out = store.assignComplaint({
      complaintId: Number(req.params.id),
      assigneeUserId: Number(req.body.assignee_user_id),
      scheduledAt: req.body.scheduled_at || null,
      note: req.body.note || ""
    });
    res.json({ ok: true, item: out });
  } catch (e) {
    const status = String(e.message || "").includes("not found") ? 404 : 400;
    res.status(status).json({ ok: false, detail: String(e.message || e) });
  }
});

app.patch("/api/v1/admin/work-orders/:id", requireAdmin, (req, res) => {
  try {
    const out = store.patchWorkOrder({
      workOrderId: Number(req.params.id),
      status: req.body.status,
      resultNote: req.body.result_note || ""
    });
    res.json({ ok: true, item: out });
  } catch (e) {
    const status = String(e.message || "").includes("not found") ? 404 : 400;
    res.status(status).json({ ok: false, detail: String(e.message || e) });
  }
});

app.post("/api/v1/admin/visits", requireAdmin, (req, res) => {
  try {
    const out = store.createVisit({
      complaintId: Number(req.body.complaint_id),
      visitorUserId: req.user.id,
      visitReason: req.body.visit_reason,
      resultNote: req.body.result_note || ""
    });
    res.json({ ok: true, item: out });
  } catch (e) {
    const status = String(e.message || "").includes("not found") ? 404 : 400;
    res.status(status).json({ ok: false, detail: String(e.message || e) });
  }
});

app.patch("/api/v1/admin/visits/:id/checkout", requireAdmin, (req, res) => {
  try {
    const out = store.checkoutVisit({
      visitId: Number(req.params.id),
      resultNote: req.body.result_note || ""
    });
    res.json({ ok: true, item: out });
  } catch (e) {
    const status = String(e.message || "").includes("not found") ? 404 : 400;
    res.status(status).json({ ok: false, detail: String(e.message || e) });
  }
});

app.post("/api/v1/admin/notices", requireAdmin, (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ ok: false, detail: "admin only" });
  try {
    const out = store.createNotice({
      authorUserId: req.user.id,
      title: req.body.title,
      content: req.body.content,
      isPinned: Boolean(req.body.is_pinned),
      publishNow: req.body.publish_now !== false
    });
    res.json({ ok: true, item: out });
  } catch (e) {
    res.status(400).json({ ok: false, detail: String(e.message || e) });
  }
});

app.patch("/api/v1/admin/notices/:id", requireAdmin, (req, res) => {
  if (req.user.role !== "admin") return res.status(403).json({ ok: false, detail: "admin only" });
  try {
    const out = store.patchNotice({
      noticeId: Number(req.params.id),
      title: req.body.title,
      content: req.body.content,
      isPinned: req.body.is_pinned,
      publishNow: Boolean(req.body.publish_now)
    });
    res.json({ ok: true, item: out });
  } catch (e) {
    const status = String(e.message || "").includes("not found") ? 404 : 400;
    res.status(status).json({ ok: false, detail: String(e.message || e) });
  }
});

app.get("/api/v1/admin/stats/complaints", requireAdmin, (req, res) => {
  const siteCode = String(req.query.site_code || "");
  res.json({ ok: true, item: store.stats({ siteCode }) });
});

app.listen(PORT, () => {
  console.log(`civil_api_node listening on ${PORT}`);
});
