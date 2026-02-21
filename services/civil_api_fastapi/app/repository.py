from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class _RepoState:
    complaint_seq: int = 0
    work_order_seq: int = 0
    visit_seq: int = 0
    notice_seq: int = 0
    comment_seq: int = 0


class InMemoryRepository:
    def __init__(self) -> None:
        self._lock = Lock()
        self._state = _RepoState()
        self.categories: list[dict[str, Any]] = [
            {"id": 1, "name": "Electric / Lighting (Common)", "scope": "COMMON", "is_active": True},
            {"id": 2, "name": "Water / Drainage (Common)", "scope": "COMMON", "is_active": True},
            {"id": 3, "name": "Fire Safety (Common)", "scope": "COMMON", "is_active": True},
            {"id": 4, "name": "Inside Unit Fixture", "scope": "PRIVATE", "is_active": True},
            {"id": 5, "name": "Emergency Leak / Blackout / Fire", "scope": "EMERGENCY", "is_active": True},
        ]
        self.complaints: dict[int, dict[str, Any]] = {}
        self.comments: list[dict[str, Any]] = []
        self.work_orders: dict[int, dict[str, Any]] = {}
        self.visits: dict[int, dict[str, Any]] = {}
        self.notices: dict[int, dict[str, Any]] = {}
        self.faqs: list[dict[str, Any]] = [
            {
                "id": 1,
                "question": "실내 조명 교체를 관리사무소가 해주나요?",
                "answer": "아니요. 실내 설비는 세대 내부(개인) 영역입니다. 관리사무소는 사용 안내를 도와드릴 수 있습니다.",
                "display_order": 10,
                "is_active": True,
            },
            {
                "id": 2,
                "question": "직원이 세대에 방문할 수 있는 경우는 언제인가요?",
                "answer": "소방시설 점검, 이웃 피해 예방, 정전·누수 등 긴급 시설 조치가 필요한 경우에 한해 가능합니다.",
                "display_order": 20,
                "is_active": True,
            },
        ]

    def _next_complaint_id(self) -> int:
        self._state.complaint_seq += 1
        return self._state.complaint_seq

    def _next_work_order_id(self) -> int:
        self._state.work_order_seq += 1
        return self._state.work_order_seq

    def _next_visit_id(self) -> int:
        self._state.visit_seq += 1
        return self._state.visit_seq

    def _next_notice_id(self) -> int:
        self._state.notice_seq += 1
        return self._state.notice_seq

    def _next_comment_id(self) -> int:
        self._state.comment_seq += 1
        return self._state.comment_seq

    @staticmethod
    def _ticket_no(complaint_id: int) -> str:
        return f"C-{_utc_now().strftime('%Y%m%d')}-{complaint_id:05d}"

    def list_categories(self) -> list[dict[str, Any]]:
        return [x for x in self.categories if x.get("is_active")]

    def list_notices(self, limit: int) -> list[dict[str, Any]]:
        rows = sorted(self.notices.values(), key=lambda x: (int(bool(x["is_pinned"])), x["created_at"]), reverse=True)
        return rows[: max(1, min(limit, 200))]

    def list_faqs(self, limit: int) -> list[dict[str, Any]]:
        rows = [x for x in self.faqs if x.get("is_active")]
        rows.sort(key=lambda x: (x["display_order"], x["id"]))
        return rows[: max(1, min(limit, 300))]

    def create_complaint(self, reporter_user_id: int, payload: dict[str, Any], *, force_emergency: bool = False) -> dict[str, Any]:
        with self._lock:
            cid = self._next_complaint_id()
            now = _utc_now()
            scope = str(payload["scope"]).upper()
            priority = str(payload.get("priority") or "NORMAL").upper()
            if force_emergency:
                scope = "EMERGENCY"
                priority = "URGENT"
            status = "RECEIVED"
            resolution_type = None
            closed_at = None
            if scope == "PRIVATE":
                status = "GUIDANCE_SENT"
                resolution_type = "GUIDANCE_ONLY"
                closed_at = now
            if scope == "EMERGENCY":
                priority = "URGENT"
            row = {
                "id": cid,
                "ticket_no": self._ticket_no(cid),
                "category_id": int(payload["category_id"]),
                "scope": scope,
                "status": status,
                "priority": priority,
                "title": str(payload["title"]).strip(),
                "description": str(payload["description"]).strip(),
                "location_detail": str(payload.get("location_detail") or "").strip(),
                "site_code": str(payload.get("site_code") or "").strip().upper(),
                "site_name": str(payload.get("site_name") or "").strip(),
                "unit_label": str(payload.get("unit_label") or "").strip(),
                "reporter_user_id": int(reporter_user_id),
                "assigned_to_user_id": None,
                "resolution_type": resolution_type,
                "created_at": now,
                "updated_at": now,
                "closed_at": closed_at,
                "attachments": [str(x).strip() for x in list(payload.get("attachments") or []) if str(x).strip()],
            }
            self.complaints[cid] = row
            return dict(row)

    def list_complaints_for_user(self, reporter_user_id: int, *, status: str, limit: int, offset: int) -> list[dict[str, Any]]:
        rows = [x for x in self.complaints.values() if int(x["reporter_user_id"]) == int(reporter_user_id)]
        clean_status = str(status or "").upper().strip()
        if clean_status:
            rows = [x for x in rows if str(x["status"]) == clean_status]
        rows.sort(key=lambda x: x["created_at"], reverse=True)
        return rows[offset : offset + max(1, min(limit, 200))]

    def get_complaint(self, complaint_id: int) -> dict[str, Any] | None:
        row = self.complaints.get(int(complaint_id))
        return dict(row) if row else None

    def add_comment(self, complaint_id: int, user_id: int, comment: str, *, is_internal: bool) -> dict[str, Any]:
        with self._lock:
            if int(complaint_id) not in self.complaints:
                raise ValueError("complaint not found")
            rid = self._next_comment_id()
            row = {
                "id": rid,
                "complaint_id": int(complaint_id),
                "user_id": int(user_id),
                "comment": str(comment).strip(),
                "is_internal": bool(is_internal),
                "created_at": _utc_now(),
            }
            self.comments.append(row)
            return dict(row)

    def list_admin_complaints(self, *, scope: str, status: str, site_code: str, limit: int, offset: int) -> list[dict[str, Any]]:
        rows = list(self.complaints.values())
        clean_scope = str(scope or "").upper().strip()
        clean_status = str(status or "").upper().strip()
        clean_site = str(site_code or "").upper().strip()
        if clean_scope:
            rows = [x for x in rows if str(x["scope"]) == clean_scope]
        if clean_status:
            rows = [x for x in rows if str(x["status"]) == clean_status]
        if clean_site:
            rows = [x for x in rows if str(x.get("site_code") or "").upper() == clean_site]
        rows.sort(key=lambda x: x["created_at"], reverse=True)
        return rows[offset : offset + max(1, min(limit, 500))]

    def triage(self, complaint_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            row = self.complaints.get(int(complaint_id))
            if not row:
                raise ValueError("complaint not found")
            row["scope"] = str(payload["scope"]).upper()
            row["priority"] = str(payload["priority"]).upper()
            row["resolution_type"] = str(payload["resolution_type"]).upper()
            row["status"] = "TRIAGED"
            if row["scope"] == "PRIVATE":
                row["resolution_type"] = "GUIDANCE_ONLY"
                row["status"] = "GUIDANCE_SENT"
                row["closed_at"] = _utc_now()
            row["updated_at"] = _utc_now()
            self.complaints[int(complaint_id)] = row
            return dict(row)

    def assign(self, complaint_id: int, assignee_user_id: int, scheduled_at: datetime | None, note: str) -> dict[str, Any]:
        with self._lock:
            row = self.complaints.get(int(complaint_id))
            if not row:
                raise ValueError("complaint not found")
            if str(row["scope"]) == "PRIVATE":
                raise ValueError("cannot assign private complaint to work order")
            row["assigned_to_user_id"] = int(assignee_user_id)
            row["status"] = "ASSIGNED"
            row["updated_at"] = _utc_now()
            wid = self._next_work_order_id()
            self.work_orders[wid] = {
                "id": wid,
                "complaint_id": int(complaint_id),
                "assignee_user_id": int(assignee_user_id),
                "status": "OPEN",
                "scheduled_at": scheduled_at,
                "completed_at": None,
                "result_note": str(note or "").strip() or None,
                "created_at": _utc_now(),
            }
            self.complaints[int(complaint_id)] = row
            out = dict(row)
            out["new_work_order_id"] = wid
            return out

    def patch_work_order(self, work_order_id: int, status: str, result_note: str) -> dict[str, Any]:
        with self._lock:
            row = self.work_orders.get(int(work_order_id))
            if not row:
                raise ValueError("work_order not found")
            clean_status = str(status).upper().strip()
            row["status"] = clean_status
            if clean_status == "DONE":
                row["completed_at"] = _utc_now()
                c = self.complaints.get(int(row["complaint_id"]))
                if c:
                    c["status"] = "COMPLETED"
                    c["closed_at"] = c["closed_at"] or _utc_now()
                    c["updated_at"] = _utc_now()
            if result_note:
                row["result_note"] = str(result_note).strip()
            self.work_orders[int(work_order_id)] = row
            return dict(row)

    def create_visit(self, complaint_id: int, visitor_user_id: int, visit_reason: str, result_note: str) -> dict[str, Any]:
        with self._lock:
            c = self.complaints.get(int(complaint_id))
            if not c:
                raise ValueError("complaint not found")
            vid = self._next_visit_id()
            row = {
                "id": vid,
                "complaint_id": int(complaint_id),
                "unit_label": c.get("unit_label") or "",
                "visitor_user_id": int(visitor_user_id),
                "visit_reason": str(visit_reason).upper().strip(),
                "check_in_at": _utc_now(),
                "check_out_at": None,
                "result_note": str(result_note).strip() or None,
                "created_at": _utc_now(),
            }
            self.visits[vid] = row
            return dict(row)

    def checkout_visit(self, visit_id: int, result_note: str) -> dict[str, Any]:
        with self._lock:
            row = self.visits.get(int(visit_id))
            if not row:
                raise ValueError("visit not found")
            row["check_out_at"] = row["check_out_at"] or _utc_now()
            if result_note:
                row["result_note"] = str(result_note).strip()
            self.visits[int(visit_id)] = row
            return dict(row)

    def create_notice(self, author_user_id: int, title: str, content: str, is_pinned: bool, publish_now: bool) -> dict[str, Any]:
        with self._lock:
            nid = self._next_notice_id()
            now = _utc_now()
            row = {
                "id": nid,
                "title": str(title).strip(),
                "content": str(content).strip(),
                "is_pinned": bool(is_pinned),
                "published_at": now if publish_now else None,
                "author_user_id": int(author_user_id),
                "created_at": now,
                "updated_at": now,
            }
            self.notices[nid] = row
            return dict(row)

    def patch_notice(
        self,
        notice_id: int,
        title: str | None,
        content: str | None,
        is_pinned: bool | None,
        publish_now: bool,
    ) -> dict[str, Any]:
        with self._lock:
            row = self.notices.get(int(notice_id))
            if not row:
                raise ValueError("notice not found")
            if title is not None:
                row["title"] = str(title).strip()
            if content is not None:
                row["content"] = str(content).strip()
            if is_pinned is not None:
                row["is_pinned"] = bool(is_pinned)
            if publish_now:
                row["published_at"] = _utc_now()
            row["updated_at"] = _utc_now()
            self.notices[int(notice_id)] = row
            return dict(row)

    def stats(self, *, site_code: str) -> dict[str, Any]:
        rows = list(self.complaints.values())
        clean_site = str(site_code or "").upper().strip()
        if clean_site:
            rows = [x for x in rows if str(x.get("site_code") or "").upper() == clean_site]
        by_status: dict[str, int] = {}
        by_scope: dict[str, int] = {}
        delayed = 0
        total_resolve_hours = 0.0
        closed_count = 0
        now = _utc_now()
        for row in rows:
            status = str(row["status"])
            scope = str(row["scope"])
            by_status[status] = by_status.get(status, 0) + 1
            by_scope[scope] = by_scope.get(scope, 0) + 1
            age_hours = (now - row["created_at"]).total_seconds() / 3600.0
            if status not in {"COMPLETED", "CLOSED"} and age_hours > 48:
                delayed += 1
            if row.get("closed_at"):
                resolve_hours = (row["closed_at"] - row["created_at"]).total_seconds() / 3600.0
                total_resolve_hours += resolve_hours
                closed_count += 1
        return {
            "total_count": len(rows),
            "emergency_count": by_scope.get("EMERGENCY", 0),
            "delayed_count": delayed,
            "avg_resolution_hours": (total_resolve_hours / closed_count) if closed_count else None,
            "by_status": [{"status": k, "count": v} for k, v in sorted(by_status.items())],
            "by_scope": [{"scope": k, "count": v} for k, v in sorted(by_scope.items())],
        }


repo = InMemoryRepository()
