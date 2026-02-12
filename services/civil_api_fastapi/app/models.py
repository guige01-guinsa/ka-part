from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

Scope = Literal["COMMON", "PRIVATE", "EMERGENCY"]
Status = Literal["RECEIVED", "TRIAGED", "GUIDANCE_SENT", "ASSIGNED", "IN_PROGRESS", "COMPLETED", "CLOSED"]
Priority = Literal["LOW", "NORMAL", "HIGH", "URGENT"]
ResolutionType = Literal["REPAIR", "GUIDANCE_ONLY", "EXTERNAL_VENDOR"]
VisitReason = Literal["FIRE_INSPECTION", "NEIGHBOR_DAMAGE", "EMERGENCY_INFRA"]
WorkOrderStatus = Literal["OPEN", "DISPATCHED", "DONE", "CANCELED"]


class ComplaintCreateIn(BaseModel):
    category_id: int = Field(..., ge=1)
    scope: Scope
    title: str = Field(..., min_length=1, max_length=140)
    description: str = Field(..., min_length=1, max_length=8000)
    location_detail: str = Field(default="", max_length=200)
    priority: Priority = "NORMAL"
    site_code: str = Field(default="", max_length=32)
    site_name: str = Field(default="", max_length=80)
    unit_label: str = Field(default="", max_length=80)
    attachments: list[str] = Field(default_factory=list)


class ComplaintOut(BaseModel):
    id: int
    ticket_no: str
    category_id: int
    scope: Scope
    status: Status
    priority: Priority
    title: str
    description: str
    location_detail: str = ""
    site_code: str = ""
    site_name: str = ""
    unit_label: str = ""
    reporter_user_id: int
    assigned_to_user_id: int | None = None
    resolution_type: ResolutionType | None = None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None
    attachments: list[str] = Field(default_factory=list)


class CommentCreateIn(BaseModel):
    comment: str = Field(..., min_length=1, max_length=8000)


class AdminTriageIn(BaseModel):
    scope: Scope
    priority: Priority = "NORMAL"
    resolution_type: ResolutionType = "REPAIR"
    guidance_template_id: int | None = None
    note: str = Field(default="", max_length=2000)


class AdminAssignIn(BaseModel):
    assignee_user_id: int = Field(..., ge=1)
    scheduled_at: datetime | None = None
    note: str = Field(default="", max_length=2000)


class WorkOrderPatchIn(BaseModel):
    status: WorkOrderStatus
    result_note: str = Field(default="", max_length=4000)


class VisitCreateIn(BaseModel):
    complaint_id: int = Field(..., ge=1)
    visit_reason: VisitReason
    result_note: str = Field(default="", max_length=4000)


class VisitCheckoutIn(BaseModel):
    result_note: str = Field(default="", max_length=4000)


class NoticeCreateIn(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    content: str = Field(..., min_length=1, max_length=20000)
    is_pinned: bool = False
    publish_now: bool = True


class NoticePatchIn(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=200)
    content: str | None = Field(default=None, min_length=1, max_length=20000)
    is_pinned: bool | None = None
    publish_now: bool = False
