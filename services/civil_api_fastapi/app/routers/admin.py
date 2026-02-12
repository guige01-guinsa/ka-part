from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from ..deps import CurrentUser, get_admin_user
from ..models import (
    AdminAssignIn,
    AdminTriageIn,
    NoticeCreateIn,
    NoticePatchIn,
    VisitCheckoutIn,
    VisitCreateIn,
    WorkOrderPatchIn,
)
from ..repository import repo

router = APIRouter(tags=["admin"])


def _site_filter(user: CurrentUser, requested_site_code: str) -> str:
    if user.role == "admin":
        return (requested_site_code or "").strip().upper()
    return ""


@router.get("/admin/complaints")
def admin_get_complaints(
    scope: str = Query(""),
    status: str = Query(""),
    site_code: str = Query(""),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    user: CurrentUser = Depends(get_admin_user),
):
    return {
        "ok": True,
        "items": repo.list_admin_complaints(
            scope=scope,
            status=status,
            site_code=_site_filter(user, site_code),
            limit=limit,
            offset=offset,
        ),
    }


@router.get("/admin/complaints/{complaint_id}")
def admin_get_complaint(complaint_id: int, user: CurrentUser = Depends(get_admin_user)):
    item = repo.get_complaint(int(complaint_id))
    if not item:
        raise HTTPException(status_code=404, detail="complaint not found")
    return {"ok": True, "item": item}


@router.patch("/admin/complaints/{complaint_id}/triage")
def admin_triage(complaint_id: int, payload: AdminTriageIn, user: CurrentUser = Depends(get_admin_user)):
    try:
        out = repo.triage(int(complaint_id), payload.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e)) from e
    return {"ok": True, "item": out}


@router.post("/admin/complaints/{complaint_id}/assign")
def admin_assign(complaint_id: int, payload: AdminAssignIn, user: CurrentUser = Depends(get_admin_user)):
    try:
        out = repo.assign(
            int(complaint_id),
            payload.assignee_user_id,
            payload.scheduled_at,
            payload.note,
        )
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e)) from e
    return {"ok": True, "item": out}


@router.patch("/admin/work-orders/{work_order_id}")
def admin_patch_work_order(work_order_id: int, payload: WorkOrderPatchIn, user: CurrentUser = Depends(get_admin_user)):
    try:
        out = repo.patch_work_order(int(work_order_id), payload.status, payload.result_note)
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e)) from e
    return {"ok": True, "item": out}


@router.post("/admin/visits")
def admin_create_visit(payload: VisitCreateIn, user: CurrentUser = Depends(get_admin_user)):
    try:
        out = repo.create_visit(payload.complaint_id, user.user_id, payload.visit_reason, payload.result_note)
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e)) from e
    return {"ok": True, "item": out}


@router.patch("/admin/visits/{visit_id}/checkout")
def admin_checkout_visit(visit_id: int, payload: VisitCheckoutIn, user: CurrentUser = Depends(get_admin_user)):
    try:
        out = repo.checkout_visit(int(visit_id), payload.result_note)
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e)) from e
    return {"ok": True, "item": out}


@router.post("/admin/notices")
def admin_create_notice(payload: NoticeCreateIn, user: CurrentUser = Depends(get_admin_user)):
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    out = repo.create_notice(user.user_id, payload.title, payload.content, payload.is_pinned, payload.publish_now)
    return {"ok": True, "item": out}


@router.patch("/admin/notices/{notice_id}")
def admin_patch_notice(notice_id: int, payload: NoticePatchIn, user: CurrentUser = Depends(get_admin_user)):
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    try:
        out = repo.patch_notice(int(notice_id), payload.title, payload.content, payload.is_pinned, payload.publish_now)
    except ValueError as e:
        raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e)) from e
    return {"ok": True, "item": out}


@router.get("/admin/stats/complaints")
def admin_get_stats(site_code: str = Query(""), user: CurrentUser = Depends(get_admin_user)):
    return {"ok": True, "item": repo.stats(site_code=_site_filter(user, site_code))}
