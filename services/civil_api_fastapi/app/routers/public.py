from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from ..deps import CurrentUser, get_current_user
from ..models import CommentCreateIn, ComplaintCreateIn
from ..repository import repo

router = APIRouter(tags=["public"])


@router.get("/codes/complaint-categories")
def get_complaint_categories():
    return {"ok": True, "items": repo.list_categories()}


@router.get("/notices")
def get_notices(limit: int = Query(50, ge=1, le=200)):
    return {"ok": True, "items": repo.list_notices(limit)}


@router.get("/faqs")
def get_faqs(limit: int = Query(100, ge=1, le=300)):
    return {"ok": True, "items": repo.list_faqs(limit)}


@router.post("/complaints")
def post_complaint(payload: ComplaintCreateIn, user: CurrentUser = Depends(get_current_user)):
    item = repo.create_complaint(user.user_id, payload.model_dump(), force_emergency=False)
    msg = "Complaint received."
    if item["scope"] == "PRIVATE":
        msg = "Private-unit issue: guidance provided, no direct repair dispatch."
    return {"ok": True, "message": msg, "item": item}


@router.post("/emergencies")
def post_emergency(payload: ComplaintCreateIn, user: CurrentUser = Depends(get_current_user)):
    item = repo.create_complaint(user.user_id, payload.model_dump(), force_emergency=True)
    return {"ok": True, "message": "Emergency complaint received with urgent priority.", "item": item}


@router.get("/complaints")
def get_my_complaints(
    status: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: CurrentUser = Depends(get_current_user),
):
    return {"ok": True, "items": repo.list_complaints_for_user(user.user_id, status=status, limit=limit, offset=offset)}


@router.get("/complaints/{complaint_id}")
def get_my_complaint(complaint_id: int, user: CurrentUser = Depends(get_current_user)):
    item = repo.get_complaint(int(complaint_id))
    if not item or (user.role == "resident" and int(item["reporter_user_id"]) != int(user.user_id)):
        raise HTTPException(status_code=404, detail="complaint not found")
    return {"ok": True, "item": item}


@router.post("/complaints/{complaint_id}/comments")
def post_comment(complaint_id: int, payload: CommentCreateIn, user: CurrentUser = Depends(get_current_user)):
    item = repo.get_complaint(int(complaint_id))
    if not item or (user.role == "resident" and int(item["reporter_user_id"]) != int(user.user_id)):
        raise HTTPException(status_code=404, detail="complaint not found")
    out = repo.add_comment(int(complaint_id), user.user_id, payload.comment, is_internal=False)
    return {"ok": True, "item": out}
