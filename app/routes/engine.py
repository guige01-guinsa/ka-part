from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse

from ..ai_service import MAX_CHAT_DIGEST_IMAGES, analyze_chat_digest, classify_complaint_text, normalize_summary_text
from ..db import (
    STORAGE_ROOT,
    append_audit_log,
    ensure_service_user,
    get_auth_user_by_token,
    get_tenant,
    get_tenant_by_api_key,
    log_usage,
    mark_tenant_used,
)
from ..engine_db import (
    add_attachment,
    create_complaint,
    dashboard_summary,
    delete_attachments,
    delete_complaint,
    generate_daily_report,
    get_complaint,
    list_complaints,
    update_complaint,
)

router = APIRouter()
AUTH_COOKIE_NAME = (os.getenv("KA_AUTH_COOKIE_NAME") or "ka_part_auth_token").strip()
UPLOAD_ROOT = (STORAGE_ROOT / "uploads" / "complaints").resolve()
DIGEST_IMAGE_MAX_BYTES = 10 * 1024 * 1024


def _access_token(request: Request) -> str:
    auth = str(request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            return token
    cookie_token = str(request.cookies.get(AUTH_COOKIE_NAME) or "").strip()
    if cookie_token:
        return cookie_token
    raise HTTPException(status_code=401, detail="인증이 필요합니다.")


def _resolve_context(request: Request) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    token = _access_token(request)
    user = get_auth_user_by_token(token)
    if user:
        tenant = get_tenant(str(user.get("tenant_id") or "")) if user.get("tenant_id") else None
        return user, tenant
    tenant = get_tenant_by_api_key(token)
    if tenant:
        mark_tenant_used(str(tenant.get("id") or ""))
        return None, tenant
    raise HTTPException(status_code=401, detail="유효한 세션 또는 API Key가 필요합니다.")


def _tenant_id_from_request(request: Request, payload: Dict[str, Any] | None = None) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    user, tenant = _resolve_context(request)
    requested = str((payload or {}).get("tenant_id") or request.query_params.get("tenant_id") or "").strip().lower()
    if tenant:
        return str(tenant.get("id") or ""), user, tenant
    if not user:
        raise HTTPException(status_code=401, detail="인증이 필요합니다.")
    if int(user.get("is_admin") or 0) == 1:
        tenant_id = requested or str(user.get("tenant_id") or "").strip().lower()
        if not tenant_id:
            raise HTTPException(status_code=400, detail="tenant_id가 필요합니다.")
        tenant = get_tenant(tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="tenant not found")
        return tenant_id, user, tenant
    tenant_id = str(user.get("tenant_id") or "").strip().lower()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="계정에 tenant_id가 연결되어 있지 않습니다.")
    tenant = get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="tenant not found")
    return tenant_id, user, tenant


def _actor_label(user: Optional[Dict[str, Any]], tenant: Optional[Dict[str, Any]]) -> str:
    if user:
        return str(user.get("name") or user.get("login_id") or "operator")
    return f"{str((tenant or {}).get('name') or 'tenant')} API"


def _build_summary_input(payload: Dict[str, Any]) -> str:
    parts = []
    building = str(payload.get("building") or "").strip()
    unit = str(payload.get("unit") or "").strip()
    if building:
        parts.append(f"{building}동")
    if unit:
        parts.append(f"{unit}호")
    parts.append(str(payload.get("content") or "").strip())
    return " ".join(part for part in parts if part).strip()


def _resolve_uploaded_path(file_url: str) -> Path | None:
    raw = str(file_url or "").strip()
    prefix = "/api/files/"
    if not raw.startswith(prefix):
        return None
    rest = raw[len(prefix):]
    tenant_part, _, filename = rest.partition("/")
    tenant_id = str(tenant_part or "").strip().lower()
    filename = str(filename or "").strip()
    if not tenant_id or not filename:
        return None
    target = (UPLOAD_ROOT / tenant_id / filename).resolve()
    if not str(target).startswith(str(UPLOAD_ROOT)):
        return None
    return target


async def _read_digest_images(files: List[UploadFile]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    uploads = list(files or [])
    if len(uploads) > MAX_CHAT_DIGEST_IMAGES:
        raise HTTPException(status_code=400, detail=f"이미지는 최대 {MAX_CHAT_DIGEST_IMAGES}장까지 업로드할 수 있습니다.")
    for upload in uploads:
        content_type = str(upload.content_type or "").strip().lower()
        if not content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="이미지 파일만 업로드할 수 있습니다.")
        raw = await upload.read()
        try:
            if len(raw) > DIGEST_IMAGE_MAX_BYTES:
                raise HTTPException(status_code=400, detail="이미지 한 장은 10MB 이하여야 합니다.")
            items.append(
                {
                    "filename": str(upload.filename or "chat-image").strip() or "chat-image",
                    "content_type": content_type or "image/jpeg",
                    "bytes": raw,
                }
            )
        finally:
            try:
                await upload.close()
            except Exception:
                pass
    return items


@router.post("/ai/classify")
def ai_classify(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    text = str(payload.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    try:
        item = classify_complaint_text(text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(tenant_id, "ai.classify")
    append_audit_log(tenant_id, "ai_classify", _actor_label(user, tenant), {"text": text[:120]})
    return {"ok": True, "item": item}


@router.post("/ai/kakao_digest")
def ai_kakao_digest(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    text = str(payload.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    try:
        item = analyze_chat_digest(text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(tenant_id, "ai.kakao_digest")
    append_audit_log(tenant_id, "ai_kakao_digest", _actor_label(user, tenant), {"lines": len(text.splitlines())})
    return {"ok": True, "item": item}


@router.post("/ai/kakao_digest/images")
async def ai_kakao_digest_images(
    request: Request,
    tenant_id: str = Form(default=""),
    text: str = Form(default=""),
    files: List[UploadFile] = File(default=[]),
) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    image_inputs = await _read_digest_images(list(files or []))
    if not str(text or "").strip() and not image_inputs:
        raise HTTPException(status_code=400, detail="text or image is required")
    try:
        item = analyze_chat_digest(str(text or "").strip(), image_inputs=image_inputs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(resolved_tenant_id, "ai.kakao_digest.images")
    append_audit_log(
        resolved_tenant_id,
        "ai_kakao_digest_images",
        _actor_label(user, tenant),
        {"lines": len(str(text or "").splitlines()), "images": len(image_inputs)},
    )
    return {"ok": True, "item": item}


@router.get("/dashboard/summary")
def dashboard(request: Request, tenant_id: str = Query(default=""), day: str = Query(default="")) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    try:
        item = dashboard_summary(tenant_id=resolved_tenant_id, target_day=day)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(resolved_tenant_id, "dashboard.summary")
    append_audit_log(resolved_tenant_id, "dashboard_summary", _actor_label(user, tenant), {"day": day or ""})
    return {"ok": True, "tenant": tenant, "item": item}


@router.get("/report/daily")
def report_daily(request: Request, tenant_id: str = Query(default=""), day: str = Query(default="")) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    try:
        item = generate_daily_report(tenant_id=resolved_tenant_id, target_day=day)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(resolved_tenant_id, "report.daily")
    append_audit_log(resolved_tenant_id, "daily_report", _actor_label(user, tenant), {"day": day or ""})
    return {"ok": True, "tenant": tenant, "item": item}


@router.post("/complaints")
def complaints_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    content = str(payload.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="content is required")
    ai_data = None
    if payload.get("auto_classify", True):
        ai_data = classify_complaint_text(_build_summary_input(payload))
    actor = user or ensure_service_user(tenant_id)
    try:
        item = create_complaint(
            tenant_id=tenant_id,
            building=str(payload.get("building") or "").strip(),
            unit=str(payload.get("unit") or "").strip(),
            complainant_phone=str(payload.get("complainant_phone") or "").strip(),
            channel=str(payload.get("channel") or "기타").strip() or "기타",
            content=content,
            summary=normalize_summary_text(
                str(payload.get("summary") or (ai_data or {}).get("summary") or "").strip(),
                building=str(payload.get("building") or "").strip(),
                unit=str(payload.get("unit") or "").strip(),
                complaint_type=str(payload.get("type") or (ai_data or {}).get("type") or "기타").strip(),
            ),
            complaint_type=str(payload.get("type") or (ai_data or {}).get("type") or "기타").strip(),
            urgency=str(payload.get("urgency") or (ai_data or {}).get("urgency") or "일반").strip(),
            status=str(payload.get("status") or "접수").strip() or "접수",
            manager=str(payload.get("manager") or "").strip(),
            image_url=str(payload.get("image_url") or "").strip(),
            source_text=str(payload.get("source_text") or "").strip(),
            ai_model=str((ai_data or {}).get("model") or "").strip(),
            created_by_user_id=int(actor.get("id")) if actor and actor.get("id") else None,
            created_by_label=_actor_label(user, tenant),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(tenant_id, "complaints.create")
    append_audit_log(tenant_id, "create_complaint", _actor_label(user, tenant), {"complaint_id": item.get("id")})
    return {"ok": True, "item": item}


@router.get("/complaints")
def complaints_list(
    request: Request,
    tenant_id: str = Query(default=""),
    status: str = Query(default=""),
    building: str = Query(default=""),
    unit: str = Query(default=""),
    complaint_type: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    try:
        items = list_complaints(
            tenant_id=resolved_tenant_id,
            status=status,
            building=building,
            unit=unit,
            complaint_type=complaint_type,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(resolved_tenant_id, "complaints.list")
    return {"ok": True, "tenant": tenant, "items": items}


@router.get("/complaints/{complaint_id}")
def complaints_get(request: Request, complaint_id: int, tenant_id: str = Query(default="")) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, _user, tenant = _tenant_id_from_request(request, payload)
    item = get_complaint(tenant_id=resolved_tenant_id, complaint_id=int(complaint_id))
    if not item:
        raise HTTPException(status_code=404, detail="complaint not found")
    log_usage(resolved_tenant_id, "complaints.detail")
    return {"ok": True, "tenant": tenant, "item": item}


@router.put("/complaints/{complaint_id}")
def complaints_update(request: Request, complaint_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    status = str(payload.get("status") or "").strip()
    if not status:
        raise HTTPException(status_code=400, detail="status is required")
    try:
        item = update_complaint(
            tenant_id=tenant_id,
            complaint_id=int(complaint_id),
            status=status,
            actor_label=_actor_label(user, tenant),
            manager=str(payload.get("manager") or "").strip(),
            note=str(payload.get("note") or "").strip(),
            summary=str(payload.get("summary") or "").strip(),
            complaint_type=str(payload.get("type") or "").strip(),
            urgency=str(payload.get("urgency") or "").strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(tenant_id, "complaints.update")
    append_audit_log(tenant_id, "update_complaint", _actor_label(user, tenant), {"complaint_id": int(complaint_id), "status": status})
    return {"ok": True, "item": item}


@router.delete("/complaints/{complaint_id}")
def complaints_delete(request: Request, complaint_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    payload = payload or {}
    tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    try:
        item = delete_complaint(tenant_id=tenant_id, complaint_id=int(complaint_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    for attachment in item.get("attachments") or []:
        target = _resolve_uploaded_path(str(attachment.get("file_url") or ""))
        if target and target.exists() and target.is_file():
            target.unlink(missing_ok=True)
    log_usage(tenant_id, "complaints.delete")
    append_audit_log(tenant_id, "delete_complaint", _actor_label(user, tenant), {"complaint_id": int(complaint_id)})
    return {"ok": True, "item": item}


@router.post("/complaints/{complaint_id}/attachments")
async def complaints_add_attachment(
    request: Request,
    complaint_id: int,
    file: UploadFile = File(...),
    tenant_id: str = Query(default=""),
) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    ext = Path(str(file.filename or "upload")).suffix.lower() or ".bin"
    target_dir = UPLOAD_ROOT / resolved_tenant_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target_name = f"{uuid.uuid4().hex}{ext}"
    target_path = target_dir / target_name
    total = 0
    try:
        with target_path.open("wb") as fp:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                fp.write(chunk)
    finally:
        try:
            await file.close()
        except Exception:
            pass
    try:
        item = add_attachment(
            tenant_id=resolved_tenant_id,
            complaint_id=int(complaint_id),
            file_url=f"/api/files/{resolved_tenant_id}/{target_name}",
            mime_type=str(file.content_type or "").strip(),
            size_bytes=total,
        )
    except ValueError as exc:
        if target_path.exists():
            target_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(resolved_tenant_id, "complaints.attachments")
    append_audit_log(resolved_tenant_id, "add_attachment", _actor_label(user, tenant), {"complaint_id": int(complaint_id)})
    return {"ok": True, "item": item}


@router.delete("/complaints/{complaint_id}/attachments")
def complaints_delete_attachments(
    request: Request,
    complaint_id: int,
    payload: Dict[str, Any] | None = Body(default=None),
) -> Dict[str, Any]:
    payload = payload or {}
    tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    attachment_ids = payload.get("attachment_ids") or []
    try:
        normalized_ids = [int(value) for value in attachment_ids]
    except Exception as exc:
        raise HTTPException(status_code=400, detail="attachment_ids must be integers") from exc
    try:
        result = delete_attachments(
            tenant_id=tenant_id,
            complaint_id=int(complaint_id),
            attachment_ids=normalized_ids,
            delete_all=bool(payload.get("delete_all")),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for item in result.get("deleted") or []:
        target = _resolve_uploaded_path(str(item.get("file_url") or ""))
        if target and target.exists() and target.is_file():
            target.unlink(missing_ok=True)
    log_usage(tenant_id, "complaints.attachments.delete")
    append_audit_log(
        tenant_id,
        "delete_attachments",
        _actor_label(user, tenant),
        {"complaint_id": int(complaint_id), "count": len(result.get("deleted") or [])},
    )
    return {"ok": True, "deleted": result.get("deleted") or [], "item": result.get("complaint")}


@router.get("/files/{tenant_id}/{filename}")
def uploaded_file(tenant_id: str, filename: str) -> FileResponse:
    target = (UPLOAD_ROOT / str(tenant_id or "").strip().lower() / str(filename or "").strip()).resolve()
    if not str(target).startswith(str(UPLOAD_ROOT)):
        raise HTTPException(status_code=404, detail="file not found")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="file not found")
    return FileResponse(target)
