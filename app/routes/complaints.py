from __future__ import annotations

import mimetypes
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from ..complaints_db import (
    SCOPE_VALUES,
    add_comment,
    add_complaint_attachments,
    assign_complaint,
    checkout_visit,
    complaint_stats,
    create_complaint,
    create_notice,
    create_visit,
    get_complaint_attachment,
    get_complaint,
    list_admin_complaints,
    list_complaint_categories,
    list_complaints_for_reporter,
    list_public_faqs,
    list_public_notices,
    triage_complaint,
    update_notice,
    update_work_order,
)
from ..db import (
    apartment_profile_defaults,
    get_auth_user_by_token,
    get_site_apartment_profile_record,
    resolve_site_identity,
)

router = APIRouter()
AUTH_COOKIE_NAME = (os.getenv("KA_AUTH_COOKIE_NAME") or "ka_part_auth_token").strip()
SECURITY_ROLE_KEYWORDS = ("보안", "경비")
ALLOW_QUERY_ACCESS_TOKEN = (os.getenv("KA_ALLOW_QUERY_ACCESS_TOKEN") or "").strip().lower() in {"1", "true", "yes", "on"}

ROOT_DIR = Path(__file__).resolve().parents[2]
COMPLAINT_UPLOAD_ROOT = Path(
    (os.getenv("KA_COMPLAINT_UPLOAD_DIR") or str(ROOT_DIR / "uploads" / "complaints")).strip()
).resolve()
COMPLAINT_UPLOAD_MAX_FILES = int(os.getenv("KA_COMPLAINT_UPLOAD_MAX_FILES") or "10")
COMPLAINT_UPLOAD_MAX_FILE_BYTES = int(os.getenv("KA_COMPLAINT_UPLOAD_MAX_FILE_BYTES") or str(8 * 1024 * 1024))
COMPLAINT_UPLOAD_MAX_TOTAL_BYTES = int(os.getenv("KA_COMPLAINT_UPLOAD_MAX_TOTAL_BYTES") or str(25 * 1024 * 1024))
_ALLOWED_IMAGE_MIME = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/webp",
    "image/gif",
    "image/heic",
    "image/heif",
}
_ALLOWED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic", ".heif"}

_COMPLAINTS_DETAIL_KO_MAP = {
    "auth required": "로그인이 필요합니다.",
    "invalid or expired session": "로그인이 필요합니다. (세션 만료)",
    "admin only": "관리자만 사용할 수 있습니다.",
    "supervisor admin only": "최고/운영관리자만 사용할 수 있습니다.",
    "complaint not found": "민원을 찾을 수 없습니다.",
    "invalid category_id": "카테고리 값이 올바르지 않습니다.",
    "invalid scope filter": "민원 구분(scope) 값이 올바르지 않습니다.",
    "invalid status filter": "상태 필터 값이 올바르지 않습니다.",
    "assignee_user_id not found or inactive": "배정할 사용자 ID를 찾을 수 없거나 비활성 상태입니다.",
    "work_order not found": "작업지시를 찾을 수 없습니다.",
    "visit not found": "방문기록을 찾을 수 없습니다.",
    "notice not found": "공지사항을 찾을 수 없습니다.",
}


def _detail_ko(value: Any) -> str:
    msg = str(value or "").strip()
    if not msg:
        return ""
    return _COMPLAINTS_DETAIL_KO_MAP.get(msg, msg)


class ComplaintCreatePayload(BaseModel):
    category_id: int = Field(..., ge=1)
    scope: str
    title: str
    description: str
    location_detail: str = ""
    priority: str = "NORMAL"
    site_code: str = ""
    site_name: str = ""
    unit_label: str = ""
    attachments: List[str] = Field(default_factory=list)


class CommentCreatePayload(BaseModel):
    comment: str


class AdminTriagePayload(BaseModel):
    scope: str
    priority: str = "NORMAL"
    resolution_type: str = "REPAIR"
    guidance_template_id: Optional[int] = None
    note: str = ""


class AdminAssignPayload(BaseModel):
    assignee_user_id: int = Field(..., ge=1)
    scheduled_at: str = ""
    note: str = ""


class WorkOrderPatchPayload(BaseModel):
    status: str
    result_note: str = ""


class VisitCreatePayload(BaseModel):
    complaint_id: int = Field(..., ge=1)
    visit_reason: str
    result_note: str = ""


class VisitCheckoutPayload(BaseModel):
    result_note: str = ""


class NoticeCreatePayload(BaseModel):
    title: str
    content: str
    is_pinned: bool = False
    publish_now: bool = True


class NoticePatchPayload(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    is_pinned: Optional[bool] = None
    publish_now: bool = False


def _extract_access_token(request: Request) -> str:
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            return token
    cookie_token = (request.cookies.get(AUTH_COOKIE_NAME) or "").strip()
    if cookie_token:
        return cookie_token
    if ALLOW_QUERY_ACCESS_TOKEN:
        token = (request.query_params.get("access_token") or "").strip()
        if token:
            return token
    raise HTTPException(status_code=401, detail="로그인이 필요합니다.")


def _normalized_role_text(value: Any) -> str:
    return str(value or "").strip()


def _is_security_role(value: Any) -> bool:
    role = _normalized_role_text(value)
    if not role:
        return False
    compact = role.replace(" ", "")
    if compact == "보안/경비":
        return True
    return any(token in role for token in SECURITY_ROLE_KEYWORDS)


def _require_auth(request: Request) -> Tuple[Dict[str, Any], str]:
    token = _extract_access_token(request)
    user = get_auth_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다. (세션 만료)")
    if _is_security_role(user.get("role")):
        raise HTTPException(status_code=403, detail="보안/경비 계정은 주차관리 모듈만 사용할 수 있습니다.")
    return user, token


def _require_admin(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    is_admin = int(user.get("is_admin") or 0) == 1
    is_site_admin = int(user.get("is_site_admin") or 0) == 1
    if not (is_admin or is_site_admin):
        raise HTTPException(status_code=403, detail="관리자만 사용할 수 있습니다.")
    return user, token


def _admin_site_scope(user: Dict[str, Any]) -> str:
    if int(user.get("is_admin") or 0) == 1:
        return ""
    return str(user.get("site_code") or "").strip().upper()


def _enforce_site_admin_scope(user: Dict[str, Any], item: Dict[str, Any] | None) -> None:
    if not item:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    if int(user.get("is_admin") or 0) == 1:
        return
    if int(user.get("is_site_admin") or 0) == 1:
        site_scope = str(user.get("site_code") or "").strip().upper()
        if site_scope and str(item.get("site_code") or "").strip().upper() != site_scope:
            raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")


def _decorate_attachment_access_urls(item: Dict[str, Any] | None) -> Dict[str, Any] | None:
    if not item or not isinstance(item, dict):
        return item
    cid = int(item.get("id") or 0)
    raw = item.get("attachments")
    if not isinstance(raw, list):
        return item
    out: List[Dict[str, Any]] = []
    for a in raw:
        if not isinstance(a, dict):
            continue
        row = dict(a)
        aid = int(row.get("id") or 0)
        file_url = str(row.get("file_url") or "").strip()
        if file_url.lower().startswith("http://") or file_url.lower().startswith("https://"):
            row["access_url"] = file_url
        elif cid > 0 and aid > 0:
            row["access_url"] = f"/api/v1/complaints/{cid}/attachments/{aid}"
        else:
            row["access_url"] = file_url
        out.append(row)
    item["attachments"] = out
    return item


def _guess_image_ext(upload: UploadFile) -> str:
    ct = str(upload.content_type or "").strip().lower()
    if ct in {"image/jpeg", "image/jpg"}:
        return ".jpg"
    if ct == "image/png":
        return ".png"
    if ct == "image/webp":
        return ".webp"
    if ct == "image/gif":
        return ".gif"
    if ct == "image/heic":
        return ".heic"
    if ct == "image/heif":
        return ".heif"
    suffix = Path(str(upload.filename or "")).suffix.lower()
    if suffix in _ALLOWED_IMAGE_EXTS:
        return suffix
    return ".jpg"


async def _save_upload_file(upload: UploadFile, dest: Path, *, max_bytes: int) -> int:
    total = 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with dest.open("wb") as f:
            while True:
                chunk = await upload.read(1024 * 1024)  # 1MB
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"사진 파일은 최대 {max_bytes // (1024 * 1024)}MB까지 업로드할 수 있습니다.",
                    )
                f.write(chunk)
    finally:
        try:
            await upload.close()
        except Exception:
            pass
    return total


@router.get("/codes/complaint-categories")
def get_complaint_categories():
    return {"ok": True, "items": list_complaint_categories(active_only=True)}


@router.get("/notices")
def get_notices(limit: int = Query(50, ge=1, le=200)):
    return {"ok": True, "items": list_public_notices(limit=limit)}


@router.get("/faqs")
def get_faqs(limit: int = Query(100, ge=1, le=300)):
    return {"ok": True, "items": list_public_faqs(limit=limit)}


@router.get("/apartment_profile")
def get_apartment_profile(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    site_id: int = Query(default=0),
):
    user, _token = _require_auth(request)
    is_admin = int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1

    target_site_name = str(site_name or "").strip()
    target_site_code = str(site_code or "").strip().upper()
    target_site_id = int(site_id or 0)

    if not is_admin:
        target_site_name = str(user.get("site_name") or "").strip()
        target_site_code = str(user.get("site_code") or "").strip().upper()
        try:
            target_site_id = int(user.get("site_id") or 0)
        except Exception:
            target_site_id = 0

    resolved = resolve_site_identity(
        site_id=(target_site_id if target_site_id > 0 else None),
        site_name=target_site_name,
        site_code=target_site_code,
        create_site_if_missing=False,
    )
    resolved_site_id = int(resolved.get("site_id") or 0)
    resolved_site_name = str(resolved.get("site_name") or target_site_name or "").strip()
    resolved_site_code = str(resolved.get("site_code") or target_site_code or "").strip().upper()

    row = get_site_apartment_profile_record(
        site_id=(resolved_site_id if resolved_site_id > 0 else 0),
        site_name=resolved_site_name,
        site_code=(resolved_site_code or None),
    )
    if not row:
        defaults = apartment_profile_defaults()
        return {
            "ok": True,
            "exists": False,
            "site_id": resolved_site_id,
            "site_name": resolved_site_name,
            "site_code": resolved_site_code,
            **defaults,
            "created_at": None,
            "updated_at": None,
        }

    return {
        "ok": True,
        "exists": True,
        "site_id": int(row.get("site_id") or resolved_site_id or 0),
        "site_name": resolved_site_name,
        "site_code": resolved_site_code,
        "households_total": int(row.get("households_total") or 0),
        "building_start": int(row.get("building_start") or 101),
        "building_count": int(row.get("building_count") or 0),
        "default_line_count": int(row.get("default_line_count") or 8),
        "default_max_floor": int(row.get("default_max_floor") or 60),
        "default_basement_floors": int(row.get("default_basement_floors") or 0),
        "building_overrides": row.get("building_overrides") if isinstance(row.get("building_overrides"), dict) else {},
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


@router.post("/complaints")
def post_complaint(payload: ComplaintCreatePayload, request: Request):
    user, _token = _require_auth(request)
    is_admin = int(user.get("is_admin") or 0) == 1
    site_code = payload.site_code or str(user.get("site_code") or "")
    site_name = payload.site_name or str(user.get("site_name") or "")
    if not is_admin:
        # Keep complaint site identity aligned with the account scope.
        site_code = str(user.get("site_code") or "")
        site_name = str(user.get("site_name") or "")
    if not is_admin and not str(site_code or "").strip():
        raise HTTPException(status_code=403, detail="소속 단지코드가 지정되지 않았습니다. 관리자에게 문의하세요.")
    try:
        created = create_complaint(
            reporter_user_id=int(user["id"]),
            site_code=site_code,
            site_name=site_name,
            unit_label=payload.unit_label,
            category_id=payload.category_id,
            scope=payload.scope,
            title=payload.title,
            description=payload.description,
            location_detail=payload.location_detail,
            priority=payload.priority,
            attachment_urls=payload.attachments,
            force_emergency=False,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    message = "민원이 접수되었습니다."
    if str(created.get("scope")) == "PRIVATE":
        message = "세대 내부 민원은 수리 출동을 제공하지 않으며 사용 안내 중심으로 처리됩니다."
    return {"ok": True, "message": message, "item": _decorate_attachment_access_urls(created)}


@router.post("/emergencies")
def post_emergency(payload: ComplaintCreatePayload, request: Request):
    user, _token = _require_auth(request)
    is_admin = int(user.get("is_admin") or 0) == 1
    site_code = payload.site_code or str(user.get("site_code") or "")
    site_name = payload.site_name or str(user.get("site_name") or "")
    if not is_admin:
        site_code = str(user.get("site_code") or "")
        site_name = str(user.get("site_name") or "")
    if not is_admin and not str(site_code or "").strip():
        raise HTTPException(status_code=403, detail="소속 단지코드가 지정되지 않았습니다. 관리자에게 문의하세요.")
    try:
        created = create_complaint(
            reporter_user_id=int(user["id"]),
            site_code=site_code,
            site_name=site_name,
            unit_label=payload.unit_label,
            category_id=payload.category_id,
            scope=payload.scope if payload.scope in SCOPE_VALUES else "EMERGENCY",
            title=payload.title,
            description=payload.description,
            location_detail=payload.location_detail,
            priority="URGENT",
            attachment_urls=payload.attachments,
            force_emergency=True,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {
        "ok": True,
        "message": "긴급 민원이 접수되었습니다. 우선순위 '긴급'으로 처리됩니다.",
        "item": _decorate_attachment_access_urls(created),
    }


@router.get("/complaints")
def get_my_complaints(
    request: Request,
    status: str = Query("", description="optional status filter"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    user, _token = _require_auth(request)
    try:
        rows = list_complaints_for_reporter(
            int(user["id"]),
            status=status,
            limit=limit,
            offset=offset,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {"ok": True, "items": rows}


@router.get("/complaints/{complaint_id}")
def get_my_complaint(complaint_id: int, request: Request):
    user, _token = _require_auth(request)
    item = get_complaint(
        int(complaint_id),
        requester_user_id=int(user["id"]),
        is_admin=(int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1),
    )
    _enforce_site_admin_scope(user, item)
    return {"ok": True, "item": _decorate_attachment_access_urls(item)}


@router.post("/complaints/{complaint_id}/attachments")
async def upload_complaint_attachments(
    complaint_id: int,
    request: Request,
    files: List[UploadFile] = File(...),
):
    user, _token = _require_auth(request)
    is_admin = (int(user.get("is_admin") or 0) == 1) or (int(user.get("is_site_admin") or 0) == 1)

    item = get_complaint(int(complaint_id), requester_user_id=int(user["id"]), is_admin=is_admin)
    _enforce_site_admin_scope(user, item)

    uploads = list(files or [])
    if not uploads:
        raise HTTPException(status_code=400, detail="업로드할 사진을 선택하세요.")
    if len(uploads) > max(1, COMPLAINT_UPLOAD_MAX_FILES):
        raise HTTPException(status_code=400, detail=f"사진은 최대 {COMPLAINT_UPLOAD_MAX_FILES}장까지 첨부할 수 있습니다.")

    existing_count = 0
    try:
        existing_count = len(item.get("attachments") or []) if isinstance(item, dict) else 0
    except Exception:
        existing_count = 0
    if existing_count + len(uploads) > 10:
        raise HTTPException(status_code=400, detail="첨부는 최대 10개까지 등록할 수 있습니다.")

    COMPLAINT_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    complaint_dir = (COMPLAINT_UPLOAD_ROOT / str(int(complaint_id))).resolve()
    complaint_dir.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    saved: List[Tuple[str, str, int]] = []
    saved_paths: List[Path] = []

    def _cleanup_saved_files() -> None:
        for p in list(saved_paths):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        saved_paths.clear()

    for upload in uploads:
        ct = str(upload.content_type or "").strip().lower()
        suffix = Path(str(upload.filename or "")).suffix.lower()
        is_image = (ct.startswith("image/") and (ct in _ALLOWED_IMAGE_MIME)) or (suffix in _ALLOWED_IMAGE_EXTS)
        if not is_image:
            _cleanup_saved_files()
            raise HTTPException(status_code=400, detail="이미지 파일(jpg/png/webp/gif/heic)만 업로드할 수 있습니다.")
        ext = _guess_image_ext(upload)
        name = f"{uuid.uuid4().hex}{ext}"
        dest = (complaint_dir / name).resolve()
        if not dest.is_relative_to(complaint_dir):
            _cleanup_saved_files()
            raise HTTPException(status_code=400, detail="파일 경로가 올바르지 않습니다.")
        try:
            size = await _save_upload_file(upload, dest, max_bytes=COMPLAINT_UPLOAD_MAX_FILE_BYTES)
        except Exception:
            try:
                if dest.exists():
                    dest.unlink()
            except Exception:
                pass
            _cleanup_saved_files()
            raise
        total_bytes += int(size)
        if total_bytes > max(1, COMPLAINT_UPLOAD_MAX_TOTAL_BYTES):
            try:
                dest.unlink()
            except Exception:
                pass
            _cleanup_saved_files()
            raise HTTPException(
                status_code=413,
                detail=f"사진 업로드 총 용량은 최대 {COMPLAINT_UPLOAD_MAX_TOTAL_BYTES // (1024 * 1024)}MB까지 가능합니다.",
            )
        rel = f"{int(complaint_id)}/{name}"
        saved.append((rel, ct, int(size)))
        saved_paths.append(dest)

    try:
        add_complaint_attachments(complaint_id=int(complaint_id), attachments=saved)
    except ValueError as e:
        for p in saved_paths:
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e

    updated = get_complaint(int(complaint_id), requester_user_id=int(user["id"]), is_admin=is_admin)
    _enforce_site_admin_scope(user, updated)
    return {"ok": True, "item": _decorate_attachment_access_urls(updated)}


@router.get("/complaints/{complaint_id}/attachments/{attachment_id}")
def download_complaint_attachment(complaint_id: int, attachment_id: int, request: Request):
    user, _token = _require_auth(request)
    is_admin = (int(user.get("is_admin") or 0) == 1) or (int(user.get("is_site_admin") or 0) == 1)

    item = get_complaint(int(complaint_id), requester_user_id=int(user["id"]), is_admin=is_admin)
    _enforce_site_admin_scope(user, item)

    att = get_complaint_attachment(complaint_id=int(complaint_id), attachment_id=int(attachment_id))
    if not att:
        raise HTTPException(status_code=404, detail="첨부파일을 찾을 수 없습니다.")
    file_url = str(att.get("file_url") or "").strip()
    if not file_url or file_url.lower().startswith("http://") or file_url.lower().startswith("https://"):
        raise HTTPException(status_code=404, detail="첨부파일을 찾을 수 없습니다.")

    COMPLAINT_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    safe_rel = file_url.replace("\\", "/").lstrip("/")
    abs_path = (COMPLAINT_UPLOAD_ROOT / safe_rel).resolve()
    if not abs_path.is_relative_to(COMPLAINT_UPLOAD_ROOT):
        raise HTTPException(status_code=404, detail="첨부파일을 찾을 수 없습니다.")
    if not abs_path.exists() or not abs_path.is_file():
        raise HTTPException(status_code=404, detail="첨부파일을 찾을 수 없습니다.")

    media_type = str(att.get("mime_type") or "").strip().lower() or None
    if not media_type:
        guessed, _enc = mimetypes.guess_type(str(abs_path))
        media_type = guessed or "application/octet-stream"
    return FileResponse(path=str(abs_path), media_type=media_type, content_disposition_type="inline")


@router.post("/complaints/{complaint_id}/comments")
def post_comment(complaint_id: int, payload: CommentCreatePayload, request: Request):
    user, _token = _require_auth(request)
    item = get_complaint(
        int(complaint_id),
        requester_user_id=int(user["id"]),
        is_admin=(int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1),
    )
    if not item:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    try:
        out = add_comment(
            complaint_id=int(complaint_id),
            user_id=int(user["id"]),
            comment=payload.comment,
            is_internal=False,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {"ok": True, "item": out}


@router.get("/admin/complaints")
def admin_get_complaints(
    request: Request,
    scope: str = Query(""),
    status: str = Query(""),
    site_code: str = Query(""),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    user, _token = _require_admin(request)
    scoped_site = _admin_site_scope(user)
    effective_site_code = scoped_site or site_code
    try:
        rows = list_admin_complaints(
            scope=scope,
            status=status,
            site_code=effective_site_code,
            limit=limit,
            offset=offset,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {"ok": True, "items": rows}


@router.get("/admin/complaints/{complaint_id}")
def admin_get_complaint(complaint_id: int, request: Request):
    user, _token = _require_admin(request)
    item = get_complaint(int(complaint_id), requester_user_id=int(user["id"]), is_admin=True)
    if not item:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    scoped_site = _admin_site_scope(user)
    if scoped_site and str(item.get("site_code") or "").strip().upper() != scoped_site:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    return {"ok": True, "item": _decorate_attachment_access_urls(item)}


@router.patch("/admin/complaints/{complaint_id}/triage")
def admin_triage_complaint(complaint_id: int, payload: AdminTriagePayload, request: Request):
    user, _token = _require_admin(request)
    existing = get_complaint(int(complaint_id), requester_user_id=int(user["id"]), is_admin=True)
    if not existing:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    scoped_site = _admin_site_scope(user)
    if scoped_site and str(existing.get("site_code") or "").strip().upper() != scoped_site:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    try:
        out = triage_complaint(
            complaint_id=int(complaint_id),
            actor_user_id=int(user["id"]),
            scope=payload.scope,
            priority=payload.priority,
            resolution_type=payload.resolution_type,
            guidance_template_id=payload.guidance_template_id,
            note=payload.note,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {"ok": True, "item": out}


@router.post("/admin/complaints/{complaint_id}/assign")
def admin_assign_complaint(complaint_id: int, payload: AdminAssignPayload, request: Request):
    user, _token = _require_admin(request)
    existing = get_complaint(int(complaint_id), requester_user_id=int(user["id"]), is_admin=True)
    if not existing:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    scoped_site = _admin_site_scope(user)
    if scoped_site and str(existing.get("site_code") or "").strip().upper() != scoped_site:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    try:
        out = assign_complaint(
            complaint_id=int(complaint_id),
            actor_user_id=int(user["id"]),
            assignee_user_id=payload.assignee_user_id,
            scheduled_at=payload.scheduled_at,
            note=payload.note,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {"ok": True, "item": out}


@router.patch("/admin/work-orders/{work_order_id}")
def admin_patch_work_order(work_order_id: int, payload: WorkOrderPatchPayload, request: Request):
    user, _token = _require_admin(request)
    try:
        out = update_work_order(
            work_order_id=int(work_order_id),
            actor_user_id=int(user["id"]),
            status=payload.status,
            result_note=payload.result_note,
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=_detail_ko(msg)) from e
        raise HTTPException(status_code=400, detail=_detail_ko(msg)) from e
    return {"ok": True, "item": out}


@router.post("/admin/visits")
def admin_create_visit(payload: VisitCreatePayload, request: Request):
    user, _token = _require_admin(request)
    existing = get_complaint(int(payload.complaint_id), requester_user_id=int(user["id"]), is_admin=True)
    if not existing:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    scoped_site = _admin_site_scope(user)
    if scoped_site and str(existing.get("site_code") or "").strip().upper() != scoped_site:
        raise HTTPException(status_code=404, detail="민원을 찾을 수 없습니다.")
    try:
        out = create_visit(
            complaint_id=int(payload.complaint_id),
            visitor_user_id=int(user["id"]),
            visit_reason=payload.visit_reason,
            result_note=payload.result_note,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {"ok": True, "item": out}


@router.patch("/admin/visits/{visit_id}/checkout")
def admin_checkout_visit(visit_id: int, payload: VisitCheckoutPayload, request: Request):
    _user, _token = _require_admin(request)
    try:
        out = checkout_visit(visit_id=int(visit_id), result_note=payload.result_note)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=_detail_ko(str(e))) from e
    return {"ok": True, "item": out}


@router.post("/admin/notices")
def admin_create_notice(payload: NoticeCreatePayload, request: Request):
    user, _token = _require_admin(request)
    if int(user.get("is_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="최고/운영관리자만 사용할 수 있습니다.")
    try:
        out = create_notice(
            author_user_id=int(user["id"]),
            title=payload.title,
            content=payload.content,
            is_pinned=payload.is_pinned,
            publish_now=payload.publish_now,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=_detail_ko(str(e))) from e
    return {"ok": True, "item": out}


@router.patch("/admin/notices/{notice_id}")
def admin_patch_notice(notice_id: int, payload: NoticePatchPayload, request: Request):
    user, _token = _require_admin(request)
    if int(user.get("is_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="최고/운영관리자만 사용할 수 있습니다.")
    try:
        out = update_notice(
            notice_id=int(notice_id),
            title=payload.title,
            content=payload.content,
            is_pinned=payload.is_pinned,
            publish_now=payload.publish_now,
        )
    except ValueError as e:
        msg = str(e)
        if "not found" in msg:
            raise HTTPException(status_code=404, detail=_detail_ko(msg)) from e
        raise HTTPException(status_code=400, detail=_detail_ko(msg)) from e
    return {"ok": True, "item": out}


@router.get("/admin/stats/complaints")
def admin_get_stats(request: Request, site_code: str = Query("")):
    user, _token = _require_admin(request)
    scoped_site = _admin_site_scope(user)
    effective_site_code = scoped_site or site_code
    return {"ok": True, "item": complaint_stats(site_code=effective_site_code)}
