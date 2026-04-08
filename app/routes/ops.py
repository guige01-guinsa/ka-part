from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request

from ..db import append_audit_log, get_tenant, log_usage
from ..ops_db import (
    create_document,
    create_notice,
    create_schedule,
    create_vendor,
    delete_document,
    delete_notice,
    delete_schedule,
    delete_vendor,
    list_documents,
    list_notices,
    list_schedules,
    list_vendors,
    ops_dashboard_summary,
    update_document,
    update_notice,
    update_schedule,
    update_vendor,
)
from .core import _require_auth

router = APIRouter()


def _resolve_ops_context(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
    user, _token = _require_auth(request)
    requested = str((payload or {}).get("tenant_id") or request.query_params.get("tenant_id") or "").strip().lower()
    if int(user.get("is_admin") or 0) == 1:
        tenant_id = requested or str(user.get("tenant_id") or "").strip().lower()
        if not tenant_id:
            raise HTTPException(status_code=400, detail="tenant_id가 필요합니다.")
        tenant = get_tenant(tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="tenant not found")
        return user, tenant_id
    tenant_id = str(user.get("tenant_id") or "").strip().lower()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="계정에 tenant_id가 연결되어 있지 않습니다.")
    return user, tenant_id


def _actor_label(user: Dict[str, Any]) -> str:
    return str(user.get("name") or user.get("login_id") or "operator")


def _can_edit_ops(user: Dict[str, Any]) -> bool:
    if int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1:
        return True
    return str(user.get("role") or "").strip() in {"manager", "desk", "staff"}


def _require_ops_editor(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
    user, tenant_id = _resolve_ops_context(request, payload)
    if not _can_edit_ops(user):
        raise HTTPException(status_code=403, detail="행정업무 수정 권한이 없습니다.")
    return user, tenant_id


@router.get("/ops/dashboard")
def ops_dashboard(request: Request, tenant_id: str = Query(default="")) -> Dict[str, Any]:
    user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    item = ops_dashboard_summary(tenant_id=resolved_tenant_id)
    log_usage(resolved_tenant_id, "ops.dashboard")
    append_audit_log(resolved_tenant_id, "ops_dashboard", _actor_label(user), {})
    return {"ok": True, "item": item}


@router.get("/ops/notices")
def ops_notices_list(request: Request, tenant_id: str = Query(default=""), status: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_notices(tenant_id=resolved_tenant_id, status=status)}


@router.post("/ops/notices")
def ops_notices_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    item = create_notice(
        tenant_id=tenant_id,
        title=str(payload.get("title") or "").strip(),
        body=str(payload.get("body") or "").strip(),
        category=str(payload.get("category") or "공지").strip(),
        status=str(payload.get("status") or "published").strip(),
        pinned=bool(payload.get("pinned")),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "ops.notices.create")
    append_audit_log(tenant_id, "create_notice", _actor_label(user), {"notice_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/ops/notices/{notice_id}")
def ops_notices_update(request: Request, notice_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    item = update_notice(
        int(notice_id),
        tenant_id=tenant_id,
        title=payload.get("title"),
        body=payload.get("body"),
        category=payload.get("category"),
        status=payload.get("status"),
        pinned=payload.get("pinned"),
    )
    log_usage(tenant_id, "ops.notices.update")
    append_audit_log(tenant_id, "update_notice", _actor_label(user), {"notice_id": int(notice_id)})
    return {"ok": True, "item": item}


@router.delete("/ops/notices/{notice_id}")
def ops_notices_delete(request: Request, notice_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload or {})
    item = delete_notice(tenant_id=tenant_id, notice_id=int(notice_id))
    log_usage(tenant_id, "ops.notices.delete")
    append_audit_log(tenant_id, "delete_notice", _actor_label(user), {"notice_id": int(notice_id)})
    return {"ok": True, "item": item}


@router.get("/ops/documents")
def ops_documents_list(request: Request, tenant_id: str = Query(default=""), status: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_documents(tenant_id=resolved_tenant_id, status=status)}


@router.post("/ops/documents")
def ops_documents_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    item = create_document(
        tenant_id=tenant_id,
        title=str(payload.get("title") or "").strip(),
        summary=str(payload.get("summary") or "").strip(),
        category=str(payload.get("category") or "기타").strip(),
        status=str(payload.get("status") or "작성중").strip(),
        owner=str(payload.get("owner") or "").strip(),
        due_date=str(payload.get("due_date") or "").strip(),
        reference_no=str(payload.get("reference_no") or "").strip(),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "ops.documents.create")
    append_audit_log(tenant_id, "create_document", _actor_label(user), {"document_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/ops/documents/{document_id}")
def ops_documents_update(request: Request, document_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    item = update_document(
        int(document_id),
        tenant_id=tenant_id,
        title=payload.get("title"),
        summary=payload.get("summary"),
        category=payload.get("category"),
        status=payload.get("status"),
        owner=payload.get("owner"),
        due_date=payload.get("due_date"),
        reference_no=payload.get("reference_no"),
    )
    log_usage(tenant_id, "ops.documents.update")
    append_audit_log(tenant_id, "update_document", _actor_label(user), {"document_id": int(document_id)})
    return {"ok": True, "item": item}


@router.delete("/ops/documents/{document_id}")
def ops_documents_delete(request: Request, document_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload or {})
    item = delete_document(tenant_id=tenant_id, document_id=int(document_id))
    log_usage(tenant_id, "ops.documents.delete")
    append_audit_log(tenant_id, "delete_document", _actor_label(user), {"document_id": int(document_id)})
    return {"ok": True, "item": item}


@router.get("/ops/vendors")
def ops_vendors_list(request: Request, tenant_id: str = Query(default=""), status: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_vendors(tenant_id=resolved_tenant_id, status=status)}


@router.post("/ops/vendors")
def ops_vendors_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    item = create_vendor(
        tenant_id=tenant_id,
        company_name=str(payload.get("company_name") or "").strip(),
        service_type=str(payload.get("service_type") or "").strip(),
        contact_name=str(payload.get("contact_name") or "").strip(),
        phone=str(payload.get("phone") or "").strip(),
        email=str(payload.get("email") or "").strip(),
        status=str(payload.get("status") or "활성").strip(),
        note=str(payload.get("note") or "").strip(),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "ops.vendors.create")
    append_audit_log(tenant_id, "create_vendor", _actor_label(user), {"vendor_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/ops/vendors/{vendor_id}")
def ops_vendors_update(request: Request, vendor_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    item = update_vendor(
        int(vendor_id),
        tenant_id=tenant_id,
        company_name=payload.get("company_name"),
        service_type=payload.get("service_type"),
        contact_name=payload.get("contact_name"),
        phone=payload.get("phone"),
        email=payload.get("email"),
        status=payload.get("status"),
        note=payload.get("note"),
    )
    log_usage(tenant_id, "ops.vendors.update")
    append_audit_log(tenant_id, "update_vendor", _actor_label(user), {"vendor_id": int(vendor_id)})
    return {"ok": True, "item": item}


@router.delete("/ops/vendors/{vendor_id}")
def ops_vendors_delete(request: Request, vendor_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload or {})
    item = delete_vendor(tenant_id=tenant_id, vendor_id=int(vendor_id))
    log_usage(tenant_id, "ops.vendors.delete")
    append_audit_log(tenant_id, "delete_vendor", _actor_label(user), {"vendor_id": int(vendor_id)})
    return {"ok": True, "item": item}


@router.get("/ops/schedules")
def ops_schedules_list(request: Request, tenant_id: str = Query(default=""), status: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_schedules(tenant_id=resolved_tenant_id, status=status)}


@router.post("/ops/schedules")
def ops_schedules_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    vendor_id_raw = payload.get("vendor_id")
    vendor_id = int(vendor_id_raw) if str(vendor_id_raw or "").strip() else None
    item = create_schedule(
        tenant_id=tenant_id,
        title=str(payload.get("title") or "").strip(),
        schedule_type=str(payload.get("schedule_type") or "행정").strip(),
        status=str(payload.get("status") or "예정").strip(),
        due_date=str(payload.get("due_date") or "").strip(),
        owner=str(payload.get("owner") or "").strip(),
        note=str(payload.get("note") or "").strip(),
        vendor_id=vendor_id,
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "ops.schedules.create")
    append_audit_log(tenant_id, "create_schedule", _actor_label(user), {"schedule_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/ops/schedules/{schedule_id}")
def ops_schedules_update(request: Request, schedule_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload)
    vendor_id_raw = payload.get("vendor_id")
    vendor_id = None
    if "vendor_id" in payload:
        vendor_id = int(vendor_id_raw) if str(vendor_id_raw or "").strip() else 0
    item = update_schedule(
        int(schedule_id),
        tenant_id=tenant_id,
        title=payload.get("title"),
        schedule_type=payload.get("schedule_type"),
        status=payload.get("status"),
        due_date=payload.get("due_date"),
        owner=payload.get("owner"),
        note=payload.get("note"),
        vendor_id=vendor_id,
    )
    log_usage(tenant_id, "ops.schedules.update")
    append_audit_log(tenant_id, "update_schedule", _actor_label(user), {"schedule_id": int(schedule_id)})
    return {"ok": True, "item": item}


@router.delete("/ops/schedules/{schedule_id}")
def ops_schedules_delete(request: Request, schedule_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_editor(request, payload or {})
    item = delete_schedule(tenant_id=tenant_id, schedule_id=int(schedule_id))
    log_usage(tenant_id, "ops.schedules.delete")
    append_audit_log(tenant_id, "delete_schedule", _actor_label(user), {"schedule_id": int(schedule_id)})
    return {"ok": True, "item": item}
