from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request

from ..db import append_audit_log, get_tenant, log_usage
from ..info_db import (
    create_building,
    create_registration,
    delete_building,
    delete_registration,
    info_dashboard_summary,
    list_buildings,
    list_registrations,
    update_building,
    update_registration,
)
from .core import _require_auth

router = APIRouter()


def _resolve_info_context(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
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


def _can_edit_info(user: Dict[str, Any]) -> bool:
    if int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1:
        return True
    return str(user.get("role") or "").strip() in {"manager", "desk", "staff"}


def _require_info_editor(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
    user, tenant_id = _resolve_info_context(request, payload)
    if not _can_edit_info(user):
        raise HTTPException(status_code=403, detail="정보관리 수정 권한이 없습니다.")
    return user, tenant_id


@router.get("/info/dashboard")
def info_dashboard(request: Request, tenant_id: str = Query(default="")) -> Dict[str, Any]:
    user, resolved_tenant_id = _resolve_info_context(request, {"tenant_id": tenant_id})
    item = info_dashboard_summary(tenant_id=resolved_tenant_id)
    log_usage(resolved_tenant_id, "info.dashboard")
    append_audit_log(resolved_tenant_id, "info_dashboard", _actor_label(user), {})
    return {"ok": True, "item": item}


@router.get("/info/buildings")
def info_buildings_list(
    request: Request,
    tenant_id: str = Query(default=""),
    usage_type: str = Query(default=""),
    status: str = Query(default=""),
) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_info_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_buildings(tenant_id=resolved_tenant_id, usage_type=usage_type, status=status)}


@router.post("/info/buildings")
def info_buildings_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_info_editor(request, payload)
    item = create_building(
        tenant_id=tenant_id,
        building_code=str(payload.get("building_code") or "").strip(),
        building_name=str(payload.get("building_name") or "").strip(),
        usage_type=str(payload.get("usage_type") or "아파트동").strip(),
        status=str(payload.get("status") or "운영중").strip(),
        floors_above=payload.get("floors_above"),
        floors_below=payload.get("floors_below"),
        household_count=payload.get("household_count"),
        note=str(payload.get("note") or "").strip(),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "info.buildings.create")
    append_audit_log(tenant_id, "create_info_building", _actor_label(user), {"building_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/info/buildings/{building_id}")
def info_buildings_update(request: Request, building_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_info_editor(request, payload)
    item = update_building(
        int(building_id),
        tenant_id=tenant_id,
        building_code=payload.get("building_code"),
        building_name=payload.get("building_name"),
        usage_type=payload.get("usage_type"),
        status=payload.get("status"),
        floors_above=payload.get("floors_above"),
        floors_below=payload.get("floors_below"),
        household_count=payload.get("household_count"),
        note=payload.get("note"),
    )
    log_usage(tenant_id, "info.buildings.update")
    append_audit_log(tenant_id, "update_info_building", _actor_label(user), {"building_id": int(building_id)})
    return {"ok": True, "item": item}


@router.delete("/info/buildings/{building_id}")
def info_buildings_delete(request: Request, building_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_info_editor(request, payload or {})
    item = delete_building(tenant_id=tenant_id, building_id=int(building_id))
    log_usage(tenant_id, "info.buildings.delete")
    append_audit_log(tenant_id, "delete_info_building", _actor_label(user), {"building_id": int(building_id)})
    return {"ok": True, "item": item}


@router.get("/info/registrations")
def info_registrations_list(
    request: Request,
    tenant_id: str = Query(default=""),
    record_type: str = Query(default=""),
    status: str = Query(default=""),
) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_info_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_registrations(tenant_id=resolved_tenant_id, record_type=record_type, status=status)}


@router.post("/info/registrations")
def info_registrations_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_info_editor(request, payload)
    item = create_registration(
        tenant_id=tenant_id,
        record_type=str(payload.get("record_type") or "기타").strip(),
        title=str(payload.get("title") or "").strip(),
        reference_no=str(payload.get("reference_no") or "").strip(),
        status=str(payload.get("status") or "유효").strip(),
        issuer_name=str(payload.get("issuer_name") or "").strip(),
        issued_on=str(payload.get("issued_on") or "").strip(),
        expires_on=str(payload.get("expires_on") or "").strip(),
        note=str(payload.get("note") or "").strip(),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "info.registrations.create")
    append_audit_log(tenant_id, "create_info_registration", _actor_label(user), {"registration_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/info/registrations/{registration_id}")
def info_registrations_update(request: Request, registration_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_info_editor(request, payload)
    item = update_registration(
        int(registration_id),
        tenant_id=tenant_id,
        record_type=payload.get("record_type"),
        title=payload.get("title"),
        reference_no=payload.get("reference_no"),
        status=payload.get("status"),
        issuer_name=payload.get("issuer_name"),
        issued_on=payload.get("issued_on"),
        expires_on=payload.get("expires_on"),
        note=payload.get("note"),
    )
    log_usage(tenant_id, "info.registrations.update")
    append_audit_log(tenant_id, "update_info_registration", _actor_label(user), {"registration_id": int(registration_id)})
    return {"ok": True, "item": item}


@router.delete("/info/registrations/{registration_id}")
def info_registrations_delete(request: Request, registration_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_info_editor(request, payload or {})
    item = delete_registration(tenant_id=tenant_id, registration_id=int(registration_id))
    log_usage(tenant_id, "info.registrations.delete")
    append_audit_log(tenant_id, "delete_info_registration", _actor_label(user), {"registration_id": int(registration_id)})
    return {"ok": True, "item": item}

