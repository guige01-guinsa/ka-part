from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request

from ..db import append_audit_log, get_tenant, log_usage
from ..engine_db import create_complaint, get_complaint
from ..facility_db import (
    create_asset,
    create_checklist,
    create_inspection,
    create_qr_asset,
    create_work_order,
    delete_asset,
    delete_checklist,
    delete_inspection,
    delete_qr_asset,
    delete_work_order,
    facility_dashboard_summary,
    get_inspection,
    get_open_work_order_by_inspection,
    get_work_order,
    list_assets,
    list_checklists,
    list_inspections,
    list_qr_assets,
    list_work_orders,
    update_asset,
    update_checklist,
    update_inspection,
    update_qr_asset,
    update_work_order,
)
from .core import _require_auth

router = APIRouter()


def _resolve_facility_context(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
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


def _can_edit_facility(user: Dict[str, Any]) -> bool:
    if int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1:
        return True
    return str(user.get("role") or "").strip() in {"manager", "desk", "staff"}


def _require_facility_editor(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
    user, tenant_id = _resolve_facility_context(request, payload)
    if not _can_edit_facility(user):
        raise HTTPException(status_code=403, detail="시설관리 수정 권한이 없습니다.")
    return user, tenant_id


def _payload_items(value: Any) -> Any:
    if isinstance(value, list):
        return value
    return str(value or "")


def _default_work_order_priority(result_status: str) -> str:
    mapping = {"조치필요": "긴급", "주의": "높음", "정상": "보통"}
    return mapping.get(str(result_status or "").strip(), "보통")


def _default_work_order_due_date(result_status: str) -> str:
    from datetime import date, timedelta

    status = str(result_status or "").strip()
    days = 0 if status == "조치필요" else 3 if status == "주의" else 7
    return (date.today() + timedelta(days=days)).isoformat()


def _map_asset_category_to_complaint_type(category: str) -> str:
    value = str(category or "").strip()
    return value if value in {"승강기", "전기"} else "시설"


def _map_priority_to_urgency(priority: str) -> str:
    value = str(priority or "").strip()
    if value == "긴급":
        return "긴급"
    if value == "높음":
        return "당일"
    return "일반"


@router.get("/facility/dashboard")
def facility_dashboard(request: Request, tenant_id: str = Query(default="")) -> Dict[str, Any]:
    user, resolved_tenant_id = _resolve_facility_context(request, {"tenant_id": tenant_id})
    item = facility_dashboard_summary(tenant_id=resolved_tenant_id)
    log_usage(resolved_tenant_id, "facility.dashboard")
    append_audit_log(resolved_tenant_id, "facility_dashboard", _actor_label(user), {})
    return {"ok": True, "item": item}


@router.get("/facility/assets")
def facility_assets_list(
    request: Request,
    tenant_id: str = Query(default=""),
    category: str = Query(default=""),
    lifecycle_state: str = Query(default=""),
) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_facility_context(request, {"tenant_id": tenant_id})
    return {
        "ok": True,
        "items": list_assets(tenant_id=resolved_tenant_id, category=category, lifecycle_state=lifecycle_state),
    }


@router.post("/facility/assets")
def facility_assets_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = create_asset(
        tenant_id=tenant_id,
        asset_code=str(payload.get("asset_code") or "").strip(),
        asset_name=str(payload.get("asset_name") or "").strip(),
        category=str(payload.get("category") or "기타").strip(),
        location_name=str(payload.get("location_name") or "").strip(),
        vendor_name=str(payload.get("vendor_name") or "").strip(),
        installed_on=str(payload.get("installed_on") or "").strip(),
        inspection_cycle_days=payload.get("inspection_cycle_days") or 30,
        lifecycle_state=str(payload.get("lifecycle_state") or "운영중").strip(),
        source=str(payload.get("source") or "manual").strip(),
        qr_id=str(payload.get("qr_id") or "").strip(),
        checklist_key=str(payload.get("checklist_key") or "").strip(),
        last_inspected_at=str(payload.get("last_inspected_at") or "").strip(),
        next_inspection_date=str(payload.get("next_inspection_date") or "").strip(),
        note=str(payload.get("note") or "").strip(),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "facility.assets.create")
    append_audit_log(tenant_id, "facility_create_asset", _actor_label(user), {"asset_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/facility/assets/{asset_id}")
def facility_assets_update(request: Request, asset_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = update_asset(
        int(asset_id),
        tenant_id=tenant_id,
        asset_code=payload.get("asset_code"),
        asset_name=payload.get("asset_name"),
        category=payload.get("category"),
        location_name=payload.get("location_name"),
        vendor_name=payload.get("vendor_name"),
        installed_on=payload.get("installed_on"),
        inspection_cycle_days=payload.get("inspection_cycle_days"),
        lifecycle_state=payload.get("lifecycle_state"),
        source=payload.get("source"),
        qr_id=payload.get("qr_id"),
        checklist_key=payload.get("checklist_key"),
        last_inspected_at=payload.get("last_inspected_at"),
        next_inspection_date=payload.get("next_inspection_date"),
        note=payload.get("note"),
    )
    log_usage(tenant_id, "facility.assets.update")
    append_audit_log(tenant_id, "facility_update_asset", _actor_label(user), {"asset_id": int(asset_id)})
    return {"ok": True, "item": item}


@router.delete("/facility/assets/{asset_id}")
def facility_assets_delete(request: Request, asset_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload or {})
    item = delete_asset(tenant_id=tenant_id, asset_id=int(asset_id))
    log_usage(tenant_id, "facility.assets.delete")
    append_audit_log(tenant_id, "facility_delete_asset", _actor_label(user), {"asset_id": int(asset_id)})
    return {"ok": True, "item": item}


@router.get("/facility/checklists")
def facility_checklists_list(request: Request, tenant_id: str = Query(default=""), lifecycle_state: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_facility_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_checklists(tenant_id=resolved_tenant_id, lifecycle_state=lifecycle_state)}


@router.post("/facility/checklists")
def facility_checklists_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = create_checklist(
        tenant_id=tenant_id,
        checklist_key=str(payload.get("checklist_key") or "").strip(),
        title=str(payload.get("title") or "").strip(),
        task_type=str(payload.get("task_type") or "").strip(),
        version_no=str(payload.get("version_no") or "").strip(),
        lifecycle_state=str(payload.get("lifecycle_state") or "운영중").strip(),
        source=str(payload.get("source") or "manual").strip(),
        note=str(payload.get("note") or "").strip(),
        items=_payload_items(payload.get("items")),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "facility.checklists.create")
    append_audit_log(tenant_id, "facility_create_checklist", _actor_label(user), {"checklist_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/facility/checklists/{checklist_id}")
def facility_checklists_update(request: Request, checklist_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = update_checklist(
        int(checklist_id),
        tenant_id=tenant_id,
        checklist_key=payload.get("checklist_key"),
        title=payload.get("title"),
        task_type=payload.get("task_type"),
        version_no=payload.get("version_no"),
        lifecycle_state=payload.get("lifecycle_state"),
        source=payload.get("source"),
        note=payload.get("note"),
        items=_payload_items(payload.get("items")) if "items" in payload else None,
    )
    log_usage(tenant_id, "facility.checklists.update")
    append_audit_log(tenant_id, "facility_update_checklist", _actor_label(user), {"checklist_id": int(checklist_id)})
    return {"ok": True, "item": item}


@router.delete("/facility/checklists/{checklist_id}")
def facility_checklists_delete(request: Request, checklist_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload or {})
    item = delete_checklist(tenant_id=tenant_id, checklist_id=int(checklist_id))
    log_usage(tenant_id, "facility.checklists.delete")
    append_audit_log(tenant_id, "facility_delete_checklist", _actor_label(user), {"checklist_id": int(checklist_id)})
    return {"ok": True, "item": item}


@router.get("/facility/qr_assets")
def facility_qr_assets_list(request: Request, tenant_id: str = Query(default=""), lifecycle_state: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_facility_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_qr_assets(tenant_id=resolved_tenant_id, lifecycle_state=lifecycle_state)}


@router.post("/facility/qr_assets")
def facility_qr_assets_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = create_qr_asset(
        tenant_id=tenant_id,
        qr_id=str(payload.get("qr_id") or "").strip(),
        asset_id=payload.get("asset_id"),
        asset_code_snapshot=str(payload.get("asset_code_snapshot") or "").strip(),
        asset_name_snapshot=str(payload.get("asset_name_snapshot") or "").strip(),
        location_snapshot=str(payload.get("location_snapshot") or "").strip(),
        default_item=str(payload.get("default_item") or "").strip(),
        checklist_key=str(payload.get("checklist_key") or "").strip(),
        lifecycle_state=str(payload.get("lifecycle_state") or "운영중").strip(),
        source=str(payload.get("source") or "manual").strip(),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "facility.qr_assets.create")
    append_audit_log(tenant_id, "facility_create_qr_asset", _actor_label(user), {"qr_asset_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/facility/qr_assets/{qr_asset_id}")
def facility_qr_assets_update(request: Request, qr_asset_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = update_qr_asset(
        int(qr_asset_id),
        tenant_id=tenant_id,
        qr_id=payload.get("qr_id"),
        asset_id=payload.get("asset_id"),
        asset_code_snapshot=payload.get("asset_code_snapshot"),
        asset_name_snapshot=payload.get("asset_name_snapshot"),
        location_snapshot=payload.get("location_snapshot"),
        default_item=payload.get("default_item"),
        checklist_key=payload.get("checklist_key"),
        lifecycle_state=payload.get("lifecycle_state"),
        source=payload.get("source"),
    )
    log_usage(tenant_id, "facility.qr_assets.update")
    append_audit_log(tenant_id, "facility_update_qr_asset", _actor_label(user), {"qr_asset_id": int(qr_asset_id)})
    return {"ok": True, "item": item}


@router.delete("/facility/qr_assets/{qr_asset_id}")
def facility_qr_assets_delete(request: Request, qr_asset_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload or {})
    item = delete_qr_asset(tenant_id=tenant_id, qr_asset_id=int(qr_asset_id))
    log_usage(tenant_id, "facility.qr_assets.delete")
    append_audit_log(tenant_id, "facility_delete_qr_asset", _actor_label(user), {"qr_asset_id": int(qr_asset_id)})
    return {"ok": True, "item": item}


@router.get("/facility/inspections")
def facility_inspections_list(request: Request, tenant_id: str = Query(default=""), result_status: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_facility_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_inspections(tenant_id=resolved_tenant_id, result_status=result_status)}


@router.post("/facility/inspections")
def facility_inspections_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = create_inspection(
        tenant_id=tenant_id,
        title=str(payload.get("title") or "").strip(),
        asset_id=payload.get("asset_id"),
        qr_asset_id=payload.get("qr_asset_id"),
        checklist_key=str(payload.get("checklist_key") or "").strip(),
        inspector=str(payload.get("inspector") or "").strip(),
        inspected_at=str(payload.get("inspected_at") or "").strip(),
        result_status=str(payload.get("result_status") or "정상").strip(),
        notes=str(payload.get("notes") or "").strip(),
        measurement=payload.get("measurement"),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "facility.inspections.create")
    append_audit_log(tenant_id, "facility_create_inspection", _actor_label(user), {"inspection_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/facility/inspections/{inspection_id}")
def facility_inspections_update(request: Request, inspection_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = update_inspection(
        int(inspection_id),
        tenant_id=tenant_id,
        title=payload.get("title"),
        asset_id=payload.get("asset_id"),
        qr_asset_id=payload.get("qr_asset_id"),
        checklist_key=payload.get("checklist_key"),
        inspector=payload.get("inspector"),
        inspected_at=payload.get("inspected_at"),
        result_status=payload.get("result_status"),
        notes=payload.get("notes"),
        measurement=payload.get("measurement"),
    )
    log_usage(tenant_id, "facility.inspections.update")
    append_audit_log(tenant_id, "facility_update_inspection", _actor_label(user), {"inspection_id": int(inspection_id)})
    return {"ok": True, "item": item}


@router.delete("/facility/inspections/{inspection_id}")
def facility_inspections_delete(request: Request, inspection_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload or {})
    item = delete_inspection(tenant_id=tenant_id, inspection_id=int(inspection_id))
    log_usage(tenant_id, "facility.inspections.delete")
    append_audit_log(tenant_id, "facility_delete_inspection", _actor_label(user), {"inspection_id": int(inspection_id)})
    return {"ok": True, "item": item}


@router.post("/facility/inspections/{inspection_id}/issue_work_order")
def facility_inspections_issue_work_order(request: Request, inspection_id: int, payload: Dict[str, Any] = Body(default={})) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload or {})
    inspection = get_inspection(tenant_id=tenant_id, inspection_id=int(inspection_id))
    if not inspection:
        raise HTTPException(status_code=404, detail="inspection not found")
    existing = get_open_work_order_by_inspection(tenant_id=tenant_id, inspection_id=int(inspection_id))
    if existing:
        return {"ok": True, "created": False, "item": existing}

    priority = str(payload.get("priority") or _default_work_order_priority(inspection.get("result_status"))).strip()
    item = create_work_order(
        tenant_id=tenant_id,
        title=str(payload.get("title") or f"{inspection.get('title') or '점검'} 후속 작업").strip(),
        description=str(payload.get("description") or inspection.get("notes") or "").strip(),
        asset_id=payload.get("asset_id") if payload.get("asset_id") is not None else inspection.get("asset_id"),
        qr_asset_id=payload.get("qr_asset_id") if payload.get("qr_asset_id") is not None else inspection.get("qr_asset_id"),
        inspection_id=int(inspection_id),
        category=str(payload.get("category") or "점검후속").strip() or "점검후속",
        priority=priority,
        status=str(payload.get("status") or "접수").strip() or "접수",
        assignee=str(payload.get("assignee") or "").strip(),
        reporter=str(payload.get("reporter") or inspection.get("inspector") or _actor_label(user)).strip(),
        due_date=str(payload.get("due_date") or _default_work_order_due_date(inspection.get("result_status"))).strip(),
        is_escalated=bool(payload.get("is_escalated")) or priority == "긴급",
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "facility.inspections.issue_work_order")
    append_audit_log(
        tenant_id,
        "facility_issue_work_order_from_inspection",
        _actor_label(user),
        {"inspection_id": int(inspection_id), "work_order_id": int(item["id"])},
    )
    return {"ok": True, "created": True, "item": item}


@router.get("/facility/work_orders")
def facility_work_orders_list(
    request: Request,
    tenant_id: str = Query(default=""),
    status: str = Query(default=""),
    priority: str = Query(default=""),
) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_facility_context(request, {"tenant_id": tenant_id})
    return {"ok": True, "items": list_work_orders(tenant_id=resolved_tenant_id, status=status, priority=priority)}


@router.post("/facility/work_orders")
def facility_work_orders_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = create_work_order(
        tenant_id=tenant_id,
        title=str(payload.get("title") or "").strip(),
        description=str(payload.get("description") or "").strip(),
        asset_id=payload.get("asset_id"),
        qr_asset_id=payload.get("qr_asset_id"),
        inspection_id=payload.get("inspection_id"),
        complaint_id=payload.get("complaint_id"),
        category=str(payload.get("category") or "기타").strip(),
        priority=str(payload.get("priority") or "보통").strip(),
        status=str(payload.get("status") or "접수").strip(),
        assignee=str(payload.get("assignee") or "").strip(),
        reporter=str(payload.get("reporter") or "").strip(),
        due_date=str(payload.get("due_date") or "").strip(),
        completed_at=str(payload.get("completed_at") or "").strip(),
        resolution_notes=str(payload.get("resolution_notes") or "").strip(),
        is_escalated=bool(payload.get("is_escalated")),
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "facility.work_orders.create")
    append_audit_log(tenant_id, "facility_create_work_order", _actor_label(user), {"work_order_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.patch("/facility/work_orders/{work_order_id}")
def facility_work_orders_update(request: Request, work_order_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload)
    item = update_work_order(
        int(work_order_id),
        tenant_id=tenant_id,
        title=payload.get("title"),
        description=payload.get("description"),
        asset_id=payload.get("asset_id"),
        qr_asset_id=payload.get("qr_asset_id"),
        inspection_id=payload.get("inspection_id"),
        complaint_id=payload.get("complaint_id"),
        category=payload.get("category"),
        priority=payload.get("priority"),
        status=payload.get("status"),
        assignee=payload.get("assignee"),
        reporter=payload.get("reporter"),
        due_date=payload.get("due_date"),
        completed_at=payload.get("completed_at"),
        resolution_notes=payload.get("resolution_notes"),
        is_escalated=payload.get("is_escalated"),
    )
    log_usage(tenant_id, "facility.work_orders.update")
    append_audit_log(tenant_id, "facility_update_work_order", _actor_label(user), {"work_order_id": int(work_order_id)})
    return {"ok": True, "item": item}


@router.delete("/facility/work_orders/{work_order_id}")
def facility_work_orders_delete(request: Request, work_order_id: int, payload: Dict[str, Any] | None = Body(default=None)) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload or {})
    item = delete_work_order(tenant_id=tenant_id, work_order_id=int(work_order_id))
    log_usage(tenant_id, "facility.work_orders.delete")
    append_audit_log(tenant_id, "facility_delete_work_order", _actor_label(user), {"work_order_id": int(work_order_id)})
    return {"ok": True, "item": item}


@router.post("/facility/work_orders/{work_order_id}/create_complaint")
def facility_work_orders_create_complaint(request: Request, work_order_id: int, payload: Dict[str, Any] = Body(default={})) -> Dict[str, Any]:
    user, tenant_id = _require_facility_editor(request, payload or {})
    work_order = get_work_order(tenant_id=tenant_id, work_order_id=int(work_order_id))
    if not work_order:
        raise HTTPException(status_code=404, detail="work order not found")
    existing_complaint_id = int(work_order.get("complaint_id") or 0)
    if existing_complaint_id:
        existing = get_complaint(tenant_id=tenant_id, complaint_id=existing_complaint_id)
        if existing:
            return {"ok": True, "created": False, "item": existing, "work_order": work_order}

    summary = str(payload.get("summary") or work_order.get("title") or "").strip()
    actor = _actor_label(user)
    complaint = create_complaint(
        tenant_id=tenant_id,
        building=str(payload.get("building") or "").strip(),
        unit=str(payload.get("unit") or "").strip(),
        complainant_phone=str(payload.get("complainant_phone") or "").strip(),
        channel=str(payload.get("channel") or "기타").strip() or "기타",
        content=str(payload.get("content") or work_order.get("description") or summary).strip() or summary,
        summary=summary,
        complaint_type=str(payload.get("type") or _map_asset_category_to_complaint_type(work_order.get("asset_category") or "")).strip() or "시설",
        urgency=str(payload.get("urgency") or _map_priority_to_urgency(work_order.get("priority"))).strip() or "일반",
        status=str(payload.get("status") or "접수").strip() or "접수",
        manager=str(payload.get("manager") or work_order.get("assignee") or "").strip(),
        source_text=str(payload.get("source_text") or f"facility-work-order:{work_order_id}").strip(),
        ai_model="facility-link",
        created_by_label=actor,
    )
    linked_work_order = update_work_order(
        int(work_order_id),
        tenant_id=tenant_id,
        complaint_id=int(complaint["id"]),
    )
    log_usage(tenant_id, "facility.work_orders.create_complaint")
    append_audit_log(
        tenant_id,
        "facility_create_complaint_from_work_order",
        actor,
        {"work_order_id": int(work_order_id), "complaint_id": int(complaint["id"])},
    )
    return {"ok": True, "created": True, "item": complaint, "work_order": linked_work_order}
