from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse

from ..db import (
    append_audit_log,
    default_document_numbering_config,
    get_tenant,
    get_tenant_document_numbering_config,
    log_usage,
    normalize_document_numbering_config,
    update_tenant_document_numbering_config,
)
from ..document_sample_service import extract_document_sample
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
    next_document_reference_no,
    ops_dashboard_summary,
    summarize_document_categories,
    update_document,
    update_notice,
    update_schedule,
    update_vendor,
)
from ..ops_document_catalog import (
    document_category_profiles,
    document_common_field_definitions,
    get_document_category_profile,
)
from ..report_excel import build_ops_document_ledger_xlsx
from ..report_pdf import build_ops_draft_pdf, build_reference_document_pdf
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


def _tenant_label(tenant_id: str) -> str:
    tenant = get_tenant(tenant_id) or {}
    tenant_name = str(tenant.get("name") or "").strip()
    if tenant_name and tenant_id:
        return f"{tenant_name} 관리사무소"
    return tenant_name or tenant_id or "관리사무소"


def _ascii_download_name(value: str, default: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", str(value or "").strip()).strip("-")
    return (cleaned or default)[:80]


def _can_edit_ops(user: Dict[str, Any]) -> bool:
    if int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1:
        return True
    return str(user.get("role") or "").strip() in {"manager", "desk", "staff"}


def _require_ops_editor(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
    user, tenant_id = _resolve_ops_context(request, payload)
    if not _can_edit_ops(user):
        raise HTTPException(status_code=403, detail="행정업무 수정 권한이 없습니다.")
    return user, tenant_id


def _require_ops_manager(request: Request, payload: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], str]:
    user, tenant_id = _resolve_ops_context(request, payload)
    if int(user.get("is_admin") or 0) != 1 and int(user.get("is_site_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="문서번호 체계 설정 권한이 없습니다.")
    return user, tenant_id


def _numbering_preview_map(*, tenant_id: str) -> Dict[str, str]:
    return {
        category: next_document_reference_no(tenant_id=tenant_id, category=category)
        for category in [item["category"] for item in document_category_profiles()]
    }


@router.get("/ops/documents/catalog")
def ops_documents_catalog(request: Request, tenant_id: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    profiles = document_category_profiles()
    return {
        "ok": True,
        "item": {
            "categories": [item["category"] for item in profiles],
            "profiles": profiles,
            "common_fields": document_common_field_definitions(),
            "preview_examples": _numbering_preview_map(tenant_id=resolved_tenant_id),
        },
    }


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
def ops_documents_list(
    request: Request,
    tenant_id: str = Query(default=""),
    status: str = Query(default=""),
    category: str = Query(default=""),
) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    return {
        "ok": True,
        "items": list_documents(tenant_id=resolved_tenant_id, status=status, category=category),
        "category_counts": summarize_document_categories(tenant_id=resolved_tenant_id),
        "selected_category": str(category or "").strip(),
    }


@router.get("/ops/documents/numbering_config")
def ops_documents_numbering_config(request: Request, tenant_id: str = Query(default="")) -> Dict[str, Any]:
    _user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    config = get_tenant_document_numbering_config(resolved_tenant_id)
    return {
        "ok": True,
        "item": {
            "config": config,
            "defaults": default_document_numbering_config(),
            "preview_examples": _numbering_preview_map(tenant_id=resolved_tenant_id),
        },
    }


@router.patch("/ops/documents/numbering_config")
def ops_documents_numbering_config_update(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, tenant_id = _require_ops_manager(request, payload)
    if bool(payload.get("reset")):
        config = update_tenant_document_numbering_config(tenant_id, default_document_numbering_config())
    else:
        raw_config = payload.get("config")
        if raw_config is None:
            raw_config = {
                "separator": payload.get("separator"),
                "date_mode": payload.get("date_mode"),
                "sequence_digits": payload.get("sequence_digits"),
                "category_codes": payload.get("category_codes"),
            }
        config = update_tenant_document_numbering_config(tenant_id, normalize_document_numbering_config(raw_config))
    log_usage(tenant_id, "ops.documents.numbering_config.update")
    append_audit_log(tenant_id, "update_document_numbering_config", _actor_label(user), {"config": config})
    return {
        "ok": True,
        "item": {
            "config": config,
            "defaults": default_document_numbering_config(),
            "preview_examples": _numbering_preview_map(tenant_id=tenant_id),
        },
    }


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
        amount_total=payload.get("amount_total"),
        vendor_name=str(payload.get("vendor_name") or "").strip(),
        target_label=str(payload.get("target_label") or "").strip(),
        basis_date=str(payload.get("basis_date") or "").strip(),
        period_start=str(payload.get("period_start") or "").strip(),
        period_end=str(payload.get("period_end") or "").strip(),
        document_meta=payload.get("document_meta") if isinstance(payload.get("document_meta"), dict) else {},
        created_by_label=_actor_label(user),
    )
    log_usage(tenant_id, "ops.documents.create")
    append_audit_log(tenant_id, "create_document", _actor_label(user), {"document_id": int(item["id"])})
    return {"ok": True, "item": item}


@router.get("/ops/documents/next_reference")
def ops_documents_next_reference(
    request: Request,
    tenant_id: str = Query(default=""),
    category: str = Query(default=""),
) -> Dict[str, Any]:
    user, resolved_tenant_id = _require_ops_editor(request, {"tenant_id": tenant_id})
    resolved_category = str(category or "").strip() or "기타"
    reference_no = next_document_reference_no(tenant_id=resolved_tenant_id, category=resolved_category)
    log_usage(resolved_tenant_id, "ops.documents.next_reference")
    append_audit_log(
        resolved_tenant_id,
        "next_document_reference",
        _actor_label(user),
        {"category": resolved_category, "reference_no": reference_no},
    )
    return {"ok": True, "item": {"reference_no": reference_no, "category": resolved_category}}


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
        amount_total=payload.get("amount_total"),
        vendor_name=payload.get("vendor_name"),
        target_label=payload.get("target_label"),
        basis_date=payload.get("basis_date"),
        period_start=payload.get("period_start"),
        period_end=payload.get("period_end"),
        document_meta=payload.get("document_meta") if isinstance(payload.get("document_meta"), dict) else None,
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


@router.get("/ops/documents/export.xlsx")
def ops_documents_export_xlsx(
    request: Request,
    tenant_id: str = Query(default=""),
    status: str = Query(default=""),
    category: str = Query(default=""),
) -> StreamingResponse:
    user, resolved_tenant_id = _resolve_ops_context(request, {"tenant_id": tenant_id})
    items = list_documents(tenant_id=resolved_tenant_id, status=status, category=category, limit=2000)
    xlsx_bytes = build_ops_document_ledger_xlsx(
        tenant_label=_tenant_label(resolved_tenant_id),
        selected_category=str(category or "").strip(),
        documents=items,
    )
    log_usage(resolved_tenant_id, "ops.documents.export_xlsx")
    append_audit_log(
        resolved_tenant_id,
        "export_document_ledger_xlsx",
        _actor_label(user),
        {"category": str(category or "").strip() or "전체", "count": len(items)},
    )
    safe_name = _ascii_download_name(f"document-ledger-{str(category or '').strip() or 'all'}", "document-ledger")
    headers = {"Content-Disposition": f'attachment; filename="{safe_name[:80]}.xlsx"'}
    return StreamingResponse(
        iter([xlsx_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/ops/documents/render_pdf")
def ops_documents_render_pdf(request: Request, payload: Dict[str, Any] = Body(...)) -> StreamingResponse:
    user, tenant_id = _require_ops_editor(request, payload)
    title = str(payload.get("title") or "").strip()
    summary = str(payload.get("summary") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="문서 제목을 입력하세요.")
    if not summary:
        raise HTTPException(status_code=400, detail="문서 내용을 입력하세요.")
    requested_category = str(payload.get("category") or "").strip() or "기타"
    profile = get_document_category_profile(requested_category)
    category = str(profile.get("category") or requested_category or "기타").strip()
    reference_no = str(payload.get("reference_no") or "").strip() or next_document_reference_no(tenant_id=tenant_id, category=category)
    pdf_bytes = build_ops_draft_pdf(
        tenant_label=_tenant_label(tenant_id),
        title=title,
        summary=summary,
        drafter_label=_actor_label(user),
        reference_no=reference_no,
        category=category,
        owner=str(payload.get("owner") or "").strip(),
        due_date=str(payload.get("due_date") or "").strip(),
        amount_total=payload.get("amount_total"),
        vendor_name=str(payload.get("vendor_name") or "").strip(),
        target_label=str(payload.get("target_label") or "").strip(),
        basis_date=str(payload.get("basis_date") or "").strip(),
        period_start=str(payload.get("period_start") or "").strip(),
        period_end=str(payload.get("period_end") or "").strip(),
        pdf_heading=str(profile.get("pdf_heading") or "").strip(),
        request_text=str(profile.get("request_text") or "").strip(),
        amount_policy=str(profile.get("amount_policy") or "").strip(),
    )
    log_usage(tenant_id, "ops.documents.render_pdf")
    append_audit_log(tenant_id, "render_document_pdf", _actor_label(user), {"title": title})
    safe_name = _ascii_download_name(title, "document")
    headers = {"Content-Disposition": f'attachment; filename="{safe_name[:80]}.pdf"'}
    return StreamingResponse(iter([pdf_bytes]), media_type="application/pdf", headers=headers)


@router.post("/ops/documents/sample_pdf")
async def ops_documents_sample_pdf(
    request: Request,
    tenant_id: str = Form(default=""),
    title: str = Form(default=""),
    source_file: UploadFile = File(...),
) -> StreamingResponse:
    user, resolved_tenant_id = _require_ops_editor(request, {"tenant_id": tenant_id})
    raw_name = str(source_file.filename or "").strip() or "sample"
    file_bytes = await source_file.read()
    try:
        extracted = extract_document_sample(raw_name, file_bytes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        try:
            await source_file.close()
        except Exception:
            pass

    final_title = str(title or "").strip() or str(extracted.get("title") or "").strip() or "기안서 샘플 PDF"
    body_lines = [str(line or "").rstrip() for line in extracted.get("lines") or [] if str(line or "").strip()]
    pdf_bytes = build_reference_document_pdf(
        title=final_title,
        source_name=str(extracted.get("source_name") or raw_name),
        body_lines=body_lines,
        preview_image_bytes=bytes(extracted.get("preview_image_bytes") or b""),
    )
    log_usage(resolved_tenant_id, "ops.documents.sample_pdf")
    append_audit_log(
        resolved_tenant_id,
        "ops_document_sample_pdf",
        _actor_label(user),
        {"source_name": raw_name, "title": final_title},
    )
    safe_name = _ascii_download_name(final_title, "document-sample")
    headers = {"Content-Disposition": f'attachment; filename="{safe_name[:80]}.pdf"'}
    return StreamingResponse(iter([pdf_bytes]), media_type="application/pdf", headers=headers)


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
