from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

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
from ..document_sample_service import extract_document_sample
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
from ..report_pdf import build_kakao_digest_pdf, build_work_report_pdf
from ..work_report_batch import (
    build_work_report_job_dir,
    complete_work_report_job,
    create_work_report_job,
    fail_work_report_job,
    get_work_report_job,
    get_work_report_job_record,
    mark_work_report_job_running,
    new_work_report_job_id,
    update_work_report_job_progress,
)
from ..work_report_service import (
    MAX_WORK_REPORT_ATTACHMENTS,
    MAX_WORK_REPORT_IMAGES,
    analyze_work_report,
)

router = APIRouter()
logger = logging.getLogger("ka-part.work-report")
AUTH_COOKIE_NAME = (os.getenv("KA_AUTH_COOKIE_NAME") or "ka_part_auth_token").strip()
UPLOAD_ROOT = (STORAGE_ROOT / "uploads" / "complaints").resolve()
DIGEST_IMAGE_MAX_BYTES = 10 * 1024 * 1024
WORK_REPORT_FILE_MAX_BYTES = 15 * 1024 * 1024
WORK_REPORT_SAMPLE_MAX_BYTES = 30 * 1024 * 1024
MAX_WORK_REPORT_SOURCE_FILES = 20
WORK_REPORT_BATCH_METADATA_FILE = "job-input.json"
WORK_REPORT_BATCH_TASKS: set[asyncio.Task[Any]] = set()


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


def _can_delete_complaint(user: Optional[Dict[str, Any]]) -> bool:
    return bool(user) and (int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1)


def _tenant_label(tenant_id: str, tenant: Optional[Dict[str, Any]]) -> str:
    item = tenant or get_tenant(tenant_id) or {}
    tenant_name = str(item.get("name") or "").strip()
    resolved_tenant_id = str(item.get("id") or tenant_id or "").strip()
    if tenant_name and resolved_tenant_id:
        return f"{tenant_name} ({resolved_tenant_id})"
    return tenant_name or resolved_tenant_id or "-"


def _download_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(value or "").strip())
    cleaned = cleaned.strip("-") or "report"
    return cleaned[:80]


def _as_row_text(row: Dict[str, Any], key: str, default: str = "") -> str:
    return str(row.get(key) or default).strip()


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


async def _read_work_report_images(files: List[UploadFile]) -> List[Dict[str, Any]]:
    uploads = list(files or [])
    if len(uploads) > MAX_WORK_REPORT_IMAGES:
        raise HTTPException(status_code=400, detail=f"업무보고 이미지는 최대 {MAX_WORK_REPORT_IMAGES}장까지 업로드할 수 있습니다.")
    items: List[Dict[str, Any]] = []
    for upload in uploads:
        content_type = str(upload.content_type or "").strip().lower()
        if not content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="업무보고 이미지는 이미지 파일만 업로드할 수 있습니다.")
        raw = await upload.read()
        try:
            if len(raw) > WORK_REPORT_FILE_MAX_BYTES:
                raise HTTPException(status_code=400, detail="업무보고 이미지 한 장은 15MB 이하여야 합니다.")
            items.append(
                {
                    "filename": str(upload.filename or "work-report-image").strip() or "work-report-image",
                    "content_type": content_type or "image/jpeg",
                    "size_bytes": len(raw),
                    "bytes": raw,
                }
            )
        finally:
            try:
                await upload.close()
            except Exception:
                pass
    return items


async def _read_work_report_attachments(files: List[UploadFile]) -> List[Dict[str, Any]]:
    uploads = list(files or [])
    if len(uploads) > MAX_WORK_REPORT_ATTACHMENTS:
        raise HTTPException(status_code=400, detail=f"업무보고 첨부파일은 최대 {MAX_WORK_REPORT_ATTACHMENTS}건까지 업로드할 수 있습니다.")
    items: List[Dict[str, Any]] = []
    for upload in uploads:
        raw = await upload.read()
        filename = str(upload.filename or "attachment").strip() or "attachment"
        content_type = str(upload.content_type or "application/octet-stream").strip() or "application/octet-stream"
        preview_text = ""
        try:
            if len(raw) > WORK_REPORT_FILE_MAX_BYTES:
                raise HTTPException(status_code=400, detail="업무보고 첨부파일 한 건은 15MB 이하여야 합니다.")
            suffix = Path(filename).suffix.lower()
            if suffix in {".hwp", ".txt", ".md"}:
                try:
                    preview = extract_document_sample(filename, raw)
                    preview_text = "\n".join(str(line or "") for line in (preview.get("lines") or [])[:8])
                except Exception:
                    preview_text = ""
            items.append(
                {
                    "filename": filename,
                    "content_type": content_type,
                    "size_bytes": len(raw),
                    "bytes": raw,
                    "preview_text": preview_text,
                }
            )
        finally:
            try:
                await upload.close()
            except Exception:
                pass
    return items


async def _read_work_report_sample(upload: Optional[UploadFile]) -> Dict[str, Any]:
    if not upload:
        return {}
    raw_name = str(upload.filename or "").strip() or "sample"
    file_bytes = await upload.read()
    try:
        if len(file_bytes) > WORK_REPORT_SAMPLE_MAX_BYTES:
            raise HTTPException(status_code=400, detail="샘플 양식 파일은 30MB 이하여야 합니다.")
        try:
            sample = extract_document_sample(raw_name, file_bytes)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        sample["source_name"] = raw_name
        return sample
    finally:
        try:
            await upload.close()
        except Exception:
            pass


def _is_work_report_image_upload(upload: UploadFile, file_name: str) -> bool:
    content_type = str(upload.content_type or "").lower()
    if content_type.startswith("image/"):
        return True
    return Path(str(file_name or "")).suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}


async def _read_work_report_sources(uploads: List[UploadFile]) -> Dict[str, Any]:
    if not uploads:
        return {}
    if len(uploads) > MAX_WORK_REPORT_SOURCE_FILES:
        raise HTTPException(status_code=400, detail=f"카톡 원문 파일은 최대 {MAX_WORK_REPORT_SOURCE_FILES}개까지 업로드할 수 있습니다.")
    source_texts: List[str] = []
    source_names: List[str] = []
    source_images: List[Dict[str, Any]] = []
    for upload in uploads:
        raw_name = str(upload.filename or "").strip() or "source"
        file_bytes = await upload.read()
        try:
            if _is_work_report_image_upload(upload, raw_name):
                if len(file_bytes) > WORK_REPORT_FILE_MAX_BYTES:
                    raise HTTPException(status_code=400, detail="카톡 캡처 이미지는 15MB 이하여야 합니다.")
                source_images.append(
                    {
                        "filename": raw_name,
                        "content_type": str(upload.content_type or "image/jpeg") or "image/jpeg",
                        "bytes": file_bytes,
                    }
                )
                source_names.append(raw_name)
                continue
            if len(file_bytes) > WORK_REPORT_SAMPLE_MAX_BYTES:
                raise HTTPException(status_code=400, detail="카톡 원문 파일은 30MB 이하여야 합니다.")
            try:
                source = extract_document_sample(raw_name, file_bytes)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            source_names.append(raw_name)
            source_text = "\n".join(str(line or "") for line in (source.get("lines") or []))
            if source_text:
                source_texts.append(source_text)
        finally:
            try:
                await upload.close()
            except Exception:
                pass
    return {
        "source_name": ", ".join(source_names[:3]) if source_names else "",
        "source_names": source_names,
        "source_text": "\n".join(part for part in source_texts if str(part or "").strip()),
        "source_images": source_images,
    }


def _safe_work_report_batch_name(filename: str, default: str) -> str:
    raw_name = Path(str(filename or "").strip() or default).name
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", ".", "(", ")", "[", "]"} else "_" for ch in raw_name)
    return cleaned[:140] or default


def _stage_work_report_batch_images(job_dir: Path, rows: List[Dict[str, Any]], folder: str) -> List[Dict[str, Any]]:
    staged: List[Dict[str, Any]] = []
    target_root = (job_dir / folder).resolve()
    target_root.mkdir(parents=True, exist_ok=True)
    for index, row in enumerate(list(rows or []), start=1):
        filename = _safe_work_report_batch_name(str(row.get("filename") or ""), f"{folder}-{index}.bin")
        target_path = (target_root / f"{index:03d}-{filename}").resolve()
        if not str(target_path).startswith(str(job_dir.resolve())):
            raise ValueError("invalid batch image path")
        raw = bytes(row.get("bytes") or b"")
        target_path.write_bytes(raw)
        staged.append(
            {
                "filename": str(row.get("filename") or filename),
                "content_type": str(row.get("content_type") or "image/jpeg"),
                "size_bytes": int(row.get("size_bytes") or len(raw)),
                "relative_path": str(target_path.relative_to(job_dir.resolve())).replace("\\", "/"),
            }
        )
    return staged


def _write_work_report_batch_payload(
    job_dir: Path,
    *,
    text: str,
    source: Dict[str, Any],
    image_inputs: List[Dict[str, Any]],
    attachment_inputs: List[Dict[str, Any]],
    sample: Dict[str, Any],
) -> None:
    target_dir = job_dir.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "text": str(text or ""),
        "source_name": str(source.get("source_name") or ""),
        "source_names": [str(value or "") for value in source.get("source_names") or []],
        "source_text": str(source.get("source_text") or ""),
        "reference_images": _stage_work_report_batch_images(target_dir, list(source.get("source_images") or []), "reference_images"),
        "images": _stage_work_report_batch_images(target_dir, list(image_inputs or []), "images"),
        "attachments": [
            {
                "filename": str(row.get("filename") or ""),
                "content_type": str(row.get("content_type") or "application/octet-stream"),
                "size_bytes": int(row.get("size_bytes") or 0),
                "preview_text": str(row.get("preview_text") or ""),
            }
            for row in list(attachment_inputs or [])
        ],
        "sample": {
            "title": str(sample.get("title") or ""),
            "lines": [str(line or "") for line in sample.get("lines") or []],
            "source_name": str(sample.get("source_name") or ""),
            "kind": str(sample.get("kind") or ""),
        },
    }
    metadata_path = (target_dir / WORK_REPORT_BATCH_METADATA_FILE).resolve()
    if not str(metadata_path).startswith(str(target_dir)):
        raise ValueError("invalid work report metadata path")
    metadata_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _read_work_report_batch_payload(job_dir: Path) -> Dict[str, Any]:
    target_dir = job_dir.resolve()
    metadata_path = (target_dir / WORK_REPORT_BATCH_METADATA_FILE).resolve()
    if not str(metadata_path).startswith(str(target_dir)) or not metadata_path.exists():
        raise ValueError("업무보고 배치 입력을 찾을 수 없습니다.")
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError("업무보고 배치 입력 형식이 잘못되었습니다.") from exc
    if not isinstance(payload, dict):
        raise ValueError("업무보고 배치 입력 형식이 잘못되었습니다.")

    def load_images(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            relative_path = str(row.get("relative_path") or "").strip()
            if not relative_path:
                continue
            file_path = (target_dir / relative_path).resolve()
            if not str(file_path).startswith(str(target_dir)) or not file_path.exists():
                continue
            items.append(
                {
                    "filename": str(row.get("filename") or file_path.name),
                    "content_type": str(row.get("content_type") or "image/jpeg"),
                    "size_bytes": int(row.get("size_bytes") or file_path.stat().st_size),
                    "bytes": file_path.read_bytes(),
                }
            )
        return items

    return {
        "text": str(payload.get("text") or ""),
        "source_name": str(payload.get("source_name") or ""),
        "source_names": [str(value or "") for value in payload.get("source_names") or []],
        "source_text": str(payload.get("source_text") or ""),
        "reference_images": load_images(list(payload.get("reference_images") or [])),
        "images": load_images(list(payload.get("images") or [])),
        "attachments": [
            {
                "filename": str(row.get("filename") or ""),
                "content_type": str(row.get("content_type") or "application/octet-stream"),
                "size_bytes": int(row.get("size_bytes") or 0),
                "preview_text": str(row.get("preview_text") or ""),
            }
            for row in list(payload.get("attachments") or [])
            if isinstance(row, dict)
        ],
        "sample": {
            "title": str((payload.get("sample") or {}).get("title") or ""),
            "lines": [str(line or "") for line in ((payload.get("sample") or {}).get("lines") or [])],
            "source_name": str((payload.get("sample") or {}).get("source_name") or ""),
            "kind": str((payload.get("sample") or {}).get("kind") or ""),
        },
    }


def _execute_work_report_batch_preview(job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    text_parts = [str(payload.get("text") or "").strip(), str(payload.get("source_text") or "").strip()]
    source_text = "\n".join(part for part in text_parts if part)

    def progress_callback(state: Dict[str, Any]) -> None:
        update_work_report_job_progress(
            job_id,
            current_step=int(state.get("current_step") or 0),
            total_steps=int(state.get("total_steps") or 5),
            summary=str(state.get("summary") or ""),
            hint=str(state.get("hint") or ""),
        )

    progress_callback(
        {
            "current_step": 0,
            "total_steps": 5,
            "summary": "업로드된 원문과 사진을 배치 작업으로 불러오고 있습니다.",
            "hint": "대량 이미지는 서버 작업 폴더에서 순차적으로 다시 읽습니다.",
        }
    )
    report = analyze_work_report(
        source_text,
        image_inputs=list(payload.get("images") or []),
        reference_image_inputs=list(payload.get("reference_images") or []),
        attachment_inputs=list(payload.get("attachments") or []),
        sample_title=str((payload.get("sample") or {}).get("title") or "").strip(),
        sample_lines=[str(line or "") for line in ((payload.get("sample") or {}).get("lines") or [])],
        progress_callback=progress_callback,
    )
    report["template_source_name"] = str((payload.get("sample") or {}).get("source_name") or "").strip()
    report["template_kind"] = str((payload.get("sample") or {}).get("kind") or "").strip()
    return report


async def _run_work_report_batch_preview(job_id: str) -> None:
    record = get_work_report_job_record(job_id)
    if not record:
        return
    mark_work_report_job_running(
        job_id,
        current_step=0,
        total_steps=5,
        summary="업무보고 미리보기 배치 작업을 시작했습니다.",
        hint="원문과 사진 수에 따라 몇 분 정도 걸릴 수 있습니다.",
    )
    try:
        payload = await run_in_threadpool(_read_work_report_batch_payload, Path(str(record.get("job_dir") or "")))
        report = await run_in_threadpool(_execute_work_report_batch_preview, job_id, payload)
        complete_work_report_job(job_id, result=report)
        result_reason = str(report.get("analysis_reason") or "").strip()
        result_model = str(report.get("analysis_model") or "").strip()
        log_message = "work report batch preview completed"
        if result_reason or result_model == "heuristic":
            logger.warning(
                "%s: job_id=%s model=%s reason=%s items=%s",
                log_message,
                job_id,
                result_model or "-",
                result_reason or "-",
                int(report.get("item_count") or 0),
            )
        else:
            logger.info(
                "%s: job_id=%s model=%s items=%s",
                log_message,
                job_id,
                result_model or "-",
                int(report.get("item_count") or 0),
            )
    except Exception as exc:
        logger.exception("work report batch preview failed: job_id=%s", job_id)
        fail_work_report_job(
            job_id,
            error_message=str(exc),
            summary="업무보고 미리보기 배치 작업이 실패했습니다.",
            hint="같은 입력으로 다시 시도해 주세요. 반복되면 최근 입력 묶음을 알려 주세요.",
        )


def _track_work_report_batch_task(task: asyncio.Task[Any]) -> None:
    WORK_REPORT_BATCH_TASKS.add(task)
    task.add_done_callback(lambda done: WORK_REPORT_BATCH_TASKS.discard(done))


def _spawn_work_report_batch_preview(job_id: str) -> None:
    task = asyncio.create_task(_run_work_report_batch_preview(job_id))
    _track_work_report_batch_task(task)


def _authorized_work_report_job(request: Request, job_id: str) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    record = get_work_report_job_record(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="work report job not found")
    user, tenant = _resolve_context(request)
    job_tenant_id = str(record.get("tenant_id") or "").strip().lower()
    if tenant and str(tenant.get("id") or "").strip().lower() != job_tenant_id:
        raise HTTPException(status_code=404, detail="work report job not found")
    if user and int(user.get("is_admin") or 0) != 1:
        if str(user.get("tenant_id") or "").strip().lower() != job_tenant_id:
            raise HTTPException(status_code=404, detail="work report job not found")
    return record, user, tenant


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


@router.post("/ai/kakao_digest/pdf")
async def ai_kakao_digest_pdf(
    request: Request,
    tenant_id: str = Form(default=""),
    text: str = Form(default=""),
    files: List[UploadFile] = File(default=[]),
) -> StreamingResponse:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    source_text = str(text or "").strip()
    image_inputs = await _read_digest_images(list(files or []))
    if not source_text and not image_inputs:
        raise HTTPException(status_code=400, detail="text or image is required")
    try:
        digest = analyze_chat_digest(source_text, image_inputs=image_inputs)
        pdf_bytes = build_kakao_digest_pdf(
            digest=digest,
            tenant_label=_tenant_label(resolved_tenant_id, tenant),
            source_text=source_text,
            image_inputs=image_inputs,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(resolved_tenant_id, "ai.kakao_digest.pdf")
    append_audit_log(
        resolved_tenant_id,
        "ai_kakao_digest_pdf",
        _actor_label(user, tenant),
        {"lines": len(source_text.splitlines()), "images": len(image_inputs)},
    )
    file_name = f"kakao-digest-{_download_name(resolved_tenant_id)}.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
    return StreamingResponse(iter([pdf_bytes]), media_type="application/pdf", headers=headers)


@router.post("/ai/work_report")
async def ai_work_report(
    request: Request,
    tenant_id: str = Form(default=""),
    text: str = Form(default=""),
    source_file: UploadFile | None = File(default=None),
    source_files: List[UploadFile] = File(default=[]),
    images: List[UploadFile] = File(default=[]),
    attachments: List[UploadFile] = File(default=[]),
    sample_file: UploadFile | None = File(default=None),
) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    merged_source_files = ([source_file] if source_file else []) + list(source_files or [])
    source = await _read_work_report_sources(merged_source_files)
    source_text_parts = [str(text or "").strip(), str(source.get("source_text") or "").strip()]
    source_text = "\n".join(part for part in source_text_parts if part)
    image_inputs = await _read_work_report_images(list(images or []))
    reference_image_inputs = list(source.get("source_images") or [])
    if len(image_inputs) > MAX_WORK_REPORT_IMAGES:
        raise HTTPException(status_code=400, detail=f"현장 사진은 최대 {MAX_WORK_REPORT_IMAGES}장까지 업로드할 수 있습니다.")
    attachment_inputs = await _read_work_report_attachments(list(attachments or []))
    sample = await _read_work_report_sample(sample_file)
    if not source_text and not image_inputs and not reference_image_inputs and not attachment_inputs:
        raise HTTPException(status_code=400, detail="text, image, or attachment is required")
    try:
        item = await run_in_threadpool(
            analyze_work_report,
            source_text,
            image_inputs=image_inputs,
            reference_image_inputs=reference_image_inputs,
            attachment_inputs=attachment_inputs,
            sample_title=str(sample.get("title") or "").strip(),
            sample_lines=[str(line or "") for line in sample.get("lines") or []],
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    item["template_source_name"] = str(sample.get("source_name") or "").strip()
    item["template_kind"] = str(sample.get("kind") or "").strip()
    log_usage(resolved_tenant_id, "ai.work_report")
    append_audit_log(
        resolved_tenant_id,
        "ai_work_report",
        _actor_label(user, tenant),
        {
            "lines": len(source_text.splitlines()),
            "images": len(image_inputs),
            "reference_images": len(reference_image_inputs),
            "attachments": len(attachment_inputs),
            "source_file": str(source.get("source_name") or ""),
            "source_file_count": len(source.get("source_names") or []),
            "sample": str(sample.get("source_name") or ""),
        },
    )
    return {"ok": True, "item": item}


@router.post("/ai/work_report/jobs")
async def ai_work_report_job_create(
    request: Request,
    tenant_id: str = Form(default=""),
    text: str = Form(default=""),
    source_file: UploadFile | None = File(default=None),
    source_files: List[UploadFile] = File(default=[]),
    images: List[UploadFile] = File(default=[]),
    attachments: List[UploadFile] = File(default=[]),
    sample_file: UploadFile | None = File(default=None),
) -> Dict[str, Any]:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    merged_source_files = ([source_file] if source_file else []) + list(source_files or [])
    source = await _read_work_report_sources(merged_source_files)
    source_text_parts = [str(text or "").strip(), str(source.get("source_text") or "").strip()]
    source_text = "\n".join(part for part in source_text_parts if part)
    image_inputs = await _read_work_report_images(list(images or []))
    reference_image_inputs = list(source.get("source_images") or [])
    if len(image_inputs) > MAX_WORK_REPORT_IMAGES:
        raise HTTPException(status_code=400, detail=f"현장 사진은 최대 {MAX_WORK_REPORT_IMAGES}장까지 업로드할 수 있습니다.")
    attachment_inputs = await _read_work_report_attachments(list(attachments or []))
    sample = await _read_work_report_sample(sample_file)
    if not source_text and not image_inputs and not reference_image_inputs and not attachment_inputs:
        raise HTTPException(status_code=400, detail="text, image, or attachment is required")

    job_id = new_work_report_job_id()
    job_dir = build_work_report_job_dir(resolved_tenant_id, job_id)
    try:
        _write_work_report_batch_payload(
            job_dir,
            text=str(text or ""),
            source=source,
            image_inputs=image_inputs,
            attachment_inputs=attachment_inputs,
            sample=sample,
        )
        item = create_work_report_job(
            job_id=job_id,
            tenant_id=resolved_tenant_id,
            actor_label=_actor_label(user, tenant),
            job_dir=job_dir,
            source_file_count=len(source.get("source_names") or []),
            image_count=len(image_inputs),
            reference_image_count=len(reference_image_inputs),
            attachment_count=len(attachment_inputs),
        )
    except ValueError as exc:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        shutil.rmtree(job_dir, ignore_errors=True)
        raise

    log_usage(resolved_tenant_id, "ai.work_report.batch")
    append_audit_log(
        resolved_tenant_id,
        "ai_work_report_batch_create",
        _actor_label(user, tenant),
        {
            "job_id": job_id,
            "lines": len(source_text.splitlines()),
            "images": len(image_inputs),
            "reference_images": len(reference_image_inputs),
            "attachments": len(attachment_inputs),
            "source_file": str(source.get("source_name") or ""),
            "source_file_count": len(source.get("source_names") or []),
            "sample": str(sample.get("source_name") or ""),
        },
    )
    try:
        _spawn_work_report_batch_preview(job_id)
    except Exception as exc:
        logger.exception("failed to spawn work report batch preview: job_id=%s", job_id)
        fail_work_report_job(
            job_id,
            error_message=str(exc),
            summary="업무보고 미리보기 배치 작업을 시작하지 못했습니다.",
            hint="잠시 후 다시 시도해 주세요.",
        )
        item = get_work_report_job(job_id) or item
    return {"ok": True, "item": item}


@router.get("/ai/work_report/jobs/{job_id}")
def ai_work_report_job_detail(request: Request, job_id: str) -> Dict[str, Any]:
    record, _user, _tenant = _authorized_work_report_job(request, job_id)
    item = get_work_report_job(str(record.get("id") or ""), include_result=True)
    if not item:
        raise HTTPException(status_code=404, detail="work report job not found")
    return {"ok": True, "item": item}


@router.post("/ai/work_report/pdf")
async def ai_work_report_pdf(
    request: Request,
    tenant_id: str = Form(default=""),
    text: str = Form(default=""),
    source_file: UploadFile | None = File(default=None),
    source_files: List[UploadFile] = File(default=[]),
    images: List[UploadFile] = File(default=[]),
    attachments: List[UploadFile] = File(default=[]),
    sample_file: UploadFile | None = File(default=None),
    report_json: str = Form(default=""),
) -> StreamingResponse:
    payload = {"tenant_id": tenant_id}
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    merged_source_files = ([source_file] if source_file else []) + list(source_files or [])
    source = await _read_work_report_sources(merged_source_files)
    source_text_parts = [str(text or "").strip(), str(source.get("source_text") or "").strip()]
    source_text = "\n".join(part for part in source_text_parts if part)
    image_inputs = await _read_work_report_images(list(images or []))
    reference_image_inputs = list(source.get("source_images") or [])
    if len(image_inputs) > MAX_WORK_REPORT_IMAGES:
        raise HTTPException(status_code=400, detail=f"현장 사진은 최대 {MAX_WORK_REPORT_IMAGES}장까지 업로드할 수 있습니다.")
    attachment_inputs = await _read_work_report_attachments(list(attachments or []))
    sample = await _read_work_report_sample(sample_file)
    if not source_text and not image_inputs and not reference_image_inputs and not attachment_inputs:
        raise HTTPException(status_code=400, detail="text, image, or attachment is required")
    try:
        cached_report = None
        if str(report_json or "").strip():
            try:
                import json

                parsed_report = json.loads(str(report_json or ""))
            except Exception as exc:
                raise HTTPException(status_code=400, detail="report_json 형식이 잘못되었습니다.") from exc
            if not isinstance(parsed_report, dict):
                raise HTTPException(status_code=400, detail="report_json 형식이 잘못되었습니다.")
            cached_report = parsed_report
        report = cached_report or await run_in_threadpool(
            analyze_work_report,
            source_text,
            image_inputs=image_inputs,
            reference_image_inputs=reference_image_inputs,
            attachment_inputs=attachment_inputs,
            sample_title=str(sample.get("title") or "").strip(),
            sample_lines=[str(line or "") for line in sample.get("lines") or []],
        )
        pdf_bytes = await run_in_threadpool(
            build_work_report_pdf,
            report=report,
            tenant_label=_tenant_label(resolved_tenant_id, tenant),
            source_text=source_text,
            image_inputs=image_inputs,
            attachment_inputs=attachment_inputs,
            template_source_name=str(sample.get("source_name") or "").strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    log_usage(resolved_tenant_id, "ai.work_report.pdf")
    append_audit_log(
        resolved_tenant_id,
        "ai_work_report_pdf",
        _actor_label(user, tenant),
        {
            "lines": len(source_text.splitlines()),
            "images": len(image_inputs),
            "reference_images": len(reference_image_inputs),
            "attachments": len(attachment_inputs),
            "source_file": str(source.get("source_name") or ""),
            "source_file_count": len(source.get("source_names") or []),
            "sample": str(sample.get("source_name") or ""),
        },
    )
    file_name = f"work-report-{_download_name(resolved_tenant_id)}.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
    return StreamingResponse(iter([pdf_bytes]), media_type="application/pdf", headers=headers)


@router.post("/ai/work_report/feedback")
def ai_work_report_feedback(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    resolved_tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    corrections_raw = payload.get("corrections") or []
    report_raw = payload.get("report") or {}
    if corrections_raw and not isinstance(corrections_raw, list):
        raise HTTPException(status_code=400, detail="corrections 형식이 잘못되었습니다.")
    if report_raw and not isinstance(report_raw, dict):
        raise HTTPException(status_code=400, detail="report 형식이 잘못되었습니다.")

    corrections: List[Dict[str, Any]] = []
    for row in list(corrections_raw or [])[:500]:
        if not isinstance(row, dict):
            continue
        corrections.append(
            {
                "image_index": int(row.get("image_index") or 0),
                "filename": str(row.get("filename") or "").strip()[:240],
                "from_item_index": int(row.get("from_item_index") or 0),
                "from_item_title": str(row.get("from_item_title") or "").strip()[:240],
                "to_item_index": int(row.get("to_item_index") or 0),
                "to_item_title": str(row.get("to_item_title") or "").strip()[:240],
                "from_stage": str(row.get("from_stage") or "").strip()[:40],
                "from_stage_label": str(row.get("from_stage_label") or "").strip()[:80],
                "to_stage": str(row.get("to_stage") or "").strip()[:40],
                "to_stage_label": str(row.get("to_stage_label") or "").strip()[:80],
            }
        )

    report = dict(report_raw or {})
    report_items = list(report.get("items") or []) if isinstance(report.get("items"), list) else []
    unmatched_images = list(report.get("unmatched_images") or []) if isinstance(report.get("unmatched_images"), list) else []
    report_summary = {
        "report_title": str(report.get("report_title") or "").strip()[:160],
        "period_label": str(report.get("period_label") or "").strip()[:120],
        "analysis_model": str(report.get("analysis_model") or "").strip()[:80],
        "analysis_reason": str(report.get("analysis_reason") or "").strip()[:80],
        "item_count": len(report_items),
        "image_item_count": sum(1 for item in report_items if isinstance(item, dict) and list(item.get("images") or [])),
        "unmatched_image_count": len(unmatched_images),
        "items": [
            {
                "index": int(item.get("index") or 0),
                "title": str(item.get("title") or "").strip()[:240],
                "summary": str(item.get("summary") or "").strip()[:320],
                "images": [
                    {
                        "index": int(image.get("index") or 0),
                        "filename": str(image.get("filename") or "").strip()[:240],
                        "stage": str(image.get("stage") or "").strip()[:40],
                        "stage_label": str(image.get("stage_label") or "").strip()[:80],
                    }
                    for image in list(item.get("images") or [])[:50]
                    if isinstance(image, dict)
                ],
            }
            for item in report_items[:200]
            if isinstance(item, dict)
        ],
        "unmatched_images": [
            {
                "index": int(image.get("index") or 0),
                "filename": str(image.get("filename") or "").strip()[:240],
                "stage": str(image.get("stage") or "").strip()[:40],
                "stage_label": str(image.get("stage_label") or "").strip()[:80],
            }
            for image in unmatched_images[:100]
            if isinstance(image, dict)
        ],
    }
    append_audit_log(
        resolved_tenant_id,
        "ai_work_report_feedback",
        _actor_label(user, tenant),
        {
            "job_id": str(payload.get("job_id") or "").strip()[:80],
            "correction_count": len(corrections),
            "corrections": corrections,
            "report": report_summary,
        },
    )
    return {
        "ok": True,
        "item": {
            "job_id": str(payload.get("job_id") or "").strip()[:80],
            "correction_count": len(corrections),
            "saved": True,
        },
    }


@router.post("/ai/kakao_digest/import")
def ai_kakao_digest_import(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    tenant_id, user, tenant = _tenant_id_from_request(request, payload)
    rows = payload.get("rows") or []
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="rows is required")
    if len(rows) > 100:
        raise HTTPException(status_code=400, detail="한 번에 최대 100건까지 등록할 수 있습니다.")

    actor = user or ensure_service_user(tenant_id)
    source_text = str(payload.get("source_text") or "").strip()
    ai_model = str(payload.get("image_analysis_model") or payload.get("ai_model") or "kakao-digest").strip()
    channel = str(payload.get("channel") or "카톡").strip() or "카톡"
    created_items: List[Dict[str, Any]] = []

    try:
        for raw_row in rows:
            if not isinstance(raw_row, dict):
                raise HTTPException(status_code=400, detail="rows must contain objects")
            building = _as_row_text(raw_row, "building")
            unit = _as_row_text(raw_row, "unit")
            complaint_type = _as_row_text(raw_row, "type", "기타") or "기타"
            summary = normalize_summary_text(
                _as_row_text(raw_row, "summary"),
                building=building,
                unit=unit,
                complaint_type=complaint_type,
            )
            item = create_complaint(
                tenant_id=tenant_id,
                building=building,
                unit=unit,
                complainant_phone=_as_row_text(raw_row, "complainant_phone"),
                channel=channel,
                content=_as_row_text(raw_row, "content") or summary,
                summary=summary,
                complaint_type=complaint_type,
                urgency=_as_row_text(raw_row, "urgency", "일반") or "일반",
                status=_as_row_text(raw_row, "status", "접수") or "접수",
                manager=_as_row_text(raw_row, "manager"),
                source_text=source_text,
                ai_model=ai_model,
                created_by_user_id=int(actor.get("id")) if actor and actor.get("id") else None,
                created_by_label=_actor_label(user, tenant),
            )
            created_items.append(item)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    log_usage(tenant_id, "ai.kakao_digest.import")
    append_audit_log(
        tenant_id,
        "ai_kakao_digest_import",
        _actor_label(user, tenant),
        {"count": len(created_items), "source_text_lines": len(source_text.splitlines())},
    )
    return {"ok": True, "created_count": len(created_items), "items": created_items}


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
    if not _can_delete_complaint(user):
        raise HTTPException(status_code=403, detail="관리자 권한으로만 민원을 삭제할 수 있습니다.")
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
