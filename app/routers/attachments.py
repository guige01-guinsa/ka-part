# app/routers/attachments.py
import os
import uuid
from pathlib import Path
from typing import Optional, Dict, Set, Any

from fastapi import APIRouter, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import FileResponse

from app.auth import get_current_user
from app.db import db_conn


router = APIRouter(prefix="/api", tags=["attachments"])

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _safe_int_env(name: str, default: int, minimum: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except Exception:
        value = int(default)
    return max(minimum, value)


ATTACHMENT_MAX_BYTES = _safe_int_env("ATTACHMENT_MAX_BYTES", 20 * 1024 * 1024, 256 * 1024)
ATTACHMENT_ALLOWED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".pdf",
    ".xlsx", ".xls", ".docx", ".doc", ".pptx", ".ppt",
    ".csv", ".txt", ".zip", ".hwp", ".hwpx",
}
ATTACHMENT_BLOCKED_EXTENSIONS = {
    ".exe", ".dll", ".bat", ".cmd", ".com", ".scr", ".msi",
    ".ps1", ".vbs", ".js", ".jar", ".sh", ".php", ".asp",
    ".aspx", ".jsp", ".py", ".pl", ".rb",
}
ATTACHMENT_BLOCKED_MIME_TYPES = {
    "application/x-msdownload",
    "application/x-dosexec",
    "application/x-msdos-program",
    "application/x-sh",
    "text/html",
    "application/javascript",
    "text/javascript",
}


# -------------------------
# helpers
# -------------------------
def _table_cols(db, table: str) -> Set[str]:
    cur = db.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    return {r[1] for r in rows}  # name


def _get_user_id(db, login: str) -> Optional[int]:
    cur = db.execute("SELECT id FROM users WHERE login=?", (login,))
    r = cur.fetchone()
    return int(r["id"]) if r else None


def _get_work_status(db, work_id: int) -> Optional[str]:
    cur = db.execute("SELECT status FROM work_orders WHERE id=?", (work_id,))
    r = cur.fetchone()
    return r["status"] if r else None


def _add_event(
    db,
    entity_type: str,
    entity_id: int,
    event_type: str,
    note: str = "",
    from_status: str = "",
    to_status: str = "",
    actor_login: str = "",
    actor_id: Optional[int] = None,
):
    """
    events.actor_id NOT NULL 대응:
    - get_current_user()가 id를 주면 그걸 사용
    - 없으면 login으로 users.id 조회
    """
    cols = _table_cols(db, "events")

    if "actor_id" in cols:
        if actor_id is None and actor_login:
            actor_id = _get_user_id(db, actor_login)
        if actor_id is None:
            # 마지막 방어: 시스템 사용자(=1) 같은 게 없을 수 있으니 에러로 명확히
            raise HTTPException(status_code=500, detail="events.actor_id required but actor not resolved")

    fields = ["entity_type", "entity_id", "event_type", "note", "from_status", "to_status"]
    vals = [entity_type, entity_id, event_type, note, from_status, to_status]

    if "actor_login" in cols:
        fields.append("actor_login")
        vals.append(actor_login or "")

    if "actor_id" in cols:
        fields.append("actor_id")
        vals.append(actor_id)

    # created_at 컬럼이 있다면 DB default를 쓰는 편이 안전하지만, 없을 수도 있으니 생략
    sql = f"INSERT INTO events ({', '.join(fields)}) VALUES ({', '.join(['?'] * len(fields))})"
    db.execute(sql, tuple(vals))


def _safe_filename(name: str) -> str:
    # 윈도우/경로문자 제거
    name = name.replace("\\", "_").replace("/", "_").replace("..", "_")
    return name.strip() or "file.bin"


def _file_ext(name: str) -> str:
    ext = Path(str(name or "")).suffix.lower().strip()
    if ext == ".jfif":
        return ".jpg"
    return ext


async def _validate_attachment_upload(file: UploadFile) -> tuple[str, bytes]:
    ext = _file_ext(file.filename or "")
    if not ext:
        raise HTTPException(status_code=400, detail="파일 확장자가 필요합니다.")
    if ext in ATTACHMENT_BLOCKED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"보안상 허용되지 않는 확장자입니다: {ext}")
    if ATTACHMENT_ALLOWED_EXTENSIONS and ext not in ATTACHMENT_ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"허용되지 않는 파일 확장자입니다: {ext}")

    ctype = str(file.content_type or "").split(";", 1)[0].strip().lower()
    if ctype and ctype in ATTACHMENT_BLOCKED_MIME_TYPES:
        raise HTTPException(status_code=400, detail=f"허용되지 않는 파일 형식입니다: {ctype}")

    content = await file.read(ATTACHMENT_MAX_BYTES + 1)
    if not content:
        raise HTTPException(status_code=400, detail="업로드 파일이 비어 있습니다.")
    if len(content) > ATTACHMENT_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"첨부파일 용량 제한({ATTACHMENT_MAX_BYTES} bytes)을 초과했습니다.",
        )

    if ext in {".jpg", ".jpeg"} and not content.startswith(b"\xff\xd8\xff"):
        raise HTTPException(status_code=400, detail="JPG 파일 헤더가 올바르지 않습니다.")
    if ext == ".png" and not content.startswith(b"\x89PNG\r\n\x1a\n"):
        raise HTTPException(status_code=400, detail="PNG 파일 헤더가 올바르지 않습니다.")
    if ext == ".webp" and not (len(content) >= 12 and content[:4] == b"RIFF" and content[8:12] == b"WEBP"):
        raise HTTPException(status_code=400, detail="WEBP 파일 헤더가 올바르지 않습니다.")
    if ext == ".pdf" and not content.startswith(b"%PDF-"):
        raise HTTPException(status_code=400, detail="PDF 파일 헤더가 올바르지 않습니다.")

    return ext, content


# -------------------------
# API
# -------------------------
@router.get("/attachments")
async def attachments_list(
    request: Request,
    entity_type: str,
    entity_id: int,
):
    """
    첨부 목록 표준화:
    GET /api/attachments?entity_type=WORK_ORDER&entity_id=1
    - 소프트삭제(deleted_at) 있으면 제외
    - created_by/created_at 등 컬럼은 PRAGMA로 감지 후 선택
    """
    user = get_current_user(request)

    with db_conn() as db:
        if entity_type == "WORK_ORDER":
            # 권한 체크: 입주민/외주업체는 자기 건만
            cur = db.execute(
                "SELECT requested_by, vendor_id FROM work_orders WHERE id=?",
                (entity_id,),
            )
            w = cur.fetchone()
            if not w:
                raise HTTPException(status_code=404, detail="Work not found")
            if user.get("roles"):
                roles = set(user.get("roles") or [])
                is_resident = "RESIDENT" in roles or "입주민" in roles
                is_vendor = "VENDOR" in roles or "외주업체" in roles
                if is_resident and int(w["requested_by"]) != int(user["id"]):
                    raise HTTPException(status_code=403, detail="forbidden")
                if is_vendor:
                    if not user.get("vendor_id") or int(w.get("vendor_id") or 0) != int(user["vendor_id"]):
                        raise HTTPException(status_code=403, detail="forbidden")

        cols = _table_cols(db, "attachments")

        base = ["id", "entity_type", "entity_id", "file_name", "file_path"]
        optional = ["mime_type", "created_at", "created_by", "deleted_at", "deleted_by"]

        select_cols = [c for c in base if c in cols]
        # normalize created_at/created_by from uploaded_* columns when needed
        if "created_at" in cols:
            select_cols.append("created_at")
        elif "uploaded_at" in cols:
            select_cols.append("uploaded_at AS created_at")

        if "created_by" in cols:
            select_cols.append("created_by")
        elif "uploaded_by" in cols:
            select_cols.append("uploaded_by AS created_by")

        for c in ("mime_type", "deleted_at", "deleted_by"):
            if c in cols:
                select_cols.append(c)

        where = "WHERE entity_type=? AND entity_id=?"
        if "deleted_at" in cols:
            where += " AND (deleted_at IS NULL OR deleted_at='')"

        order_by = "ORDER BY id DESC"
        if "created_at" in cols:
            order_by = "ORDER BY created_at DESC, id DESC"
        elif "uploaded_at" in cols:
            order_by = "ORDER BY uploaded_at DESC, id DESC"

        sql = f"""
            SELECT {", ".join(select_cols)}
            FROM attachments
            {where}
            {order_by}
        """
        cur = db.execute(sql, (entity_type, entity_id))
        rows = cur.fetchall()
        items = [dict(r) for r in rows]

        # 프론트가 기대하는 키 기본값
        for it in items:
            it.setdefault("mime_type", None)
            it.setdefault("created_at", None)
            it.setdefault("created_by", None)
            it.setdefault("deleted_at", None)
            it.setdefault("deleted_by", None)

    return {"ok": True, "items": items}


@router.post("/attachments")
async def attachments_create(
    request: Request,
    entity_type: str = Form(...),
    entity_id: int = Form(...),
    file: UploadFile = File(...),
):
    """
    첨부 업로드 (multipart/form-data)
    - WORK_ORDER + DONE 상태: 관리자만 추가 가능
    """
    user = get_current_user(request)

    with db_conn() as db:
        # 권한: WORK_ORDER DONE 잠금
        if entity_type == "WORK_ORDER":
            st = _get_work_status(db, entity_id)
            if st == "DONE" and not user.get("is_admin"):
                raise HTTPException(
                    status_code=403,
                    detail="DONE 상태의 작업에는 관리자만 첨부를 추가할 수 있습니다."
                )

        cols = _table_cols(db, "attachments")

        ext, content = await _validate_attachment_upload(file)
        base_name = Path(file.filename or "upload.bin").stem
        fn = _safe_filename(f"{base_name}{ext}")
        uid = str(uuid.uuid4())
        save_name = f"{uid}__{fn}"
        save_path = UPLOAD_DIR / save_name

        # 저장
        save_path.write_bytes(content)

        # INSERT 구성(컬럼 유무 대응)
        fields = ["entity_type", "entity_id", "file_name", "file_path"]
        vals: list[Any] = [entity_type, entity_id, fn, str(save_path).replace("/", "\\")]

        if "mime_type" in cols:
            fields.append("mime_type")
            vals.append(file.content_type or "application/octet-stream")

        # created_at/created_by (legacy) or uploaded_at/uploaded_by (schema)
        if "created_at" in cols:
            fields.append("created_at")
            vals.append(None)
        if "uploaded_at" in cols:
            fields.append("uploaded_at")
            vals.append(None)

        if "created_by" in cols:
            fields.append("created_by")
            vals.append(user.get("id") or _get_user_id(db, user.get("login", "")) or 0)
        if "uploaded_by" in cols:
            fields.append("uploaded_by")
            vals.append(user.get("id") or _get_user_id(db, user.get("login", "")) or 0)

        # created_at/uploaded_at은 datetime('now')로 넣기 위해 placeholder 조정
        placeholders = []
        final_vals = []
        for f, v in zip(fields, vals):
            if f in ("created_at", "uploaded_at") and v is None:
                placeholders.append("datetime('now')")
            else:
                placeholders.append("?")
                final_vals.append(v)

        sql = f"INSERT INTO attachments ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
        cur = db.execute(sql, tuple(final_vals))
        db.commit()

        att_id = cur.lastrowid

        # 이벤트 기록(가능하면)
        try:
            _add_event(
                db,
                entity_type,
                int(entity_id),
                "ATTACH_ADD",
                note=f"attachment#{att_id}",
                actor_login=user.get("login", ""),
                actor_id=user.get("id"),
            )
            db.commit()
        except Exception:
            # 이벤트가 실패해도 업로드 자체는 성공 처리(운영상 더 실용적)
            pass

    return {
        "id": att_id,
        "file_name": fn,
        "file_path": str(save_path).replace("/", "\\"),
        "mime_type": file.content_type or "application/octet-stream",
    }


@router.delete("/attachments/{attachment_id}")
async def attachments_delete(request: Request, attachment_id: int):
    """
    첨부 삭제(권장: 소프트삭제)
    - WORK_ORDER + DONE 상태: 관리자만 삭제 가능
    - 소프트삭제 컬럼이 없으면 하드삭제로 fallback
    """
    user = get_current_user(request)

    with db_conn() as db:
        cols = _table_cols(db, "attachments")

        cur = db.execute(
            "SELECT id, entity_type, entity_id, file_path FROM attachments WHERE id=?",
            (attachment_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Attachment not found")

        entity_type = row["entity_type"]
        entity_id = int(row["entity_id"])

        # DONE 잠금 권한
        if entity_type == "WORK_ORDER":
            st = _get_work_status(db, entity_id)
            if st == "DONE" and not user.get("is_admin"):
                raise HTTPException(
                    status_code=403,
                    detail="DONE 상태의 작업에는 관리자만 첨부를 삭제할 수 있습니다."
                )

        # 소프트삭제 가능 여부
        if "deleted_at" in cols:
            sets = ["deleted_at=datetime('now')"]
            params: list[Any] = []

            if "deleted_by" in cols:
                sets.append("deleted_by=?")
                params.append(user.get("id") or _get_user_id(db, user.get("login", "")) or 0)

            sql = f"UPDATE attachments SET {', '.join(sets)} WHERE id=?"
            params.append(attachment_id)
            db.execute(sql, tuple(params))
        else:
            # fallback: 하드삭제
            db.execute("DELETE FROM attachments WHERE id=?", (attachment_id,))

        # 이벤트 기록
        try:
            _add_event(
                db,
                entity_type,
                entity_id,
                "ATTACH_DELETE",
                note=f"attachment#{attachment_id}",
                actor_login=user.get("login", ""),
                actor_id=user.get("id"),
            )
        except Exception:
            pass

        db.commit()

    return {"ok": True}


@router.get("/attachments/file/{attachment_id}")
async def attachments_file(request: Request, attachment_id: int):
    """
    (옵션) 서버에 저장된 첨부 파일 다운로드
    - 소프트삭제된 건 404 처리
    """
    user = get_current_user(request)

    with db_conn() as db:
        cols = _table_cols(db, "attachments")
        where = "WHERE id=?"
        if "deleted_at" in cols:
            where += " AND (deleted_at IS NULL OR deleted_at='')"

        cur = db.execute(
            f"SELECT id, entity_type, entity_id, file_name, file_path, mime_type FROM attachments {where}",
            (attachment_id,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Attachment not found")

    if row["entity_type"] == "WORK_ORDER":
        with db_conn() as db:
            cur = db.execute(
                "SELECT requested_by, vendor_id FROM work_orders WHERE id=?",
                (row["entity_id"],),
            )
            w = cur.fetchone()
        if not w:
            raise HTTPException(status_code=404, detail="Work not found")
        if user.get("roles"):
            roles = set(user.get("roles") or [])
            is_resident = "RESIDENT" in roles or "입주민" in roles
            is_vendor = "VENDOR" in roles or "외주업체" in roles
            if is_resident and int(w["requested_by"]) != int(user["id"]):
                raise HTTPException(status_code=403, detail="forbidden")
            if is_vendor:
                if not user.get("vendor_id") or int(w.get("vendor_id") or 0) != int(user["vendor_id"]):
                    raise HTTPException(status_code=403, detail="forbidden")

    fp = row["file_path"]
    # 경로 표준화
    p = Path(fp.replace("\\", os.sep))
    if not p.exists():
        raise HTTPException(status_code=404, detail="File missing on disk")

    row_dict = dict(row)
    media_type = row_dict.get("mime_type") or "application/octet-stream"
    filename = row_dict.get("file_name") or f"attachment_{attachment_id}"
    headers = {"Content-Disposition": f'inline; filename="{filename}"'}
    return FileResponse(
        str(p),
        media_type=media_type,
        filename=filename,
        headers=headers,
    )
