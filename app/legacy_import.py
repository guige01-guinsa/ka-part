from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from . import db as core_db
from .ai_service import classify_complaint_text, normalize_summary_text
from .db import (
    hash_password,
    now_iso,
)
from .engine_db import COMPLAINT_TYPES, STATUS_VALUES, URGENCY_VALUES
from .ops_db import (
    DOCUMENT_CATEGORY_VALUES,
    DOCUMENT_STATUS_VALUES,
    NOTICE_CATEGORY_VALUES,
    NOTICE_STATUS_VALUES,
    SCHEDULE_STATUS_VALUES,
    SCHEDULE_TYPE_VALUES,
    VENDOR_STATUS_VALUES,
)

LEGACY_TABLE_ALIASES: Dict[str, tuple[str, ...]] = {
    "users": ("staff_users", "users", "employees", "legacy_users"),
    "complaints": ("complaints", "complaint_items", "legacy_complaints", "minwon"),
    "notices": ("ops_notices", "notices", "announcements", "legacy_notices"),
    "documents": ("ops_documents", "documents", "docs", "legacy_documents"),
    "vendors": ("ops_vendors", "vendors", "contractors", "legacy_vendors"),
    "schedules": ("ops_schedules", "schedules", "tasks", "inspections", "legacy_schedules"),
}

FIELD_ALIASES: Dict[str, tuple[str, ...]] = {
    "login_id": ("login_id", "username", "user_id", "id_text"),
    "name": ("name", "full_name", "display_name"),
    "role": ("role", "user_role", "position"),
    "phone": ("phone", "mobile", "contact_phone", "tel"),
    "note": ("note", "memo", "remark", "description"),
    "is_active": ("is_active", "active", "enabled", "use_yn"),
    "is_site_admin": ("is_site_admin", "site_admin", "manager_flag"),
    "password": ("password", "raw_password", "initial_password", "temp_password"),
    "building": ("building", "dong", "building_no", "building_code"),
    "unit": ("unit", "ho", "unit_no", "room"),
    "complainant_phone": ("complainant_phone", "contact_phone", "phone", "caller_phone"),
    "channel": ("channel", "source_channel", "source"),
    "content": ("content", "body", "description", "text"),
    "summary": ("summary", "title", "subject"),
    "type": ("type", "complaint_type", "category"),
    "urgency": ("urgency", "priority"),
    "status": ("status", "progress_status"),
    "manager": ("manager", "owner", "assignee"),
    "image_url": ("image_url", "photo_url"),
    "source_text": ("source_text", "raw_text"),
    "ai_model": ("ai_model", "model"),
    "created_at": ("created_at", "created", "created_on", "reg_date"),
    "updated_at": ("updated_at", "updated", "updated_on", "mod_date"),
    "closed_at": ("closed_at", "completed_at"),
    "title": ("title", "subject"),
    "body": ("body", "content", "description", "text"),
    "category": ("category", "type"),
    "pinned": ("pinned", "is_pinned", "top_fixed"),
    "owner": ("owner", "manager", "assignee"),
    "due_date": ("due_date", "target_date", "scheduled_date"),
    "reference_no": ("reference_no", "doc_no", "reference"),
    "company_name": ("company_name", "vendor_name", "company"),
    "service_type": ("service_type", "service", "work_type"),
    "contact_name": ("contact_name", "manager_name", "contact"),
    "email": ("email", "mail"),
    "schedule_type": ("schedule_type", "type", "category"),
    "vendor_name": ("vendor_name", "company_name", "vendor"),
    "vendor_service_type": ("vendor_service_type", "service_type"),
}

ROLE_MAP = {
    "desk": "desk",
    "manager": "manager",
    "staff": "staff",
    "vendor": "vendor",
    "reader": "reader",
    "integration": "integration",
    "admin": "manager",
    "operator": "staff",
    "employee": "staff",
}

COMPLAINT_TYPE_MAP = {
    "주차": "주차",
    "parking": "주차",
    "소음": "소음",
    "noise": "소음",
    "승강기": "승강기",
    "elevator": "승강기",
    "전기": "전기",
    "electric": "전기",
    "수도": "수도",
    "water": "수도",
    "누수": "누수",
    "leak": "누수",
    "시설": "시설",
    "facility": "시설",
    "미화": "미화",
    "cleaning": "미화",
    "경비": "경비",
    "security": "경비",
    "관리비": "관리비",
    "fee": "관리비",
    "기타": "기타",
    "other": "기타",
}

URGENCY_MAP = {
    "긴급": "긴급",
    "urgent": "긴급",
    "high": "긴급",
    "당일": "당일",
    "today": "당일",
    "normal": "일반",
    "일반": "일반",
    "단순문의": "단순문의",
    "inquiry": "단순문의",
}

STATUS_MAP = {
    "접수": "접수",
    "received": "접수",
    "open": "접수",
    "처리중": "처리중",
    "in_progress": "처리중",
    "processing": "처리중",
    "완료": "완료",
    "done": "완료",
    "closed": "완료",
    "이월": "이월",
    "carry": "이월",
    "deferred": "이월",
}

NOTICE_STATUS_MAP = {
    "draft": "draft",
    "임시": "draft",
    "published": "published",
    "게시": "published",
    "게시중": "published",
    "archived": "archived",
    "보관": "archived",
}

DOCUMENT_STATUS_MAP = {
    "작성중": "작성중",
    "draft": "작성중",
    "검토중": "검토중",
    "review": "검토중",
    "완료": "완료",
    "done": "완료",
    "보관": "보관",
    "archived": "보관",
}

SCHEDULE_STATUS_MAP = {
    "예정": "예정",
    "planned": "예정",
    "진행중": "진행중",
    "in_progress": "진행중",
    "완료": "완료",
    "done": "완료",
    "보류": "보류",
    "hold": "보류",
}

BOOL_TRUE = {"1", "true", "yes", "y", "on", "활성", "사용", "예"}


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(core_db.DB_PATH), timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con


def _clean_text(value: Any, max_len: int = 4000) -> str:
    text = str(value or "").strip()
    return text[:max_len]


def _pick(row: Dict[str, Any], field: str, default: Any = "") -> Any:
    aliases = FIELD_ALIASES.get(field, (field,))
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for alias in aliases:
        if alias.lower() in lowered:
            return lowered[alias.lower()]
    return default


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in BOOL_TRUE


def _normalize_choice(value: Any, *, allowed: Iterable[str], mapping: Optional[Dict[str, str]] = None, default: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return default
    if raw in allowed:
        return raw
    lowered = raw.lower()
    normalized = (mapping or {}).get(raw) or (mapping or {}).get(lowered)
    if normalized in allowed:
        return normalized
    return default


def _normalize_role(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return ROLE_MAP.get(raw, "staff")


def _normalize_timestamp(value: Any) -> str:
    raw = str(value or "").strip()
    return raw or now_iso()


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json_bundle(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("JSON source must be an object")
    return data


def _sqlite_rows(path: Path, table_names: tuple[str, ...]) -> List[Dict[str, Any]]:
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    try:
        names = {str(row[0]) for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        for table_name in table_names:
            if table_name in names:
                rows = con.execute(f"SELECT * FROM {table_name}").fetchall()
                return [dict(row) for row in rows]
        return []
    finally:
        con.close()


def load_legacy_source(source_path: str | Path) -> Dict[str, Any]:
    path = Path(source_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"legacy source not found: {path}")

    if path.is_file() and path.suffix.lower() == ".json":
        data = _read_json_bundle(path)
        return {
            "tenant": data.get("tenant") or {},
            "users": list(data.get("users") or []),
            "complaints": list(data.get("complaints") or []),
            "notices": list(data.get("notices") or []),
            "documents": list(data.get("documents") or []),
            "vendors": list(data.get("vendors") or []),
            "schedules": list(data.get("schedules") or []),
        }

    if path.is_dir():
        def rows(name: str) -> List[Dict[str, Any]]:
            target = path / f"{name}.csv"
            return _read_csv_rows(target) if target.exists() else []

        return {
            "tenant": {},
            "users": rows("users"),
            "complaints": rows("complaints"),
            "notices": rows("notices"),
            "documents": rows("documents"),
            "vendors": rows("vendors"),
            "schedules": rows("schedules"),
        }

    if path.is_file() and path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
        return {
            "tenant": {},
            "users": _sqlite_rows(path, LEGACY_TABLE_ALIASES["users"]),
            "complaints": _sqlite_rows(path, LEGACY_TABLE_ALIASES["complaints"]),
            "notices": _sqlite_rows(path, LEGACY_TABLE_ALIASES["notices"]),
            "documents": _sqlite_rows(path, LEGACY_TABLE_ALIASES["documents"]),
            "vendors": _sqlite_rows(path, LEGACY_TABLE_ALIASES["vendors"]),
            "schedules": _sqlite_rows(path, LEGACY_TABLE_ALIASES["schedules"]),
        }

    raise ValueError(f"unsupported legacy source: {path}")


def _ensure_tenant(con: sqlite3.Connection, tenant_id: str, tenant_name: str, site_code: str = "", site_name: str = "") -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT id, name, site_code, site_name, status, created_at, updated_at, last_used_at
        FROM tenants
        WHERE id=?
        LIMIT 1
        """,
        (tenant_id,),
    ).fetchone()
    ts = now_iso()
    clean_site_code = _clean_text(site_code, 32) or None
    clean_site_name = _clean_text(site_name, 120) or None
    if row:
        con.execute(
            """
            UPDATE tenants
            SET name=?, site_code=?, site_name=?, status='active', updated_at=?
            WHERE id=?
            """,
            (tenant_name, clean_site_code, clean_site_name, ts, tenant_id),
        )
        fresh = con.execute(
            """
            SELECT id, name, site_code, site_name, status, created_at, updated_at, last_used_at
            FROM tenants
            WHERE id=?
            LIMIT 1
            """,
            (tenant_id,),
        ).fetchone()
        return dict(fresh) if fresh else {"id": tenant_id, "name": tenant_name}

    api_key = core_db._generate_api_key()
    con.execute(
        """
        INSERT INTO tenants(id, name, site_code, site_name, api_key_hash, status, created_at, updated_at, last_used_at)
        VALUES(?,?,?,?,?,'active',?,?,NULL)
        """,
        (
            tenant_id,
            tenant_name,
            clean_site_code,
            clean_site_name,
            core_db._hash_api_key(api_key),
            ts,
            ts,
        ),
    )
    if clean_site_code or clean_site_name:
        core_db._ensure_site(con, site_code=clean_site_code, site_name=clean_site_name)
    fresh = con.execute(
        """
        SELECT id, name, site_code, site_name, status, created_at, updated_at, last_used_at
        FROM tenants
        WHERE id=?
        LIMIT 1
        """,
        (tenant_id,),
    ).fetchone()
    out = dict(fresh) if fresh else {"id": tenant_id, "name": tenant_name}
    out["api_key"] = api_key
    return out


def _find_existing_row_id(con: sqlite3.Connection, table: str, where: Dict[str, Any]) -> Optional[int]:
    pairs = [(key, value) for key, value in where.items() if value not in (None, "")]
    if not pairs:
        return None
    sql = f"SELECT id FROM {table} WHERE " + " AND ".join(f"{key}=?" for key, _ in pairs) + " LIMIT 1"
    row = con.execute(sql, tuple(value for _, value in pairs)).fetchone()
    return int(row["id"]) if row else None


def _apply_timestamps(con: sqlite3.Connection, table: str, row_id: int, created_at: str = "", updated_at: str = "", closed_at: str = "") -> None:
    fields: List[str] = []
    params: List[Any] = []
    if created_at:
        fields.append("created_at=?")
        params.append(created_at)
    if updated_at:
        fields.append("updated_at=?")
        params.append(updated_at)
    if closed_at:
        fields.append("closed_at=?")
        params.append(closed_at)
    if not fields:
        return
    params.extend([int(row_id)])
    con.execute(f"UPDATE {table} SET {', '.join(fields)} WHERE id=?", tuple(params))


def _import_users(con: sqlite3.Connection, *, tenant_id: str, rows: List[Dict[str, Any]], default_password: str) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        login_id = _clean_text(_pick(row, "login_id"), 32).lower()
        name = _clean_text(_pick(row, "name"), 40)
        if not login_id or not name:
            skipped += 1
            continue
        role = _normalize_role(_pick(row, "role"))
        phone = _clean_text(_pick(row, "phone"), 40)
        note = _clean_text(_pick(row, "note"), 2000)
        is_active = _truthy(_pick(row, "is_active", 1))
        is_site_admin = _truthy(_pick(row, "is_site_admin", 0))
        password = _clean_text(_pick(row, "password"), 72) or default_password
        existing = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, is_site_admin, is_active
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (login_id,),
        ).fetchone()
        if existing:
            con.execute(
                """
                UPDATE staff_users
                SET tenant_id=?, name=?, role=?, phone=?, note=?, is_site_admin=?, is_active=?, password_hash=?, updated_at=?
                WHERE id=?
                """,
                (
                    tenant_id,
                    name,
                    role,
                    phone or None,
                    note or None,
                    1 if is_site_admin else 0,
                    1 if is_active else 0,
                    hash_password(password),
                    now_iso(),
                    int(existing["id"]),
                ),
            )
            updated += 1
            continue
        ts = now_iso()
        con.execute(
            """
            INSERT INTO staff_users(
              tenant_id, login_id, name, role, phone, note, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,0,?,NULL,?,?,?)
            """,
            (
                tenant_id,
                login_id,
                name,
                role,
                phone or None,
                note or None,
                hash_password(password),
                1 if is_site_admin else 0,
                1 if is_active else 0,
                ts,
                ts,
            ),
        )
        created += 1
    return {"created": created, "updated": updated, "skipped": skipped}


def _import_complaints(con: sqlite3.Connection, *, tenant_id: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        building = _clean_text(_pick(row, "building"), 20)
        unit = _clean_text(_pick(row, "unit"), 20)
        content = _clean_text(_pick(row, "content"), 8000)
        if not content:
            skipped += 1
            continue
        classification = classify_complaint_text(" ".join(part for part in [building and f"{building}동", unit and f"{unit}호", content] if part))
        complaint_type = _normalize_choice(_pick(row, "type"), allowed=COMPLAINT_TYPES, mapping=COMPLAINT_TYPE_MAP, default=classification["type"])
        urgency = _normalize_choice(_pick(row, "urgency"), allowed=URGENCY_VALUES, mapping=URGENCY_MAP, default=classification["urgency"])
        status = _normalize_choice(_pick(row, "status"), allowed=STATUS_VALUES, mapping=STATUS_MAP, default="접수")
        summary = normalize_summary_text(
            _clean_text(_pick(row, "summary"), 160) or classification["summary"],
            building=building,
            unit=unit,
            complaint_type=complaint_type,
        )
        created_at = _normalize_timestamp(_pick(row, "created_at"))
        updated_at = _normalize_timestamp(_pick(row, "updated_at")) or created_at
        closed_at = _normalize_timestamp(_pick(row, "closed_at")) if status == "완료" else ""
        where = {
            "tenant_id": tenant_id,
            "building": building or None,
            "unit": unit or None,
            "content": content,
            "created_at": created_at,
        }
        existing_id = _find_existing_row_id(con, "complaints", where)
        if existing_id:
            con.execute(
                """
                UPDATE complaints
                SET complainant_phone=?, channel=?, summary=?, type=?, urgency=?, status=?, manager=?, image_url=?,
                    source_text=?, ai_model=?, created_by_label=?, updated_at=?, closed_at=?
                WHERE id=? AND tenant_id=?
                """,
                (
                    _clean_text(_pick(row, "complainant_phone"), 40) or None,
                    _clean_text(_pick(row, "channel"), 20) or "기타",
                    summary or None,
                    complaint_type,
                    urgency,
                    status,
                    _clean_text(_pick(row, "manager"), 60) or None,
                    _clean_text(_pick(row, "image_url"), 500) or None,
                    _clean_text(_pick(row, "source_text"), 20000) or None,
                    _clean_text(_pick(row, "ai_model"), 80) or None,
                    "legacy-import",
                    updated_at,
                    closed_at or None,
                    int(existing_id),
                    tenant_id,
                ),
            )
            updated += 1
            continue
        cur = con.execute(
            """
            INSERT INTO complaints(
              tenant_id, building, unit, complainant_phone, channel, content, summary, type, urgency, status,
              manager, image_url, source_text, ai_model, repeat_count, created_by_label, created_at, updated_at, closed_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?)
            """,
            (
                tenant_id,
                building or None,
                unit or None,
                _clean_text(_pick(row, "complainant_phone"), 40) or None,
                _clean_text(_pick(row, "channel"), 20) or "기타",
                content,
                summary or None,
                complaint_type,
                urgency,
                status,
                _clean_text(_pick(row, "manager"), 60) or None,
                _clean_text(_pick(row, "image_url"), 500) or None,
                _clean_text(_pick(row, "source_text"), 20000) or None,
                _clean_text(_pick(row, "ai_model"), 80) or None,
                "legacy-import",
                created_at,
                updated_at,
                closed_at or None,
            ),
        )
        created += 1
        attachments = row.get("attachments") if isinstance(row.get("attachments"), list) else []
        for attachment in attachments:
            file_url = _clean_text((attachment or {}).get("file_url"), 500)
            if not file_url:
                continue
            con.execute(
                """
                INSERT INTO complaint_attachments(complaint_id, file_url, mime_type, size_bytes, created_at)
                VALUES(?,?,?,?,?)
                """,
                (
                    int(cur.lastrowid),
                    file_url,
                    _clean_text((attachment or {}).get("mime_type"), 120) or None,
                    int((attachment or {}).get("size_bytes") or 0) or None,
                    _normalize_timestamp((attachment or {}).get("created_at")),
                ),
            )
        history = row.get("history") if isinstance(row.get("history"), list) else []
        for hist in history:
            to_status = _normalize_choice((hist or {}).get("to_status"), allowed=STATUS_VALUES, mapping=STATUS_MAP, default=status)
            con.execute(
                """
                INSERT INTO complaint_history(complaint_id, from_status, to_status, note, actor_label, created_at)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    int(cur.lastrowid),
                    _clean_text((hist or {}).get("from_status"), 40) or None,
                    to_status,
                    _clean_text((hist or {}).get("note"), 4000) or None,
                    _clean_text((hist or {}).get("actor_label"), 120) or "legacy-import",
                    _normalize_timestamp((hist or {}).get("created_at")),
                ),
            )
    return {"created": created, "updated": updated, "skipped": skipped}


def _import_notices(con: sqlite3.Connection, *, tenant_id: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        title = _clean_text(_pick(row, "title"), 160)
        body = _clean_text(_pick(row, "body"), 12000)
        if not title or not body:
            skipped += 1
            continue
        category = _normalize_choice(_pick(row, "category"), allowed=NOTICE_CATEGORY_VALUES, default="공지")
        status = _normalize_choice(_pick(row, "status"), allowed=NOTICE_STATUS_VALUES, mapping=NOTICE_STATUS_MAP, default="published")
        pinned = _truthy(_pick(row, "pinned"))
        created_at = _normalize_timestamp(_pick(row, "created_at"))
        updated_at = _normalize_timestamp(_pick(row, "updated_at")) or created_at
        existing_id = _find_existing_row_id(con, "ops_notices", {"tenant_id": tenant_id, "title": title, "body": body})
        if existing_id:
            con.execute(
                "UPDATE ops_notices SET category=?, status=?, pinned=?, updated_at=? WHERE id=? AND tenant_id=?",
                (category, status, 1 if pinned else 0, updated_at, int(existing_id), tenant_id),
            )
            updated += 1
            continue
        cur = con.execute(
            """
            INSERT INTO ops_notices(tenant_id, title, body, category, status, pinned, created_by_label, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (tenant_id, title, body, category, status, 1 if pinned else 0, "legacy-import", created_at, updated_at),
        )
        _apply_timestamps(con, "ops_notices", int(cur.lastrowid), created_at, updated_at)
        created += 1
    return {"created": created, "updated": updated, "skipped": skipped}


def _import_documents(con: sqlite3.Connection, *, tenant_id: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        title = _clean_text(_pick(row, "title"), 160)
        if not title:
            skipped += 1
            continue
        category = _normalize_choice(_pick(row, "category"), allowed=DOCUMENT_CATEGORY_VALUES, default="기타")
        status = _normalize_choice(_pick(row, "status"), allowed=DOCUMENT_STATUS_VALUES, mapping=DOCUMENT_STATUS_MAP, default="작성중")
        summary = _clean_text(_pick(row, "summary"), 4000)
        owner = _clean_text(_pick(row, "owner"), 80)
        due_date = _clean_text(_pick(row, "due_date"), 20)
        reference_no = _clean_text(_pick(row, "reference_no"), 80)
        created_at = _normalize_timestamp(_pick(row, "created_at"))
        updated_at = _normalize_timestamp(_pick(row, "updated_at")) or created_at
        existing_id = _find_existing_row_id(
            con,
            "ops_documents",
            {"tenant_id": tenant_id, "title": title, "reference_no": reference_no or None},
        )
        if existing_id:
            con.execute(
                """
                UPDATE ops_documents
                SET summary=?, category=?, status=?, owner=?, due_date=?, updated_at=?
                WHERE id=? AND tenant_id=?
                """,
                (summary or None, category, status, owner or None, due_date or None, updated_at, int(existing_id), tenant_id),
            )
            updated += 1
            continue
        cur = con.execute(
            """
            INSERT INTO ops_documents(
              tenant_id, title, summary, category, status, owner, due_date, reference_no, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (tenant_id, title, summary or None, category, status, owner or None, due_date or None, reference_no or None, "legacy-import", created_at, updated_at),
        )
        _apply_timestamps(con, "ops_documents", int(cur.lastrowid), created_at, updated_at)
        created += 1
    return {"created": created, "updated": updated, "skipped": skipped}


def _import_vendors(con: sqlite3.Connection, *, tenant_id: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        company_name = _clean_text(_pick(row, "company_name"), 160)
        service_type = _clean_text(_pick(row, "service_type"), 80)
        if not company_name or not service_type:
            skipped += 1
            continue
        status = _normalize_choice(_pick(row, "status"), allowed=VENDOR_STATUS_VALUES, default="활성")
        created_at = _normalize_timestamp(_pick(row, "created_at"))
        updated_at = _normalize_timestamp(_pick(row, "updated_at")) or created_at
        existing_id = _find_existing_row_id(
            con,
            "ops_vendors",
            {"tenant_id": tenant_id, "company_name": company_name, "service_type": service_type},
        )
        values = (
            _clean_text(_pick(row, "contact_name"), 80) or None,
            _clean_text(_pick(row, "phone"), 40) or None,
            _clean_text(_pick(row, "email"), 120) or None,
            status,
            _clean_text(_pick(row, "note"), 4000) or None,
            updated_at,
        )
        if existing_id:
            con.execute(
                """
                UPDATE ops_vendors
                SET contact_name=?, phone=?, email=?, status=?, note=?, updated_at=?
                WHERE id=? AND tenant_id=?
                """,
                (*values, int(existing_id), tenant_id),
            )
            updated += 1
            continue
        cur = con.execute(
            """
            INSERT INTO ops_vendors(
              tenant_id, company_name, service_type, contact_name, phone, email, status, note, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (tenant_id, company_name, service_type, values[0], values[1], values[2], values[3], values[4], "legacy-import", created_at, updated_at),
        )
        _apply_timestamps(con, "ops_vendors", int(cur.lastrowid), created_at, updated_at)
        created += 1
    return {"created": created, "updated": updated, "skipped": skipped}


def _resolve_vendor_id(con: sqlite3.Connection, *, tenant_id: str, row: Dict[str, Any]) -> Optional[int]:
    vendor_name = _clean_text(_pick(row, "vendor_name"), 160)
    vendor_service_type = _clean_text(_pick(row, "vendor_service_type"), 80)
    if not vendor_name:
        return None
    return _find_existing_row_id(
        con,
        "ops_vendors",
        {
            "tenant_id": tenant_id,
            "company_name": vendor_name,
            "service_type": vendor_service_type or None,
        },
    ) or _find_existing_row_id(con, "ops_vendors", {"tenant_id": tenant_id, "company_name": vendor_name})


def _import_schedules(con: sqlite3.Connection, *, tenant_id: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    created = 0
    updated = 0
    skipped = 0
    for row in rows:
        title = _clean_text(_pick(row, "title"), 160)
        if not title:
            skipped += 1
            continue
        schedule_type = _normalize_choice(_pick(row, "schedule_type"), allowed=SCHEDULE_TYPE_VALUES, default="행정")
        status = _normalize_choice(_pick(row, "status"), allowed=SCHEDULE_STATUS_VALUES, mapping=SCHEDULE_STATUS_MAP, default="예정")
        due_date = _clean_text(_pick(row, "due_date"), 20)
        owner = _clean_text(_pick(row, "owner"), 80)
        note = _clean_text(_pick(row, "note"), 4000)
        vendor_id = _resolve_vendor_id(con, tenant_id=tenant_id, row=row)
        created_at = _normalize_timestamp(_pick(row, "created_at"))
        updated_at = _normalize_timestamp(_pick(row, "updated_at")) or created_at
        existing_id = _find_existing_row_id(con, "ops_schedules", {"tenant_id": tenant_id, "title": title, "due_date": due_date or None})
        if existing_id:
            con.execute(
                """
                UPDATE ops_schedules
                SET schedule_type=?, status=?, due_date=?, owner=?, note=?, vendor_id=?, updated_at=?
                WHERE id=? AND tenant_id=?
                """,
                (schedule_type, status, due_date or None, owner or None, note or None, vendor_id, updated_at, int(existing_id), tenant_id),
            )
            updated += 1
            continue
        cur = con.execute(
            """
            INSERT INTO ops_schedules(
              tenant_id, title, schedule_type, status, due_date, owner, note, vendor_id, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (tenant_id, title, schedule_type, status, due_date or None, owner or None, note or None, vendor_id, "legacy-import", created_at, updated_at),
        )
        _apply_timestamps(con, "ops_schedules", int(cur.lastrowid), created_at, updated_at)
        created += 1
    return {"created": created, "updated": updated, "skipped": skipped}


def import_legacy_source(
    *,
    source_path: str | Path,
    tenant_id: str,
    tenant_name: str,
    site_code: str = "",
    site_name: str = "",
    default_user_password: str = "ChangeMe123!",
    dry_run: bool = False,
) -> Dict[str, Any]:
    bundle = load_legacy_source(source_path)
    tenant_meta = bundle.get("tenant") or {}
    resolved_tenant_id = _clean_text(tenant_id or tenant_meta.get("id"), 32).lower()
    resolved_tenant_name = _clean_text(tenant_name or tenant_meta.get("name"), 120)
    if not resolved_tenant_id or not resolved_tenant_name:
        raise ValueError("tenant_id and tenant_name are required")

    con = _connect()
    try:
        _ensure_tenant(
            con,
            resolved_tenant_id,
            resolved_tenant_name,
            site_code=_clean_text(site_code or tenant_meta.get("site_code"), 32),
            site_name=_clean_text(site_name or tenant_meta.get("site_name"), 120),
        )
        summary = {
            "tenant_id": resolved_tenant_id,
            "tenant_name": resolved_tenant_name,
            "source_path": str(Path(source_path).resolve()),
            "users": _import_users(con, tenant_id=resolved_tenant_id, rows=list(bundle.get("users") or []), default_password=default_user_password),
            "complaints": _import_complaints(con, tenant_id=resolved_tenant_id, rows=list(bundle.get("complaints") or [])),
            "notices": _import_notices(con, tenant_id=resolved_tenant_id, rows=list(bundle.get("notices") or [])),
            "documents": _import_documents(con, tenant_id=resolved_tenant_id, rows=list(bundle.get("documents") or [])),
            "vendors": _import_vendors(con, tenant_id=resolved_tenant_id, rows=list(bundle.get("vendors") or [])),
            "schedules": _import_schedules(con, tenant_id=resolved_tenant_id, rows=list(bundle.get("schedules") or [])),
        }
        if dry_run:
            con.rollback()
            summary["dry_run"] = True
        else:
            con.commit()
            summary["dry_run"] = False
        return summary
    finally:
        con.close()
