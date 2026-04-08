from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .db import DB_PATH, now_iso

NOTICE_CATEGORY_VALUES = ("공지", "기안", "구매", "견적및발주", "작업내용", "기타")
NOTICE_STATUS_VALUES = ("draft", "published", "archived")
DOCUMENT_CATEGORY_VALUES = ("계약", "공문", "보고", "예산", "입주", "점검", "기타")
DOCUMENT_STATUS_VALUES = ("작성중", "검토중", "완료", "보관")
SCHEDULE_TYPE_VALUES = ("행정", "점검", "회의", "계약", "민원", "기타")
SCHEDULE_STATUS_VALUES = ("예정", "진행중", "완료", "보류")
VENDOR_STATUS_VALUES = ("활성", "중지", "종료")


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH), timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    try:
        con.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass
    return con


def _clean_text(value: Any, *, field: str, required: bool = False, max_len: int = 4000) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_len:
        raise ValueError(f"{field} length must be <= {max_len}")
    return text


def _clean_choice(value: Any, allowed: Tuple[str, ...], *, field: str, default: str = "") -> str:
    text = str(value or "").strip() or default
    if text not in allowed:
        raise ValueError(f"{field} must be one of: {', '.join(allowed)}")
    return text


def _clean_date(value: Any, *, field: str, required: bool = False) -> str:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise ValueError(f"{field} is required")
        return ""
    try:
        return date.fromisoformat(raw).isoformat()
    except Exception as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS ops_notices (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          title TEXT NOT NULL,
          body TEXT NOT NULL,
          category TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'published',
          pinned INTEGER NOT NULL DEFAULT 0 CHECK(pinned IN (0,1)),
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ops_documents (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          title TEXT NOT NULL,
          summary TEXT,
          category TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT '작성중',
          owner TEXT,
          due_date TEXT,
          reference_no TEXT,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ops_vendors (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          company_name TEXT NOT NULL,
          service_type TEXT NOT NULL,
          contact_name TEXT,
          phone TEXT,
          email TEXT,
          status TEXT NOT NULL DEFAULT '활성',
          note TEXT,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ops_schedules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          title TEXT NOT NULL,
          schedule_type TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT '예정',
          due_date TEXT,
          owner TEXT,
          note TEXT,
          vendor_id INTEGER REFERENCES ops_vendors(id) ON DELETE SET NULL,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_ops_notices_tenant_updated
          ON ops_notices(tenant_id, status, pinned DESC, updated_at DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_ops_documents_tenant_due
          ON ops_documents(tenant_id, status, due_date ASC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_ops_vendors_tenant_status
          ON ops_vendors(tenant_id, status, company_name ASC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_ops_schedules_tenant_due
          ON ops_schedules(tenant_id, status, due_date ASC, id DESC);
        """
    )


def init_ops_db() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        con.commit()
    finally:
        con.close()


def _notice_detail(con: sqlite3.Connection, notice_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, title, body, category, status, pinned, created_by_label, created_at, updated_at
        FROM ops_notices
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(notice_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("notice not found")
    return dict(row)


def _document_detail(con: sqlite3.Connection, document_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, title, summary, category, status, owner, due_date, reference_no,
          created_by_label, created_at, updated_at
        FROM ops_documents
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(document_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("document not found")
    return dict(row)


def _vendor_detail(con: sqlite3.Connection, vendor_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, company_name, service_type, contact_name, phone, email, status, note,
          created_by_label, created_at, updated_at
        FROM ops_vendors
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(vendor_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("vendor not found")
    return dict(row)


def _schedule_detail(con: sqlite3.Connection, schedule_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          s.id, s.tenant_id, s.title, s.schedule_type, s.status, s.due_date, s.owner, s.note,
          s.vendor_id, v.company_name AS vendor_name, s.created_by_label, s.created_at, s.updated_at
        FROM ops_schedules s
        LEFT JOIN ops_vendors v ON v.id = s.vendor_id
        WHERE s.id=? AND s.tenant_id=?
        LIMIT 1
        """,
        (int(schedule_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("schedule not found")
    return dict(row)


def create_notice(
    *,
    tenant_id: str,
    title: str,
    body: str,
    category: str,
    status: str = "published",
    pinned: bool = False,
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_title = _clean_text(title, field="title", required=True, max_len=160)
    clean_body = _clean_text(body, field="body", required=True, max_len=12000)
    clean_category = _clean_choice(category, NOTICE_CATEGORY_VALUES, field="category", default="공지")
    clean_status = _clean_choice(status, NOTICE_STATUS_VALUES, field="status", default="published")
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO ops_notices(tenant_id, title, body, category, status, pinned, created_by_label, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_title,
                clean_body,
                clean_category,
                clean_status,
                1 if pinned else 0,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _notice_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_notices(*, tenant_id: str, status: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, title, body, category, status, pinned, created_by_label, created_at, updated_at
            FROM ops_notices
            WHERE tenant_id=?
        """
        params: List[Any] = [clean_tenant_id]
        if str(status or "").strip():
            sql += " AND status=?"
            params.append(_clean_choice(status, NOTICE_STATUS_VALUES, field="status"))
        sql += " ORDER BY pinned DESC, updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(500, int(limit))))
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def update_notice(
    notice_id: int,
    *,
    tenant_id: str,
    title: Optional[str] = None,
    body: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    pinned: Optional[bool] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _notice_detail(con, int(notice_id), clean_tenant_id)
        con.execute(
            """
            UPDATE ops_notices
            SET title=?, body=?, category=?, status=?, pinned=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_text(title, field="title", required=True, max_len=160) if title is not None else current["title"],
                _clean_text(body, field="body", required=True, max_len=12000) if body is not None else current["body"],
                _clean_choice(category, NOTICE_CATEGORY_VALUES, field="category") if category is not None else current["category"],
                _clean_choice(status, NOTICE_STATUS_VALUES, field="status") if status is not None else current["status"],
                1 if (current["pinned"] if pinned is None else pinned) else 0,
                now_iso(),
                int(notice_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _notice_detail(con, int(notice_id), clean_tenant_id)
    finally:
        con.close()


def delete_notice(*, tenant_id: str, notice_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _notice_detail(con, int(notice_id), clean_tenant_id)
        con.execute("DELETE FROM ops_notices WHERE id=? AND tenant_id=?", (int(notice_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_document(
    *,
    tenant_id: str,
    title: str,
    summary: str,
    category: str,
    status: str = "작성중",
    owner: str = "",
    due_date: str = "",
    reference_no: str = "",
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_title = _clean_text(title, field="title", required=True, max_len=160)
    clean_summary = _clean_text(summary, field="summary", max_len=4000)
    clean_category = _clean_choice(category, DOCUMENT_CATEGORY_VALUES, field="category", default="기타")
    clean_status = _clean_choice(status, DOCUMENT_STATUS_VALUES, field="status", default="작성중")
    clean_owner = _clean_text(owner, field="owner", max_len=80)
    clean_due_date = _clean_date(due_date, field="due_date")
    clean_reference = _clean_text(reference_no, field="reference_no", max_len=80)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO ops_documents(
              tenant_id, title, summary, category, status, owner, due_date, reference_no, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_title,
                clean_summary or None,
                clean_category,
                clean_status,
                clean_owner or None,
                clean_due_date or None,
                clean_reference or None,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _document_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_documents(*, tenant_id: str, status: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, title, summary, category, status, owner, due_date, reference_no,
              created_by_label, created_at, updated_at
            FROM ops_documents
            WHERE tenant_id=?
        """
        params: List[Any] = [clean_tenant_id]
        if str(status or "").strip():
            sql += " AND status=?"
            params.append(_clean_choice(status, DOCUMENT_STATUS_VALUES, field="status"))
        sql += " ORDER BY CASE WHEN due_date IS NULL OR due_date='' THEN 1 ELSE 0 END, due_date ASC, updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(500, int(limit))))
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def update_document(
    document_id: int,
    *,
    tenant_id: str,
    title: Optional[str] = None,
    summary: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    owner: Optional[str] = None,
    due_date: Optional[str] = None,
    reference_no: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _document_detail(con, int(document_id), clean_tenant_id)
        con.execute(
            """
            UPDATE ops_documents
            SET title=?, summary=?, category=?, status=?, owner=?, due_date=?, reference_no=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_text(title, field="title", required=True, max_len=160) if title is not None else current["title"],
                _clean_text(summary, field="summary", max_len=4000) if summary is not None else current["summary"],
                _clean_choice(category, DOCUMENT_CATEGORY_VALUES, field="category") if category is not None else current["category"],
                _clean_choice(status, DOCUMENT_STATUS_VALUES, field="status") if status is not None else current["status"],
                _clean_text(owner, field="owner", max_len=80) if owner is not None else current["owner"],
                _clean_date(due_date, field="due_date") if due_date is not None else (current["due_date"] or ""),
                _clean_text(reference_no, field="reference_no", max_len=80) if reference_no is not None else current["reference_no"],
                now_iso(),
                int(document_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _document_detail(con, int(document_id), clean_tenant_id)
    finally:
        con.close()


def delete_document(*, tenant_id: str, document_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _document_detail(con, int(document_id), clean_tenant_id)
        con.execute("DELETE FROM ops_documents WHERE id=? AND tenant_id=?", (int(document_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_vendor(
    *,
    tenant_id: str,
    company_name: str,
    service_type: str,
    contact_name: str = "",
    phone: str = "",
    email: str = "",
    status: str = "활성",
    note: str = "",
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_company_name = _clean_text(company_name, field="company_name", required=True, max_len=160)
    clean_service_type = _clean_text(service_type, field="service_type", required=True, max_len=80)
    clean_contact_name = _clean_text(contact_name, field="contact_name", max_len=80)
    clean_phone = _clean_text(phone, field="phone", max_len=40)
    clean_email = _clean_text(email, field="email", max_len=120)
    clean_status = _clean_choice(status, VENDOR_STATUS_VALUES, field="status", default="활성")
    clean_note = _clean_text(note, field="note", max_len=4000)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO ops_vendors(
              tenant_id, company_name, service_type, contact_name, phone, email, status, note, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_company_name,
                clean_service_type,
                clean_contact_name or None,
                clean_phone or None,
                clean_email or None,
                clean_status,
                clean_note or None,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _vendor_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_vendors(*, tenant_id: str, status: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, company_name, service_type, contact_name, phone, email, status, note,
              created_by_label, created_at, updated_at
            FROM ops_vendors
            WHERE tenant_id=?
        """
        params: List[Any] = [clean_tenant_id]
        if str(status or "").strip():
            sql += " AND status=?"
            params.append(_clean_choice(status, VENDOR_STATUS_VALUES, field="status"))
        sql += " ORDER BY CASE WHEN status='활성' THEN 0 ELSE 1 END, company_name ASC, id DESC LIMIT ?"
        params.append(max(1, min(500, int(limit))))
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def update_vendor(
    vendor_id: int,
    *,
    tenant_id: str,
    company_name: Optional[str] = None,
    service_type: Optional[str] = None,
    contact_name: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    status: Optional[str] = None,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _vendor_detail(con, int(vendor_id), clean_tenant_id)
        con.execute(
            """
            UPDATE ops_vendors
            SET company_name=?, service_type=?, contact_name=?, phone=?, email=?, status=?, note=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_text(company_name, field="company_name", required=True, max_len=160) if company_name is not None else current["company_name"],
                _clean_text(service_type, field="service_type", required=True, max_len=80) if service_type is not None else current["service_type"],
                _clean_text(contact_name, field="contact_name", max_len=80) if contact_name is not None else current["contact_name"],
                _clean_text(phone, field="phone", max_len=40) if phone is not None else current["phone"],
                _clean_text(email, field="email", max_len=120) if email is not None else current["email"],
                _clean_choice(status, VENDOR_STATUS_VALUES, field="status") if status is not None else current["status"],
                _clean_text(note, field="note", max_len=4000) if note is not None else current["note"],
                now_iso(),
                int(vendor_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _vendor_detail(con, int(vendor_id), clean_tenant_id)
    finally:
        con.close()


def delete_vendor(*, tenant_id: str, vendor_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _vendor_detail(con, int(vendor_id), clean_tenant_id)
        con.execute("UPDATE ops_schedules SET vendor_id=NULL, updated_at=? WHERE tenant_id=? AND vendor_id=?", (now_iso(), clean_tenant_id, int(vendor_id)))
        con.execute("DELETE FROM ops_vendors WHERE id=? AND tenant_id=?", (int(vendor_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_schedule(
    *,
    tenant_id: str,
    title: str,
    schedule_type: str,
    status: str = "예정",
    due_date: str = "",
    owner: str = "",
    note: str = "",
    vendor_id: Optional[int] = None,
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_title = _clean_text(title, field="title", required=True, max_len=160)
    clean_type = _clean_choice(schedule_type, SCHEDULE_TYPE_VALUES, field="schedule_type", default="행정")
    clean_status = _clean_choice(status, SCHEDULE_STATUS_VALUES, field="status", default="예정")
    clean_due_date = _clean_date(due_date, field="due_date")
    clean_owner = _clean_text(owner, field="owner", max_len=80)
    clean_note = _clean_text(note, field="note", max_len=4000)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    vendor_id_value = int(vendor_id) if vendor_id else None
    con = _connect()
    try:
        _ensure_schema(con)
        if vendor_id_value:
            _vendor_detail(con, vendor_id_value, clean_tenant_id)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO ops_schedules(
              tenant_id, title, schedule_type, status, due_date, owner, note, vendor_id, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_title,
                clean_type,
                clean_status,
                clean_due_date or None,
                clean_owner or None,
                clean_note or None,
                vendor_id_value,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _schedule_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_schedules(*, tenant_id: str, status: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              s.id, s.tenant_id, s.title, s.schedule_type, s.status, s.due_date, s.owner, s.note,
              s.vendor_id, v.company_name AS vendor_name, s.created_by_label, s.created_at, s.updated_at
            FROM ops_schedules s
            LEFT JOIN ops_vendors v ON v.id = s.vendor_id
            WHERE s.tenant_id=?
        """
        params: List[Any] = [clean_tenant_id]
        if str(status or "").strip():
            sql += " AND s.status=?"
            params.append(_clean_choice(status, SCHEDULE_STATUS_VALUES, field="status"))
        sql += " ORDER BY CASE WHEN s.due_date IS NULL OR s.due_date='' THEN 1 ELSE 0 END, s.due_date ASC, s.updated_at DESC, s.id DESC LIMIT ?"
        params.append(max(1, min(500, int(limit))))
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def update_schedule(
    schedule_id: int,
    *,
    tenant_id: str,
    title: Optional[str] = None,
    schedule_type: Optional[str] = None,
    status: Optional[str] = None,
    due_date: Optional[str] = None,
    owner: Optional[str] = None,
    note: Optional[str] = None,
    vendor_id: Optional[int] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _schedule_detail(con, int(schedule_id), clean_tenant_id)
        vendor_id_value = current["vendor_id"] if vendor_id is None else (int(vendor_id) if vendor_id else None)
        if vendor_id_value:
            _vendor_detail(con, int(vendor_id_value), clean_tenant_id)
        con.execute(
            """
            UPDATE ops_schedules
            SET title=?, schedule_type=?, status=?, due_date=?, owner=?, note=?, vendor_id=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_text(title, field="title", required=True, max_len=160) if title is not None else current["title"],
                _clean_choice(schedule_type, SCHEDULE_TYPE_VALUES, field="schedule_type") if schedule_type is not None else current["schedule_type"],
                _clean_choice(status, SCHEDULE_STATUS_VALUES, field="status") if status is not None else current["status"],
                _clean_date(due_date, field="due_date") if due_date is not None else (current["due_date"] or ""),
                _clean_text(owner, field="owner", max_len=80) if owner is not None else current["owner"],
                _clean_text(note, field="note", max_len=4000) if note is not None else current["note"],
                vendor_id_value,
                now_iso(),
                int(schedule_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _schedule_detail(con, int(schedule_id), clean_tenant_id)
    finally:
        con.close()


def delete_schedule(*, tenant_id: str, schedule_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _schedule_detail(con, int(schedule_id), clean_tenant_id)
        con.execute("DELETE FROM ops_schedules WHERE id=? AND tenant_id=?", (int(schedule_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def ops_dashboard_summary(*, tenant_id: str) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    today = date.today().isoformat()
    con = _connect()
    try:
        _ensure_schema(con)
        notices_total = con.execute(
            "SELECT COUNT(*) AS c FROM ops_notices WHERE tenant_id=? AND status='published'",
            (clean_tenant_id,),
        ).fetchone()
        docs_open = con.execute(
            "SELECT COUNT(*) AS c FROM ops_documents WHERE tenant_id=? AND status IN ('작성중','검토중')",
            (clean_tenant_id,),
        ).fetchone()
        schedules_open = con.execute(
            "SELECT COUNT(*) AS c FROM ops_schedules WHERE tenant_id=? AND status IN ('예정','진행중','보류')",
            (clean_tenant_id,),
        ).fetchone()
        vendors_active = con.execute(
            "SELECT COUNT(*) AS c FROM ops_vendors WHERE tenant_id=? AND status='활성'",
            (clean_tenant_id,),
        ).fetchone()
        overdue_documents = [
            dict(row)
            for row in con.execute(
                """
                SELECT id, title, category, status, owner, due_date, reference_no, updated_at
                FROM ops_documents
                WHERE tenant_id=? AND due_date IS NOT NULL AND due_date<>'' AND due_date<? AND status!='완료' AND status!='보관'
                ORDER BY due_date ASC, id DESC
                LIMIT 5
                """,
                (clean_tenant_id, today),
            ).fetchall()
        ]
        upcoming_schedules = [
            dict(row)
            for row in con.execute(
                """
                SELECT s.id, s.title, s.schedule_type, s.status, s.due_date, s.owner, v.company_name AS vendor_name
                FROM ops_schedules s
                LEFT JOIN ops_vendors v ON v.id = s.vendor_id
                WHERE s.tenant_id=? AND s.status IN ('예정','진행중','보류')
                ORDER BY CASE WHEN s.due_date IS NULL OR s.due_date='' THEN 1 ELSE 0 END, s.due_date ASC, s.id DESC
                LIMIT 5
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        recent_notices = [
            dict(row)
            for row in con.execute(
                """
                SELECT id, title, category, status, pinned, updated_at
                FROM ops_notices
                WHERE tenant_id=?
                ORDER BY pinned DESC, updated_at DESC, id DESC
                LIMIT 5
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        return {
            "published_notices": int(notices_total["c"] if notices_total else 0),
            "open_documents": int(docs_open["c"] if docs_open else 0),
            "open_schedules": int(schedules_open["c"] if schedules_open else 0),
            "active_vendors": int(vendors_active["c"] if vendors_active else 0),
            "overdue_documents": overdue_documents,
            "upcoming_schedules": upcoming_schedules,
            "recent_notices": recent_notices,
        }
    finally:
        con.close()
