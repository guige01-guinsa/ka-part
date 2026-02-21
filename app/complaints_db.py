from __future__ import annotations

import os
import sqlite3
import uuid
import urllib.parse
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .db import DB_PATH, now_iso

SCOPE_VALUES = {"COMMON", "PRIVATE", "EMERGENCY"}
STATUS_VALUES = {
    "RECEIVED",
    "TRIAGED",
    "GUIDANCE_SENT",
    "ASSIGNED",
    "IN_PROGRESS",
    "COMPLETED",
    "CLOSED",
}
PRIORITY_VALUES = {"LOW", "NORMAL", "HIGH", "URGENT"}
RESOLUTION_VALUES = {"REPAIR", "GUIDANCE_ONLY", "EXTERNAL_VENDOR"}
VISIT_REASON_VALUES = {"FIRE_INSPECTION", "NEIGHBOR_DAMAGE", "EMERGENCY_INFRA"}
WORK_ORDER_STATUS_VALUES = {"OPEN", "DISPATCHED", "DONE", "CANCELED"}
MAX_ATTACHMENT_URLS = 10
MAX_ATTACHMENT_URL_LENGTH = 500


def _connect() -> sqlite3.Connection:
    timeout_sec = 30.0
    try:
        raw = str(os.getenv("KA_SQLITE_TIMEOUT_SEC") or "").strip()
        if raw:
            timeout_sec = float(raw)
    except Exception:
        timeout_sec = 30.0
    timeout_sec = max(1.0, min(60.0, timeout_sec))
    con = sqlite3.connect(str(DB_PATH), timeout=timeout_sec)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    try:
        busy_ms = 30000
        raw_busy = str(os.getenv("KA_SQLITE_BUSY_TIMEOUT_MS") or "").strip()
        if raw_busy:
            busy_ms = int(raw_busy)
        busy_ms = max(1000, min(60000, busy_ms))
        con.execute(f"PRAGMA busy_timeout={busy_ms};")
    except Exception:
        pass
    return con


def _clean_text(value: Any, *, required: bool, max_len: int, field: str) -> str:
    txt = str(value or "").strip()
    if required and not txt:
        raise ValueError(f"{field} is required")
    if txt and len(txt) > max_len:
        raise ValueError(f"{field} length must be <= {max_len}")
    return txt


def _clean_enum(value: Any, allowed: set[str], field: str, *, required: bool = True) -> str:
    txt = str(value or "").strip().upper()
    if not txt:
        if required:
            raise ValueError(f"{field} is required")
        return ""
    if txt not in allowed:
        raise ValueError(f"{field} must be one of: {', '.join(sorted(allowed))}")
    return txt


def _clean_attachment_urls(values: Optional[List[str]]) -> List[str]:
    rows = [str(x or "").strip() for x in list(values or []) if str(x or "").strip()]
    if len(rows) > MAX_ATTACHMENT_URLS:
        raise ValueError(f"attachments length must be <= {MAX_ATTACHMENT_URLS}")
    out: List[str] = []
    for raw in rows:
        if len(raw) > MAX_ATTACHMENT_URL_LENGTH:
            raise ValueError(f"attachment url length must be <= {MAX_ATTACHMENT_URL_LENGTH}")
        parsed = urllib.parse.urlparse(raw)
        scheme = str(parsed.scheme or "").strip().lower()
        if scheme not in {"http", "https"}:
            raise ValueError("attachment url scheme must be http or https")
        if not str(parsed.netloc or "").strip():
            raise ValueError("attachment url host is required")
        out.append(raw)
    return out


def _normalize_unit_label(value: Any) -> str:
    raw = _clean_text(value, required=False, max_len=80, field="unit_label")
    if not raw:
        return ""
    compact = re.sub(r"\s+", "", raw)

    m = re.match(r"^(\d{2,4})-(\d{3,4})$", compact)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.match(r"^(\d{2,4})동(\d{3,4})호?$", compact)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.match(r"^(\d{2,4})(\d{3,4})$", compact)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.match(r"^(\d{3,4})호?$", compact)
    if m:
        return m.group(1)

    return raw


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS complaint_categories (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          scope TEXT NOT NULL CHECK(scope IN ('COMMON','PRIVATE','EMERGENCY')),
          is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
          created_at TEXT NOT NULL,
          UNIQUE(name, scope)
        );

        CREATE TABLE IF NOT EXISTS complaint_guidance_templates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT NOT NULL,
          content TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaints (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ticket_no TEXT NOT NULL UNIQUE,
          site_code TEXT,
          site_name TEXT,
          unit_label TEXT,
          reporter_user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE RESTRICT,
          category_id INTEGER NOT NULL REFERENCES complaint_categories(id) ON DELETE RESTRICT,
          assigned_to_user_id INTEGER REFERENCES staff_users(id) ON DELETE SET NULL,
          guidance_template_id INTEGER REFERENCES complaint_guidance_templates(id) ON DELETE SET NULL,
          scope TEXT NOT NULL CHECK(scope IN ('COMMON','PRIVATE','EMERGENCY')),
          status TEXT NOT NULL CHECK(status IN ('RECEIVED','TRIAGED','GUIDANCE_SENT','ASSIGNED','IN_PROGRESS','COMPLETED','CLOSED')),
          priority TEXT NOT NULL DEFAULT 'NORMAL' CHECK(priority IN ('LOW','NORMAL','HIGH','URGENT')),
          resolution_type TEXT CHECK(resolution_type IN ('REPAIR','GUIDANCE_ONLY','EXTERNAL_VENDOR')),
          title TEXT NOT NULL,
          description TEXT NOT NULL,
          location_detail TEXT,
          requires_visit INTEGER NOT NULL DEFAULT 0 CHECK(requires_visit IN (0,1)),
          visit_reason TEXT CHECK(visit_reason IS NULL OR visit_reason IN ('FIRE_INSPECTION','NEIGHBOR_DAMAGE','EMERGENCY_INFRA')),
          created_at TEXT NOT NULL,
          triaged_at TEXT,
          closed_at TEXT,
          updated_at TEXT NOT NULL,
          CHECK (scope <> 'PRIVATE' OR COALESCE(resolution_type,'GUIDANCE_ONLY')='GUIDANCE_ONLY'),
          CHECK ((requires_visit=0 AND visit_reason IS NULL) OR (requires_visit=1 AND visit_reason IS NOT NULL))
        );

        CREATE TABLE IF NOT EXISTS complaint_attachments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          complaint_id INTEGER NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
          file_url TEXT NOT NULL,
          mime_type TEXT,
          size_bytes INTEGER,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaint_status_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          complaint_id INTEGER NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
          from_status TEXT,
          to_status TEXT NOT NULL,
          changed_by_user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE RESTRICT,
          note TEXT,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaint_comments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          complaint_id INTEGER NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
          user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE RESTRICT,
          comment TEXT NOT NULL,
          is_internal INTEGER NOT NULL DEFAULT 0 CHECK(is_internal IN (0,1)),
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaint_work_orders (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          complaint_id INTEGER NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
          assignee_user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE RESTRICT,
          status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN','DISPATCHED','DONE','CANCELED')),
          scheduled_at TEXT,
          completed_at TEXT,
          result_note TEXT,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaint_visit_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          complaint_id INTEGER NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
          unit_label TEXT,
          visitor_user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE RESTRICT,
          visit_reason TEXT NOT NULL CHECK(visit_reason IN ('FIRE_INSPECTION','NEIGHBOR_DAMAGE','EMERGENCY_INFRA')),
          check_in_at TEXT NOT NULL,
          check_out_at TEXT,
          result_note TEXT,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaint_notices (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT NOT NULL,
          content TEXT NOT NULL,
          is_pinned INTEGER NOT NULL DEFAULT 0 CHECK(is_pinned IN (0,1)),
          published_at TEXT,
          author_user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE RESTRICT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaint_faqs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          question TEXT NOT NULL,
          answer TEXT NOT NULL,
          display_order INTEGER NOT NULL DEFAULT 100,
          is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_complaints_status_created_at
          ON complaints(status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_complaints_scope_status
          ON complaints(scope, status);
        CREATE INDEX IF NOT EXISTS idx_complaints_reporter_created
          ON complaints(reporter_user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_complaint_history_complaint
          ON complaint_status_history(complaint_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_work_orders_complaint
          ON complaint_work_orders(complaint_id);
        CREATE INDEX IF NOT EXISTS idx_visit_logs_complaint
          ON complaint_visit_logs(complaint_id, check_in_at DESC);
        """
    )
    con.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_complaint_work_order_scope_guard
        BEFORE INSERT ON complaint_work_orders
        FOR EACH ROW
        BEGIN
          SELECT CASE
            WHEN (SELECT scope FROM complaints WHERE id = NEW.complaint_id) = 'PRIVATE'
            THEN RAISE(ABORT, 'work_orders are allowed only for COMMON or EMERGENCY complaints')
          END;
        END;
        """
    )
    _seed_defaults(con)


def _seed_defaults(con: sqlite3.Connection) -> None:
    ts = now_iso()
    categories = [
        ("Electric / Lighting (Common)", "COMMON"),
        ("Water / Drainage (Common)", "COMMON"),
        ("Fire Safety (Common)", "COMMON"),
        ("Elevator (Common)", "COMMON"),
        ("Parking Device (Common)", "COMMON"),
        ("Inside Unit Fixture", "PRIVATE"),
        ("Emergency Leak / Blackout / Fire", "EMERGENCY"),
    ]
    for name, scope in categories:
        con.execute(
            """
            INSERT INTO complaint_categories(name, scope, is_active, created_at)
            VALUES(?,?,1,?)
            ON CONFLICT(name, scope) DO UPDATE SET is_active=excluded.is_active
            """,
            (name, scope, ts),
        )

    if not con.execute("SELECT 1 FROM complaint_guidance_templates LIMIT 1").fetchone():
        con.execute(
            """
            INSERT INTO complaint_guidance_templates(title, content, is_active, created_at)
            VALUES(?,?,1,?)
            """,
            (
                "Inside unit repair guide",
                "Inside-unit fixtures are private assets. Management office provides usage guidance only.",
                ts,
            ),
        )

    # Legacy English FAQ rows are translated in-place so existing DB data is updated automatically.
    faq_legacy_map = [
        (
            "Can management office replace indoor lights?",
            "실내 조명 교체를 관리사무소가 해주나요?",
            "아니요. 실내 전등/스위치/콘센트 등은 세대 내부(개인) 영역입니다. 관리사무소는 사용 안내를 도와드릴 수 있습니다.",
            10,
        ),
        (
            "When can staff enter a unit?",
            "직원이 세대에 방문할 수 있는 경우는 언제인가요?",
            "소방시설 점검, 이웃 피해 예방, 정전·누수 등 긴급 시설 조치가 필요한 경우에 한해 가능합니다.",
            20,
        ),
    ]
    for old_q, new_q, new_a, order_no in faq_legacy_map:
        con.execute(
            """
            UPDATE complaint_faqs
               SET question=?,
                   answer=?,
                   display_order=?,
                   is_active=1
             WHERE question=?
            """,
            (new_q, new_a, order_no, old_q),
        )

    if not con.execute("SELECT 1 FROM complaint_faqs LIMIT 1").fetchone():
        faq_seed = [(x[1], x[2], x[3]) for x in faq_legacy_map]
        for q, a, order_no in faq_seed:
            con.execute(
                """
                INSERT INTO complaint_faqs(question, answer, display_order, is_active, created_at)
                VALUES(?,?,?,?,?)
                """,
                (q, a, order_no, 1, ts),
            )


def _with_schema() -> sqlite3.Connection:
    con = _connect()
    _ensure_schema(con)
    return con


def init_complaints_db() -> None:
    con = _with_schema()
    try:
        con.commit()
    finally:
        con.close()


def _ticket_no(complaint_id: int) -> str:
    ymd = datetime.now().strftime("%Y%m%d")
    return f"C-{ymd}-{int(complaint_id):05d}"


def list_complaint_categories(*, active_only: bool = True) -> List[Dict[str, Any]]:
    con = _with_schema()
    try:
        sql = """
            SELECT id, name, scope, is_active, created_at
            FROM complaint_categories
        """
        if active_only:
            sql += " WHERE is_active=1"
        sql += " ORDER BY scope, name, id"
        rows = con.execute(sql).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def list_public_notices(*, limit: int = 50) -> List[Dict[str, Any]]:
    con = _with_schema()
    try:
        lim = max(1, min(200, int(limit)))
        rows = con.execute(
            """
            SELECT id, title, content, is_pinned, published_at, author_user_id, created_at, updated_at
            FROM complaint_notices
            ORDER BY is_pinned DESC, COALESCE(published_at, created_at) DESC, id DESC
            LIMIT ?
            """,
            (lim,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def list_public_faqs(*, limit: int = 100) -> List[Dict[str, Any]]:
    con = _with_schema()
    try:
        lim = max(1, min(300, int(limit)))
        rows = con.execute(
            """
            SELECT id, question, answer, display_order, is_active, created_at
            FROM complaint_faqs
            WHERE is_active=1
            ORDER BY display_order ASC, id ASC
            LIMIT ?
            """,
            (lim,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def _get_default_guidance_template_id(con: sqlite3.Connection) -> Optional[int]:
    row = con.execute(
        """
        SELECT id
        FROM complaint_guidance_templates
        WHERE is_active=1
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    return int(row["id"]) if row else None


def _insert_status_history(
    con: sqlite3.Connection,
    *,
    complaint_id: int,
    from_status: Optional[str],
    to_status: str,
    changed_by_user_id: int,
    note: str = "",
) -> None:
    con.execute(
        """
        INSERT INTO complaint_status_history(
          complaint_id, from_status, to_status, changed_by_user_id, note, created_at
        ) VALUES(?,?,?,?,?,?)
        """,
        (
            int(complaint_id),
            from_status,
            to_status,
            int(changed_by_user_id),
            str(note or "").strip() or None,
            now_iso(),
        ),
    )


def _get_complaint_row(con: sqlite3.Connection, complaint_id: int) -> Optional[sqlite3.Row]:
    return con.execute(
        """
        SELECT
          c.id,
          c.ticket_no,
          c.site_code,
          c.site_name,
          c.unit_label,
          c.reporter_user_id,
          c.category_id,
          c.assigned_to_user_id,
          c.guidance_template_id,
          c.scope,
          c.status,
          c.priority,
          c.resolution_type,
          c.title,
          c.description,
          c.location_detail,
          c.requires_visit,
          c.visit_reason,
          c.created_at,
          c.triaged_at,
          c.closed_at,
          c.updated_at,
          cat.name AS category_name,
          ru.name AS reporter_name,
          au.name AS assignee_name
        FROM complaints c
        JOIN complaint_categories cat ON cat.id = c.category_id
        JOIN staff_users ru ON ru.id = c.reporter_user_id
        LEFT JOIN staff_users au ON au.id = c.assigned_to_user_id
        WHERE c.id=?
        LIMIT 1
        """,
        (int(complaint_id),),
    ).fetchone()


def _complaint_detail(con: sqlite3.Connection, complaint_id: int, *, include_internal: bool) -> Dict[str, Any]:
    row = _get_complaint_row(con, complaint_id)
    if not row:
        raise ValueError("complaint not found")
    data = dict(row)
    data["requires_visit"] = bool(data.get("requires_visit"))
    data["attachments"] = [
        dict(r)
        for r in con.execute(
            """
            SELECT id, complaint_id, file_url, mime_type, size_bytes, created_at
            FROM complaint_attachments
            WHERE complaint_id=?
            ORDER BY id ASC
            """,
            (int(complaint_id),),
        ).fetchall()
    ]
    if include_internal:
        comments_sql = """
            SELECT cc.id, cc.complaint_id, cc.user_id, u.name AS user_name, cc.comment, cc.is_internal, cc.created_at
            FROM complaint_comments cc
            JOIN staff_users u ON u.id = cc.user_id
            WHERE cc.complaint_id=?
            ORDER BY cc.id ASC
        """
    else:
        comments_sql = """
            SELECT cc.id, cc.complaint_id, cc.user_id, u.name AS user_name, cc.comment, cc.is_internal, cc.created_at
            FROM complaint_comments cc
            JOIN staff_users u ON u.id = cc.user_id
            WHERE cc.complaint_id=? AND cc.is_internal=0
            ORDER BY cc.id ASC
        """
    data["comments"] = [dict(r) for r in con.execute(comments_sql, (int(complaint_id),)).fetchall()]
    data["history"] = [
        dict(r)
        for r in con.execute(
            """
            SELECT h.id, h.complaint_id, h.from_status, h.to_status, h.changed_by_user_id, u.name AS changed_by_name, h.note, h.created_at
            FROM complaint_status_history h
            JOIN staff_users u ON u.id = h.changed_by_user_id
            WHERE h.complaint_id=?
            ORDER BY h.id ASC
            """,
            (int(complaint_id),),
        ).fetchall()
    ]
    data["work_orders"] = [
        dict(r)
        for r in con.execute(
            """
            SELECT wo.id, wo.complaint_id, wo.assignee_user_id, u.name AS assignee_name, wo.status,
                   wo.scheduled_at, wo.completed_at, wo.result_note, wo.created_at
            FROM complaint_work_orders wo
            JOIN staff_users u ON u.id = wo.assignee_user_id
            WHERE wo.complaint_id=?
            ORDER BY wo.id DESC
            """,
            (int(complaint_id),),
        ).fetchall()
    ]
    data["visits"] = [
        dict(r)
        for r in con.execute(
            """
            SELECT v.id, v.complaint_id, v.unit_label, v.visitor_user_id, u.name AS visitor_name,
                   v.visit_reason, v.check_in_at, v.check_out_at, v.result_note, v.created_at
            FROM complaint_visit_logs v
            JOIN staff_users u ON u.id = v.visitor_user_id
            WHERE v.complaint_id=?
            ORDER BY v.id DESC
            """,
            (int(complaint_id),),
        ).fetchall()
    ]
    return data


def create_complaint(
    *,
    reporter_user_id: int,
    site_code: str = "",
    site_name: str = "",
    unit_label: str = "",
    category_id: int,
    scope: str,
    title: str,
    description: str,
    location_detail: str = "",
    priority: str = "NORMAL",
    attachment_urls: Optional[List[str]] = None,
    force_emergency: bool = False,
) -> Dict[str, Any]:
    clean_scope = _clean_enum(scope, SCOPE_VALUES, "scope")
    clean_priority = _clean_enum(priority, PRIORITY_VALUES, "priority")
    clean_title = _clean_text(title, required=True, max_len=140, field="title")
    clean_description = _clean_text(description, required=True, max_len=8000, field="description")
    clean_location = _clean_text(location_detail, required=False, max_len=200, field="location_detail")
    clean_site_code = _clean_text(site_code, required=False, max_len=32, field="site_code").upper()
    clean_site_name = _clean_text(site_name, required=False, max_len=80, field="site_name")
    clean_unit = _normalize_unit_label(unit_label)
    clean_attachments = _clean_attachment_urls(attachment_urls)

    if force_emergency:
        clean_scope = "EMERGENCY"
        clean_priority = "URGENT"

    con = _with_schema()
    try:
        cat = con.execute(
            "SELECT id FROM complaint_categories WHERE id=? AND is_active=1",
            (int(category_id),),
        ).fetchone()
        if not cat:
            raise ValueError("invalid category_id")

        status = "RECEIVED"
        resolution_type = None
        triaged_at = None
        closed_at = None
        guidance_template_id = None
        if clean_scope == "PRIVATE":
            status = "GUIDANCE_SENT"
            resolution_type = "GUIDANCE_ONLY"
            triaged_at = now_iso()
            closed_at = triaged_at
            guidance_template_id = _get_default_guidance_template_id(con)
        elif clean_scope == "EMERGENCY":
            clean_priority = "URGENT"

        ts = now_iso()
        ticket_seed = f"TEMP-{uuid.uuid4().hex}"
        cur = con.execute(
            """
            INSERT INTO complaints(
              ticket_no, site_code, site_name, unit_label,
              reporter_user_id, category_id, assigned_to_user_id, guidance_template_id,
              scope, status, priority, resolution_type, title, description, location_detail,
              requires_visit, visit_reason,
              created_at, triaged_at, closed_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                ticket_seed,
                clean_site_code or None,
                clean_site_name or None,
                clean_unit or None,
                int(reporter_user_id),
                int(category_id),
                None,
                guidance_template_id,
                clean_scope,
                status,
                clean_priority,
                resolution_type,
                clean_title,
                clean_description,
                clean_location or None,
                0,
                None,
                ts,
                triaged_at,
                closed_at,
                ts,
            ),
        )
        complaint_id = int(cur.lastrowid)
        ticket_no = _ticket_no(complaint_id)
        con.execute("UPDATE complaints SET ticket_no=? WHERE id=?", (ticket_no, complaint_id))

        for clean_url in clean_attachments:
            con.execute(
                """
                INSERT INTO complaint_attachments(complaint_id, file_url, mime_type, size_bytes, created_at)
                VALUES(?,?,?,?,?)
                """,
                (complaint_id, clean_url, None, None, ts),
            )

        _insert_status_history(
            con,
            complaint_id=complaint_id,
            from_status=None,
            to_status=status,
            changed_by_user_id=int(reporter_user_id),
            note="최초 접수",
        )

        if status == "GUIDANCE_SENT":
            tmpl = None
            if guidance_template_id:
                tmpl = con.execute(
                    "SELECT content FROM complaint_guidance_templates WHERE id=?",
                    (int(guidance_template_id),),
                ).fetchone()
            con.execute(
                """
                INSERT INTO complaint_comments(complaint_id, user_id, comment, is_internal, created_at)
                VALUES(?,?,?,?,?)
                """,
                (
                    complaint_id,
                    int(reporter_user_id),
                    str((tmpl["content"] if tmpl else "세대 내부 민원은 사용 안내 중심으로 처리됩니다.")),
                    0,
                    ts,
                ),
            )
        con.commit()
        return _complaint_detail(con, complaint_id, include_internal=True)
    finally:
        con.close()


def add_complaint_attachments(
    *,
    complaint_id: int,
    attachments: List[Tuple[str, str, int]],
) -> List[Dict[str, Any]]:
    """
    Add file attachments for an existing complaint.

    - file_url: storage-relative path (recommended) or http(s) URL.
    - mime_type/size_bytes are optional but recommended for image previews.
    """
    clean_id = int(complaint_id)
    rows = list(attachments or [])
    if not rows:
        return []

    con = _with_schema()
    try:
        existing = _get_complaint_row(con, clean_id)
        if not existing:
            raise ValueError("complaint not found")

        cnt_row = con.execute(
            "SELECT COUNT(*) AS cnt FROM complaint_attachments WHERE complaint_id=?",
            (clean_id,),
        ).fetchone()
        existing_count = int(cnt_row["cnt"] if cnt_row else 0)
        if existing_count + len(rows) > MAX_ATTACHMENT_URLS:
            raise ValueError(f"attachments length must be <= {MAX_ATTACHMENT_URLS}")

        ts = now_iso()
        out: List[Dict[str, Any]] = []
        for file_url, mime_type, size_bytes in rows:
            clean_url = _clean_text(
                file_url,
                required=True,
                max_len=MAX_ATTACHMENT_URL_LENGTH,
                field="file_url",
            )
            clean_mime = _clean_text(mime_type, required=False, max_len=80, field="mime_type")
            size_val: Optional[int] = None
            try:
                if size_bytes is not None:
                    size_val = int(size_bytes)
            except Exception:
                size_val = None
            cur = con.execute(
                """
                INSERT INTO complaint_attachments(complaint_id, file_url, mime_type, size_bytes, created_at)
                VALUES(?,?,?,?,?)
                """,
                (clean_id, clean_url, (clean_mime or None), size_val, ts),
            )
            att_id = int(cur.lastrowid)
            out.append(
                {
                    "id": att_id,
                    "complaint_id": clean_id,
                    "file_url": clean_url,
                    "mime_type": clean_mime or None,
                    "size_bytes": size_val,
                    "created_at": ts,
                }
            )

        con.execute("UPDATE complaints SET updated_at=? WHERE id=?", (ts, clean_id))
        con.commit()
        return out
    finally:
        con.close()


def get_complaint_attachment(*, complaint_id: int, attachment_id: int) -> Optional[Dict[str, Any]]:
    con = _with_schema()
    try:
        row = con.execute(
            """
            SELECT id, complaint_id, file_url, mime_type, size_bytes, created_at
            FROM complaint_attachments
            WHERE id=? AND complaint_id=?
            LIMIT 1
            """,
            (int(attachment_id), int(complaint_id)),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def list_complaints_for_reporter(
    reporter_user_id: int,
    *,
    status: str = "",
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    con = _with_schema()
    try:
        lim = max(1, min(200, int(limit)))
        off = max(0, int(offset))
        sql = """
            SELECT
              c.id, c.ticket_no, c.scope, c.status, c.priority, c.title, c.location_detail,
              c.created_at, c.updated_at, c.closed_at,
              cat.name AS category_name
            FROM complaints c
            JOIN complaint_categories cat ON cat.id = c.category_id
            WHERE c.reporter_user_id=?
        """
        params: List[Any] = [int(reporter_user_id)]
        clean_status = str(status or "").strip().upper()
        if clean_status:
            if clean_status not in STATUS_VALUES:
                raise ValueError("invalid status filter")
            sql += " AND c.status=?"
            params.append(clean_status)
        sql += " ORDER BY c.created_at DESC, c.id DESC LIMIT ? OFFSET ?"
        params.extend([lim, off])
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_complaint(
    complaint_id: int,
    *,
    requester_user_id: int,
    is_admin: bool,
) -> Optional[Dict[str, Any]]:
    con = _with_schema()
    try:
        row = _get_complaint_row(con, int(complaint_id))
        if not row:
            return None
        if not is_admin and int(row["reporter_user_id"]) != int(requester_user_id):
            return None
        return _complaint_detail(con, int(complaint_id), include_internal=is_admin)
    finally:
        con.close()


def add_comment(
    *,
    complaint_id: int,
    user_id: int,
    comment: str,
    is_internal: bool = False,
) -> Dict[str, Any]:
    clean_comment = _clean_text(comment, required=True, max_len=8000, field="comment")
    con = _with_schema()
    try:
        row = _get_complaint_row(con, int(complaint_id))
        if not row:
            raise ValueError("complaint not found")
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO complaint_comments(complaint_id, user_id, comment, is_internal, created_at)
            VALUES(?,?,?,?,?)
            """,
            (int(complaint_id), int(user_id), clean_comment, 1 if is_internal else 0, ts),
        )
        con.commit()
        out = con.execute(
            """
            SELECT cc.id, cc.complaint_id, cc.user_id, u.name AS user_name, cc.comment, cc.is_internal, cc.created_at
            FROM complaint_comments cc
            JOIN staff_users u ON u.id = cc.user_id
            WHERE cc.id=?
            LIMIT 1
            """,
            (int(cur.lastrowid),),
        ).fetchone()
        return dict(out) if out else {}
    finally:
        con.close()


def list_admin_complaints(
    *,
    scope: str = "",
    status: str = "",
    site_code: str = "",
    limit: int = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    con = _with_schema()
    try:
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        sql = """
            SELECT
              c.id, c.ticket_no, c.site_code, c.site_name, c.unit_label,
              c.scope, c.status, c.priority, c.resolution_type,
              c.title, c.location_detail, c.created_at, c.updated_at,
              c.assigned_to_user_id, au.name AS assignee_name,
              c.reporter_user_id, ru.name AS reporter_name,
              cat.name AS category_name
            FROM complaints c
            JOIN complaint_categories cat ON cat.id = c.category_id
            JOIN staff_users ru ON ru.id = c.reporter_user_id
            LEFT JOIN staff_users au ON au.id = c.assigned_to_user_id
            WHERE 1=1
        """
        params: List[Any] = []
        clean_scope = str(scope or "").strip().upper()
        if clean_scope:
            if clean_scope not in SCOPE_VALUES:
                raise ValueError("invalid scope filter")
            sql += " AND c.scope=?"
            params.append(clean_scope)
        clean_status = str(status or "").strip().upper()
        if clean_status:
            if clean_status not in STATUS_VALUES:
                raise ValueError("invalid status filter")
            sql += " AND c.status=?"
            params.append(clean_status)
        clean_site_code = str(site_code or "").strip().upper()
        if clean_site_code:
            sql += " AND UPPER(COALESCE(c.site_code,''))=?"
            params.append(clean_site_code)
        sql += " ORDER BY c.created_at DESC, c.id DESC LIMIT ? OFFSET ?"
        params.extend([lim, off])
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def triage_complaint(
    *,
    complaint_id: int,
    actor_user_id: int,
    scope: str,
    priority: str,
    resolution_type: str,
    guidance_template_id: Optional[int],
    note: str = "",
) -> Dict[str, Any]:
    clean_scope = _clean_enum(scope, SCOPE_VALUES, "scope")
    clean_priority = _clean_enum(priority, PRIORITY_VALUES, "priority")
    clean_resolution = _clean_enum(resolution_type, RESOLUTION_VALUES, "resolution_type")
    clean_note = _clean_text(note, required=False, max_len=2000, field="note")

    con = _with_schema()
    try:
        row = _get_complaint_row(con, int(complaint_id))
        if not row:
            raise ValueError("complaint not found")
        current_status = str(row["status"])
        next_status = "TRIAGED"
        triaged_at = now_iso()
        closed_at = None
        assigned_user = row["assigned_to_user_id"]

        effective_guidance_template_id = guidance_template_id
        if clean_scope == "PRIVATE":
            clean_resolution = "GUIDANCE_ONLY"
            next_status = "GUIDANCE_SENT"
            closed_at = triaged_at
            assigned_user = None
            if not effective_guidance_template_id:
                effective_guidance_template_id = _get_default_guidance_template_id(con)
        elif assigned_user:
            next_status = "ASSIGNED"

        con.execute(
            """
            UPDATE complaints
            SET scope=?, priority=?, resolution_type=?, guidance_template_id=?, assigned_to_user_id=?,
                status=?, triaged_at=?, closed_at=?, updated_at=?
            WHERE id=?
            """,
            (
                clean_scope,
                clean_priority,
                clean_resolution,
                int(effective_guidance_template_id) if effective_guidance_template_id else None,
                int(assigned_user) if assigned_user else None,
                next_status,
                triaged_at,
                closed_at,
                now_iso(),
                int(complaint_id),
            ),
        )
        if current_status != next_status:
            _insert_status_history(
                con,
                complaint_id=int(complaint_id),
                from_status=current_status,
                to_status=next_status,
                changed_by_user_id=int(actor_user_id),
                note=clean_note or "triage",
            )
        if clean_note:
            con.execute(
                """
                INSERT INTO complaint_comments(complaint_id, user_id, comment, is_internal, created_at)
                VALUES(?,?,?,?,?)
                """,
                (int(complaint_id), int(actor_user_id), clean_note, 1, now_iso()),
            )
        con.commit()
        return _complaint_detail(con, int(complaint_id), include_internal=True)
    finally:
        con.close()


def assign_complaint(
    *,
    complaint_id: int,
    actor_user_id: int,
    assignee_user_id: int,
    scheduled_at: str = "",
    note: str = "",
) -> Dict[str, Any]:
    clean_note = _clean_text(note, required=False, max_len=2000, field="note")
    clean_scheduled = str(scheduled_at or "").strip()
    con = _with_schema()
    try:
        row = _get_complaint_row(con, int(complaint_id))
        if not row:
            raise ValueError("complaint not found")
        if str(row["scope"]) == "PRIVATE":
            raise ValueError("cannot assign private complaint to work order")
        assignee = con.execute(
            "SELECT id FROM staff_users WHERE id=? AND is_active=1",
            (int(assignee_user_id),),
        ).fetchone()
        if not assignee:
            raise ValueError("assignee_user_id not found or inactive")

        current_status = str(row["status"])
        next_status = "ASSIGNED"
        ts = now_iso()
        con.execute(
            """
            UPDATE complaints
            SET assigned_to_user_id=?, status=?, updated_at=?, closed_at=NULL
            WHERE id=?
            """,
            (int(assignee_user_id), next_status, ts, int(complaint_id)),
        )
        cur = con.execute(
            """
            INSERT INTO complaint_work_orders(
              complaint_id, assignee_user_id, status, scheduled_at, completed_at, result_note, created_at
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                int(complaint_id),
                int(assignee_user_id),
                "OPEN",
                clean_scheduled or None,
                None,
                clean_note or None,
                ts,
            ),
        )
        _insert_status_history(
            con,
            complaint_id=int(complaint_id),
            from_status=current_status,
            to_status=next_status,
            changed_by_user_id=int(actor_user_id),
            note=clean_note or "assigned",
        )
        con.commit()
        detail = _complaint_detail(con, int(complaint_id), include_internal=True)
        detail["new_work_order_id"] = int(cur.lastrowid)
        return detail
    finally:
        con.close()


def update_work_order(
    *,
    work_order_id: int,
    actor_user_id: int,
    status: str,
    result_note: str = "",
) -> Dict[str, Any]:
    clean_status = _clean_enum(status, WORK_ORDER_STATUS_VALUES, "status")
    clean_note = _clean_text(result_note, required=False, max_len=4000, field="result_note")
    con = _with_schema()
    try:
        row = con.execute(
            """
            SELECT id, complaint_id, assignee_user_id, status, scheduled_at, completed_at, result_note, created_at
            FROM complaint_work_orders
            WHERE id=?
            LIMIT 1
            """,
            (int(work_order_id),),
        ).fetchone()
        if not row:
            raise ValueError("work_order not found")
        completed_at = now_iso() if clean_status == "DONE" else None
        con.execute(
            """
            UPDATE complaint_work_orders
            SET status=?, completed_at=?, result_note=COALESCE(?, result_note)
            WHERE id=?
            """,
            (
                clean_status,
                completed_at,
                clean_note or None,
                int(work_order_id),
            ),
        )

        complaint_id = int(row["complaint_id"])
        if clean_status == "DONE":
            c_row = _get_complaint_row(con, complaint_id)
            if c_row:
                c_from = str(c_row["status"])
                c_to = "COMPLETED"
                con.execute(
                    """
                    UPDATE complaints
                    SET status=?, updated_at=?, closed_at=COALESCE(closed_at,?)
                    WHERE id=?
                    """,
                    (c_to, now_iso(), now_iso(), complaint_id),
                )
                if c_from != c_to:
                    _insert_status_history(
                        con,
                        complaint_id=complaint_id,
                        from_status=c_from,
                        to_status=c_to,
                        changed_by_user_id=int(actor_user_id),
                        note="work order done",
                    )
        elif clean_status == "DISPATCHED":
            c_row = _get_complaint_row(con, complaint_id)
            if c_row and str(c_row["status"]) != "IN_PROGRESS":
                con.execute(
                    "UPDATE complaints SET status=?, updated_at=? WHERE id=?",
                    ("IN_PROGRESS", now_iso(), complaint_id),
                )
                _insert_status_history(
                    con,
                    complaint_id=complaint_id,
                    from_status=str(c_row["status"]),
                    to_status="IN_PROGRESS",
                    changed_by_user_id=int(actor_user_id),
                    note="work order dispatched",
                )
        if clean_note:
            con.execute(
                """
                INSERT INTO complaint_comments(complaint_id, user_id, comment, is_internal, created_at)
                VALUES(?,?,?,?,?)
                """,
                (complaint_id, int(actor_user_id), clean_note, 1, now_iso()),
            )
        con.commit()
        out = con.execute(
            """
            SELECT id, complaint_id, assignee_user_id, status, scheduled_at, completed_at, result_note, created_at
            FROM complaint_work_orders
            WHERE id=?
            LIMIT 1
            """,
            (int(work_order_id),),
        ).fetchone()
        return dict(out) if out else {}
    finally:
        con.close()


def create_visit(
    *,
    complaint_id: int,
    visitor_user_id: int,
    visit_reason: str,
    result_note: str = "",
) -> Dict[str, Any]:
    clean_reason = _clean_enum(visit_reason, VISIT_REASON_VALUES, "visit_reason")
    clean_note = _clean_text(result_note, required=False, max_len=4000, field="result_note")
    con = _with_schema()
    try:
        row = _get_complaint_row(con, int(complaint_id))
        if not row:
            raise ValueError("complaint not found")
        unit_label = str(row["unit_label"] or "").strip() or None
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO complaint_visit_logs(
              complaint_id, unit_label, visitor_user_id, visit_reason, check_in_at, check_out_at, result_note, created_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                int(complaint_id),
                unit_label,
                int(visitor_user_id),
                clean_reason,
                ts,
                None,
                clean_note or None,
                ts,
            ),
        )
        con.execute(
            """
            UPDATE complaints
            SET requires_visit=1, visit_reason=?, updated_at=?
            WHERE id=?
            """,
            (clean_reason, now_iso(), int(complaint_id)),
        )
        con.commit()
        out = con.execute(
            """
            SELECT id, complaint_id, unit_label, visitor_user_id, visit_reason, check_in_at, check_out_at, result_note, created_at
            FROM complaint_visit_logs
            WHERE id=?
            LIMIT 1
            """,
            (int(cur.lastrowid),),
        ).fetchone()
        return dict(out) if out else {}
    finally:
        con.close()


def checkout_visit(
    *,
    visit_id: int,
    result_note: str = "",
) -> Dict[str, Any]:
    clean_note = _clean_text(result_note, required=False, max_len=4000, field="result_note")
    con = _with_schema()
    try:
        row = con.execute(
            """
            SELECT id, complaint_id, unit_label, visitor_user_id, visit_reason, check_in_at, check_out_at, result_note, created_at
            FROM complaint_visit_logs
            WHERE id=?
            LIMIT 1
            """,
            (int(visit_id),),
        ).fetchone()
        if not row:
            raise ValueError("visit not found")
        con.execute(
            """
            UPDATE complaint_visit_logs
            SET check_out_at=COALESCE(check_out_at, ?), result_note=COALESCE(?, result_note)
            WHERE id=?
            """,
            (now_iso(), clean_note or None, int(visit_id)),
        )
        con.commit()
        out = con.execute(
            """
            SELECT id, complaint_id, unit_label, visitor_user_id, visit_reason, check_in_at, check_out_at, result_note, created_at
            FROM complaint_visit_logs
            WHERE id=?
            LIMIT 1
            """,
            (int(visit_id),),
        ).fetchone()
        return dict(out) if out else {}
    finally:
        con.close()


def create_notice(
    *,
    author_user_id: int,
    title: str,
    content: str,
    is_pinned: bool = False,
    publish_now: bool = True,
) -> Dict[str, Any]:
    clean_title = _clean_text(title, required=True, max_len=200, field="title")
    clean_content = _clean_text(content, required=True, max_len=20000, field="content")
    ts = now_iso()
    con = _with_schema()
    try:
        cur = con.execute(
            """
            INSERT INTO complaint_notices(title, content, is_pinned, published_at, author_user_id, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                clean_title,
                clean_content,
                1 if is_pinned else 0,
                ts if publish_now else None,
                int(author_user_id),
                ts,
                ts,
            ),
        )
        con.commit()
        row = con.execute(
            """
            SELECT id, title, content, is_pinned, published_at, author_user_id, created_at, updated_at
            FROM complaint_notices
            WHERE id=?
            LIMIT 1
            """,
            (int(cur.lastrowid),),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def update_notice(
    *,
    notice_id: int,
    title: Optional[str],
    content: Optional[str],
    is_pinned: Optional[bool],
    publish_now: bool,
) -> Dict[str, Any]:
    con = _with_schema()
    try:
        row = con.execute(
            """
            SELECT id, title, content, is_pinned, published_at, author_user_id, created_at, updated_at
            FROM complaint_notices
            WHERE id=?
            LIMIT 1
            """,
            (int(notice_id),),
        ).fetchone()
        if not row:
            raise ValueError("notice not found")

        next_title = _clean_text(title, required=True, max_len=200, field="title") if title is not None else str(row["title"])
        next_content = (
            _clean_text(content, required=True, max_len=20000, field="content")
            if content is not None
            else str(row["content"])
        )
        next_pinned = int(bool(is_pinned)) if is_pinned is not None else int(row["is_pinned"] or 0)
        next_published = now_iso() if publish_now else row["published_at"]
        con.execute(
            """
            UPDATE complaint_notices
            SET title=?, content=?, is_pinned=?, published_at=?, updated_at=?
            WHERE id=?
            """,
            (next_title, next_content, next_pinned, next_published, now_iso(), int(notice_id)),
        )
        con.commit()
        out = con.execute(
            """
            SELECT id, title, content, is_pinned, published_at, author_user_id, created_at, updated_at
            FROM complaint_notices
            WHERE id=?
            LIMIT 1
            """,
            (int(notice_id),),
        ).fetchone()
        return dict(out) if out else {}
    finally:
        con.close()


def complaint_stats(*, site_code: str = "") -> Dict[str, Any]:
    con = _with_schema()
    try:
        params: List[Any] = []
        where_clause = ""
        clean_site_code = str(site_code or "").strip().upper()
        if clean_site_code:
            where_clause = " WHERE UPPER(COALESCE(site_code,''))=? "
            params.append(clean_site_code)

        total_row = con.execute(f"SELECT COUNT(*) AS c FROM complaints{where_clause}", tuple(params)).fetchone()
        emergency_row = con.execute(
            f"SELECT COUNT(*) AS c FROM complaints{where_clause}{' AND ' if where_clause else ' WHERE '}scope='EMERGENCY'",
            tuple(params),
        ).fetchone()
        delayed_row = con.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM complaints
            {where_clause}{' AND ' if where_clause else ' WHERE '}
            status NOT IN ('COMPLETED','CLOSED')
            AND ((julianday('now','localtime') - julianday(created_at)) * 24) > 48
            """,
            tuple(params),
        ).fetchone()
        avg_row = con.execute(
            f"""
            SELECT AVG((julianday(closed_at) - julianday(created_at)) * 24.0) AS h
            FROM complaints
            {where_clause}{' AND ' if where_clause else ' WHERE '}
            closed_at IS NOT NULL
            """,
            tuple(params),
        ).fetchone()

        by_status = [
            dict(r)
            for r in con.execute(
                f"""
                SELECT status, COUNT(*) AS count
                FROM complaints
                {where_clause}
                GROUP BY status
                ORDER BY status
                """,
                tuple(params),
            ).fetchall()
        ]
        by_scope = [
            dict(r)
            for r in con.execute(
                f"""
                SELECT scope, COUNT(*) AS count
                FROM complaints
                {where_clause}
                GROUP BY scope
                ORDER BY scope
                """,
                tuple(params),
            ).fetchall()
        ]
        return {
            "total_count": int(total_row["c"] if total_row else 0),
            "emergency_count": int(emergency_row["c"] if emergency_row else 0),
            "delayed_count": int(delayed_row["c"] if delayed_row else 0),
            "avg_resolution_hours": float(avg_row["h"]) if avg_row and avg_row["h"] is not None else None,
            "by_status": by_status,
            "by_scope": by_scope,
        }
    finally:
        con.close()
