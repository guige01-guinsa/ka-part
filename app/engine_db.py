from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .db import DB_PATH, now_iso

COMPLAINT_TYPES = ("주차", "소음", "승강기", "전기", "수도", "누수", "시설", "미화", "경비", "관리비", "기타")
URGENCY_VALUES = ("긴급", "당일", "일반", "단순문의")
STATUS_VALUES = ("접수", "처리중", "완료", "이월")
CHANNEL_VALUES = ("전화", "카톡", "방문", "앱", "기타")
MAX_ATTACHMENTS_PER_COMPLAINT = 6


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH), timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    try:
        con.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass
    return con


def _clean_text(value: Any, *, field: str, required: bool, max_len: int) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_len:
        raise ValueError(f"{field} length must be <= {max_len}")
    return text


def _clean_choice(value: Any, allowed: Tuple[str, ...], *, field: str, default: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        text = default
    if text not in allowed:
        raise ValueError(f"{field} must be one of: {', '.join(allowed)}")
    return text


def _ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(row["name"]) for row in rows}
    if column not in names:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS complaints (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL,
          building TEXT,
          unit TEXT,
          complainant_phone TEXT,
          channel TEXT NOT NULL,
          content TEXT NOT NULL,
          summary TEXT,
          type TEXT NOT NULL,
          urgency TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT '접수',
          manager TEXT,
          image_url TEXT,
          source_text TEXT,
          ai_model TEXT,
          repeat_count INTEGER NOT NULL DEFAULT 0,
          created_by_user_id INTEGER,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          closed_at TEXT,
          FOREIGN KEY(created_by_user_id) REFERENCES staff_users(id) ON DELETE SET NULL,
          FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS complaint_attachments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          complaint_id INTEGER NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
          file_url TEXT NOT NULL,
          mime_type TEXT,
          size_bytes INTEGER,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS complaint_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          complaint_id INTEGER NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
          from_status TEXT,
          to_status TEXT NOT NULL,
          note TEXT,
          actor_label TEXT,
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_engine_complaints_tenant_created
          ON complaints(tenant_id, created_at DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_engine_complaints_tenant_status
          ON complaints(tenant_id, status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_engine_complaints_tenant_type
          ON complaints(tenant_id, type, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_engine_history_complaint
          ON complaint_history(complaint_id, id ASC);
        CREATE INDEX IF NOT EXISTS idx_engine_attachments_complaint
          ON complaint_attachments(complaint_id, id ASC);
        """
    )
    _ensure_column(con, "complaints", "complainant_phone", "complainant_phone TEXT")


def init_engine_db() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        con.commit()
    finally:
        con.close()


def _detail(con: sqlite3.Connection, complaint_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, building, unit, complainant_phone, channel, content, summary, type, urgency, status,
          manager, image_url, source_text, ai_model, repeat_count, created_by_user_id,
          created_by_label, created_at, updated_at, closed_at
        FROM complaints
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(complaint_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("complaint not found")
    item = dict(row)
    item["attachments"] = [
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
    item["history"] = [
        dict(r)
        for r in con.execute(
            """
            SELECT id, complaint_id, from_status, to_status, note, actor_label, created_at
            FROM complaint_history
            WHERE complaint_id=?
            ORDER BY id ASC
            """,
            (int(complaint_id),),
        ).fetchall()
    ]
    return item


def _insert_history(
    con: sqlite3.Connection,
    *,
    complaint_id: int,
    from_status: Optional[str],
    to_status: str,
    actor_label: str,
    note: str = "",
) -> None:
    con.execute(
        """
        INSERT INTO complaint_history(complaint_id, from_status, to_status, note, actor_label, created_at)
        VALUES(?,?,?,?,?,?)
        """,
        (
            int(complaint_id),
            from_status,
            str(to_status or "").strip(),
            str(note or "").strip() or None,
            str(actor_label or "").strip()[:120] or None,
            now_iso(),
        ),
    )


def _repeat_count(
    con: sqlite3.Connection,
    *,
    tenant_id: str,
    building: str,
    unit: str,
    complaint_type: str,
) -> int:
    row = con.execute(
        """
        SELECT COUNT(*) AS c
        FROM complaints
        WHERE tenant_id=?
          AND COALESCE(building,'')=?
          AND COALESCE(unit,'')=?
          AND type=?
        """,
        (
            str(tenant_id or "").strip().lower(),
            str(building or "").strip(),
            str(unit or "").strip(),
            str(complaint_type or "").strip(),
        ),
    ).fetchone()
    return int(row["c"] if row else 0)


def _attachment_rows(con: sqlite3.Connection, complaint_id: int) -> List[Dict[str, Any]]:
    return [
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


def create_complaint(
    *,
    tenant_id: str,
    building: str,
    unit: str,
    complainant_phone: str,
    channel: str,
    content: str,
    summary: str,
    complaint_type: str,
    urgency: str,
    status: str = "접수",
    manager: str = "",
    image_url: str = "",
    source_text: str = "",
    ai_model: str = "",
    created_by_user_id: Optional[int] = None,
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    if not clean_tenant_id:
        raise ValueError("tenant_id is required")
    clean_building = _clean_text(building, field="building", required=False, max_len=20)
    clean_unit = _clean_text(unit, field="unit", required=False, max_len=20)
    clean_phone = _clean_text(complainant_phone, field="complainant_phone", required=False, max_len=40)
    clean_channel = _clean_choice(channel, CHANNEL_VALUES, field="channel", default="기타")
    clean_content = _clean_text(content, field="content", required=True, max_len=8000)
    clean_summary = _clean_text(summary, field="summary", required=False, max_len=160)
    clean_type = _clean_choice(complaint_type, COMPLAINT_TYPES, field="type", default="기타")
    clean_urgency = _clean_choice(urgency, URGENCY_VALUES, field="urgency", default="일반")
    clean_status = _clean_choice(status, STATUS_VALUES, field="status", default="접수")
    clean_manager = _clean_text(manager, field="manager", required=False, max_len=60)
    clean_image_url = _clean_text(image_url, field="image_url", required=False, max_len=500)
    clean_source_text = _clean_text(source_text, field="source_text", required=False, max_len=20000)
    clean_ai_model = _clean_text(ai_model, field="ai_model", required=False, max_len=80)
    clean_actor = _clean_text(created_by_label, field="created_by_label", required=False, max_len=120) or "system"

    con = _connect()
    try:
        _ensure_schema(con)
        repeat_count = _repeat_count(
            con,
            tenant_id=clean_tenant_id,
            building=clean_building,
            unit=clean_unit,
            complaint_type=clean_type,
        ) + 1
        ts = now_iso()
        closed_at = ts if clean_status == "완료" else None
        cur = con.execute(
            """
            INSERT INTO complaints(
              tenant_id, building, unit, complainant_phone, channel, content, summary, type, urgency, status,
              manager, image_url, source_text, ai_model, repeat_count,
              created_by_user_id, created_by_label, created_at, updated_at, closed_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_building or None,
                clean_unit or None,
                clean_phone or None,
                clean_channel,
                clean_content,
                clean_summary or None,
                clean_type,
                clean_urgency,
                clean_status,
                clean_manager or None,
                clean_image_url or None,
                clean_source_text or None,
                clean_ai_model or None,
                repeat_count,
                int(created_by_user_id) if created_by_user_id else None,
                clean_actor,
                ts,
                ts,
                closed_at,
            ),
        )
        complaint_id = int(cur.lastrowid)
        _insert_history(
            con,
            complaint_id=complaint_id,
            from_status=None,
            to_status=clean_status,
            actor_label=clean_actor,
            note="최초 접수",
        )
        con.commit()
        return _detail(con, complaint_id, clean_tenant_id)
    finally:
        con.close()


def add_attachment(
    *,
    tenant_id: str,
    complaint_id: int,
    file_url: str,
    mime_type: str = "",
    size_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    clean_url = _clean_text(file_url, field="file_url", required=True, max_len=500)
    clean_mime = _clean_text(mime_type, field="mime_type", required=False, max_len=120)
    size_value = int(size_bytes) if size_bytes is not None else None
    clean_tenant_id = str(tenant_id or "").strip().lower()
    con = _connect()
    try:
        _ensure_schema(con)
        detail = _detail(con, int(complaint_id), clean_tenant_id)
        if len(detail.get("attachments") or []) >= MAX_ATTACHMENTS_PER_COMPLAINT:
            raise ValueError(f"attachments limit exceeded: max {MAX_ATTACHMENTS_PER_COMPLAINT}")
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO complaint_attachments(complaint_id, file_url, mime_type, size_bytes, created_at)
            VALUES(?,?,?,?,?)
            """,
            (int(complaint_id), clean_url, clean_mime or None, size_value, ts),
        )
        con.execute(
            """
            UPDATE complaints
            SET image_url=COALESCE(image_url, ?), updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (clean_url, ts, int(complaint_id), clean_tenant_id),
        )
        con.commit()
        row = con.execute(
            """
            SELECT id, complaint_id, file_url, mime_type, size_bytes, created_at
            FROM complaint_attachments
            WHERE id=?
            LIMIT 1
            """,
            (int(cur.lastrowid),),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def delete_attachments(
    *,
    tenant_id: str,
    complaint_id: int,
    attachment_ids: Optional[List[int]] = None,
    delete_all: bool = False,
) -> Dict[str, Any]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    con = _connect()
    try:
        _ensure_schema(con)
        _detail(con, int(complaint_id), clean_tenant_id)
        rows = _attachment_rows(con, int(complaint_id))
        if delete_all:
            target_rows = rows
        else:
            wanted = {int(value) for value in (attachment_ids or [])}
            target_rows = [row for row in rows if int(row.get("id") or 0) in wanted]
        if not target_rows:
            raise ValueError("attachment not found")
        con.executemany(
            "DELETE FROM complaint_attachments WHERE id=? AND complaint_id=?",
            [(int(row["id"]), int(complaint_id)) for row in target_rows],
        )
        remaining = _attachment_rows(con, int(complaint_id))
        primary_image = str(remaining[0]["file_url"]) if remaining else None
        con.execute(
            """
            UPDATE complaints
            SET image_url=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (primary_image, now_iso(), int(complaint_id), clean_tenant_id),
        )
        con.commit()
        return {
            "deleted": target_rows,
            "remaining": remaining,
            "complaint": _detail(con, int(complaint_id), clean_tenant_id),
        }
    finally:
        con.close()


def delete_complaint(*, tenant_id: str, complaint_id: int) -> Dict[str, Any]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _detail(con, int(complaint_id), clean_tenant_id)
        con.execute(
            "DELETE FROM complaints WHERE id=? AND tenant_id=?",
            (int(complaint_id), clean_tenant_id),
        )
        con.commit()
        return item
    finally:
        con.close()


def list_complaints(
    *,
    tenant_id: str,
    status: str = "",
    building: str = "",
    unit: str = "",
    complaint_type: str = "",
    limit: int = 100,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    if not clean_tenant_id:
        raise ValueError("tenant_id is required")
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, building, unit, complainant_phone, channel, content, summary, type, urgency, status,
              manager, image_url, repeat_count, created_by_label, created_at, updated_at, closed_at
            FROM complaints
            WHERE tenant_id=?
        """
        params: List[Any] = [clean_tenant_id]
        clean_status = str(status or "").strip()
        if clean_status:
            clean_status = _clean_choice(clean_status, STATUS_VALUES, field="status")
            sql += " AND status=?"
            params.append(clean_status)
        clean_building = str(building or "").strip()
        if clean_building:
            sql += " AND COALESCE(building,'')=?"
            params.append(clean_building)
        clean_unit = str(unit or "").strip()
        if clean_unit:
            sql += " AND COALESCE(unit,'')=?"
            params.append(clean_unit)
        clean_type = str(complaint_type or "").strip()
        if clean_type:
            clean_type = _clean_choice(clean_type, COMPLAINT_TYPES, field="type")
            sql += " AND type=?"
            params.append(clean_type)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
        params.extend([max(1, min(500, int(limit))), max(0, int(offset))])
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def get_complaint(*, tenant_id: str, complaint_id: int) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        try:
            return _detail(con, int(complaint_id), str(tenant_id or "").strip().lower())
        except ValueError:
            return None
    finally:
        con.close()


def update_complaint(
    *,
    tenant_id: str,
    complaint_id: int,
    status: str,
    actor_label: str,
    manager: str = "",
    note: str = "",
    summary: str = "",
    complaint_type: str = "",
    urgency: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    clean_status = _clean_choice(status, STATUS_VALUES, field="status")
    clean_actor = _clean_text(actor_label, field="actor_label", required=False, max_len=120) or "operator"
    clean_manager = _clean_text(manager, field="manager", required=False, max_len=60)
    clean_note = _clean_text(note, field="note", required=False, max_len=4000)
    clean_summary = _clean_text(summary, field="summary", required=False, max_len=160)
    clean_type = _clean_choice(complaint_type, COMPLAINT_TYPES, field="type", default="") if complaint_type else ""
    clean_urgency = _clean_choice(urgency, URGENCY_VALUES, field="urgency", default="") if urgency else ""

    con = _connect()
    try:
        _ensure_schema(con)
        current = _detail(con, int(complaint_id), clean_tenant_id)
        next_manager = clean_manager if clean_manager is not None else current.get("manager")
        next_summary = clean_summary if clean_summary is not None else current.get("summary")
        next_type = clean_type or str(current.get("type") or "")
        next_urgency = clean_urgency or str(current.get("urgency") or "")
        closed_at = now_iso() if clean_status == "완료" else None
        con.execute(
            """
            UPDATE complaints
            SET status=?, manager=?, summary=?, type=?, urgency=?, updated_at=?, closed_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                clean_status,
                next_manager or None,
                next_summary or None,
                next_type,
                next_urgency,
                now_iso(),
                closed_at,
                int(complaint_id),
                clean_tenant_id,
            ),
        )
        _insert_history(
            con,
            complaint_id=int(complaint_id),
            from_status=str(current.get("status") or ""),
            to_status=clean_status,
            actor_label=clean_actor,
            note=clean_note or "",
        )
        con.commit()
        return _detail(con, int(complaint_id), clean_tenant_id)
    finally:
        con.close()


def dashboard_summary(*, tenant_id: str, target_day: str = "") -> Dict[str, Any]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    if not clean_tenant_id:
        raise ValueError("tenant_id is required")
    target = date.fromisoformat(target_day) if str(target_day or "").strip() else date.today()
    day_text = target.isoformat()
    con = _connect()
    try:
        _ensure_schema(con)
        total_row = con.execute(
            """
            SELECT COUNT(*) AS c
            FROM complaints
            WHERE tenant_id=? AND date(created_at)=?
            """,
            (clean_tenant_id, day_text),
        ).fetchone()
        done_row = con.execute(
            """
            SELECT COUNT(*) AS c
            FROM complaints
            WHERE tenant_id=? AND date(created_at)=? AND status='완료'
            """,
            (clean_tenant_id, day_text),
        ).fetchone()
        pending_row = con.execute(
            """
            SELECT COUNT(*) AS c
            FROM complaints
            WHERE tenant_id=? AND status IN ('접수','처리중','이월')
            """,
            (clean_tenant_id,),
        ).fetchone()
        carry_row = con.execute(
            """
            SELECT COUNT(*) AS c
            FROM complaints
            WHERE tenant_id=? AND date(created_at)<? AND status IN ('접수','처리중','이월')
            """,
            (clean_tenant_id, day_text),
        ).fetchone()
        urgent_items = [
            dict(r)
            for r in con.execute(
                """
                SELECT id, building, unit, summary, type, status, urgency, created_at
                FROM complaints
                WHERE tenant_id=? AND urgency='긴급' AND status!='완료'
                ORDER BY created_at DESC, id DESC
                LIMIT 5
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        type_counts = [
            dict(r)
            for r in con.execute(
                """
                SELECT type, COUNT(*) AS count
                FROM complaints
                WHERE tenant_id=? AND date(created_at)=?
                GROUP BY type
                ORDER BY count DESC, type ASC
                """,
                (clean_tenant_id, day_text),
            ).fetchall()
        ]
        pending_top = [
            dict(r)
            for r in con.execute(
                """
                SELECT id, building, unit, summary, type, urgency, status, manager, created_at
                FROM complaints
                WHERE tenant_id=? AND status IN ('접수','처리중','이월')
                ORDER BY urgency='긴급' DESC, created_at ASC, id ASC
                LIMIT 5
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        manager_load = [
            dict(r)
            for r in con.execute(
                """
                SELECT COALESCE(manager,'미배정') AS manager, COUNT(*) AS count
                FROM complaints
                WHERE tenant_id=? AND status IN ('접수','처리중','이월')
                GROUP BY COALESCE(manager,'미배정')
                ORDER BY count DESC, manager ASC
                LIMIT 10
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        repeat_items = [
            dict(r)
            for r in con.execute(
                """
                SELECT building, unit, type, COUNT(*) AS count
                FROM complaints
                WHERE tenant_id=?
                GROUP BY building, unit, type
                HAVING COUNT(*) > 1
                ORDER BY count DESC, type ASC
                LIMIT 10
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        return {
            "target_day": day_text,
            "today_total": int(total_row["c"] if total_row else 0),
            "today_done": int(done_row["c"] if done_row else 0),
            "pending_total": int(pending_row["c"] if pending_row else 0),
            "carry_total": int(carry_row["c"] if carry_row else 0),
            "urgent_items": urgent_items,
            "type_counts": type_counts,
            "pending_top5": pending_top,
            "manager_load": manager_load,
            "repeat_items": repeat_items,
        }
    finally:
        con.close()


def generate_daily_report(*, tenant_id: str, target_day: str = "") -> Dict[str, Any]:
    clean_tenant_id = str(tenant_id or "").strip().lower()
    if not clean_tenant_id:
        raise ValueError("tenant_id is required")
    target = date.fromisoformat(target_day) if str(target_day or "").strip() else date.today()
    day_text = target.isoformat()
    con = _connect()
    try:
        _ensure_schema(con)
        rows = [
            dict(r)
            for r in con.execute(
                """
                SELECT
                  id, building, unit, complainant_phone, channel, content, summary, type, urgency, status,
                  manager, image_url, repeat_count, created_at, updated_at
                FROM complaints
                WHERE tenant_id=? AND date(created_at)=?
                ORDER BY created_at DESC, id DESC
                """,
                (clean_tenant_id, day_text),
            ).fetchall()
        ]
        total = len(rows)
        done = sum(1 for row in rows if row.get("status") == "완료")
        carry = con.execute(
            """
            SELECT COUNT(*) AS c
            FROM complaints
            WHERE tenant_id=? AND date(created_at)<? AND status IN ('접수','처리중','이월')
            """,
            (clean_tenant_id, day_text),
        ).fetchone()
        carry_count = int(carry["c"] if carry else 0)
        pending = total - done
        urgent_rows = [row for row in rows if row.get("urgency") == "긴급" and row.get("status") != "완료"]
        major_rows = sorted(
            rows,
            key=lambda row: (
                0 if row.get("urgency") == "긴급" else 1,
                0 if row.get("status") != "완료" else 1,
                -int(row.get("repeat_count") or 0),
                row.get("created_at") or "",
            ),
        )[:5]
        tomorrow_rows = [row for row in rows if row.get("status") in {"접수", "처리중", "이월"}][:5]
        issues = [f"{row.get('building') or '-'}동 {row.get('summary') or row.get('type')}" for row in major_rows[:3]]

        report_lines = [
            "📊 일일 요약",
            f"총 민원: {total}",
            f"완료: {done}",
            f"진행: {pending}",
            f"이월: {carry_count}",
            "",
            "🚨 긴급 민원",
        ]
        if urgent_rows:
            for row in urgent_rows[:5]:
                report_lines.append(f"- {(row.get('building') or '-') }동 {(row.get('summary') or row.get('content') or '-')}")
        else:
            report_lines.append("없음")
        report_lines.extend(["", "🔧 주요 민원"])
        if major_rows:
            for row in major_rows:
                report_lines.append(f"- {(row.get('building') or '-') }동 / {row.get('type')} / {row.get('summary') or row.get('content')}")
        else:
            report_lines.append("없음")
        report_lines.extend(["", "📌 내일 처리"])
        if tomorrow_rows:
            for row in tomorrow_rows:
                report_lines.append(f"- {(row.get('building') or '-') }동 / {row.get('status')} / {row.get('summary') or row.get('content')}")
        else:
            report_lines.append("없음")

        return {
            "target_day": day_text,
            "total": total,
            "done": done,
            "pending": pending,
            "carry": carry_count,
            "issues": issues,
            "urgent_items": urgent_rows,
            "major_items": major_rows,
            "tomorrow_items": tomorrow_rows,
            "items": rows,
            "report_text": "\n".join(report_lines),
        }
    finally:
        con.close()
