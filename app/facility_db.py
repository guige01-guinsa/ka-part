from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

from .db import DB_PATH, now_iso

ASSET_CATEGORY_VALUES = ("승강기", "전기", "기계", "소방", "건축", "미화", "보안", "공용부", "기타")
ASSET_LIFECYCLE_VALUES = ("운영중", "점검중", "중지", "폐기")
QR_LIFECYCLE_VALUES = ("운영중", "중지", "폐기")
INSPECTION_STATUS_VALUES = ("정상", "주의", "조치필요")
WORK_ORDER_CATEGORY_VALUES = ("점검후속", "고장수리", "예방정비", "외주요청", "기타")
WORK_ORDER_PRIORITY_VALUES = ("낮음", "보통", "높음", "긴급")
WORK_ORDER_STATUS_VALUES = ("접수", "진행중", "완료", "보류")


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


def _clean_choice(value: Any, allowed: Sequence[str], *, field: str, default: str) -> str:
    text = str(value or "").strip() or str(default or "")
    if text not in allowed:
        raise ValueError(f"{field} must be one of: {', '.join(allowed)}")
    return text


def _clean_int(value: Any, *, field: str, default: int, minimum: int = 1, maximum: int = 3650) -> int:
    raw = str(value or "").strip()
    if not raw:
        return int(default)
    try:
        parsed = int(raw)
    except Exception as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return parsed


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


def _clean_datetime(value: Any, *, field: str, required: bool = False) -> str:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise ValueError(f"{field} is required")
        return ""
    normalized = raw.replace("T", " ")
    try:
        datetime.fromisoformat(normalized)
    except Exception as exc:
        raise ValueError(f"{field} must be ISO datetime") from exc
    return normalized


def _normalize_key(value: Any, *, field: str, prefix: str, max_len: int = 80) -> str:
    text = _clean_text(value, field=field, required=False, max_len=max_len).upper()
    text = "".join(ch for ch in text if ch.isalnum() or ch in {"-", "_"})
    if text:
        return text[:max_len]
    return f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S')}"


def _normalize_json_text(value: Any, *, field: str, max_len: int = 12000) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
    else:
        try:
            text = json.dumps(value, ensure_ascii=False)
        except Exception as exc:
            raise ValueError(f"{field} must be JSON serializable") from exc
    if len(text) > max_len:
        raise ValueError(f"{field} length must be <= {max_len}")
    return text


def _normalize_items(value: Any) -> List[str]:
    if isinstance(value, str):
        raw_items = [line.strip() for line in value.splitlines()]
    elif isinstance(value, (list, tuple)):
        raw_items = [str(item or "").strip() for item in value]
    else:
        raw_items = []
    items: List[str] = []
    for item in raw_items:
        if not item:
            continue
        if len(item) > 200:
            raise ValueError("checklist item length must be <= 200")
        items.append(item)
    return items


def _ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(row["name"]) for row in rows}
    if column not in names:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _calc_next_inspection_date(inspected_at: str, cycle_days: int) -> str:
    raw = str(inspected_at or "").strip()
    if not raw:
        return ""
    try:
        base_day = date.fromisoformat(raw[:10])
    except Exception:
        return ""
    return (base_day + timedelta(days=max(1, int(cycle_days or 30)))).isoformat()


def _normalize_complaint_id(value: Any) -> Optional[int]:
    if value in (None, "", 0, "0"):
        return None
    return int(value)


def _sync_asset_after_inspection(
    con: sqlite3.Connection,
    *,
    tenant_id: str,
    asset_id: Optional[int],
    inspected_at: str,
    result_status: str,
    updated_at: str,
) -> None:
    if not asset_id:
        return
    row = con.execute(
        """
        SELECT inspection_cycle_days
        FROM facility_assets
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(asset_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        return
    cycle_days = _clean_int(row["inspection_cycle_days"], field="inspection_cycle_days", default=30)
    next_date = _calc_next_inspection_date(inspected_at, cycle_days)
    con.execute(
        """
        UPDATE facility_assets
        SET last_inspected_at=?, next_inspection_date=?, last_result_status=?, updated_at=?
        WHERE id=? AND tenant_id=?
        """,
        (
            str(inspected_at or "").strip(),
            next_date,
            str(result_status or "").strip() or "정상",
            str(updated_at or now_iso()),
            int(asset_id),
            str(tenant_id or "").strip().lower(),
        ),
    )


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS facility_assets (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          asset_code TEXT NOT NULL,
          asset_name TEXT NOT NULL,
          category TEXT NOT NULL,
          location_name TEXT,
          vendor_name TEXT,
          installed_on TEXT,
          inspection_cycle_days INTEGER NOT NULL DEFAULT 30,
          last_result_status TEXT,
          lifecycle_state TEXT NOT NULL DEFAULT '운영중',
          source TEXT NOT NULL DEFAULT 'manual',
          qr_id TEXT,
          checklist_key TEXT,
          last_inspected_at TEXT,
          next_inspection_date TEXT,
          note TEXT,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(tenant_id, asset_code)
        );

        CREATE TABLE IF NOT EXISTS facility_checklists (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          checklist_key TEXT NOT NULL,
          title TEXT NOT NULL,
          task_type TEXT,
          version_no TEXT,
          lifecycle_state TEXT NOT NULL DEFAULT '운영중',
          source TEXT NOT NULL DEFAULT 'manual',
          note TEXT,
          items_json TEXT NOT NULL DEFAULT '[]',
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(tenant_id, checklist_key)
        );

        CREATE TABLE IF NOT EXISTS facility_qr_assets (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          qr_id TEXT NOT NULL,
          asset_id INTEGER REFERENCES facility_assets(id) ON DELETE SET NULL,
          asset_code_snapshot TEXT,
          asset_name_snapshot TEXT,
          location_snapshot TEXT,
          default_item TEXT,
          checklist_key TEXT,
          lifecycle_state TEXT NOT NULL DEFAULT '운영중',
          source TEXT NOT NULL DEFAULT 'manual',
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(tenant_id, qr_id)
        );

        CREATE TABLE IF NOT EXISTS facility_inspections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          title TEXT NOT NULL,
          asset_id INTEGER REFERENCES facility_assets(id) ON DELETE SET NULL,
          qr_asset_id INTEGER REFERENCES facility_qr_assets(id) ON DELETE SET NULL,
          checklist_key TEXT,
          inspector TEXT,
          inspected_at TEXT NOT NULL,
          result_status TEXT NOT NULL DEFAULT '정상',
          notes TEXT,
          measurement_json TEXT,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS facility_work_orders (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          title TEXT NOT NULL,
          description TEXT,
          asset_id INTEGER REFERENCES facility_assets(id) ON DELETE SET NULL,
          qr_asset_id INTEGER REFERENCES facility_qr_assets(id) ON DELETE SET NULL,
          inspection_id INTEGER REFERENCES facility_inspections(id) ON DELETE SET NULL,
          complaint_id INTEGER REFERENCES complaints(id) ON DELETE SET NULL,
          category TEXT NOT NULL DEFAULT '기타',
          priority TEXT NOT NULL DEFAULT '보통',
          status TEXT NOT NULL DEFAULT '접수',
          assignee TEXT,
          reporter TEXT,
          due_date TEXT,
          completed_at TEXT,
          resolution_notes TEXT,
          is_escalated INTEGER NOT NULL DEFAULT 0 CHECK(is_escalated IN (0,1)),
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_facility_assets_tenant
          ON facility_assets(tenant_id, category, lifecycle_state, asset_name ASC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_facility_checklists_tenant
          ON facility_checklists(tenant_id, lifecycle_state, task_type, title ASC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_facility_qr_assets_tenant
          ON facility_qr_assets(tenant_id, lifecycle_state, qr_id, id DESC);
        CREATE INDEX IF NOT EXISTS idx_facility_inspections_tenant
          ON facility_inspections(tenant_id, inspected_at DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_facility_work_orders_tenant
          ON facility_work_orders(tenant_id, status, priority, due_date ASC, id DESC);
        """
    )
    _ensure_column(con, "facility_assets", "vendor_name", "vendor_name TEXT")
    _ensure_column(con, "facility_assets", "installed_on", "installed_on TEXT")
    _ensure_column(con, "facility_assets", "inspection_cycle_days", "inspection_cycle_days INTEGER NOT NULL DEFAULT 30")
    _ensure_column(con, "facility_assets", "last_result_status", "last_result_status TEXT")
    _ensure_column(con, "facility_work_orders", "complaint_id", "complaint_id INTEGER")
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_facility_work_orders_complaint
          ON facility_work_orders(tenant_id, complaint_id, id DESC)
        """
    )


def init_facility_db() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        con.commit()
    finally:
        con.close()


def _asset_detail(con: sqlite3.Connection, asset_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, asset_code, asset_name, category, location_name, vendor_name, installed_on,
          inspection_cycle_days, last_result_status, lifecycle_state, source, qr_id,
          checklist_key, last_inspected_at, next_inspection_date, note, created_by_label, created_at, updated_at
        FROM facility_assets
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(asset_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("asset not found")
    return dict(row)


def _checklist_detail(con: sqlite3.Connection, checklist_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, checklist_key, title, task_type, version_no, lifecycle_state, source,
          note, items_json, created_by_label, created_at, updated_at
        FROM facility_checklists
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(checklist_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("checklist not found")
    item = dict(row)
    try:
        item["items"] = json.loads(str(item.get("items_json") or "[]"))
    except Exception:
        item["items"] = []
    return item


def _qr_asset_detail(con: sqlite3.Connection, qr_asset_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          q.id, q.tenant_id, q.qr_id, q.asset_id, q.asset_code_snapshot, q.asset_name_snapshot,
          q.location_snapshot, q.default_item, q.checklist_key, q.lifecycle_state, q.source,
          q.created_by_label, q.created_at, q.updated_at,
          a.asset_code, a.asset_name, a.category
        FROM facility_qr_assets q
        LEFT JOIN facility_assets a ON a.id = q.asset_id
        WHERE q.id=? AND q.tenant_id=?
        LIMIT 1
        """,
        (int(qr_asset_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("qr asset not found")
    return dict(row)


def _inspection_detail(con: sqlite3.Connection, inspection_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          i.id, i.tenant_id, i.title, i.asset_id, i.qr_asset_id, i.checklist_key, i.inspector,
          i.inspected_at, i.result_status, i.notes, i.measurement_json, i.created_by_label,
          i.created_at, i.updated_at,
          a.asset_code, a.asset_name, a.location_name, q.qr_id
        FROM facility_inspections i
        LEFT JOIN facility_assets a ON a.id = i.asset_id
        LEFT JOIN facility_qr_assets q ON q.id = i.qr_asset_id
        WHERE i.id=? AND i.tenant_id=?
        LIMIT 1
        """,
        (int(inspection_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("inspection not found")
    item = dict(row)
    try:
        item["measurement"] = json.loads(str(item.get("measurement_json") or "{}"))
    except Exception:
        item["measurement"] = {}
    return item


def _work_order_detail(con: sqlite3.Connection, work_order_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          w.id, w.tenant_id, w.title, w.description, w.asset_id, w.qr_asset_id, w.inspection_id,
          w.complaint_id, w.category, w.priority, w.status, w.assignee, w.reporter, w.due_date, w.completed_at,
          w.resolution_notes, w.is_escalated, w.created_by_label, w.created_at, w.updated_at,
          a.asset_code, a.asset_name, a.category AS asset_category, a.location_name, q.qr_id,
          c.summary AS complaint_summary, c.status AS complaint_status
        FROM facility_work_orders w
        LEFT JOIN facility_assets a ON a.id = w.asset_id
        LEFT JOIN facility_qr_assets q ON q.id = w.qr_asset_id
        LEFT JOIN complaints c ON c.id = w.complaint_id AND c.tenant_id = w.tenant_id
        WHERE w.id=? AND w.tenant_id=?
        LIMIT 1
        """,
        (int(work_order_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("work order not found")
    return dict(row)


def create_asset(
    *,
    tenant_id: str,
    asset_code: str,
    asset_name: str,
    category: str,
    location_name: str = "",
    vendor_name: str = "",
    installed_on: str = "",
    inspection_cycle_days: int = 30,
    lifecycle_state: str = "운영중",
    source: str = "manual",
    qr_id: str = "",
    checklist_key: str = "",
    last_inspected_at: str = "",
    next_inspection_date: str = "",
    note: str = "",
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_asset_code = _normalize_key(asset_code, field="asset_code", prefix="AST")
    clean_asset_name = _clean_text(asset_name, field="asset_name", required=True, max_len=160)
    clean_category = _clean_choice(category, ASSET_CATEGORY_VALUES, field="category", default="기타")
    clean_location = _clean_text(location_name, field="location_name", max_len=160)
    clean_vendor = _clean_text(vendor_name, field="vendor_name", max_len=120)
    clean_installed_on = _clean_date(installed_on, field="installed_on") if str(installed_on or "").strip() else ""
    clean_cycle_days = _clean_int(inspection_cycle_days, field="inspection_cycle_days", default=30)
    clean_lifecycle = _clean_choice(lifecycle_state, ASSET_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중")
    clean_source = _clean_text(source, field="source", max_len=40) or "manual"
    clean_qr_id = _normalize_key(qr_id, field="qr_id", prefix="QR", max_len=80) if str(qr_id or "").strip() else ""
    clean_checklist_key = _normalize_key(checklist_key, field="checklist_key", prefix="CHK", max_len=80) if str(checklist_key or "").strip() else ""
    clean_last_inspected_at = _clean_datetime(last_inspected_at, field="last_inspected_at") if str(last_inspected_at or "").strip() else ""
    clean_next_inspection_date = _clean_date(next_inspection_date, field="next_inspection_date") if str(next_inspection_date or "").strip() else ""
    clean_note = _clean_text(note, field="note", max_len=4000)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO facility_assets(
              tenant_id, asset_code, asset_name, category, location_name, vendor_name, installed_on, inspection_cycle_days,
              lifecycle_state, source, qr_id,
              checklist_key, last_inspected_at, next_inspection_date, note, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_asset_code,
                clean_asset_name,
                clean_category,
                clean_location,
                clean_vendor,
                clean_installed_on or None,
                clean_cycle_days,
                clean_lifecycle,
                clean_source,
                clean_qr_id,
                clean_checklist_key,
                clean_last_inspected_at,
                clean_next_inspection_date,
                clean_note,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _asset_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_assets(*, tenant_id: str, category: str = "", lifecycle_state: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clauses = ["tenant_id=?"]
    params: List[Any] = [clean_tenant_id]
    if category:
        clauses.append("category=?")
        params.append(_clean_choice(category, ASSET_CATEGORY_VALUES, field="category", default="기타"))
    if lifecycle_state:
        clauses.append("lifecycle_state=?")
        params.append(_clean_choice(lifecycle_state, ASSET_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중"))
    params.append(max(1, min(int(limit), 500)))
    con = _connect()
    try:
        _ensure_schema(con)
        return [
            dict(row)
            for row in con.execute(
                f"""
                SELECT
                  id, tenant_id, asset_code, asset_name, category, location_name, vendor_name, installed_on,
                  inspection_cycle_days, last_result_status, lifecycle_state, source, qr_id,
                  checklist_key, last_inspected_at, next_inspection_date, note, created_by_label, created_at, updated_at
                FROM facility_assets
                WHERE {' AND '.join(clauses)}
                ORDER BY
                  CASE lifecycle_state WHEN '운영중' THEN 0 WHEN '점검중' THEN 1 WHEN '중지' THEN 2 ELSE 3 END,
                  CASE WHEN next_inspection_date IS NULL OR next_inspection_date='' THEN 1 ELSE 0 END,
                  next_inspection_date ASC,
                  asset_name ASC,
                  id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        ]
    finally:
        con.close()


def update_asset(
    asset_id: int,
    *,
    tenant_id: str,
    asset_code: Any = None,
    asset_name: Any = None,
    category: Any = None,
    location_name: Any = None,
    vendor_name: Any = None,
    installed_on: Any = None,
    inspection_cycle_days: Any = None,
    lifecycle_state: Any = None,
    source: Any = None,
    qr_id: Any = None,
    checklist_key: Any = None,
    last_inspected_at: Any = None,
    next_inspection_date: Any = None,
    note: Any = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _asset_detail(con, int(asset_id), clean_tenant_id)
        con.execute(
            """
            UPDATE facility_assets
            SET asset_code=?, asset_name=?, category=?, location_name=?, vendor_name=?, installed_on=?, inspection_cycle_days=?,
                lifecycle_state=?, source=?, qr_id=?, checklist_key=?,
                last_inspected_at=?, next_inspection_date=?, note=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _normalize_key(asset_code, field="asset_code", prefix="AST") if asset_code is not None else current["asset_code"],
                _clean_text(asset_name, field="asset_name", required=True, max_len=160) if asset_name is not None else current["asset_name"],
                _clean_choice(category, ASSET_CATEGORY_VALUES, field="category", default="기타") if category is not None else current["category"],
                _clean_text(location_name, field="location_name", max_len=160) if location_name is not None else current["location_name"],
                _clean_text(vendor_name, field="vendor_name", max_len=120) if vendor_name is not None else current.get("vendor_name"),
                (_clean_date(installed_on, field="installed_on") if str(installed_on or "").strip() else "") if installed_on is not None else current.get("installed_on"),
                _clean_int(inspection_cycle_days, field="inspection_cycle_days", default=30) if inspection_cycle_days is not None else current.get("inspection_cycle_days"),
                _clean_choice(lifecycle_state, ASSET_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중") if lifecycle_state is not None else current["lifecycle_state"],
                (_clean_text(source, field="source", max_len=40) or "manual") if source is not None else current["source"],
                (_normalize_key(qr_id, field="qr_id", prefix="QR", max_len=80) if str(qr_id or "").strip() else "") if qr_id is not None else current["qr_id"],
                (_normalize_key(checklist_key, field="checklist_key", prefix="CHK", max_len=80) if str(checklist_key or "").strip() else "") if checklist_key is not None else current["checklist_key"],
                (_clean_datetime(last_inspected_at, field="last_inspected_at") if str(last_inspected_at or "").strip() else "") if last_inspected_at is not None else current["last_inspected_at"],
                (_clean_date(next_inspection_date, field="next_inspection_date") if str(next_inspection_date or "").strip() else "") if next_inspection_date is not None else current["next_inspection_date"],
                _clean_text(note, field="note", max_len=4000) if note is not None else current["note"],
                now_iso(),
                int(asset_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _asset_detail(con, int(asset_id), clean_tenant_id)
    finally:
        con.close()


def delete_asset(*, tenant_id: str, asset_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _asset_detail(con, int(asset_id), clean_tenant_id)
        con.execute("DELETE FROM facility_assets WHERE id=? AND tenant_id=?", (int(asset_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_checklist(
    *,
    tenant_id: str,
    checklist_key: str,
    title: str,
    task_type: str = "",
    version_no: str = "",
    lifecycle_state: str = "운영중",
    source: str = "manual",
    note: str = "",
    items: Any = None,
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_key = _normalize_key(checklist_key, field="checklist_key", prefix="CHK")
    clean_title = _clean_text(title, field="title", required=True, max_len=160)
    clean_task_type = _clean_text(task_type, field="task_type", max_len=80)
    clean_version = _clean_text(version_no, field="version_no", max_len=40)
    clean_lifecycle = _clean_choice(lifecycle_state, ASSET_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중")
    clean_source = _clean_text(source, field="source", max_len=40) or "manual"
    clean_note = _clean_text(note, field="note", max_len=4000)
    clean_items = _normalize_items(items)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO facility_checklists(
              tenant_id, checklist_key, title, task_type, version_no, lifecycle_state, source, note,
              items_json, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_key,
                clean_title,
                clean_task_type,
                clean_version,
                clean_lifecycle,
                clean_source,
                clean_note,
                json.dumps(clean_items, ensure_ascii=False),
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _checklist_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_checklists(*, tenant_id: str, lifecycle_state: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clauses = ["tenant_id=?"]
    params: List[Any] = [clean_tenant_id]
    if lifecycle_state:
        clauses.append("lifecycle_state=?")
        params.append(_clean_choice(lifecycle_state, ASSET_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중"))
    params.append(max(1, min(int(limit), 500)))
    con = _connect()
    try:
        _ensure_schema(con)
        rows = con.execute(
            f"""
            SELECT
              id, tenant_id, checklist_key, title, task_type, version_no, lifecycle_state, source,
              note, items_json, created_by_label, created_at, updated_at
            FROM facility_checklists
            WHERE {' AND '.join(clauses)}
            ORDER BY title ASC, id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                parsed = json.loads(str(item.get("items_json") or "[]"))
            except Exception:
                parsed = []
            item["items"] = parsed
            item["item_count"] = len(parsed)
            items.append(item)
        return items
    finally:
        con.close()


def update_checklist(
    checklist_id: int,
    *,
    tenant_id: str,
    checklist_key: Any = None,
    title: Any = None,
    task_type: Any = None,
    version_no: Any = None,
    lifecycle_state: Any = None,
    source: Any = None,
    note: Any = None,
    items: Any = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _checklist_detail(con, int(checklist_id), clean_tenant_id)
        next_items = current.get("items") if items is None else _normalize_items(items)
        con.execute(
            """
            UPDATE facility_checklists
            SET checklist_key=?, title=?, task_type=?, version_no=?, lifecycle_state=?, source=?, note=?, items_json=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _normalize_key(checklist_key, field="checklist_key", prefix="CHK") if checklist_key is not None else current["checklist_key"],
                _clean_text(title, field="title", required=True, max_len=160) if title is not None else current["title"],
                _clean_text(task_type, field="task_type", max_len=80) if task_type is not None else current["task_type"],
                _clean_text(version_no, field="version_no", max_len=40) if version_no is not None else current["version_no"],
                _clean_choice(lifecycle_state, ASSET_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중") if lifecycle_state is not None else current["lifecycle_state"],
                (_clean_text(source, field="source", max_len=40) or "manual") if source is not None else current["source"],
                _clean_text(note, field="note", max_len=4000) if note is not None else current["note"],
                json.dumps(next_items, ensure_ascii=False),
                now_iso(),
                int(checklist_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _checklist_detail(con, int(checklist_id), clean_tenant_id)
    finally:
        con.close()


def delete_checklist(*, tenant_id: str, checklist_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _checklist_detail(con, int(checklist_id), clean_tenant_id)
        con.execute("DELETE FROM facility_checklists WHERE id=? AND tenant_id=?", (int(checklist_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_qr_asset(
    *,
    tenant_id: str,
    qr_id: str,
    asset_id: Optional[int] = None,
    asset_code_snapshot: str = "",
    asset_name_snapshot: str = "",
    location_snapshot: str = "",
    default_item: str = "",
    checklist_key: str = "",
    lifecycle_state: str = "운영중",
    source: str = "manual",
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_qr_id = _normalize_key(qr_id, field="qr_id", prefix="QR")
    clean_asset_code_snapshot = _clean_text(asset_code_snapshot, field="asset_code_snapshot", max_len=120)
    clean_asset_name_snapshot = _clean_text(asset_name_snapshot, field="asset_name_snapshot", max_len=160)
    clean_location_snapshot = _clean_text(location_snapshot, field="location_snapshot", max_len=160)
    clean_default_item = _clean_text(default_item, field="default_item", max_len=200)
    clean_checklist_key = _normalize_key(checklist_key, field="checklist_key", prefix="CHK", max_len=80) if str(checklist_key or "").strip() else ""
    clean_lifecycle = _clean_choice(lifecycle_state, QR_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중")
    clean_source = _clean_text(source, field="source", max_len=40) or "manual"
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    asset_id_value = int(asset_id) if asset_id not in (None, "", 0, "0") else None
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO facility_qr_assets(
              tenant_id, qr_id, asset_id, asset_code_snapshot, asset_name_snapshot, location_snapshot,
              default_item, checklist_key, lifecycle_state, source, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_qr_id,
                asset_id_value,
                clean_asset_code_snapshot,
                clean_asset_name_snapshot,
                clean_location_snapshot,
                clean_default_item,
                clean_checklist_key,
                clean_lifecycle,
                clean_source,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _qr_asset_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_qr_assets(*, tenant_id: str, lifecycle_state: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clauses = ["q.tenant_id=?"]
    params: List[Any] = [clean_tenant_id]
    if lifecycle_state:
        clauses.append("q.lifecycle_state=?")
        params.append(_clean_choice(lifecycle_state, QR_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중"))
    params.append(max(1, min(int(limit), 500)))
    con = _connect()
    try:
        _ensure_schema(con)
        return [
            dict(row)
            for row in con.execute(
                f"""
                SELECT
                  q.id, q.tenant_id, q.qr_id, q.asset_id, q.asset_code_snapshot, q.asset_name_snapshot,
                  q.location_snapshot, q.default_item, q.checklist_key, q.lifecycle_state, q.source,
                  q.created_by_label, q.created_at, q.updated_at,
                  a.asset_code, a.asset_name, a.category
                FROM facility_qr_assets q
                LEFT JOIN facility_assets a ON a.id = q.asset_id
                WHERE {' AND '.join(clauses)}
                ORDER BY q.qr_id ASC, q.id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        ]
    finally:
        con.close()


def update_qr_asset(
    qr_asset_id: int,
    *,
    tenant_id: str,
    qr_id: Any = None,
    asset_id: Any = None,
    asset_code_snapshot: Any = None,
    asset_name_snapshot: Any = None,
    location_snapshot: Any = None,
    default_item: Any = None,
    checklist_key: Any = None,
    lifecycle_state: Any = None,
    source: Any = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _qr_asset_detail(con, int(qr_asset_id), clean_tenant_id)
        con.execute(
            """
            UPDATE facility_qr_assets
            SET qr_id=?, asset_id=?, asset_code_snapshot=?, asset_name_snapshot=?, location_snapshot=?,
                default_item=?, checklist_key=?, lifecycle_state=?, source=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _normalize_key(qr_id, field="qr_id", prefix="QR") if qr_id is not None else current["qr_id"],
                (int(asset_id) if asset_id not in (None, "", 0, "0") else None) if asset_id is not None else current["asset_id"],
                _clean_text(asset_code_snapshot, field="asset_code_snapshot", max_len=120) if asset_code_snapshot is not None else current["asset_code_snapshot"],
                _clean_text(asset_name_snapshot, field="asset_name_snapshot", max_len=160) if asset_name_snapshot is not None else current["asset_name_snapshot"],
                _clean_text(location_snapshot, field="location_snapshot", max_len=160) if location_snapshot is not None else current["location_snapshot"],
                _clean_text(default_item, field="default_item", max_len=200) if default_item is not None else current["default_item"],
                (_normalize_key(checklist_key, field="checklist_key", prefix="CHK", max_len=80) if str(checklist_key or "").strip() else "") if checklist_key is not None else current["checklist_key"],
                _clean_choice(lifecycle_state, QR_LIFECYCLE_VALUES, field="lifecycle_state", default="운영중") if lifecycle_state is not None else current["lifecycle_state"],
                (_clean_text(source, field="source", max_len=40) or "manual") if source is not None else current["source"],
                now_iso(),
                int(qr_asset_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _qr_asset_detail(con, int(qr_asset_id), clean_tenant_id)
    finally:
        con.close()


def delete_qr_asset(*, tenant_id: str, qr_asset_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _qr_asset_detail(con, int(qr_asset_id), clean_tenant_id)
        con.execute("DELETE FROM facility_qr_assets WHERE id=? AND tenant_id=?", (int(qr_asset_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_inspection(
    *,
    tenant_id: str,
    title: str,
    asset_id: Optional[int] = None,
    qr_asset_id: Optional[int] = None,
    checklist_key: str = "",
    inspector: str = "",
    inspected_at: str = "",
    result_status: str = "정상",
    notes: str = "",
    measurement: Any = None,
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_title = _clean_text(title, field="title", required=True, max_len=200)
    clean_checklist_key = _normalize_key(checklist_key, field="checklist_key", prefix="CHK", max_len=80) if str(checklist_key or "").strip() else ""
    clean_inspector = _clean_text(inspector, field="inspector", max_len=80)
    clean_inspected_at = _clean_datetime(inspected_at or now_iso(), field="inspected_at", required=True)
    clean_result_status = _clean_choice(result_status, INSPECTION_STATUS_VALUES, field="result_status", default="정상")
    clean_notes = _clean_text(notes, field="notes", max_len=4000)
    clean_measurement = _normalize_json_text(measurement, field="measurement")
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    asset_id_value = int(asset_id) if asset_id not in (None, "", 0, "0") else None
    qr_asset_id_value = int(qr_asset_id) if qr_asset_id not in (None, "", 0, "0") else None
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO facility_inspections(
              tenant_id, title, asset_id, qr_asset_id, checklist_key, inspector, inspected_at, result_status,
              notes, measurement_json, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_title,
                asset_id_value,
                qr_asset_id_value,
                clean_checklist_key,
                clean_inspector,
                clean_inspected_at,
                clean_result_status,
                clean_notes,
                clean_measurement,
                clean_actor,
                ts,
                ts,
            ),
        )
        _sync_asset_after_inspection(
            con,
            tenant_id=clean_tenant_id,
            asset_id=asset_id_value,
            inspected_at=clean_inspected_at,
            result_status=clean_result_status,
            updated_at=ts,
        )
        con.commit()
        return _inspection_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_inspections(*, tenant_id: str, result_status: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clauses = ["i.tenant_id=?"]
    params: List[Any] = [clean_tenant_id]
    if result_status:
        clauses.append("i.result_status=?")
        params.append(_clean_choice(result_status, INSPECTION_STATUS_VALUES, field="result_status", default="정상"))
    params.append(max(1, min(int(limit), 500)))
    con = _connect()
    try:
        _ensure_schema(con)
        rows = con.execute(
            f"""
            SELECT
              i.id, i.tenant_id, i.title, i.asset_id, i.qr_asset_id, i.checklist_key, i.inspector,
              i.inspected_at, i.result_status, i.notes, i.measurement_json, i.created_by_label,
              i.created_at, i.updated_at,
              a.asset_code, a.asset_name, a.location_name, q.qr_id
            FROM facility_inspections i
            LEFT JOIN facility_assets a ON a.id = i.asset_id
            LEFT JOIN facility_qr_assets q ON q.id = i.qr_asset_id
            WHERE {' AND '.join(clauses)}
            ORDER BY i.inspected_at DESC, i.id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            try:
                item["measurement"] = json.loads(str(item.get("measurement_json") or "{}"))
            except Exception:
                item["measurement"] = {}
            items.append(item)
        return items
    finally:
        con.close()


def update_inspection(
    inspection_id: int,
    *,
    tenant_id: str,
    title: Any = None,
    asset_id: Any = None,
    qr_asset_id: Any = None,
    checklist_key: Any = None,
    inspector: Any = None,
    inspected_at: Any = None,
    result_status: Any = None,
    notes: Any = None,
    measurement: Any = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _inspection_detail(con, int(inspection_id), clean_tenant_id)
        next_asset_id = (int(asset_id) if asset_id not in (None, "", 0, "0") else None) if asset_id is not None else current["asset_id"]
        next_qr_asset_id = (int(qr_asset_id) if qr_asset_id not in (None, "", 0, "0") else None) if qr_asset_id is not None else current["qr_asset_id"]
        next_checklist_key = (_normalize_key(checklist_key, field="checklist_key", prefix="CHK", max_len=80) if str(checklist_key or "").strip() else "") if checklist_key is not None else current["checklist_key"]
        next_inspector = _clean_text(inspector, field="inspector", max_len=80) if inspector is not None else current["inspector"]
        next_inspected_at = _clean_datetime(inspected_at, field="inspected_at", required=True) if inspected_at is not None else current["inspected_at"]
        next_result_status = _clean_choice(result_status, INSPECTION_STATUS_VALUES, field="result_status", default="정상") if result_status is not None else current["result_status"]
        next_notes = _clean_text(notes, field="notes", max_len=4000) if notes is not None else current["notes"]
        next_measurement = _normalize_json_text(measurement, field="measurement") if measurement is not None else current["measurement_json"]
        ts = now_iso()
        con.execute(
            """
            UPDATE facility_inspections
            SET title=?, asset_id=?, qr_asset_id=?, checklist_key=?, inspector=?, inspected_at=?, result_status=?, notes=?, measurement_json=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_text(title, field="title", required=True, max_len=200) if title is not None else current["title"],
                next_asset_id,
                next_qr_asset_id,
                next_checklist_key,
                next_inspector,
                next_inspected_at,
                next_result_status,
                next_notes,
                next_measurement,
                ts,
                int(inspection_id),
                clean_tenant_id,
            ),
        )
        _sync_asset_after_inspection(
            con,
            tenant_id=clean_tenant_id,
            asset_id=next_asset_id,
            inspected_at=next_inspected_at,
            result_status=next_result_status,
            updated_at=ts,
        )
        con.commit()
        return _inspection_detail(con, int(inspection_id), clean_tenant_id)
    finally:
        con.close()


def delete_inspection(*, tenant_id: str, inspection_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _inspection_detail(con, int(inspection_id), clean_tenant_id)
        con.execute("DELETE FROM facility_inspections WHERE id=? AND tenant_id=?", (int(inspection_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_work_order(
    *,
    tenant_id: str,
    title: str,
    description: str = "",
    asset_id: Optional[int] = None,
    qr_asset_id: Optional[int] = None,
    inspection_id: Optional[int] = None,
    complaint_id: Optional[int] = None,
    category: str = "기타",
    priority: str = "보통",
    status: str = "접수",
    assignee: str = "",
    reporter: str = "",
    due_date: str = "",
    completed_at: str = "",
    resolution_notes: str = "",
    is_escalated: bool = False,
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_title = _clean_text(title, field="title", required=True, max_len=200)
    clean_description = _clean_text(description, field="description", max_len=8000)
    clean_category = _clean_choice(category, WORK_ORDER_CATEGORY_VALUES, field="category", default="기타")
    clean_priority = _clean_choice(priority, WORK_ORDER_PRIORITY_VALUES, field="priority", default="보통")
    clean_status = _clean_choice(status, WORK_ORDER_STATUS_VALUES, field="status", default="접수")
    clean_assignee = _clean_text(assignee, field="assignee", max_len=80)
    clean_reporter = _clean_text(reporter, field="reporter", max_len=80)
    clean_due_date = _clean_date(due_date, field="due_date") if str(due_date or "").strip() else ""
    clean_completed_at = _clean_datetime(completed_at, field="completed_at") if str(completed_at or "").strip() else ""
    clean_resolution_notes = _clean_text(resolution_notes, field="resolution_notes", max_len=4000)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    asset_id_value = int(asset_id) if asset_id not in (None, "", 0, "0") else None
    qr_asset_id_value = int(qr_asset_id) if qr_asset_id not in (None, "", 0, "0") else None
    inspection_id_value = int(inspection_id) if inspection_id not in (None, "", 0, "0") else None
    complaint_id_value = _normalize_complaint_id(complaint_id)
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO facility_work_orders(
              tenant_id, title, description, asset_id, qr_asset_id, inspection_id, complaint_id, category, priority, status,
              assignee, reporter, due_date, completed_at, resolution_notes, is_escalated, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_title,
                clean_description,
                asset_id_value,
                qr_asset_id_value,
                inspection_id_value,
                complaint_id_value,
                clean_category,
                clean_priority,
                clean_status,
                clean_assignee,
                clean_reporter,
                clean_due_date,
                clean_completed_at,
                clean_resolution_notes,
                1 if is_escalated else 0,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _work_order_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_work_orders(*, tenant_id: str, status: str = "", priority: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clauses = ["w.tenant_id=?"]
    params: List[Any] = [clean_tenant_id]
    if status:
        clauses.append("w.status=?")
        params.append(_clean_choice(status, WORK_ORDER_STATUS_VALUES, field="status", default="접수"))
    if priority:
        clauses.append("w.priority=?")
        params.append(_clean_choice(priority, WORK_ORDER_PRIORITY_VALUES, field="priority", default="보통"))
    params.append(max(1, min(int(limit), 500)))
    con = _connect()
    try:
        _ensure_schema(con)
        return [
            dict(row)
            for row in con.execute(
                f"""
                SELECT
                  w.id, w.tenant_id, w.title, w.description, w.asset_id, w.qr_asset_id, w.inspection_id,
                  w.complaint_id, w.category, w.priority, w.status, w.assignee, w.reporter, w.due_date, w.completed_at,
                  w.resolution_notes, w.is_escalated, w.created_by_label, w.created_at, w.updated_at,
                  a.asset_code, a.asset_name, a.category AS asset_category, a.location_name, q.qr_id,
                  c.summary AS complaint_summary, c.status AS complaint_status
                FROM facility_work_orders w
                LEFT JOIN facility_assets a ON a.id = w.asset_id
                LEFT JOIN facility_qr_assets q ON q.id = w.qr_asset_id
                LEFT JOIN complaints c ON c.id = w.complaint_id AND c.tenant_id = w.tenant_id
                WHERE {' AND '.join(clauses)}
                ORDER BY
                  CASE w.status WHEN '접수' THEN 0 WHEN '진행중' THEN 1 WHEN '보류' THEN 2 ELSE 3 END,
                  CASE w.priority WHEN '긴급' THEN 0 WHEN '높음' THEN 1 WHEN '보통' THEN 2 ELSE 3 END,
                  CASE WHEN w.due_date IS NULL OR w.due_date='' THEN 1 ELSE 0 END,
                  w.due_date ASC,
                  w.id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        ]
    finally:
        con.close()


def update_work_order(
    work_order_id: int,
    *,
    tenant_id: str,
    title: Any = None,
    description: Any = None,
    asset_id: Any = None,
    qr_asset_id: Any = None,
    inspection_id: Any = None,
    complaint_id: Any = None,
    category: Any = None,
    priority: Any = None,
    status: Any = None,
    assignee: Any = None,
    reporter: Any = None,
    due_date: Any = None,
    completed_at: Any = None,
    resolution_notes: Any = None,
    is_escalated: Any = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _work_order_detail(con, int(work_order_id), clean_tenant_id)
        con.execute(
            """
            UPDATE facility_work_orders
            SET title=?, description=?, asset_id=?, qr_asset_id=?, inspection_id=?, complaint_id=?, category=?, priority=?, status=?, assignee=?, reporter=?,
                due_date=?, completed_at=?, resolution_notes=?, is_escalated=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_text(title, field="title", required=True, max_len=200) if title is not None else current["title"],
                _clean_text(description, field="description", max_len=8000) if description is not None else current["description"],
                (int(asset_id) if asset_id not in (None, "", 0, "0") else None) if asset_id is not None else current["asset_id"],
                (int(qr_asset_id) if qr_asset_id not in (None, "", 0, "0") else None) if qr_asset_id is not None else current["qr_asset_id"],
                (int(inspection_id) if inspection_id not in (None, "", 0, "0") else None) if inspection_id is not None else current["inspection_id"],
                _normalize_complaint_id(complaint_id) if complaint_id is not None else current.get("complaint_id"),
                _clean_choice(category, WORK_ORDER_CATEGORY_VALUES, field="category", default="기타") if category is not None else current["category"],
                _clean_choice(priority, WORK_ORDER_PRIORITY_VALUES, field="priority", default="보통") if priority is not None else current["priority"],
                _clean_choice(status, WORK_ORDER_STATUS_VALUES, field="status", default="접수") if status is not None else current["status"],
                _clean_text(assignee, field="assignee", max_len=80) if assignee is not None else current["assignee"],
                _clean_text(reporter, field="reporter", max_len=80) if reporter is not None else current["reporter"],
                (_clean_date(due_date, field="due_date") if str(due_date or "").strip() else "") if due_date is not None else current["due_date"],
                (_clean_datetime(completed_at, field="completed_at") if str(completed_at or "").strip() else "") if completed_at is not None else current["completed_at"],
                _clean_text(resolution_notes, field="resolution_notes", max_len=4000) if resolution_notes is not None else current["resolution_notes"],
                (1 if bool(is_escalated) else 0) if is_escalated is not None else current["is_escalated"],
                now_iso(),
                int(work_order_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _work_order_detail(con, int(work_order_id), clean_tenant_id)
    finally:
        con.close()


def delete_work_order(*, tenant_id: str, work_order_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _work_order_detail(con, int(work_order_id), clean_tenant_id)
        con.execute("DELETE FROM facility_work_orders WHERE id=? AND tenant_id=?", (int(work_order_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def get_inspection(*, tenant_id: str, inspection_id: int) -> Optional[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        try:
            return _inspection_detail(con, int(inspection_id), clean_tenant_id)
        except ValueError:
            return None
    finally:
        con.close()


def get_work_order(*, tenant_id: str, work_order_id: int) -> Optional[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        try:
            return _work_order_detail(con, int(work_order_id), clean_tenant_id)
        except ValueError:
            return None
    finally:
        con.close()


def get_open_work_order_by_inspection(*, tenant_id: str, inspection_id: int) -> Optional[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT id
            FROM facility_work_orders
            WHERE tenant_id=? AND inspection_id=? AND status IN ('접수','진행중','보류')
            ORDER BY
              CASE status WHEN '접수' THEN 0 WHEN '진행중' THEN 1 ELSE 2 END,
              id DESC
            LIMIT 1
            """,
            (clean_tenant_id, int(inspection_id)),
        ).fetchone()
        if not row:
            return None
        return _work_order_detail(con, int(row["id"]), clean_tenant_id)
    finally:
        con.close()


def facility_dashboard_summary(*, tenant_id: str) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    today = date.today().isoformat()
    next_month = (date.today() + timedelta(days=30)).isoformat()
    month_prefix = date.today().strftime("%Y-%m")
    con = _connect()
    try:
        _ensure_schema(con)
        active_assets = con.execute(
            "SELECT COUNT(*) AS c FROM facility_assets WHERE tenant_id=? AND lifecycle_state='운영중'",
            (clean_tenant_id,),
        ).fetchone()
        active_qr_assets = con.execute(
            "SELECT COUNT(*) AS c FROM facility_qr_assets WHERE tenant_id=? AND lifecycle_state='운영중'",
            (clean_tenant_id,),
        ).fetchone()
        open_work_orders = con.execute(
            "SELECT COUNT(*) AS c FROM facility_work_orders WHERE tenant_id=? AND status IN ('접수','진행중','보류')",
            (clean_tenant_id,),
        ).fetchone()
        month_inspections = con.execute(
            "SELECT COUNT(*) AS c FROM facility_inspections WHERE tenant_id=? AND substr(inspected_at,1,7)=?",
            (clean_tenant_id, month_prefix),
        ).fetchone()
        due_assets = [
            dict(row)
            for row in con.execute(
                """
                SELECT id, asset_code, asset_name, category, location_name, next_inspection_date, lifecycle_state
                FROM facility_assets
                WHERE tenant_id=? AND next_inspection_date<>'' AND next_inspection_date<=? AND lifecycle_state IN ('운영중','점검중')
                ORDER BY next_inspection_date ASC, id DESC
                LIMIT 5
                """,
                (clean_tenant_id, next_month),
            ).fetchall()
        ]
        urgent_work_orders = [
            dict(row)
            for row in con.execute(
                """
                SELECT
                  w.id, w.title, w.priority, w.status, w.assignee, w.due_date,
                  a.asset_name, a.location_name
                FROM facility_work_orders w
                LEFT JOIN facility_assets a ON a.id = w.asset_id
                WHERE w.tenant_id=? AND w.status!='완료' AND (w.priority='긴급' OR w.is_escalated=1 OR (w.due_date<>'' AND w.due_date<?))
                ORDER BY
                  CASE w.priority WHEN '긴급' THEN 0 WHEN '높음' THEN 1 ELSE 2 END,
                  CASE WHEN w.due_date='' THEN 1 ELSE 0 END,
                  w.due_date ASC,
                  w.id DESC
                LIMIT 5
                """,
                (clean_tenant_id, today),
            ).fetchall()
        ]
        recent_inspections = [
            dict(row)
            for row in con.execute(
                """
                SELECT
                  i.id, i.title, i.result_status, i.inspected_at, i.inspector,
                  a.asset_name, a.location_name
                FROM facility_inspections i
                LEFT JOIN facility_assets a ON a.id = i.asset_id
                WHERE i.tenant_id=?
                ORDER BY i.inspected_at DESC, i.id DESC
                LIMIT 5
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        recent_checklists = [
            dict(row)
            for row in con.execute(
                """
                SELECT checklist_key, title, task_type, lifecycle_state, updated_at
                FROM facility_checklists
                WHERE tenant_id=?
                ORDER BY updated_at DESC, id DESC
                LIMIT 5
                """,
                (clean_tenant_id,),
            ).fetchall()
        ]
        return {
            "active_assets": int(active_assets["c"] if active_assets else 0),
            "active_qr_assets": int(active_qr_assets["c"] if active_qr_assets else 0),
            "open_work_orders": int(open_work_orders["c"] if open_work_orders else 0),
            "month_inspections": int(month_inspections["c"] if month_inspections else 0),
            "due_assets": due_assets,
            "urgent_work_orders": urgent_work_orders,
            "recent_inspections": recent_inspections,
            "recent_checklists": recent_checklists,
        }
    finally:
        con.close()
