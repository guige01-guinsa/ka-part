from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from .db import DB_PATH, list_staff_users, now_iso
from .facility_db import list_assets
from .ops_db import list_vendors

BUILDING_USAGE_VALUES = ("아파트동", "상가동", "관리동", "부속동", "기타")
BUILDING_STATUS_VALUES = ("운영중", "휴관", "폐쇄")
REGISTRATION_TYPE_VALUES = ("사업자등록", "보험", "면허", "법정등록", "계약등록", "기타")
REGISTRATION_STATUS_VALUES = ("유효", "만료예정", "만료", "보류")


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


def _clean_choice(value: Any, allowed: tuple[str, ...], *, field: str, default: str) -> str:
    text = str(value or "").strip() or default
    if text not in allowed:
        raise ValueError(f"{field} must be one of: {', '.join(allowed)}")
    return text


def _clean_int(value: Any, *, field: str, minimum: int = 0, maximum: int = 99999) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = int(raw)
    except Exception as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return parsed


def _clean_date(value: Any, *, field: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if len(raw) != 10 or raw[4] != "-" or raw[7] != "-":
        raise ValueError(f"{field} must be YYYY-MM-DD")
    return raw


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS info_buildings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          building_code TEXT NOT NULL,
          building_name TEXT NOT NULL,
          usage_type TEXT NOT NULL DEFAULT '아파트동',
          status TEXT NOT NULL DEFAULT '운영중',
          floors_above INTEGER,
          floors_below INTEGER,
          household_count INTEGER,
          note TEXT,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(tenant_id, building_code)
        );

        CREATE TABLE IF NOT EXISTS info_registrations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          record_type TEXT NOT NULL DEFAULT '기타',
          title TEXT NOT NULL,
          reference_no TEXT,
          status TEXT NOT NULL DEFAULT '유효',
          issuer_name TEXT,
          issued_on TEXT,
          expires_on TEXT,
          note TEXT,
          created_by_label TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_info_buildings_tenant
          ON info_buildings(tenant_id, usage_type, status, building_code ASC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_info_registrations_tenant
          ON info_registrations(tenant_id, record_type, status, expires_on ASC, id DESC);
        """
    )


def init_info_db() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        con.commit()
    finally:
        con.close()


def _building_detail(con: sqlite3.Connection, building_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, building_code, building_name, usage_type, status,
          floors_above, floors_below, household_count, note, created_by_label, created_at, updated_at
        FROM info_buildings
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(building_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("building not found")
    return dict(row)


def _registration_detail(con: sqlite3.Connection, registration_id: int, tenant_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          id, tenant_id, record_type, title, reference_no, status, issuer_name, issued_on, expires_on,
          note, created_by_label, created_at, updated_at
        FROM info_registrations
        WHERE id=? AND tenant_id=?
        LIMIT 1
        """,
        (int(registration_id), str(tenant_id or "").strip().lower()),
    ).fetchone()
    if not row:
        raise ValueError("registration not found")
    return dict(row)


def create_building(
    *,
    tenant_id: str,
    building_code: str,
    building_name: str,
    usage_type: str = "아파트동",
    status: str = "운영중",
    floors_above: Any = None,
    floors_below: Any = None,
    household_count: Any = None,
    note: str = "",
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_building_code = _clean_text(building_code, field="building_code", required=True, max_len=40)
    clean_building_name = _clean_text(building_name, field="building_name", required=True, max_len=160)
    clean_usage_type = _clean_choice(usage_type, BUILDING_USAGE_VALUES, field="usage_type", default="아파트동")
    clean_status = _clean_choice(status, BUILDING_STATUS_VALUES, field="status", default="운영중")
    clean_note = _clean_text(note, field="note", max_len=4000)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO info_buildings(
              tenant_id, building_code, building_name, usage_type, status,
              floors_above, floors_below, household_count, note, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_building_code,
                clean_building_name,
                clean_usage_type,
                clean_status,
                _clean_int(floors_above, field="floors_above"),
                _clean_int(floors_below, field="floors_below"),
                _clean_int(household_count, field="household_count"),
                clean_note or None,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _building_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_buildings(*, tenant_id: str, usage_type: str = "", status: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, building_code, building_name, usage_type, status,
              floors_above, floors_below, household_count, note, created_by_label, created_at, updated_at
            FROM info_buildings
            WHERE tenant_id=?
        """
        params: List[Any] = [clean_tenant_id]
        if str(usage_type or "").strip():
            sql += " AND usage_type=?"
            params.append(_clean_choice(usage_type, BUILDING_USAGE_VALUES, field="usage_type", default="아파트동"))
        if str(status or "").strip():
            sql += " AND status=?"
            params.append(_clean_choice(status, BUILDING_STATUS_VALUES, field="status", default="운영중"))
        sql += " ORDER BY building_code ASC, id DESC LIMIT ?"
        params.append(max(1, min(500, int(limit))))
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def update_building(
    building_id: int,
    *,
    tenant_id: str,
    building_code: Optional[str] = None,
    building_name: Optional[str] = None,
    usage_type: Optional[str] = None,
    status: Optional[str] = None,
    floors_above: Any = None,
    floors_below: Any = None,
    household_count: Any = None,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _building_detail(con, int(building_id), clean_tenant_id)
        con.execute(
            """
            UPDATE info_buildings
            SET building_code=?, building_name=?, usage_type=?, status=?, floors_above=?, floors_below=?, household_count=?, note=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_text(building_code, field="building_code", required=True, max_len=40) if building_code is not None else current["building_code"],
                _clean_text(building_name, field="building_name", required=True, max_len=160) if building_name is not None else current["building_name"],
                _clean_choice(usage_type, BUILDING_USAGE_VALUES, field="usage_type", default="아파트동") if usage_type is not None else current["usage_type"],
                _clean_choice(status, BUILDING_STATUS_VALUES, field="status", default="운영중") if status is not None else current["status"],
                _clean_int(floors_above, field="floors_above") if floors_above is not None else current["floors_above"],
                _clean_int(floors_below, field="floors_below") if floors_below is not None else current["floors_below"],
                _clean_int(household_count, field="household_count") if household_count is not None else current["household_count"],
                _clean_text(note, field="note", max_len=4000) if note is not None else current["note"],
                now_iso(),
                int(building_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _building_detail(con, int(building_id), clean_tenant_id)
    finally:
        con.close()


def delete_building(*, tenant_id: str, building_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _building_detail(con, int(building_id), clean_tenant_id)
        con.execute("DELETE FROM info_buildings WHERE id=? AND tenant_id=?", (int(building_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def create_registration(
    *,
    tenant_id: str,
    record_type: str,
    title: str,
    reference_no: str = "",
    status: str = "유효",
    issuer_name: str = "",
    issued_on: str = "",
    expires_on: str = "",
    note: str = "",
    created_by_label: str = "",
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    clean_record_type = _clean_choice(record_type, REGISTRATION_TYPE_VALUES, field="record_type", default="기타")
    clean_title = _clean_text(title, field="title", required=True, max_len=160)
    clean_reference_no = _clean_text(reference_no, field="reference_no", max_len=80)
    clean_status = _clean_choice(status, REGISTRATION_STATUS_VALUES, field="status", default="유효")
    clean_issuer_name = _clean_text(issuer_name, field="issuer_name", max_len=120)
    clean_issued_on = _clean_date(issued_on, field="issued_on")
    clean_expires_on = _clean_date(expires_on, field="expires_on")
    clean_note = _clean_text(note, field="note", max_len=4000)
    clean_actor = _clean_text(created_by_label, field="created_by_label", max_len=120) or "operator"
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO info_registrations(
              tenant_id, record_type, title, reference_no, status, issuer_name, issued_on, expires_on,
              note, created_by_label, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_record_type,
                clean_title,
                clean_reference_no or None,
                clean_status,
                clean_issuer_name or None,
                clean_issued_on or None,
                clean_expires_on or None,
                clean_note or None,
                clean_actor,
                ts,
                ts,
            ),
        )
        con.commit()
        return _registration_detail(con, int(cur.lastrowid), clean_tenant_id)
    finally:
        con.close()


def list_registrations(*, tenant_id: str, record_type: str = "", status: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, record_type, title, reference_no, status, issuer_name, issued_on, expires_on,
              note, created_by_label, created_at, updated_at
            FROM info_registrations
            WHERE tenant_id=?
        """
        params: List[Any] = [clean_tenant_id]
        if str(record_type or "").strip():
            sql += " AND record_type=?"
            params.append(_clean_choice(record_type, REGISTRATION_TYPE_VALUES, field="record_type", default="기타"))
        if str(status or "").strip():
            sql += " AND status=?"
            params.append(_clean_choice(status, REGISTRATION_STATUS_VALUES, field="status", default="유효"))
        sql += " ORDER BY CASE WHEN expires_on IS NULL OR expires_on='' THEN 1 ELSE 0 END, expires_on ASC, id DESC LIMIT ?"
        params.append(max(1, min(500, int(limit))))
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def update_registration(
    registration_id: int,
    *,
    tenant_id: str,
    record_type: Optional[str] = None,
    title: Optional[str] = None,
    reference_no: Optional[str] = None,
    status: Optional[str] = None,
    issuer_name: Optional[str] = None,
    issued_on: Optional[str] = None,
    expires_on: Optional[str] = None,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        current = _registration_detail(con, int(registration_id), clean_tenant_id)
        con.execute(
            """
            UPDATE info_registrations
            SET record_type=?, title=?, reference_no=?, status=?, issuer_name=?, issued_on=?, expires_on=?, note=?, updated_at=?
            WHERE id=? AND tenant_id=?
            """,
            (
                _clean_choice(record_type, REGISTRATION_TYPE_VALUES, field="record_type", default="기타") if record_type is not None else current["record_type"],
                _clean_text(title, field="title", required=True, max_len=160) if title is not None else current["title"],
                _clean_text(reference_no, field="reference_no", max_len=80) if reference_no is not None else current["reference_no"],
                _clean_choice(status, REGISTRATION_STATUS_VALUES, field="status", default="유효") if status is not None else current["status"],
                _clean_text(issuer_name, field="issuer_name", max_len=120) if issuer_name is not None else current["issuer_name"],
                _clean_date(issued_on, field="issued_on") if issued_on is not None else (current["issued_on"] or ""),
                _clean_date(expires_on, field="expires_on") if expires_on is not None else (current["expires_on"] or ""),
                _clean_text(note, field="note", max_len=4000) if note is not None else current["note"],
                now_iso(),
                int(registration_id),
                clean_tenant_id,
            ),
        )
        con.commit()
        return _registration_detail(con, int(registration_id), clean_tenant_id)
    finally:
        con.close()


def delete_registration(*, tenant_id: str, registration_id: int) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    con = _connect()
    try:
        _ensure_schema(con)
        item = _registration_detail(con, int(registration_id), clean_tenant_id)
        con.execute("DELETE FROM info_registrations WHERE id=? AND tenant_id=?", (int(registration_id), clean_tenant_id))
        con.commit()
        return item
    finally:
        con.close()


def info_dashboard_summary(*, tenant_id: str) -> Dict[str, Any]:
    clean_tenant_id = _clean_text(tenant_id, field="tenant_id", required=True, max_len=32).lower()
    buildings = list_buildings(tenant_id=clean_tenant_id, limit=5)
    registrations = list_registrations(tenant_id=clean_tenant_id, limit=5)
    vendors = list_vendors(tenant_id=clean_tenant_id, limit=5)
    staff = list_staff_users(tenant_id=clean_tenant_id, active_only=False)[:5]
    assets = list_assets(tenant_id=clean_tenant_id, limit=5)
    return {
        "vendor_count": len(list_vendors(tenant_id=clean_tenant_id, limit=500)),
        "staff_count": len(list_staff_users(tenant_id=clean_tenant_id, active_only=False)),
        "asset_count": len(list_assets(tenant_id=clean_tenant_id, limit=500)),
        "building_count": len(list_buildings(tenant_id=clean_tenant_id, limit=500)),
        "registration_count": len(list_registrations(tenant_id=clean_tenant_id, limit=500)),
        "recent_vendors": vendors,
        "recent_staff": staff,
        "recent_assets": assets,
        "recent_buildings": buildings,
        "recent_registrations": registrations,
    }

