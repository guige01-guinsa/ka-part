from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schema_defs import (
    LEGACY_FIELD_ALIASES,
    SCHEMA_DEFS,
    TAB_STORAGE_SPECS,
    canonicalize_tab_fields,
    schema_field_keys,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "ka.db"
SCHEMA_PATH = BASE_DIR / "sql" / "schema.sql"

_TABLE_COL_CACHE: Dict[str, List[str]] = {}


def _connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con


def now_iso() -> str:
    import datetime as _dt

    return _dt.datetime.now().replace(microsecond=0).isoformat(sep=" ")


def table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    cols = _TABLE_COL_CACHE.get(table)
    if cols:
        return cols
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
    _TABLE_COL_CACHE[table] = cols
    return cols


def _invalidate_col_cache(table: str) -> None:
    _TABLE_COL_CACHE.pop(table, None)


def _ensure_column(con: sqlite3.Connection, table: str, col_def: str) -> None:
    col_name = col_def.split()[0]
    cols = table_columns(con, table)
    if col_name in cols:
        return
    con.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
    _invalidate_col_cache(table)


def _entry_values_value_col(con: sqlite3.Connection) -> str:
    cols = table_columns(con, "entry_values")
    if "value_text" in cols:
        return "value_text"
    if "field_value" in cols:
        return "field_value"
    raise RuntimeError("entry_values table missing value column (value_text/field_value)")


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _to_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def dynamic_upsert(
    con: sqlite3.Connection,
    table: str,
    key_cols: List[str],
    data: Dict[str, Any],
    *,
    touch_updated_at: bool = True,
    ts: Optional[str] = None,
) -> None:
    if ts is None:
        ts = now_iso()
    cols = table_columns(con, table)
    clean: Dict[str, Any] = {k: data[k] for k in data.keys() if k in cols}

    if "created_at" in cols and "created_at" not in clean:
        clean["created_at"] = ts
    if touch_updated_at and "updated_at" in cols:
        clean["updated_at"] = ts

    for k in key_cols:
        if k not in clean:
            raise ValueError(f"dynamic_upsert missing key col '{k}' for table '{table}'")

    insert_cols = list(dict.fromkeys(key_cols + [c for c in clean.keys() if c not in key_cols]))
    placeholders = ",".join(["?"] * len(insert_cols))
    insert_sql = f"INSERT INTO {table}({', '.join(insert_cols)}) VALUES ({placeholders})"

    upd_cols = [c for c in insert_cols if c not in key_cols and c not in ("id", "created_at")]
    if not upd_cols:
        sql = insert_sql + f" ON CONFLICT({', '.join(key_cols)}) DO NOTHING"
    else:
        set_sql = ", ".join([f"{c}=excluded.{c}" for c in upd_cols])
        sql = insert_sql + f" ON CONFLICT({', '.join(key_cols)}) DO UPDATE SET " + set_sql

    values = tuple(clean.get(c) for c in insert_cols)
    con.execute(sql, values)


def ensure_domain_tables(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS transformer_450_reads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          lv1_l1_v REAL, lv1_l1_a REAL, lv1_l1_kw REAL,
          lv1_l2_v REAL, lv1_l2_a REAL, lv1_l2_kw REAL,
          lv1_l3_v REAL, lv1_l3_a REAL, lv1_l3_kw REAL,
          lv1_temp REAL,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "transformer_450_reads", "entry_date TEXT")
    _ensure_column(con, "transformer_450_reads", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_tr450_site_date
        ON transformer_450_reads(site_name, entry_date);
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS transformer_400_reads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          lv2_l1_v REAL, lv2_l1_a REAL, lv2_l1_kw REAL,
          lv2_l2_v REAL, lv2_l2_a REAL, lv2_l2_kw REAL,
          lv2_l3_v REAL, lv2_l3_a REAL, lv2_l3_kw REAL,
          lv2_temp REAL,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "transformer_400_reads", "entry_date TEXT")
    _ensure_column(con, "transformer_400_reads", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_tr400_site_date
        ON transformer_400_reads(site_name, entry_date);
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS power_meter_reads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          aiss_l1_a REAL, aiss_l2_a REAL, aiss_l3_a REAL,
          main_kwh REAL, industry_kwh REAL, street_kwh REAL,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "power_meter_reads", "entry_date TEXT")
    _ensure_column(con, "power_meter_reads", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_meter_site_date
        ON power_meter_reads(site_name, entry_date);
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS facility_checks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          tank_level_1 REAL, tank_level_2 REAL,
          hydrant_pressure REAL, sp_pump_pressure REAL,
          high_pressure REAL, low_pressure REAL,
          office_pressure REAL, shop_pressure REAL,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "facility_checks", "entry_date TEXT")
    _ensure_column(con, "facility_checks", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_fac_site_date
        ON facility_checks(site_name, entry_date);
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS facility_subtasks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          domain_key TEXT NOT NULL CHECK(domain_key IN ('fire','mechanical','telecom')),
          task_title TEXT,
          status TEXT,
          criticality TEXT,
          detail TEXT,
          next_due TEXT,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "facility_subtasks", "entry_date TEXT")
    _ensure_column(con, "facility_subtasks", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_subtasks_site_date_domain
        ON facility_subtasks(site_name, entry_date, domain_key);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS staff_users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          login_id TEXT NOT NULL UNIQUE COLLATE NOCASE,
          name TEXT NOT NULL,
          role TEXT NOT NULL,
          phone TEXT,
          note TEXT,
          is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_staff_users_active
        ON staff_users(is_active, name);
        """
    )


def _rename_entry_value_key(con: sqlite3.Connection, tab_key: str, old_key: str, new_key: str) -> None:
    con.execute(
        """
        DELETE FROM entry_values
        WHERE tab_key=? AND field_key=?
          AND EXISTS (
            SELECT 1 FROM entry_values ev2
            WHERE ev2.entry_id=entry_values.entry_id
              AND ev2.tab_key=entry_values.tab_key
              AND ev2.field_key=?
          );
        """,
        (tab_key, old_key, new_key),
    )
    con.execute(
        """
        UPDATE entry_values
        SET field_key=?
        WHERE tab_key=? AND field_key=?;
        """,
        (new_key, tab_key, old_key),
    )


def migrate_legacy_entry_values(con: sqlite3.Connection) -> None:
    for tab_key, alias_map in LEGACY_FIELD_ALIASES.items():
        for old_key, new_key in alias_map.items():
            _rename_entry_value_key(con, tab_key, old_key, new_key)


def init_db() -> None:
    con = _connect()
    try:
        if SCHEMA_PATH.exists():
            con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        ensure_domain_tables(con)
        migrate_legacy_entry_values(con)
        con.commit()
    finally:
        con.close()


def ensure_site(name: str) -> int:
    con = _connect()
    try:
        row = con.execute("SELECT id FROM sites WHERE name=?", (name,)).fetchone()
        if row:
            return int(row["id"])
        ts = now_iso()
        con.execute("INSERT INTO sites(name, created_at) VALUES(?,?)", (name, ts))
        con.commit()
        row2 = con.execute("SELECT id FROM sites WHERE name=?", (name,)).fetchone()
        return int(row2["id"])
    finally:
        con.close()


def upsert_entry(site_id: int, entry_date: str) -> int:
    con = _connect()
    try:
        row = con.execute(
            "SELECT id FROM entries WHERE site_id=? AND entry_date=?",
            (site_id, entry_date),
        ).fetchone()
        ts = now_iso()
        if row:
            entry_id = int(row["id"])
            cols = table_columns(con, "entries")
            if "updated_at" in cols:
                con.execute("UPDATE entries SET updated_at=? WHERE id=?", (ts, entry_id))
            con.commit()
            return entry_id

        dynamic_upsert(
            con,
            "entries",
            ["site_id", "entry_date"],
            {"site_id": site_id, "entry_date": entry_date},
            ts=ts,
        )
        con.commit()
        row2 = con.execute(
            "SELECT id FROM entries WHERE site_id=? AND entry_date=?",
            (site_id, entry_date),
        ).fetchone()
        return int(row2["id"])
    finally:
        con.close()


def save_tab_values(entry_id: int, tab_key: str, values: Dict[str, Any]) -> None:
    con = _connect()
    try:
        value_col = _entry_values_value_col(con)
        ts = now_iso()
        clean = canonicalize_tab_fields(tab_key, values)
        sql = f"""
            INSERT INTO entry_values(entry_id, tab_key, field_key, {value_col}, created_at, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(entry_id, tab_key, field_key)
            DO UPDATE SET {value_col}=excluded.{value_col}, updated_at=excluded.updated_at
        """
        for k, v in clean.items():
            con.execute(sql, (entry_id, tab_key, str(k), "" if v is None else str(v), ts, ts))
        con.commit()
    finally:
        con.close()


def _load_entry_values_for_entry(con: sqlite3.Connection, entry_id: int) -> Dict[str, Dict[str, str]]:
    value_col = _entry_values_value_col(con)
    rows = con.execute(
        f"SELECT tab_key, field_key, {value_col} AS value_col FROM entry_values WHERE entry_id=?",
        (entry_id,),
    ).fetchall()
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        tab_key = str(r["tab_key"])
        raw_key = str(r["field_key"])
        key = LEGACY_FIELD_ALIASES.get(tab_key, {}).get(raw_key, raw_key)
        if tab_key in SCHEMA_DEFS and key not in set(schema_field_keys(tab_key)):
            continue
        out.setdefault(tab_key, {})[key] = str(r["value_col"])
    return out


def load_entry(site_id: int, entry_date: str) -> Dict[str, Dict[str, str]]:
    con = _connect()
    try:
        row = con.execute(
            "SELECT id FROM entries WHERE site_id=? AND entry_date=?",
            (site_id, entry_date),
        ).fetchone()
        if not row:
            return {}
        return _load_entry_values_for_entry(con, int(row["id"]))
    finally:
        con.close()


def delete_entry(site_id: int, entry_date: str) -> bool:
    con = _connect()
    try:
        row = con.execute(
            "SELECT id FROM entries WHERE site_id=? AND entry_date=?",
            (site_id, entry_date),
        ).fetchone()
        if not row:
            return False
        con.execute("DELETE FROM entries WHERE id=?", (int(row["id"]),))
        con.commit()
        return True
    finally:
        con.close()


def list_entries(site_id: int, date_from: str, date_to: str) -> List[sqlite3.Row]:
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT id, entry_date, created_at, updated_at
            FROM entries
            WHERE site_id=? AND entry_date BETWEEN ? AND ?
            ORDER BY entry_date ASC
            """,
            (site_id, date_from, date_to),
        ).fetchall()
        return rows
    finally:
        con.close()


def load_entry_by_id(entry_id: int) -> Dict[str, Dict[str, str]]:
    con = _connect()
    try:
        return _load_entry_values_for_entry(con, entry_id)
    finally:
        con.close()


def upsert_tab_domain_data(site_name: str, entry_date: str, tab_key: str, fields: Dict[str, Any]) -> bool:
    spec = TAB_STORAGE_SPECS.get(tab_key)
    if not spec:
        return False
    clean = canonicalize_tab_fields(tab_key, fields)
    payload: Dict[str, Any] = {"site_name": site_name, "entry_date": entry_date}
    payload.update(dict(spec.get("fixed") or {}))

    numeric_tabs = {"tr450", "tr400", "meter", "facility_check"}
    for form_key, db_col in dict(spec.get("column_map") or {}).items():
        raw_val = clean.get(form_key)
        payload[db_col] = _to_float(raw_val) if tab_key in numeric_tabs else _to_text(raw_val)

    con = _connect()
    try:
        dynamic_upsert(
            con,
            str(spec["table"]),
            list(spec.get("key_cols") or []),
            payload,
        )
        con.commit()
        return True
    finally:
        con.close()


def upsert_transformer_450(site_name: str, entry_date: str, fields: Dict[str, Any]) -> None:
    upsert_tab_domain_data(site_name, entry_date, "tr450", fields)


def upsert_transformer_400(site_name: str, entry_date: str, fields: Dict[str, Any]) -> None:
    upsert_tab_domain_data(site_name, entry_date, "tr400", fields)


def upsert_power_meter(site_name: str, entry_date: str, fields: Dict[str, Any]) -> None:
    upsert_tab_domain_data(site_name, entry_date, "meter", fields)


def upsert_facility_check(site_name: str, entry_date: str, fields: Dict[str, Any]) -> None:
    upsert_tab_domain_data(site_name, entry_date, "facility_check", fields)


def schema_alignment_report() -> Dict[str, Any]:
    con = _connect()
    try:
        issues: List[str] = []
        for tab_key, spec in TAB_STORAGE_SPECS.items():
            schema_keys = set(schema_field_keys(tab_key))
            map_keys = set((spec.get("column_map") or {}).keys())
            if schema_keys != map_keys:
                missing_in_schema = sorted(map_keys - schema_keys)
                missing_in_map = sorted(schema_keys - map_keys)
                if missing_in_schema:
                    issues.append(f"[{tab_key}] schema missing keys: {', '.join(missing_in_schema)}")
                if missing_in_map:
                    issues.append(f"[{tab_key}] storage map missing keys: {', '.join(missing_in_map)}")

            table = str(spec["table"])
            cols = set(table_columns(con, table))
            required_cols = set((spec.get("column_map") or {}).values())
            required_cols |= set(spec.get("key_cols") or [])
            required_cols |= set((spec.get("fixed") or {}).keys())
            for col in sorted(required_cols):
                if col not in cols:
                    issues.append(f"[{tab_key}] table '{table}' missing column '{col}'")

        ok = len(issues) == 0
        return {"ok": ok, "issue_count": len(issues), "issues": issues}
    finally:
        con.close()


def list_staff_users(*, active_only: bool = False) -> List[Dict[str, Any]]:
    con = _connect()
    try:
        sql = """
            SELECT id, login_id, name, role, phone, note, is_active, created_at, updated_at
            FROM staff_users
        """
        params: List[Any] = []
        if active_only:
            sql += " WHERE is_active=1"
        sql += " ORDER BY is_active DESC, name ASC, id ASC"
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_staff_user(user_id: int) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, is_active, created_at, updated_at
            FROM staff_users
            WHERE id=?
            """,
            (int(user_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def create_staff_user(
    *,
    login_id: str,
    name: str,
    role: str,
    phone: Optional[str] = None,
    note: Optional[str] = None,
    is_active: int = 1,
) -> Dict[str, Any]:
    con = _connect()
    try:
        ts = now_iso()
        con.execute(
            """
            INSERT INTO staff_users(login_id, name, role, phone, note, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (login_id, name, role, phone, note, int(1 if is_active else 0), ts, ts),
        )
        con.commit()
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, is_active, created_at, updated_at
            FROM staff_users
            WHERE login_id=?
            """,
            (login_id,),
        ).fetchone()
        return dict(row)
    finally:
        con.close()


def update_staff_user(
    user_id: int,
    *,
    login_id: str,
    name: str,
    role: str,
    phone: Optional[str] = None,
    note: Optional[str] = None,
    is_active: int = 1,
) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        ts = now_iso()
        cur = con.execute(
            """
            UPDATE staff_users
            SET login_id=?, name=?, role=?, phone=?, note=?, is_active=?, updated_at=?
            WHERE id=?
            """,
            (login_id, name, role, phone, note, int(1 if is_active else 0), ts, int(user_id)),
        )
        con.commit()
        if cur.rowcount == 0:
            return None
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, is_active, created_at, updated_at
            FROM staff_users
            WHERE id=?
            """,
            (int(user_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def delete_staff_user(user_id: int) -> bool:
    con = _connect()
    try:
        cur = con.execute("DELETE FROM staff_users WHERE id=?", (int(user_id),))
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()
