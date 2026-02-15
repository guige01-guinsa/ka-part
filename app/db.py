from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schema_defs import (
    LEGACY_FIELD_ALIASES,
    LEGACY_TAB_ALIASES,
    SCHEMA_DEFS,
    TAB_STORAGE_SPECS,
    build_effective_schema,
    canonical_tab_key,
    canonicalize_tab_fields,
    normalize_site_env_config,
    schema_field_keys,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "ka.db"
SCHEMA_PATH = BASE_DIR / "sql" / "schema.sql"

_TABLE_COL_CACHE: Dict[str, List[str]] = {}
VALID_ADMIN_SCOPES = {"super_admin", "ops_admin"}
_SITE_CODE_REMOVE_RE = re.compile(r"[\s-]+")
_SITE_CODE_FULL_RE = re.compile(r"^[A-Z]{3}[0-9]{5}$")
_raw_site_prefix = str(os.getenv("KA_SITE_CODE_PREFIX", "APT") or "APT").strip().upper()
SITE_CODE_PREFIX = _raw_site_prefix if re.fullmatch(r"[A-Z]{3}", _raw_site_prefix) else "APT"


def _connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
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
        # busy_timeout is best-effort; ignore if unsupported.
        pass
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


def _clean_site_code_value(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = _SITE_CODE_REMOVE_RE.sub("", raw).upper()
    if not normalized:
        return None
    if not _SITE_CODE_FULL_RE.fullmatch(normalized):
        return None
    return normalized


def _normalize_site_code_columns(con: sqlite3.Connection) -> None:
    tables = [
        ("site_registry", "id", "site_code"),
        ("site_env_configs", "id", "site_code"),
        ("site_env_config_versions", "id", "site_code"),
        ("staff_users", "id", "site_code"),
        ("backup_history", "id", "site_code"),
        ("security_audit_logs", "id", "target_site_code"),
        ("privileged_change_requests", "id", "target_site_code"),
    ]
    for table, id_col, code_col in tables:
        exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
        if not exists:
            continue
        cols = table_columns(con, table)
        if id_col not in cols or code_col not in cols:
            continue
        rows = con.execute(
            f"""
            SELECT {id_col} AS row_id, {code_col} AS site_code
            FROM {table}
            WHERE {code_col} IS NOT NULL AND TRIM({code_col})<>''
            """
        ).fetchall()
        for row in rows:
            rid = int(row["row_id"] or 0)
            if rid <= 0:
                continue
            raw = str(row["site_code"] or "").strip().upper()
            if not raw:
                continue
            normalized = _clean_site_code_value(raw)
            if not normalized or normalized == raw:
                continue
            con.execute(
                f"UPDATE {table} SET {code_col}=? WHERE {id_col}=?",
                (normalized, rid),
            )


def _require_site_name_value(site_name: Any) -> str:
    clean = str(site_name or "").strip()
    if not clean:
        raise ValueError("site_name is required")
    if len(clean) > 80:
        raise ValueError("site_name length must be <= 80")
    return clean


def _clean_site_id_value(value: Any) -> int | None:
    try:
        num = int(value or 0)
    except Exception:
        return None
    return num if num > 0 else None


def _ensure_site_id_for_name_in_tx(con: sqlite3.Connection, site_name: str, *, ts: str | None = None) -> int:
    clean_name = _require_site_name_value(site_name)
    row = con.execute("SELECT id FROM sites WHERE name=? LIMIT 1", (clean_name,)).fetchone()
    if row:
        return int(row["id"])
    stamp = str(ts or now_iso())
    con.execute("INSERT INTO sites(name, created_at) VALUES(?,?)", (clean_name, stamp))
    row2 = con.execute("SELECT id FROM sites WHERE name=? LIMIT 1", (clean_name,)).fetchone()
    if not row2:
        raise ValueError("failed to ensure site row")
    return int(row2["id"])


def _resolve_site_id_from_identity_in_tx(
    con: sqlite3.Connection,
    *,
    site_id: Any = None,
    site_name: Any = None,
    site_code: Any = None,
    create_if_missing: bool = False,
    ts: str | None = None,
) -> int | None:
    clean_site_id = _clean_site_id_value(site_id)
    if clean_site_id:
        return clean_site_id

    clean_site_name = str(site_name or "").strip()
    clean_site_code = _clean_site_code_value(site_code)
    stamp = str(ts or now_iso())

    if clean_site_name:
        if create_if_missing:
            return _ensure_site_id_for_name_in_tx(con, clean_site_name, ts=stamp)
        row = con.execute("SELECT id FROM sites WHERE name=? LIMIT 1", (clean_site_name,)).fetchone()
        if row:
            return int(row["id"])

    if clean_site_code:
        mapped = con.execute(
            """
            SELECT site_id, site_name
            FROM site_registry
            WHERE site_code=?
            LIMIT 1
            """,
            (clean_site_code,),
        ).fetchone()
        if mapped:
            mapped_site_id = _clean_site_id_value(mapped["site_id"])
            if mapped_site_id:
                return mapped_site_id
            mapped_name = str(mapped["site_name"] or "").strip()
            if mapped_name:
                return _ensure_site_id_for_name_in_tx(con, mapped_name, ts=stamp) if create_if_missing else (
                    _resolve_site_id_from_identity_in_tx(
                        con,
                        site_name=mapped_name,
                        create_if_missing=False,
                        ts=stamp,
                    )
                )
        mapped_env = con.execute(
            """
            SELECT site_id, site_name
            FROM site_env_configs
            WHERE site_code=?
            ORDER BY updated_at DESC, id DESC
            LIMIT 1
            """,
            (clean_site_code,),
        ).fetchone()
        if mapped_env:
            mapped_site_id = _clean_site_id_value(mapped_env["site_id"])
            if mapped_site_id:
                return mapped_site_id
            mapped_name = str(mapped_env["site_name"] or "").strip()
            if mapped_name:
                return _ensure_site_id_for_name_in_tx(con, mapped_name, ts=stamp) if create_if_missing else (
                    _resolve_site_id_from_identity_in_tx(
                        con,
                        site_name=mapped_name,
                        create_if_missing=False,
                        ts=stamp,
                    )
                )
    return None


def _backfill_site_identity_columns(con: sqlite3.Connection) -> None:
    ts = now_iso()
    seen_names: set[str] = set()
    for table_name in ("site_registry", "site_env_configs", "staff_users"):
        rows = con.execute(
            f"""
            SELECT DISTINCT site_name
            FROM {table_name}
            WHERE site_name IS NOT NULL AND TRIM(site_name)<>''
            """
        ).fetchall()
        for row in rows:
            name = str(row["site_name"] or "").strip()
            if not name or name in seen_names:
                continue
            _ensure_site_id_for_name_in_tx(con, name, ts=ts)
            seen_names.add(name)

    con.execute(
        """
        UPDATE site_registry
        SET site_id=(SELECT s.id FROM sites s WHERE s.name=site_registry.site_name),
            updated_at=?
        WHERE (site_id IS NULL OR site_id<=0)
          AND site_name IS NOT NULL
          AND TRIM(site_name)<>''
          AND EXISTS(SELECT 1 FROM sites s WHERE s.name=site_registry.site_name);
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE site_env_configs
        SET site_id=(SELECT s.id FROM sites s WHERE s.name=site_env_configs.site_name),
            updated_at=?
        WHERE (site_id IS NULL OR site_id<=0)
          AND site_name IS NOT NULL
          AND TRIM(site_name)<>''
          AND EXISTS(SELECT 1 FROM sites s WHERE s.name=site_env_configs.site_name);
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE staff_users
        SET site_id=(SELECT s.id FROM sites s WHERE s.name=staff_users.site_name),
            updated_at=?
        WHERE (site_id IS NULL OR site_id<=0)
          AND site_name IS NOT NULL
          AND TRIM(site_name)<>''
          AND EXISTS(SELECT 1 FROM sites s WHERE s.name=staff_users.site_name);
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE site_registry
        SET site_id=(SELECT r2.site_id FROM site_registry r2 WHERE r2.site_code=site_registry.site_code LIMIT 1),
            updated_at=?
        WHERE (site_id IS NULL OR site_id<=0)
          AND site_code IS NOT NULL
          AND TRIM(site_code)<>''
          AND EXISTS(
            SELECT 1
            FROM site_registry r2
            WHERE r2.site_code=site_registry.site_code
              AND r2.site_id IS NOT NULL
              AND r2.site_id>0
          );
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE site_env_configs
        SET site_id=(SELECT r.site_id FROM site_registry r WHERE r.site_code=site_env_configs.site_code LIMIT 1),
            updated_at=?
        WHERE (site_id IS NULL OR site_id<=0)
          AND site_code IS NOT NULL
          AND TRIM(site_code)<>''
          AND EXISTS(
            SELECT 1
            FROM site_registry r
            WHERE r.site_code=site_env_configs.site_code
              AND r.site_id IS NOT NULL
              AND r.site_id>0
          );
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE staff_users
        SET site_id=(SELECT r.site_id FROM site_registry r WHERE r.site_code=staff_users.site_code LIMIT 1),
            updated_at=?
        WHERE (site_id IS NULL OR site_id<=0)
          AND site_code IS NOT NULL
          AND TRIM(site_code)<>''
          AND EXISTS(
            SELECT 1
            FROM site_registry r
            WHERE r.site_code=staff_users.site_code
              AND r.site_id IS NOT NULL
              AND r.site_id>0
          );
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE site_registry
        SET site_name=(SELECT s.name FROM sites s WHERE s.id=site_registry.site_id),
            updated_at=?
        WHERE site_id IS NOT NULL
          AND site_id>0
          AND EXISTS(SELECT 1 FROM sites s WHERE s.id=site_registry.site_id)
          AND TRIM(COALESCE(site_name,''))<>(
            SELECT TRIM(COALESCE(s.name,'')) FROM sites s WHERE s.id=site_registry.site_id
          );
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE staff_users
        SET site_name=(SELECT s.name FROM sites s WHERE s.id=staff_users.site_id),
            updated_at=?
        WHERE site_id IS NOT NULL
          AND site_id>0
          AND EXISTS(SELECT 1 FROM sites s WHERE s.id=staff_users.site_id)
          AND TRIM(COALESCE(site_name,''))<>(
            SELECT TRIM(COALESCE(s.name,'')) FROM sites s WHERE s.id=staff_users.site_id
          );
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE site_env_configs
        SET site_name=(SELECT s.name FROM sites s WHERE s.id=site_env_configs.site_id),
            updated_at=?
        WHERE site_id IS NOT NULL
          AND site_id>0
          AND EXISTS(SELECT 1 FROM sites s WHERE s.id=site_env_configs.site_id)
          AND TRIM(COALESCE(site_name,''))<>(
            SELECT TRIM(COALESCE(s.name,'')) FROM sites s WHERE s.id=site_env_configs.site_id
          );
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE staff_users
        SET site_code=(SELECT r.site_code FROM site_registry r WHERE r.site_id=staff_users.site_id LIMIT 1),
            updated_at=?
        WHERE site_id IS NOT NULL
          AND site_id>0
          AND EXISTS(
            SELECT 1 FROM site_registry r
            WHERE r.site_id=staff_users.site_id
              AND r.site_code IS NOT NULL
              AND TRIM(r.site_code)<>''
          )
          AND TRIM(COALESCE(site_code,''))<>(
            SELECT TRIM(COALESCE(r.site_code,'')) FROM site_registry r WHERE r.site_id=staff_users.site_id LIMIT 1
          );
        """,
        (ts,),
    )
    con.execute(
        """
        UPDATE site_env_configs
        SET site_code=(SELECT r.site_code FROM site_registry r WHERE r.site_id=site_env_configs.site_id LIMIT 1),
            updated_at=?
        WHERE site_id IS NOT NULL
          AND site_id>0
          AND EXISTS(
            SELECT 1 FROM site_registry r
            WHERE r.site_id=site_env_configs.site_id
              AND r.site_code IS NOT NULL
              AND TRIM(r.site_code)<>''
          )
          AND TRIM(COALESCE(site_code,''))<>(
            SELECT TRIM(COALESCE(r.site_code,'')) FROM site_registry r WHERE r.site_id=site_env_configs.site_id LIMIT 1
          );
        """,
        (ts,),
    )

def _normalize_staff_permission_flags(is_admin: Any, is_site_admin: Any) -> tuple[int, int]:
    admin = 1 if int(is_admin or 0) == 1 else 0
    site_admin = 1 if int(is_site_admin or 0) == 1 else 0
    if admin:
        site_admin = 0
    return admin, site_admin


def _normalize_admin_scope_value(value: Any, *, is_admin: bool) -> str:
    if not is_admin:
        return ""
    raw = str(value or "").strip().lower()
    if raw in VALID_ADMIN_SCOPES:
        return raw
    return "ops_admin"


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
        CREATE TABLE IF NOT EXISTS main_vcb_reads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          main_vcb_kv REAL,
          main_vcb_l1_a REAL,
          main_vcb_l2_a REAL,
          main_vcb_l3_a REAL,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "main_vcb_reads", "site_name TEXT")
    _ensure_column(con, "main_vcb_reads", "entry_date TEXT")
    _ensure_column(con, "main_vcb_reads", "main_vcb_kv REAL")
    _ensure_column(con, "main_vcb_reads", "main_vcb_l1_a REAL")
    _ensure_column(con, "main_vcb_reads", "main_vcb_l2_a REAL")
    _ensure_column(con, "main_vcb_reads", "main_vcb_l3_a REAL")
    _ensure_column(con, "main_vcb_reads", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_main_vcb_site_date
        ON main_vcb_reads(site_name, entry_date);
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dc_panel_reads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          dc_panel_v REAL,
          dc_panel_a REAL,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "dc_panel_reads", "site_name TEXT")
    _ensure_column(con, "dc_panel_reads", "entry_date TEXT")
    _ensure_column(con, "dc_panel_reads", "dc_panel_v REAL")
    _ensure_column(con, "dc_panel_reads", "dc_panel_a REAL")
    _ensure_column(con, "dc_panel_reads", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_dc_panel_site_date
        ON dc_panel_reads(site_name, entry_date);
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS temperature_reads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL,
          entry_date TEXT,
          temperature_tr1 REAL,
          temperature_tr2 REAL,
          temperature_tr3 REAL,
          temperature_tr4 REAL,
          temperature_indoor REAL,
          created_at TEXT DEFAULT (datetime('now','localtime')),
          updated_at TEXT
        );
        """
    )
    _ensure_column(con, "temperature_reads", "site_name TEXT")
    _ensure_column(con, "temperature_reads", "entry_date TEXT")
    _ensure_column(con, "temperature_reads", "temperature_tr1 REAL")
    _ensure_column(con, "temperature_reads", "temperature_tr2 REAL")
    _ensure_column(con, "temperature_reads", "temperature_tr3 REAL")
    _ensure_column(con, "temperature_reads", "temperature_tr4 REAL")
    _ensure_column(con, "temperature_reads", "temperature_indoor REAL")
    _ensure_column(con, "temperature_reads", "updated_at TEXT")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_temperature_site_date
        ON temperature_reads(site_name, entry_date);
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
          password_hash TEXT,
          is_admin INTEGER NOT NULL DEFAULT 0 CHECK(is_admin IN (0,1)),
          is_site_admin INTEGER NOT NULL DEFAULT 0 CHECK(is_site_admin IN (0,1)),
          is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
          last_login_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    _ensure_column(con, "staff_users", "password_hash TEXT")
    _ensure_column(con, "staff_users", "is_admin INTEGER NOT NULL DEFAULT 0 CHECK(is_admin IN (0,1))")
    _ensure_column(con, "staff_users", "is_site_admin INTEGER NOT NULL DEFAULT 0 CHECK(is_site_admin IN (0,1))")
    _ensure_column(con, "staff_users", "last_login_at TEXT")
    _ensure_column(con, "staff_users", "site_code TEXT")
    _ensure_column(con, "staff_users", "site_name TEXT")
    _ensure_column(con, "staff_users", "site_id INTEGER")
    _ensure_column(con, "staff_users", "address TEXT")
    _ensure_column(con, "staff_users", "office_phone TEXT")
    _ensure_column(con, "staff_users", "office_fax TEXT")
    _ensure_column(con, "staff_users", "unit_label TEXT")
    _ensure_column(con, "staff_users", "household_key TEXT")
    _ensure_column(con, "staff_users", "admin_scope TEXT NOT NULL DEFAULT ''")
    con.execute(
        """
        UPDATE staff_users
        SET admin_scope='ops_admin'
        WHERE is_admin=1 AND (admin_scope IS NULL OR TRIM(admin_scope)='');
        """
    )
    con.execute(
        """
        UPDATE staff_users
        SET admin_scope=''
        WHERE is_admin=0 AND admin_scope IS NOT NULL AND TRIM(admin_scope)<>'';
        """
    )
    con.execute(
        """
        UPDATE staff_users
        SET admin_scope='super_admin'
        WHERE is_admin=1 AND lower(login_id)='admin';
        """
    )
    # Normalize role taxonomy to the six-category model for stable permission/display behavior.
    con.execute(
        """
        UPDATE staff_users
        SET role='최고/운영관리자'
        WHERE is_admin=1 AND TRIM(COALESCE(role,''))<>'최고/운영관리자';
        """
    )
    con.execute(
        """
        UPDATE staff_users
        SET role='단지대표자'
        WHERE is_admin=0 AND is_site_admin=1 AND TRIM(COALESCE(role,''))<>'단지대표자';
        """
    )
    con.execute(
        """
        UPDATE staff_users
        SET role='보안/경비'
        WHERE is_admin=0
          AND is_site_admin=0
          AND (instr(TRIM(COALESCE(role,'')), '보안')>0 OR instr(TRIM(COALESCE(role,'')), '경비')>0);
        """
    )
    con.execute(
        """
        UPDATE staff_users
        SET role='입주민'
        WHERE is_admin=0
          AND is_site_admin=0
          AND TRIM(COALESCE(role,'')) IN ('입주민','주민','세대주민');
        """
    )
    con.execute(
        """
        UPDATE staff_users
        SET role='입대의'
        WHERE is_admin=0
          AND is_site_admin=0
          AND TRIM(COALESCE(role,'')) IN ('입대의','입주자대표','입주자대표회의');
        """
    )
    con.execute(
        """
        UPDATE staff_users
        SET role='사용자'
        WHERE is_admin=0
          AND is_site_admin=0
          AND TRIM(COALESCE(role,'')) NOT IN ('보안/경비','입주민','입대의');
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_staff_users_active
        ON staff_users(is_active, name);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_staff_users_household
        ON staff_users(site_code, household_key, is_active);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS signup_phone_verifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          phone TEXT NOT NULL,
          code_hash TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          purpose TEXT NOT NULL DEFAULT 'signup',
          expires_at TEXT NOT NULL,
          consumed_at TEXT,
          issued_login_id TEXT,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          request_ip TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_signup_phone_verifications_phone
        ON signup_phone_verifications(phone, purpose, consumed_at, created_at);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE CASCADE,
          token_hash TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          revoked_at TEXT,
          user_agent TEXT,
          ip_address TEXT
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_auth_sessions_user
        ON auth_sessions(user_id, revoked_at, expires_at);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS site_env_configs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_code TEXT,
          site_name TEXT NOT NULL UNIQUE,
          env_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    _ensure_column(con, "site_env_configs", "site_code TEXT")
    _ensure_column(con, "site_env_configs", "site_id INTEGER")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS site_registry (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_name TEXT NOT NULL UNIQUE,
          site_code TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    _ensure_column(con, "site_registry", "site_id INTEGER")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS site_apartment_profiles (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_id INTEGER,
          site_code TEXT,
          site_name TEXT,
          households_total INTEGER NOT NULL DEFAULT 0,
          building_start INTEGER NOT NULL DEFAULT 101,
          building_count INTEGER NOT NULL DEFAULT 0,
          default_line_count INTEGER NOT NULL DEFAULT 8,
          default_max_floor INTEGER NOT NULL DEFAULT 60,
          default_basement_floors INTEGER NOT NULL DEFAULT 0,
          building_overrides_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        """
    )
    _ensure_column(con, "site_apartment_profiles", "site_id INTEGER")
    _ensure_column(con, "site_apartment_profiles", "site_code TEXT")
    _ensure_column(con, "site_apartment_profiles", "site_name TEXT")
    _ensure_column(con, "site_apartment_profiles", "households_total INTEGER NOT NULL DEFAULT 0")
    _ensure_column(con, "site_apartment_profiles", "building_start INTEGER NOT NULL DEFAULT 101")
    _ensure_column(con, "site_apartment_profiles", "building_count INTEGER NOT NULL DEFAULT 0")
    _ensure_column(con, "site_apartment_profiles", "default_line_count INTEGER NOT NULL DEFAULT 8")
    _ensure_column(con, "site_apartment_profiles", "default_max_floor INTEGER NOT NULL DEFAULT 60")
    _ensure_column(con, "site_apartment_profiles", "default_basement_floors INTEGER NOT NULL DEFAULT 0")
    _ensure_column(con, "site_apartment_profiles", "building_overrides_json TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(con, "site_apartment_profiles", "created_at TEXT NOT NULL")
    _ensure_column(con, "site_apartment_profiles", "updated_at TEXT NOT NULL")
    _backfill_site_identity_columns(con)
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_registry_name
        ON site_registry(site_name);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_registry_code
        ON site_registry(site_code);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_env_configs_site
        ON site_env_configs(site_name);
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_site_apartment_profiles_site_id
        ON site_apartment_profiles(site_id);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_apartment_profiles_code
        ON site_apartment_profiles(site_code);
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_site_env_configs_code
        ON site_env_configs(site_code)
        WHERE site_code IS NOT NULL AND site_code <> '';
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_env_configs_code
        ON site_env_configs(site_code);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_staff_users_site_id
        ON staff_users(site_id);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_env_configs_site_id
        ON site_env_configs(site_id);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_registry_site_id
        ON site_registry(site_id);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS site_env_config_versions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_code TEXT,
          site_name TEXT NOT NULL,
          version_no INTEGER NOT NULL,
          action TEXT NOT NULL,
          reason TEXT,
          actor_login TEXT,
          config_json TEXT NOT NULL,
          before_json TEXT,
          config_hash TEXT,
          created_at TEXT NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_site_env_versions_scope_version
        ON site_env_config_versions(site_name, version_no);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_site_env_versions_scope_updated
        ON site_env_config_versions(site_code, site_name, created_at DESC);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS security_audit_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_type TEXT NOT NULL,
          severity TEXT NOT NULL DEFAULT 'INFO',
          outcome TEXT NOT NULL DEFAULT 'ok',
          actor_user_id INTEGER,
          actor_login TEXT,
          target_site_code TEXT,
          target_site_name TEXT,
          request_id INTEGER,
          detail_json TEXT,
          created_at TEXT NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_security_audit_logs_created
        ON security_audit_logs(created_at DESC);
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_security_audit_logs_event
        ON security_audit_logs(event_type, outcome, created_at DESC);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS privileged_change_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          change_type TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          target_site_name TEXT,
          target_site_code TEXT,
          reason TEXT,
          payload_json TEXT NOT NULL,
          result_json TEXT,
          requested_by_user_id INTEGER NOT NULL,
          requested_by_login TEXT NOT NULL,
          approved_by_user_id INTEGER,
          approved_by_login TEXT,
          executed_by_user_id INTEGER,
          executed_by_login TEXT,
          created_at TEXT NOT NULL,
          approved_at TEXT,
          executed_at TEXT,
          expires_at TEXT
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_priv_change_status
        ON privileged_change_requests(status, change_type, created_at DESC);
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


def _rename_entry_tab_key(con: sqlite3.Connection, old_tab: str, new_tab: str) -> None:
    con.execute(
        """
        DELETE FROM entry_values
        WHERE tab_key=?
          AND EXISTS (
            SELECT 1 FROM entry_values ev2
            WHERE ev2.entry_id=entry_values.entry_id
              AND ev2.tab_key=?
              AND ev2.field_key=entry_values.field_key
          );
        """,
        (old_tab, new_tab),
    )
    con.execute(
        """
        UPDATE entry_values
        SET tab_key=?
        WHERE tab_key=?;
        """,
        (new_tab, old_tab),
    )


def migrate_legacy_tab_keys(con: sqlite3.Connection) -> None:
    for old_tab, new_tab in LEGACY_TAB_ALIASES.items():
        _rename_entry_tab_key(con, str(old_tab), str(new_tab))


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
        _normalize_site_code_columns(con)
        migrate_legacy_tab_keys(con)
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


def _used_site_codes(con: sqlite3.Connection) -> set[str]:
    codes: set[str] = set()
    queries = [
        "SELECT site_code FROM site_registry WHERE site_code IS NOT NULL AND TRIM(site_code)<>''",
        "SELECT site_code FROM site_env_configs WHERE site_code IS NOT NULL AND TRIM(site_code)<>''",
        "SELECT site_code FROM staff_users WHERE site_code IS NOT NULL AND TRIM(site_code)<>''",
    ]
    for sql in queries:
        for row in con.execute(sql).fetchall():
            code = _clean_site_code_value(row["site_code"]) or ""
            if code:
                codes.add(code)
    return codes


def _next_site_code(con: sqlite3.Connection) -> str:
    used = _used_site_codes(con)
    max_seq = 0
    for code in used:
        m = re.fullmatch(rf"{SITE_CODE_PREFIX}(\d{{5}})", code)
        if not m:
            continue
        try:
            max_seq = max(max_seq, int(m.group(1)))
        except Exception:
            continue
    seq = max_seq + 1
    while True:
        candidate = f"{SITE_CODE_PREFIX}{seq:05d}"
        if candidate not in used:
            return candidate
        seq += 1


def resolve_or_create_site_code(
    site_name: str,
    *,
    preferred_code: str | None = None,
    allow_create: bool = True,
    allow_remap: bool = False,
) -> str:
    clean_site_name = _require_site_name_value(site_name)
    clean_preferred = _clean_site_code_value(preferred_code)

    con = _connect()
    try:
        ts = now_iso()
        clean_site_id = _ensure_site_id_for_name_in_tx(con, clean_site_name, ts=ts)
        by_name = con.execute(
            """
            SELECT id, site_id, site_name, site_code
            FROM site_registry
            WHERE site_id=? OR site_name=?
            ORDER BY CASE WHEN site_id=? THEN 0 ELSE 1 END, id ASC
            LIMIT 1
            """,
            (clean_site_id, clean_site_name, clean_site_id),
        ).fetchone()
        by_code = None
        if clean_preferred:
            by_code = con.execute(
                """
                SELECT id, site_id, site_name, site_code
                FROM site_registry
                WHERE site_code=?
                LIMIT 1
                """,
                (clean_preferred,),
            ).fetchone()

        resolved_code = ""

        if by_name:
            resolved_code = _clean_site_code_value(by_name["site_code"]) or ""
            existing_site_id = _clean_site_id_value(by_name["site_id"])
            existing_name = str(by_name["site_name"] or "").strip()
            update_cols: List[str] = []
            update_params: List[Any] = []

            if existing_site_id != clean_site_id:
                update_cols.append("site_id=?")
                update_params.append(clean_site_id)
            if existing_name != clean_site_name:
                update_cols.append("site_name=?")
                update_params.append(clean_site_name)

            if not resolved_code:
                if clean_preferred:
                    update_cols.append("site_code=?")
                    update_params.append(clean_preferred)
                    resolved_code = clean_preferred
                elif allow_create:
                    generated = _next_site_code(con)
                    update_cols.append("site_code=?")
                    update_params.append(generated)
                    resolved_code = generated
                else:
                    raise ValueError("site_code mapping not found")

            if clean_preferred and resolved_code and clean_preferred != resolved_code:
                if not allow_remap:
                    raise ValueError("site_code is immutable for existing site_name; use approved migration")
                if by_code and int(by_code["id"]) != int(by_name["id"]):
                    raise ValueError("site_code already mapped to another site_name")
                update_cols.append("site_code=?")
                update_params.append(clean_preferred)
                resolved_code = clean_preferred
            if update_cols:
                update_cols.append("updated_at=?")
                update_params.append(ts)
                update_params.append(int(by_name["id"]))
                con.execute(
                    f"""
                    UPDATE site_registry
                    SET {", ".join(update_cols)}
                    WHERE id=?
                    """,
                    tuple(update_params),
                )
        elif by_code:
            mapped_name = str(by_code["site_name"] or "").strip()
            mapped_site_id = _clean_site_id_value(by_code["site_id"])
            if mapped_site_id and mapped_site_id != clean_site_id:
                raise ValueError("site_code already mapped to another site_name")
            if mapped_name and mapped_name != clean_site_name and mapped_site_id != clean_site_id:
                raise ValueError("site_code already mapped to another site_name")
            if mapped_name != clean_site_name or mapped_site_id != clean_site_id:
                con.execute(
                    """
                    UPDATE site_registry
                    SET site_name=?, site_id=?, updated_at=?
                    WHERE id=?
                    """,
                    (clean_site_name, clean_site_id, ts, int(by_code["id"])),
                )
            resolved_code = _clean_site_code_value(by_code["site_code"]) or clean_preferred or ""
        else:
            if not allow_create:
                raise ValueError("site_code mapping not found")
            legacy = None
            if clean_preferred:
                legacy = clean_preferred
            if not legacy:
                row_env = con.execute(
                    """
                    SELECT site_code
                    FROM site_env_configs
                    WHERE site_name=? AND site_code IS NOT NULL AND TRIM(site_code)<>''
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                    """,
                    (clean_site_name,),
                ).fetchone()
                if row_env:
                    legacy = _clean_site_code_value(row_env["site_code"])
            if not legacy:
                row_user = con.execute(
                    """
                    SELECT site_code
                    FROM staff_users
                    WHERE site_name=? AND site_code IS NOT NULL AND TRIM(site_code)<>''
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (clean_site_name,),
                ).fetchone()
                if row_user:
                    legacy = _clean_site_code_value(row_user["site_code"])
            resolved_code = legacy or _next_site_code(con)
            con.execute(
                """
                INSERT INTO site_registry(site_name, site_code, site_id, created_at, updated_at)
                VALUES(?,?,?,?,?)
                """,
                (clean_site_name, resolved_code, clean_site_id, ts, ts),
            )

        if not resolved_code:
            raise ValueError("failed to resolve site_code")

        con.execute(
            """
            UPDATE site_registry
            SET site_name=?, site_id=?, updated_at=?
            WHERE site_code=?
              AND (
                site_name<>?
                OR site_id IS NULL
                OR site_id<=0
                OR site_id<>?
              )
            """,
            (clean_site_name, clean_site_id, ts, resolved_code, clean_site_name, clean_site_id),
        )
        con.execute(
            """
            UPDATE staff_users
            SET site_id=?, updated_at=?
            WHERE (site_name=? OR site_code=?)
              AND (site_id IS NULL OR site_id<=0)
            """,
            (clean_site_id, ts, clean_site_name, resolved_code),
        )
        con.execute(
            """
            UPDATE staff_users
            SET site_code=?, updated_at=?
            WHERE site_name=?
              AND (
                site_code IS NULL
                OR TRIM(site_code)=''
                OR REPLACE(REPLACE(UPPER(TRIM(site_code)), '-', ''), ' ', '')<>?
              )
            """,
            (resolved_code, ts, clean_site_name, resolved_code),
        )
        con.execute(
            """
            UPDATE site_env_configs
            SET site_id=?, updated_at=?
            WHERE (site_name=? OR site_code=?)
              AND (site_id IS NULL OR site_id<=0)
            """,
            (clean_site_id, ts, clean_site_name, resolved_code),
        )
        con.execute(
            """
            UPDATE site_env_configs
            SET site_code=?, updated_at=?
            WHERE site_name=?
              AND (
                site_code IS NULL
                OR TRIM(site_code)=''
                OR REPLACE(REPLACE(UPPER(TRIM(site_code)), '-', ''), ' ', '')<>?
              )
            """,
            (resolved_code, ts, clean_site_name, resolved_code),
        )
        con.commit()
        return resolved_code
    finally:
        con.close()


def find_site_name_by_id(site_id: Any) -> str | None:
    clean_site_id = _clean_site_id_value(site_id)
    if not clean_site_id:
        return None
    con = _connect()
    try:
        row = con.execute("SELECT name FROM sites WHERE id=? LIMIT 1", (clean_site_id,)).fetchone()
        if not row:
            return None
        name = str(row["name"] or "").strip()
        return name if name else None
    finally:
        con.close()


def find_site_code_by_id(site_id: Any) -> str | None:
    clean_site_id = _clean_site_id_value(site_id)
    if not clean_site_id:
        return None
    con = _connect()
    try:
        queries = [
            (
                """
                SELECT site_code
                FROM site_registry
                WHERE site_id=?
                  AND site_code IS NOT NULL
                  AND TRIM(site_code)<>''
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_id,),
            ),
            (
                """
                SELECT site_code
                FROM site_env_configs
                WHERE site_id=?
                  AND site_code IS NOT NULL
                  AND TRIM(site_code)<>''
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_id,),
            ),
            (
                """
                SELECT site_code
                FROM staff_users
                WHERE site_id=?
                  AND site_code IS NOT NULL
                  AND TRIM(site_code)<>''
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_id,),
            ),
        ]
        for sql, params in queries:
            row = con.execute(sql, params).fetchone()
            if not row:
                continue
            code = _clean_site_code_value(row["site_code"])
            if code:
                return code
        return None
    finally:
        con.close()


def resolve_site_identity(
    *,
    site_id: Any = None,
    site_name: Any = None,
    site_code: Any = None,
    create_site_if_missing: bool = False,
) -> Dict[str, Any]:
    clean_site_id = _clean_site_id_value(site_id)
    clean_site_name = str(site_name or "").strip()
    clean_site_code = _clean_site_code_value(site_code)

    con = _connect()
    try:
        ts = now_iso()
        resolved_id = clean_site_id
        resolved_name = clean_site_name
        resolved_code = clean_site_code

        if clean_site_id:
            row_site = con.execute("SELECT id, name FROM sites WHERE id=? LIMIT 1", (clean_site_id,)).fetchone()
            if row_site:
                resolved_id = int(row_site["id"])
                resolved_name = str(row_site["name"] or "").strip() or resolved_name
            row_reg_by_id = con.execute(
                """
                SELECT site_id, site_name, site_code
                FROM site_registry
                WHERE site_id=?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_id,),
            ).fetchone()
            if row_reg_by_id:
                resolved_id = _clean_site_id_value(row_reg_by_id["site_id"]) or resolved_id
                resolved_name = str(row_reg_by_id["site_name"] or "").strip() or resolved_name
                resolved_code = _clean_site_code_value(row_reg_by_id["site_code"]) or resolved_code

        row_reg_by_code = None
        if clean_site_code:
            row_reg_by_code = con.execute(
                """
                SELECT site_id, site_name, site_code
                FROM site_registry
                WHERE site_code=?
                LIMIT 1
                """,
                (clean_site_code,),
            ).fetchone()
            if row_reg_by_code:
                resolved_id = _clean_site_id_value(row_reg_by_code["site_id"]) or resolved_id
                resolved_name = str(row_reg_by_code["site_name"] or "").strip() or resolved_name
                resolved_code = _clean_site_code_value(row_reg_by_code["site_code"]) or resolved_code

        if clean_site_name and (not resolved_id or not resolved_code):
            row_reg_by_name = con.execute(
                """
                SELECT site_id, site_name, site_code
                FROM site_registry
                WHERE site_name=?
                LIMIT 1
                """,
                (clean_site_name,),
            ).fetchone()
            if row_reg_by_name:
                resolved_id = _clean_site_id_value(row_reg_by_name["site_id"]) or resolved_id
                resolved_name = str(row_reg_by_name["site_name"] or "").strip() or resolved_name
                resolved_code = _clean_site_code_value(row_reg_by_name["site_code"]) or resolved_code

        if not resolved_name and resolved_id:
            row_site = con.execute("SELECT name FROM sites WHERE id=? LIMIT 1", (resolved_id,)).fetchone()
            if row_site:
                resolved_name = str(row_site["name"] or "").strip()

        if resolved_name and not resolved_id:
            if create_site_if_missing:
                resolved_id = _ensure_site_id_for_name_in_tx(con, resolved_name, ts=ts)
            else:
                row_site = con.execute("SELECT id FROM sites WHERE name=? LIMIT 1", (resolved_name,)).fetchone()
                if row_site:
                    resolved_id = int(row_site["id"])

        if not resolved_code and resolved_id:
            row_reg_by_id = con.execute(
                """
                SELECT site_code
                FROM site_registry
                WHERE site_id=?
                  AND site_code IS NOT NULL
                  AND TRIM(site_code)<>''
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (resolved_id,),
            ).fetchone()
            if row_reg_by_id:
                resolved_code = _clean_site_code_value(row_reg_by_id["site_code"]) or resolved_code

        return {
            "site_id": _clean_site_id_value(resolved_id),
            "site_name": str(resolved_name or "").strip(),
            "site_code": _clean_site_code_value(resolved_code) or "",
        }
    finally:
        con.close()


def find_site_code_by_name(site_name: str) -> str | None:
    clean_site_name = _require_site_name_value(site_name)
    con = _connect()
    try:
        queries = [
            (
                """
                SELECT site_code
                FROM site_registry
                WHERE site_name=?
                LIMIT 1
                """,
                (clean_site_name,),
            ),
            (
                """
                SELECT site_code
                FROM site_env_configs
                WHERE site_name=?
                  AND site_code IS NOT NULL
                  AND TRIM(site_code)<>''
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_name,),
            ),
            (
                """
                SELECT site_code
                FROM staff_users
                WHERE site_name=?
                  AND site_code IS NOT NULL
                  AND TRIM(site_code)<>''
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """,
                (clean_site_name,),
            ),
        ]
        for sql, params in queries:
            row = con.execute(sql, params).fetchone()
            if not row:
                continue
            code = _clean_site_code_value(row["site_code"])
            if code:
                return code
        return None
    finally:
        con.close()


def find_site_name_by_code(site_code: str) -> str | None:
    clean_site_code = _clean_site_code_value(site_code)
    if not clean_site_code:
        return None

    con = _connect()
    try:
        queries = [
            (
                """
                SELECT site_name
                FROM site_registry
                WHERE site_code=?
                LIMIT 1
                """,
                (clean_site_code,),
            ),
            (
                """
                SELECT site_name
                FROM site_env_configs
                WHERE site_code=?
                  AND site_name IS NOT NULL
                  AND TRIM(site_name)<>''
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_code,),
            ),
            (
                """
                SELECT site_name
                FROM staff_users
                WHERE site_code=?
                  AND site_name IS NOT NULL
                  AND TRIM(site_name)<>''
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """,
                (clean_site_code,),
            ),
        ]
        for sql, params in queries:
            row = con.execute(sql, params).fetchone()
            if not row:
                continue
            name = str(row["site_name"] or "").strip()
            if name:
                return name
        return None
    finally:
        con.close()


def count_staff_users_for_site(site_name: str, *, site_code: str | None = None) -> int:
    clean_site_name = _require_site_name_value(site_name)
    clean_site_code = _clean_site_code_value(site_code)
    con = _connect()
    try:
        if clean_site_code:
            row = con.execute(
                """
                SELECT COUNT(*) AS c
                FROM staff_users
                WHERE site_name=? OR site_code=?
                """,
                (clean_site_name, clean_site_code),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT COUNT(*) AS c
                FROM staff_users
                WHERE site_name=?
                """,
                (clean_site_name,),
            ).fetchone()
        return int(row["c"] if row else 0)
    finally:
        con.close()


def count_active_resident_household_users(
    *,
    site_code: str,
    household_key: str,
    exclude_user_id: int | None = None,
) -> int:
    clean_site_code = _clean_site_code_value(site_code)
    clean_household_key = str(household_key or "").strip().upper()
    if not clean_site_code or not clean_household_key:
        return 0
    con = _connect()
    try:
        sql = """
            SELECT COUNT(*) AS c
            FROM staff_users
            WHERE is_active=1
              AND site_code=?
              AND household_key=?
              AND TRIM(role) IN ('입주민', '주민')
        """
        params: List[Any] = [clean_site_code, clean_household_key]
        if exclude_user_id is not None and int(exclude_user_id) > 0:
            sql += " AND id<>?"
            params.append(int(exclude_user_id))
        row = con.execute(sql, tuple(params)).fetchone()
        return int(row["c"] if row else 0)
    finally:
        con.close()


def set_staff_user_site_code(user_id: int, site_code: str) -> bool:
    clean_site_code = _clean_site_code_value(site_code)
    if not clean_site_code:
        return False
    con = _connect()
    try:
        ts = now_iso()
        resolved_site_id = _resolve_site_id_from_identity_in_tx(
            con,
            site_code=clean_site_code,
            create_if_missing=False,
            ts=ts,
        )
        if resolved_site_id:
            cur = con.execute(
                """
                UPDATE staff_users
                SET site_code=?, site_id=?, updated_at=?
                WHERE id=?
                """,
                (clean_site_code, resolved_site_id, ts, int(user_id)),
            )
        else:
            cur = con.execute(
                """
                UPDATE staff_users
                SET site_code=?, updated_at=?
                WHERE id=?
                """,
                (clean_site_code, ts, int(user_id)),
            )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def normalize_staff_user_site_identity(user_id: int) -> Optional[Dict[str, Any]]:
    uid = int(user_id or 0)
    if uid <= 0:
        return None

    current = get_staff_user(uid)
    if not current:
        return None

    identity = resolve_site_identity(
        site_id=current.get("site_id"),
        site_name=current.get("site_name"),
        site_code=current.get("site_code"),
        create_site_if_missing=bool(str(current.get("site_name") or "").strip()),
    )
    resolved_site_id = _clean_site_id_value(identity.get("site_id"))
    resolved_site_name = str(identity.get("site_name") or "").strip()
    resolved_site_code = _clean_site_code_value(identity.get("site_code"))

    current_site_id = _clean_site_id_value(current.get("site_id"))
    current_site_name = str(current.get("site_name") or "").strip()
    current_site_code = _clean_site_code_value(current.get("site_code"))

    next_site_id = resolved_site_id or current_site_id
    next_site_name = resolved_site_name or current_site_name
    next_site_code = resolved_site_code or current_site_code

    if (
        next_site_id == current_site_id
        and next_site_name == current_site_name
        and next_site_code == current_site_code
    ):
        return current

    con = _connect()
    try:
        ts = now_iso()
        con.execute(
            """
            UPDATE staff_users
            SET site_id=?,
                site_name=?,
                site_code=?,
                updated_at=?
            WHERE id=?
            """,
            (
                next_site_id,
                next_site_name or None,
                next_site_code,
                ts,
                uid,
            ),
        )
        con.commit()
    finally:
        con.close()

    return get_staff_user(uid)


def get_first_staff_user_for_site(site_name: str, *, site_code: str | None = None) -> Optional[Dict[str, Any]]:
    clean_site_name = _require_site_name_value(site_name)
    clean_site_code = _clean_site_code_value(site_code)
    con = _connect()
    try:
        if clean_site_code:
            row = con.execute(
                """
                SELECT id, login_id, name, site_name, site_code, site_id, created_at
                FROM staff_users
                WHERE site_name=? OR site_code=?
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """,
                (clean_site_name, clean_site_code),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT id, login_id, name, site_name, site_code, site_id, created_at
                FROM staff_users
                WHERE site_name=?
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """,
                (clean_site_name,),
            ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def get_latest_home_complex_name(site_name: str) -> str | None:
    clean_site_name = _require_site_name_value(site_name)
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT ev.value_text AS complex_name
            FROM entries e
            JOIN sites s ON s.id = e.site_id
            JOIN entry_values ev ON ev.entry_id = e.id
            WHERE s.name=?
              AND ev.tab_key='home'
              AND ev.field_key='complex_name'
              AND TRIM(COALESCE(ev.value_text,''))<>''
            ORDER BY e.entry_date DESC, e.id DESC, ev.id DESC
            LIMIT 1
            """,
            (clean_site_name,),
        ).fetchone()
        if not row:
            return None
        val = str(row["complex_name"] or "").strip()
        return val if val else None
    finally:
        con.close()


def get_site_env_record(site_name: str = "", site_code: str | None = None) -> Dict[str, Any] | None:
    con = _connect()
    try:
        clean_site_name = str(site_name or "").strip()
        clean_site_code = _clean_site_code_value(site_code)

        row = None
        if clean_site_code:
            row = con.execute(
                """
                SELECT site_code, site_name, site_id, env_json, created_at, updated_at
                FROM site_env_configs
                WHERE site_code=?
                """,
                (clean_site_code,),
            ).fetchone()
        if row is None and clean_site_name:
            row = con.execute(
                """
                SELECT site_code, site_name, site_id, env_json, created_at, updated_at
                FROM site_env_configs
                WHERE site_name=?
                """,
                (clean_site_name,),
            ).fetchone()
        if not row:
            return None
        out = dict(row)
        raw = out.get("env_json")
        try:
            data = json.loads(str(raw or "{}"))
            out["config"] = data if isinstance(data, dict) else {}
        except Exception:
            out["config"] = {}
        return out
    finally:
        con.close()


def get_site_env_config(site_name: str, site_code: str | None = None) -> Dict[str, Any] | None:
    row = get_site_env_record(site_name, site_code=site_code)
    if row is None:
        return None
    cfg = row.get("config")
    return cfg if isinstance(cfg, dict) else {}


def upsert_site_env_config(
    site_name: str,
    config: Dict[str, Any],
    *,
    site_code: str | None = None,
    action: str = "update",
    actor_login: str = "",
    reason: str = "",
    record_version: bool = False,
) -> Dict[str, Any]:
    con = _connect()
    try:
        ts = now_iso()
        clean_site_name = str(site_name or "").strip()
        clean_site_code = _clean_site_code_value(site_code)
        resolved_site_id = _resolve_site_id_from_identity_in_tx(
            con,
            site_name=clean_site_name,
            site_code=clean_site_code,
            create_if_missing=bool(clean_site_name),
            ts=ts,
        )
        env_json = json.dumps(config if isinstance(config, dict) else {}, ensure_ascii=False, separators=(",", ":"))

        target = None
        before_cfg: Dict[str, Any] = {}
        if clean_site_code:
            target = con.execute(
                """
                SELECT id, site_code, site_name, site_id, env_json
                FROM site_env_configs
                WHERE site_code=?
                LIMIT 1
                """,
                (clean_site_code,),
            ).fetchone()
        if target is None:
            target = con.execute(
                """
                SELECT id, site_code, site_name, site_id, env_json
                FROM site_env_configs
                WHERE site_name=?
                LIMIT 1
                """,
                (clean_site_name,),
            ).fetchone()

        if target is not None:
            before_cfg = _parse_json_object(target["env_json"])
            existing_code = _clean_site_code_value(target["site_code"])
            if existing_code and clean_site_code and existing_code != clean_site_code:
                raise ValueError("site_code is immutable for existing site_env")

        if target is None:
            con.execute(
                """
                INSERT INTO site_env_configs(site_code, site_name, site_id, env_json, created_at, updated_at)
                VALUES(?,?,?,?,?,?)
                """,
                (clean_site_code, clean_site_name, resolved_site_id, env_json, ts, ts),
            )
        else:
            con.execute(
                """
                UPDATE site_env_configs
                SET site_code=?, site_name=?, site_id=?, env_json=?, updated_at=?
                WHERE id=?
                """,
                (clean_site_code, clean_site_name, resolved_site_id, env_json, ts, int(target["id"])),
            )

        row = con.execute(
            """
            SELECT site_code, site_name, site_id, env_json, created_at, updated_at
            FROM site_env_configs
            WHERE site_name=? OR (site_code=? AND site_code IS NOT NULL AND TRIM(site_code)<>'')
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (clean_site_name, clean_site_code or ""),
        ).fetchone()
        out = (
            dict(row)
            if row
            else {
                "site_code": clean_site_code,
                "site_name": clean_site_name,
                "site_id": resolved_site_id,
                "env_json": env_json,
                "created_at": ts,
                "updated_at": ts,
            }
        )
        try:
            out["config"] = json.loads(str(out.get("env_json") or "{}"))
        except Exception:
            out["config"] = {}
        if record_version:
            _insert_site_env_version(
                con,
                site_name=str(out.get("site_name") or clean_site_name),
                site_code=_clean_site_code_value(out.get("site_code") or clean_site_code),
                config=out.get("config") if isinstance(out.get("config"), dict) else {},
                action=action,
                actor_login=actor_login,
                reason=reason,
                before_config=before_cfg,
                ts=ts,
            )
        con.commit()
        return out
    finally:
        con.close()


def delete_site_env_config(
    site_name: str = "",
    *,
    site_code: str | None = None,
    actor_login: str = "",
    reason: str = "",
    record_version: bool = False,
) -> bool:
    con = _connect()
    try:
        clean_site_code = _clean_site_code_value(site_code)
        clean_site_name = str(site_name or "").strip()
        target = None
        if clean_site_code:
            target = con.execute(
                "SELECT site_code, site_name, site_id, env_json FROM site_env_configs WHERE site_code=? LIMIT 1",
                (clean_site_code,),
            ).fetchone()
        if target is None and clean_site_name:
            target = con.execute(
                "SELECT site_code, site_name, site_id, env_json FROM site_env_configs WHERE site_name=? LIMIT 1",
                (clean_site_name,),
            ).fetchone()
        if target is None:
            return False
        before_cfg = _parse_json_object(target["env_json"])
        target_site_name = str(target["site_name"] or "").strip()
        target_site_code = _clean_site_code_value(target["site_code"])

        if clean_site_code:
            cur = con.execute("DELETE FROM site_env_configs WHERE site_code=?", (clean_site_code,))
        else:
            cur = con.execute("DELETE FROM site_env_configs WHERE site_name=?", (clean_site_name,))
        if record_version and int(cur.rowcount or 0) > 0:
            _insert_site_env_version(
                con,
                site_name=target_site_name,
                site_code=target_site_code,
                config={},
                action="delete",
                actor_login=actor_login,
                reason=reason,
                before_config=before_cfg,
                ts=now_iso(),
            )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def list_site_env_configs() -> List[Dict[str, Any]]:
    con = _connect()
    try:
        rows = con.execute(
            """
            SELECT site_code, site_name, site_id, env_json, created_at, updated_at
            FROM site_env_configs
            ORDER BY COALESCE(site_code, ''), site_name ASC
            """
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            try:
                item["config"] = json.loads(str(item.get("env_json") or "{}"))
            except Exception:
                item["config"] = {}
            out.append(item)
        return out
    finally:
        con.close()


_APT_BUILDING_KEY_RE = re.compile(r"^[0-9]{1,4}$")
_APT_LINE_KEY_RE = re.compile(r"^[0-9]{1,2}$")


def apartment_profile_defaults() -> Dict[str, Any]:
    # Defaults align with the current unit-selector UX assumptions.
    return {
        "households_total": 0,
        "building_start": 101,
        "building_count": 20,
        "default_line_count": 8,
        "default_max_floor": 60,
        "default_basement_floors": 0,
        "building_overrides": {},
    }


def _clean_apartment_int(
    value: Any,
    *,
    field: str,
    default: int,
    min_value: int,
    max_value: int,
) -> int:
    if value is None or str(value).strip() == "":
        return int(default)
    try:
        num = int(value)
    except Exception as e:
        raise ValueError(f"{field} must be an integer") from e
    if num < int(min_value) or num > int(max_value):
        raise ValueError(f"{field} must be between {min_value} and {max_value}")
    return int(num)


def _normalize_line_key(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("line key is empty")
    if not _APT_LINE_KEY_RE.fullmatch(raw):
        raise ValueError("line key must be numeric")
    try:
        n = int(raw)
    except Exception as e:
        raise ValueError("line key must be numeric") from e
    if n < 1 or n > 8:
        raise ValueError("line key must be 1..8")
    return f"{n:02d}"


def normalize_apartment_building_overrides(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, Any] = {}
    for b_key, raw_item in value.items():
        building = str(b_key or "").strip()
        if not building:
            continue
        if not _APT_BUILDING_KEY_RE.fullmatch(building):
            raise ValueError("building key must be 1..4 digits")
        if not isinstance(raw_item, dict):
            continue

        item: Dict[str, Any] = {}
        if "line_count" in raw_item:
            item["line_count"] = _clean_apartment_int(
                raw_item.get("line_count"),
                field="line_count",
                default=0,
                min_value=1,
                max_value=8,
            )
        if "max_floor" in raw_item:
            item["max_floor"] = _clean_apartment_int(
                raw_item.get("max_floor"),
                field="max_floor",
                default=0,
                min_value=1,
                max_value=60,
            )
        if "basement_floors" in raw_item:
            item["basement_floors"] = _clean_apartment_int(
                raw_item.get("basement_floors"),
                field="basement_floors",
                default=0,
                min_value=0,
                max_value=20,
            )

        line_max = raw_item.get("line_max_floors")
        if isinstance(line_max, dict) and line_max:
            line_out: Dict[str, int] = {}
            for lk, lv in line_max.items():
                line_key = _normalize_line_key(lk)
                line_out[line_key] = _clean_apartment_int(
                    lv,
                    field=f"line_max_floors.{line_key}",
                    default=0,
                    min_value=1,
                    max_value=60,
                )
            if line_out:
                item["line_max_floors"] = line_out

        if item:
            out[building] = item
    return out


def normalize_site_apartment_profile(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    data = payload if isinstance(payload, dict) else {}
    defaults = apartment_profile_defaults()
    out: Dict[str, Any] = {}
    out["households_total"] = _clean_apartment_int(
        data.get("households_total", defaults["households_total"]),
        field="households_total",
        default=int(defaults["households_total"]),
        min_value=0,
        max_value=200000,
    )
    out["building_start"] = _clean_apartment_int(
        data.get("building_start", defaults["building_start"]),
        field="building_start",
        default=int(defaults["building_start"]),
        min_value=1,
        max_value=9999,
    )
    out["building_count"] = _clean_apartment_int(
        data.get("building_count", defaults["building_count"]),
        field="building_count",
        default=int(defaults["building_count"]),
        min_value=0,
        max_value=500,
    )
    out["default_line_count"] = _clean_apartment_int(
        data.get("default_line_count", defaults["default_line_count"]),
        field="default_line_count",
        default=int(defaults["default_line_count"]),
        min_value=1,
        max_value=8,
    )
    out["default_max_floor"] = _clean_apartment_int(
        data.get("default_max_floor", defaults["default_max_floor"]),
        field="default_max_floor",
        default=int(defaults["default_max_floor"]),
        min_value=1,
        max_value=60,
    )
    out["default_basement_floors"] = _clean_apartment_int(
        data.get("default_basement_floors", defaults["default_basement_floors"]),
        field="default_basement_floors",
        default=int(defaults["default_basement_floors"]),
        min_value=0,
        max_value=20,
    )
    out["building_overrides"] = normalize_apartment_building_overrides(
        data.get("building_overrides", data.get("buildings") or {})
    )
    return out


def get_site_apartment_profile_record(
    *,
    site_id: Any = 0,
    site_name: str = "",
    site_code: str | None = None,
) -> Dict[str, Any] | None:
    con = _connect()
    try:
        clean_site_id = _clean_site_id_value(site_id)
        clean_site_name = str(site_name or "").strip()
        clean_site_code = _clean_site_code_value(site_code)

        row = None
        if clean_site_id:
            row = con.execute(
                """
                SELECT site_id, site_code, site_name,
                       households_total, building_start, building_count,
                       default_line_count, default_max_floor, default_basement_floors,
                       building_overrides_json, created_at, updated_at
                FROM site_apartment_profiles
                WHERE site_id=?
                LIMIT 1
                """,
                (clean_site_id,),
            ).fetchone()
        if row is None and clean_site_code:
            row = con.execute(
                """
                SELECT site_id, site_code, site_name,
                       households_total, building_start, building_count,
                       default_line_count, default_max_floor, default_basement_floors,
                       building_overrides_json, created_at, updated_at
                FROM site_apartment_profiles
                WHERE site_code=?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_code,),
            ).fetchone()
        if row is None and clean_site_name:
            row = con.execute(
                """
                SELECT site_id, site_code, site_name,
                       households_total, building_start, building_count,
                       default_line_count, default_max_floor, default_basement_floors,
                       building_overrides_json, created_at, updated_at
                FROM site_apartment_profiles
                WHERE site_name=?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (clean_site_name,),
            ).fetchone()
        if not row:
            return None
        out = dict(row)
        out["building_overrides"] = _parse_json_object(out.get("building_overrides_json"))
        return out
    finally:
        con.close()


def upsert_site_apartment_profile(
    *,
    site_name: str,
    site_code: str | None = None,
    site_id: Any = None,
    profile: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    clean_site_name = str(site_name or "").strip()
    clean_site_code = _clean_site_code_value(site_code)
    norm = normalize_site_apartment_profile(profile)

    con = _connect()
    try:
        ts = now_iso()
        resolved_site_id = _resolve_site_id_from_identity_in_tx(
            con,
            site_id=site_id,
            site_name=clean_site_name,
            site_code=clean_site_code,
            create_if_missing=bool(clean_site_name),
            ts=ts,
        )
        if not resolved_site_id:
            raise ValueError("site_id is required")
        building_overrides_json = json.dumps(
            norm.get("building_overrides") or {},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        dynamic_upsert(
            con,
            "site_apartment_profiles",
            ["site_id"],
            {
                "site_id": int(resolved_site_id),
                "site_code": clean_site_code,
                "site_name": clean_site_name or None,
                "households_total": int(norm["households_total"]),
                "building_start": int(norm["building_start"]),
                "building_count": int(norm["building_count"]),
                "default_line_count": int(norm["default_line_count"]),
                "default_max_floor": int(norm["default_max_floor"]),
                "default_basement_floors": int(norm["default_basement_floors"]),
                "building_overrides_json": building_overrides_json,
                "created_at": ts,
                "updated_at": ts,
            },
            ts=ts,
            touch_updated_at=True,
        )
        row = con.execute(
            """
            SELECT site_id, site_code, site_name,
                   households_total, building_start, building_count,
                   default_line_count, default_max_floor, default_basement_floors,
                   building_overrides_json, created_at, updated_at
            FROM site_apartment_profiles
            WHERE site_id=?
            LIMIT 1
            """,
            (int(resolved_site_id),),
        ).fetchone()
        out = dict(row) if row else {}
        out["building_overrides"] = _parse_json_object(out.get("building_overrides_json"))
        con.commit()
        return out
    finally:
        con.close()


def delete_site_apartment_profile(
    *,
    site_id: Any = 0,
    site_name: str = "",
    site_code: str | None = None,
) -> bool:
    con = _connect()
    try:
        clean_site_id = _clean_site_id_value(site_id)
        clean_site_name = str(site_name or "").strip()
        clean_site_code = _clean_site_code_value(site_code)
        if clean_site_id:
            cur = con.execute("DELETE FROM site_apartment_profiles WHERE site_id=?", (clean_site_id,))
        elif clean_site_code:
            cur = con.execute("DELETE FROM site_apartment_profiles WHERE site_code=?", (clean_site_code,))
        elif clean_site_name:
            cur = con.execute("DELETE FROM site_apartment_profiles WHERE site_name=?", (clean_site_name,))
        else:
            return False
        con.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        con.close()


def _stable_json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _parse_json_object(raw: Any) -> Dict[str, Any]:
    txt = str(raw or "").strip()
    if not txt:
        return {}
    try:
        obj = json.loads(txt)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _next_site_env_version_no(con: sqlite3.Connection, site_name: str) -> int:
    row = con.execute(
        """
        SELECT COALESCE(MAX(version_no), 0) AS max_no
        FROM site_env_config_versions
        WHERE site_name=?
        """,
        (site_name,),
    ).fetchone()
    return int(row["max_no"] if row else 0) + 1


def _insert_site_env_version(
    con: sqlite3.Connection,
    *,
    site_name: str,
    site_code: str | None,
    config: Dict[str, Any],
    action: str,
    actor_login: str,
    reason: str,
    before_config: Dict[str, Any] | None = None,
    ts: str,
) -> Dict[str, Any]:
    clean_name = _require_site_name_value(site_name)
    clean_code = _clean_site_code_value(site_code)
    version_no = _next_site_env_version_no(con, clean_name)
    cfg = config if isinstance(config, dict) else {}
    before = before_config if isinstance(before_config, dict) else {}
    config_json = _stable_json_text(cfg)
    before_json = _stable_json_text(before)
    config_hash = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
    con.execute(
        """
        INSERT INTO site_env_config_versions(
          site_code, site_name, version_no, action, reason, actor_login,
          config_json, before_json, config_hash, created_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            clean_code,
            clean_name,
            version_no,
            str(action or "update").strip() or "update",
            str(reason or "").strip() or None,
            str(actor_login or "").strip() or None,
            config_json,
            before_json if before else None,
            config_hash,
            ts,
        ),
    )
    row = con.execute(
        """
        SELECT id, site_code, site_name, version_no, action, reason, actor_login,
               config_json, before_json, config_hash, created_at
        FROM site_env_config_versions
        WHERE site_name=? AND version_no=?
        LIMIT 1
        """,
        (clean_name, version_no),
    ).fetchone()
    out = dict(row) if row else {}
    out["config"] = _parse_json_object(out.get("config_json"))
    out["before_config"] = _parse_json_object(out.get("before_json"))
    return out


def record_site_env_config_version(
    *,
    site_name: str,
    site_code: str | None,
    config: Dict[str, Any],
    action: str = "update",
    actor_login: str = "",
    reason: str = "",
    before_config: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    con = _connect()
    try:
        ts = now_iso()
        item = _insert_site_env_version(
            con,
            site_name=site_name,
            site_code=site_code,
            config=config,
            action=action,
            actor_login=actor_login,
            reason=reason,
            before_config=before_config,
            ts=ts,
        )
        con.commit()
        return item
    finally:
        con.close()


def list_site_env_config_versions(
    *,
    site_name: str = "",
    site_code: str | None = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    con = _connect()
    try:
        clean_name = str(site_name or "").strip()
        clean_code = _clean_site_code_value(site_code)
        sql = """
            SELECT id, site_code, site_name, version_no, action, reason, actor_login,
                   config_json, before_json, config_hash, created_at
            FROM site_env_config_versions
        """
        params: List[Any] = []
        clauses: List[str] = []
        if clean_code:
            clauses.append("site_code=?")
            params.append(clean_code)
        elif clean_name:
            clauses.append("site_name=?")
            params.append(clean_name)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 300)))
        rows = con.execute(sql, tuple(params)).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["config"] = _parse_json_object(item.get("config_json"))
            item["before_config"] = _parse_json_object(item.get("before_json"))
            out.append(item)
        return out
    finally:
        con.close()


def get_site_env_config_version(version_id: int) -> Dict[str, Any] | None:
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT id, site_code, site_name, version_no, action, reason, actor_login,
                   config_json, before_json, config_hash, created_at
            FROM site_env_config_versions
            WHERE id=?
            LIMIT 1
            """,
            (int(version_id),),
        ).fetchone()
        if not row:
            return None
        out = dict(row)
        out["config"] = _parse_json_object(out.get("config_json"))
        out["before_config"] = _parse_json_object(out.get("before_json"))
        return out
    finally:
        con.close()


def rollback_site_env_config_version(
    *,
    version_id: int,
    actor_login: str = "",
    reason: str = "",
) -> Dict[str, Any]:
    item = get_site_env_config_version(int(version_id))
    if not item:
        raise ValueError("site_env version not found")
    site_name = _require_site_name_value(item.get("site_name"))
    site_code = _clean_site_code_value(item.get("site_code"))
    config = item.get("config") if isinstance(item.get("config"), dict) else {}
    row = upsert_site_env_config(
        site_name,
        config,
        site_code=site_code,
        action="rollback",
        actor_login=actor_login,
        reason=reason or f"rollback version #{int(version_id)}",
        record_version=True,
    )
    return {"target_version": item, "site_env": row}


def write_security_audit_log(
    *,
    event_type: str,
    severity: str = "INFO",
    outcome: str = "ok",
    actor_user_id: int | None = None,
    actor_login: str = "",
    target_site_code: str = "",
    target_site_name: str = "",
    request_id: int | None = None,
    detail: Dict[str, Any] | List[Any] | str | None = None,
) -> int:
    clean_event = str(event_type or "").strip() or "unknown"
    clean_sev = str(severity or "INFO").strip().upper() or "INFO"
    clean_outcome = str(outcome or "ok").strip().lower() or "ok"
    payload: Any
    if isinstance(detail, (dict, list)):
        payload = detail
    elif detail is None:
        payload = {}
    else:
        payload = {"message": str(detail)}
    detail_json = _stable_json_text(payload)
    con = _connect()
    try:
        ts = now_iso()
        cur = con.execute(
            """
            INSERT INTO security_audit_logs(
              event_type, severity, outcome, actor_user_id, actor_login,
              target_site_code, target_site_name, request_id, detail_json, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_event,
                clean_sev,
                clean_outcome,
                int(actor_user_id) if actor_user_id else None,
                str(actor_login or "").strip() or None,
                _clean_site_code_value(target_site_code),
                str(target_site_name or "").strip() or None,
                int(request_id) if request_id else None,
                detail_json,
                ts,
            ),
        )
        con.commit()
        return int(cur.lastrowid or 0)
    finally:
        con.close()


def list_security_audit_logs(*, limit: int = 200, event_type: str = "", outcome: str = "") -> List[Dict[str, Any]]:
    con = _connect()
    try:
        sql = """
            SELECT id, event_type, severity, outcome, actor_user_id, actor_login,
                   target_site_code, target_site_name, request_id, detail_json, created_at
            FROM security_audit_logs
        """
        params: List[Any] = []
        clauses: List[str] = []
        clean_event = str(event_type or "").strip()
        clean_outcome = str(outcome or "").strip().lower()
        if clean_event:
            clauses.append("event_type=?")
            params.append(clean_event)
        if clean_outcome:
            clauses.append("outcome=?")
            params.append(clean_outcome)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 500)))
        rows = con.execute(sql, tuple(params)).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["detail"] = _parse_json_object(item.get("detail_json"))
            out.append(item)
        return out
    finally:
        con.close()


def create_privileged_change_request(
    *,
    change_type: str,
    payload: Dict[str, Any],
    requested_by_user_id: int,
    requested_by_login: str,
    target_site_name: str = "",
    target_site_code: str = "",
    reason: str = "",
    expires_hours: int = 24,
) -> Dict[str, Any]:
    clean_change_type = str(change_type or "").strip().lower()
    if not clean_change_type:
        raise ValueError("change_type is required")
    clean_payload = payload if isinstance(payload, dict) else {}
    clean_reason = str(reason or "").strip()
    clean_login = str(requested_by_login or "").strip().lower()
    if not clean_login:
        raise ValueError("requested_by_login is required")
    con = _connect()
    try:
        ts = now_iso()
        expires_at = (datetime.now() + timedelta(hours=max(1, int(expires_hours)))).replace(microsecond=0).isoformat(sep=" ")
        cur = con.execute(
            """
            INSERT INTO privileged_change_requests(
              change_type, status, target_site_name, target_site_code, reason,
              payload_json, requested_by_user_id, requested_by_login,
              created_at, expires_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_change_type,
                "pending",
                str(target_site_name or "").strip() or None,
                _clean_site_code_value(target_site_code),
                clean_reason or None,
                _stable_json_text(clean_payload),
                int(requested_by_user_id),
                clean_login,
                ts,
                expires_at,
            ),
        )
        req_id = int(cur.lastrowid or 0)
        con.commit()
        row = con.execute(
            """
            SELECT *
            FROM privileged_change_requests
            WHERE id=?
            LIMIT 1
            """,
            (req_id,),
        ).fetchone()
        out = dict(row) if row else {}
        out["payload"] = _parse_json_object(out.get("payload_json"))
        out["result"] = _parse_json_object(out.get("result_json"))
        return out
    finally:
        con.close()


def get_privileged_change_request(request_id: int) -> Dict[str, Any] | None:
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT *
            FROM privileged_change_requests
            WHERE id=?
            LIMIT 1
            """,
            (int(request_id),),
        ).fetchone()
        if not row:
            return None
        out = dict(row)
        out["payload"] = _parse_json_object(out.get("payload_json"))
        out["result"] = _parse_json_object(out.get("result_json"))
        return out
    finally:
        con.close()


def list_privileged_change_requests(
    *,
    change_type: str = "",
    status: str = "",
    limit: int = 100,
) -> List[Dict[str, Any]]:
    con = _connect()
    try:
        sql = "SELECT * FROM privileged_change_requests"
        params: List[Any] = []
        clauses: List[str] = []
        clean_change_type = str(change_type or "").strip().lower()
        clean_status = str(status or "").strip().lower()
        if clean_change_type:
            clauses.append("change_type=?")
            params.append(clean_change_type)
        if clean_status:
            clauses.append("status=?")
            params.append(clean_status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 500)))
        rows = con.execute(sql, tuple(params)).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload"] = _parse_json_object(item.get("payload_json"))
            item["result"] = _parse_json_object(item.get("result_json"))
            out.append(item)
        return out
    finally:
        con.close()


def delete_privileged_change_request(*, request_id: int) -> bool:
    con = _connect()
    try:
        cur = con.execute("DELETE FROM privileged_change_requests WHERE id=?", (int(request_id),))
        con.commit()
        return int(getattr(cur, "rowcount", 0) or 0) > 0
    finally:
        con.close()


def approve_privileged_change_request(
    *,
    request_id: int,
    approver_user_id: int,
    approver_login: str,
) -> Dict[str, Any]:
    con = _connect()
    try:
        row = con.execute(
            "SELECT * FROM privileged_change_requests WHERE id=? LIMIT 1",
            (int(request_id),),
        ).fetchone()
        if not row:
            raise ValueError("request not found")
        item = dict(row)
        if str(item.get("status") or "").strip().lower() != "pending":
            raise ValueError("request is not pending")
        if int(item.get("requested_by_user_id") or 0) == int(approver_user_id):
            raise ValueError("requester cannot approve own request")
        expires_at = str(item.get("expires_at") or "").strip()
        if expires_at and expires_at <= now_iso():
            raise ValueError("request is expired")
        ts = now_iso()
        con.execute(
            """
            UPDATE privileged_change_requests
            SET status='approved', approved_by_user_id=?, approved_by_login=?, approved_at=?
            WHERE id=?
            """,
            (int(approver_user_id), str(approver_login or "").strip().lower(), ts, int(request_id)),
        )
        con.commit()
        refreshed = con.execute(
            "SELECT * FROM privileged_change_requests WHERE id=? LIMIT 1",
            (int(request_id),),
        ).fetchone()
        out = dict(refreshed) if refreshed else {}
        out["payload"] = _parse_json_object(out.get("payload_json"))
        out["result"] = _parse_json_object(out.get("result_json"))
        return out
    finally:
        con.close()


def mark_privileged_change_request_executed(
    *,
    request_id: int,
    executed_by_user_id: int,
    executed_by_login: str,
    result: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    con = _connect()
    try:
        row = con.execute(
            "SELECT * FROM privileged_change_requests WHERE id=? LIMIT 1",
            (int(request_id),),
        ).fetchone()
        if not row:
            raise ValueError("request not found")
        item = dict(row)
        if str(item.get("status") or "").strip().lower() != "approved":
            raise ValueError("request is not approved")
        ts = now_iso()
        con.execute(
            """
            UPDATE privileged_change_requests
            SET status='executed',
                executed_by_user_id=?,
                executed_by_login=?,
                executed_at=?,
                result_json=?
            WHERE id=?
            """,
            (
                int(executed_by_user_id),
                str(executed_by_login or "").strip().lower(),
                ts,
                _stable_json_text(result if isinstance(result, dict) else {}),
                int(request_id),
            ),
        )
        con.commit()
        refreshed = con.execute(
            "SELECT * FROM privileged_change_requests WHERE id=? LIMIT 1",
            (int(request_id),),
        ).fetchone()
        out = dict(refreshed) if refreshed else {}
        out["payload"] = _parse_json_object(out.get("payload_json"))
        out["result"] = _parse_json_object(out.get("result_json"))
        return out
    finally:
        con.close()


def migrate_site_code(
    *,
    site_name: str,
    old_site_code: str,
    new_site_code: str,
) -> Dict[str, Any]:
    clean_site_name = _require_site_name_value(site_name)
    old_code = _clean_site_code_value(old_site_code)
    new_code = _clean_site_code_value(new_site_code)
    if not old_code:
        raise ValueError("old_site_code is required")
    if not new_code:
        raise ValueError("new_site_code is required")
    if old_code == new_code:
        raise ValueError("new_site_code must differ from old_site_code")

    con = _connect()
    try:
        ts = now_iso()
        clean_site_id = _ensure_site_id_for_name_in_tx(con, clean_site_name, ts=ts)
        row_by_name = con.execute(
            """
            SELECT id, site_name, site_code, site_id
            FROM site_registry
            WHERE site_id=? OR site_name=?
            ORDER BY CASE WHEN site_id=? THEN 0 ELSE 1 END, id ASC
            LIMIT 1
            """,
            (clean_site_id, clean_site_name, clean_site_id),
        ).fetchone()
        if row_by_name:
            current = _clean_site_code_value(row_by_name["site_code"])
            if current and current != old_code:
                raise ValueError("site_name currently mapped to another site_code")

        row_by_old = con.execute(
            "SELECT id, site_name, site_id FROM site_registry WHERE site_code=? LIMIT 1",
            (old_code,),
        ).fetchone()
        if row_by_old:
            old_name = str(row_by_old["site_name"] or "").strip()
            old_site_id = _clean_site_id_value(row_by_old["site_id"])
            if old_site_id and old_site_id != clean_site_id:
                raise ValueError("old_site_code is mapped to another site_name")
            if old_name and old_name != clean_site_name and old_site_id != clean_site_id:
                raise ValueError("old_site_code is mapped to another site_name")

        row_by_new = con.execute(
            "SELECT id, site_name, site_id FROM site_registry WHERE site_code=? LIMIT 1",
            (new_code,),
        ).fetchone()
        if row_by_new:
            new_name = str(row_by_new["site_name"] or "").strip()
            new_site_id = _clean_site_id_value(row_by_new["site_id"])
            if new_site_id and new_site_id != clean_site_id:
                raise ValueError("new_site_code is already mapped to another site_name")
            if new_name and new_name != clean_site_name and new_site_id != clean_site_id:
                raise ValueError("new_site_code is already mapped to another site_name")

        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        skip_tables = {
            "security_audit_logs",
            "privileged_change_requests",
            "site_env_config_versions",
        }
        touched_tables: Dict[str, int] = {}
        for t in tables:
            table_name = str(t["name"] or "").strip()
            if not table_name:
                continue
            if table_name in skip_tables:
                continue
            cols = set(table_columns(con, table_name))
            if "site_code" not in cols:
                continue
            quoted = '"' + table_name.replace('"', '""') + '"'
            try:
                cur = con.execute(f"UPDATE {quoted} SET site_code=? WHERE site_code=?", (new_code, old_code))
            except sqlite3.IntegrityError as e:
                raise ValueError(f"site_code migration conflict at table '{table_name}'") from e
            if int(cur.rowcount or 0) > 0:
                touched_tables[table_name] = int(cur.rowcount)

        if row_by_name:
            con.execute(
                "UPDATE site_registry SET site_name=?, site_code=?, site_id=?, updated_at=? WHERE id=?",
                (clean_site_name, new_code, clean_site_id, ts, int(row_by_name["id"])),
            )
        elif row_by_old:
            con.execute(
                "UPDATE site_registry SET site_name=?, site_code=?, site_id=?, updated_at=? WHERE id=?",
                (clean_site_name, new_code, clean_site_id, ts, int(row_by_old["id"])),
            )
        else:
            con.execute(
                "INSERT INTO site_registry(site_name, site_code, site_id, created_at, updated_at) VALUES(?,?,?,?,?)",
                (clean_site_name, new_code, clean_site_id, ts, ts),
            )

        con.commit()
        return {
            "site_name": clean_site_name,
            "old_site_code": old_code,
            "new_site_code": new_code,
            "updated_tables": touched_tables,
            "updated_table_count": len(touched_tables),
        }
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


def save_tab_values(
    entry_id: int,
    tab_key: str,
    values: Dict[str, Any],
    *,
    schema_defs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    con = _connect()
    try:
        value_col = _entry_values_value_col(con)
        ts = now_iso()
        clean = canonicalize_tab_fields(tab_key, values, schema_defs=schema_defs)
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


def _load_entry_values_for_entry(
    con: sqlite3.Connection,
    entry_id: int,
    *,
    allowed_keys_by_tab: Optional[Dict[str, set[str]]] = None,
) -> Dict[str, Dict[str, str]]:
    value_col = _entry_values_value_col(con)
    rows = con.execute(
        f"SELECT tab_key, field_key, {value_col} AS value_col FROM entry_values WHERE entry_id=?",
        (entry_id,),
    ).fetchall()
    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        tab_key = canonical_tab_key(str(r["tab_key"]))
        raw_key = str(r["field_key"])
        key = LEGACY_FIELD_ALIASES.get(tab_key, {}).get(raw_key, raw_key)
        if isinstance(allowed_keys_by_tab, dict):
            allowed = set(allowed_keys_by_tab.get(tab_key) or set())
            if not allowed:
                continue
            if key not in allowed:
                continue
        out.setdefault(tab_key, {})[key] = str(r["value_col"])
    return out


def load_entry(
    site_id: int,
    entry_date: str,
    *,
    allowed_keys_by_tab: Optional[Dict[str, set[str]]] = None,
) -> Dict[str, Dict[str, str]]:
    con = _connect()
    try:
        row = con.execute(
            "SELECT id FROM entries WHERE site_id=? AND entry_date=?",
            (site_id, entry_date),
        ).fetchone()
        if not row:
            return {}
        return _load_entry_values_for_entry(con, int(row["id"]), allowed_keys_by_tab=allowed_keys_by_tab)
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


def load_entry_by_id(entry_id: int, *, allowed_keys_by_tab: Optional[Dict[str, set[str]]] = None) -> Dict[str, Dict[str, str]]:
    con = _connect()
    try:
        return _load_entry_values_for_entry(con, entry_id, allowed_keys_by_tab=allowed_keys_by_tab)
    finally:
        con.close()


def upsert_tab_domain_data(site_name: str, entry_date: str, tab_key: str, fields: Dict[str, Any]) -> bool:
    canonical_key = canonical_tab_key(tab_key)
    spec = TAB_STORAGE_SPECS.get(canonical_key)
    if not spec:
        return False
    clean = canonicalize_tab_fields(canonical_key, fields)
    payload: Dict[str, Any] = {"site_name": site_name, "entry_date": entry_date}
    payload.update(dict(spec.get("fixed") or {}))

    numeric_tabs = {"tr1", "tr2", "main_vcb", "dc_panel", "temperature", "meter", "facility_check"}
    for form_key, db_col in dict(spec.get("column_map") or {}).items():
        if form_key not in clean:
            continue
        raw_val = clean.get(form_key)
        payload[db_col] = _to_float(raw_val) if canonical_key in numeric_tabs else _to_text(raw_val)

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
    upsert_tab_domain_data(site_name, entry_date, "tr1", fields)


def upsert_transformer_400(site_name: str, entry_date: str, fields: Dict[str, Any]) -> None:
    upsert_tab_domain_data(site_name, entry_date, "tr2", fields)


def upsert_power_meter(site_name: str, entry_date: str, fields: Dict[str, Any]) -> None:
    upsert_tab_domain_data(site_name, entry_date, "meter", fields)


def upsert_facility_check(site_name: str, entry_date: str, fields: Dict[str, Any]) -> None:
    upsert_tab_domain_data(site_name, entry_date, "facility_check", fields)


def schema_alignment_report() -> Dict[str, Any]:
    con = _connect()
    try:
        issues: List[str] = []
        for tab_key, tab_def in SCHEMA_DEFS.items():
            fields = (tab_def or {}).get("fields")
            if not isinstance(fields, list) or not fields:
                issues.append(f"[{tab_key}] schema fields must be a non-empty list")
                continue

            field_set = set()
            for idx, field in enumerate(fields, start=1):
                if not isinstance(field, dict):
                    issues.append(f"[{tab_key}] field #{idx} is not an object")
                    continue
                fkey = str(field.get("k") or "").strip()
                if not fkey:
                    issues.append(f"[{tab_key}] field #{idx} has empty key")
                    continue
                if fkey in field_set:
                    issues.append(f"[{tab_key}] duplicate field key '{fkey}'")
                    continue
                field_set.add(fkey)

            rows = (tab_def or {}).get("rows")
            if rows is not None and not isinstance(rows, list):
                issues.append(f"[{tab_key}] rows must be list")
            if isinstance(rows, list):
                used = set()
                for r_index, row in enumerate(rows, start=1):
                    if not isinstance(row, list):
                        issues.append(f"[{tab_key}] row #{r_index} is not a list")
                        continue
                    for c_index, raw_key in enumerate(row, start=1):
                        key = str(raw_key or "").strip()
                        if not key:
                            issues.append(f"[{tab_key}] row #{r_index} col #{c_index} is empty")
                            continue
                        if key not in field_set:
                            issues.append(f"[{tab_key}] row key '{key}' is not in fields")
                            continue
                        if key in used:
                            issues.append(f"[{tab_key}] row key '{key}' is duplicated")
                        used.add(key)

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

        site_env_rows = con.execute(
            """
            SELECT site_code, site_name, env_json
            FROM site_env_configs
            ORDER BY site_name ASC
            """
        ).fetchall()
        for row in site_env_rows:
            site_code = str(row["site_code"] or "").strip()
            site_name = str(row["site_name"] or "").strip() or "(empty-site-name)"
            site_ref = f"{site_name}" if not site_code else f"{site_name}/{site_code}"
            raw_text = str(row["env_json"] or "").strip()
            try:
                raw = json.loads(raw_text) if raw_text else {}
            except Exception:
                issues.append(f"[site_env:{site_ref}] env_json is not valid json")
                continue
            if not isinstance(raw, dict):
                issues.append(f"[site_env:{site_ref}] env_json root must be object")
                continue
            normalized = normalize_site_env_config(raw)
            effective = build_effective_schema(base_schema=SCHEMA_DEFS, site_env_config=normalized)
            if not effective:
                issues.append(f"[site_env:{site_ref}] effective schema is empty")

        ok = len(issues) == 0
        return {"ok": ok, "issue_count": len(issues), "issues": issues}
    finally:
        con.close()


def list_staff_users(*, active_only: bool = False) -> List[Dict[str, Any]]:
    con = _connect()
    try:
        sql = """
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
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
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
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
    site_code: Optional[str] = None,
    site_name: Optional[str] = None,
    site_id: Optional[int] = None,
    address: Optional[str] = None,
    office_phone: Optional[str] = None,
    office_fax: Optional[str] = None,
    unit_label: Optional[str] = None,
    household_key: Optional[str] = None,
    note: Optional[str] = None,
    password_hash: Optional[str] = None,
    is_admin: int = 0,
    is_site_admin: int = 0,
    admin_scope: Optional[str] = None,
    is_active: int = 1,
) -> Dict[str, Any]:
    con = _connect()
    try:
        ts = now_iso()
        clean_site_code = _clean_site_code_value(site_code)
        clean_site_name = str(site_name or "").strip() or None
        clean_site_id = _resolve_site_id_from_identity_in_tx(
            con,
            site_id=site_id,
            site_name=clean_site_name,
            site_code=clean_site_code,
            create_if_missing=bool(clean_site_name),
            ts=ts,
        )
        clean_unit_label = str(unit_label or "").strip() or None
        clean_household_key = str(household_key or "").strip().upper() or None
        if clean_unit_label and not clean_household_key:
            clean_household_key = clean_unit_label.upper()
        if clean_household_key and not clean_unit_label:
            clean_unit_label = clean_household_key
        clean_is_admin, clean_is_site_admin = _normalize_staff_permission_flags(is_admin, is_site_admin)
        clean_admin_scope = _normalize_admin_scope_value(admin_scope, is_admin=bool(clean_is_admin))
        con.execute(
            """
            INSERT INTO staff_users(
              login_id, name, role, phone, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, note, password_hash, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                login_id,
                name,
                role,
                phone,
                clean_site_code,
                clean_site_name,
                clean_site_id,
                address,
                office_phone,
                office_fax,
                clean_unit_label,
                clean_household_key,
                note,
                password_hash,
                clean_is_admin,
                clean_is_site_admin,
                clean_admin_scope,
                int(1 if is_active else 0),
                ts,
                ts,
            ),
        )
        con.commit()
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
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
    site_code: Optional[str] = None,
    site_name: Optional[str] = None,
    site_id: Optional[int] = None,
    address: Optional[str] = None,
    office_phone: Optional[str] = None,
    office_fax: Optional[str] = None,
    unit_label: Optional[str] = None,
    household_key: Optional[str] = None,
    note: Optional[str] = None,
    is_admin: int = 0,
    is_site_admin: int = 0,
    admin_scope: Optional[str] = None,
    is_active: int = 1,
) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        ts = now_iso()
        clean_site_code = _clean_site_code_value(site_code)
        clean_site_name = str(site_name or "").strip() or None
        clean_site_id = _resolve_site_id_from_identity_in_tx(
            con,
            site_id=site_id,
            site_name=clean_site_name,
            site_code=clean_site_code,
            create_if_missing=bool(clean_site_name),
            ts=ts,
        )
        clean_unit_label = str(unit_label or "").strip() or None
        clean_household_key = str(household_key or "").strip().upper() or None
        if clean_unit_label and not clean_household_key:
            clean_household_key = clean_unit_label.upper()
        if clean_household_key and not clean_unit_label:
            clean_unit_label = clean_household_key
        clean_is_admin, clean_is_site_admin = _normalize_staff_permission_flags(is_admin, is_site_admin)
        clean_admin_scope = _normalize_admin_scope_value(admin_scope, is_admin=bool(clean_is_admin))
        cur = con.execute(
            """
            UPDATE staff_users
            SET login_id=?, name=?, role=?, phone=?, site_code=?, site_name=?, site_id=COALESCE(?, site_id), address=?, office_phone=?, office_fax=?, unit_label=?, household_key=?, note=?, is_admin=?, is_site_admin=?, admin_scope=?, is_active=?, updated_at=?
            WHERE id=?
            """,
            (
                login_id,
                name,
                role,
                phone,
                clean_site_code,
                clean_site_name,
                clean_site_id,
                address,
                office_phone,
                office_fax,
                clean_unit_label,
                clean_household_key,
                note,
                clean_is_admin,
                clean_is_site_admin,
                clean_admin_scope,
                int(1 if is_active else 0),
                ts,
                int(user_id),
            ),
        )
        con.commit()
        if cur.rowcount == 0:
            return None
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE id=?
            """,
            (int(user_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def update_staff_user_profile_fields(
    user_id: int,
    *,
    name: str,
    phone: Optional[str] = None,
    address: Optional[str] = None,
    office_phone: Optional[str] = None,
    office_fax: Optional[str] = None,
    unit_label: Optional[str] = None,
    household_key: Optional[str] = None,
    note: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Update only user-owned profile fields without mutating site identity or permission flags."""
    con = _connect()
    try:
        ts = now_iso()
        clean_unit_label = str(unit_label or "").strip() or None
        clean_household_key = str(household_key or "").strip().upper() or None
        if clean_unit_label and not clean_household_key:
            clean_household_key = clean_unit_label.upper()
        if clean_household_key and not clean_unit_label:
            clean_unit_label = clean_household_key

        cur = con.execute(
            """
            UPDATE staff_users
            SET name=?,
                phone=?,
                address=?,
                office_phone=?,
                office_fax=?,
                unit_label=?,
                household_key=?,
                note=?,
                updated_at=?
            WHERE id=?
            """,
            (
                name,
                phone,
                address,
                office_phone,
                office_fax,
                clean_unit_label,
                clean_household_key,
                note,
                ts,
                int(user_id),
            ),
        )
        con.commit()
        if cur.rowcount == 0:
            return None
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE id=?
            """,
            (int(user_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def withdraw_staff_user(user_id: int) -> Optional[Dict[str, Any]]:
    """Deactivate account and scrub personal fields (FK-safe) to satisfy withdrawal requests."""
    con = _connect()
    try:
        ts = now_iso()
        cur = con.execute(
            """
            UPDATE staff_users
            SET name=?,
                phone=NULL,
                address=NULL,
                office_phone=NULL,
                office_fax=NULL,
                unit_label=NULL,
                household_key=NULL,
                note=NULL,
                password_hash=NULL,
                is_active=0,
                updated_at=?
            WHERE id=?
            """,
            ("탈퇴회원", ts, int(user_id)),
        )
        con.commit()
        if cur.rowcount == 0:
            return None
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
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


def _b64u_encode(v: bytes) -> str:
    return base64.urlsafe_b64encode(v).decode("ascii").rstrip("=")


def _b64u_decode(v: str) -> bytes:
    return base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))


def hash_password(password: str, *, iterations: int = 310000) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=32)
    return f"pbkdf2_sha256${iterations}${_b64u_encode(salt)}${_b64u_encode(dk)}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algo, iter_s, salt_b64, hash_b64 = str(password_hash or "").split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iter_s)
        salt = _b64u_decode(salt_b64)
        expected = _b64u_decode(hash_b64)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=len(expected))
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def set_staff_user_password(user_id: int, password: str) -> bool:
    con = _connect()
    try:
        ts = now_iso()
        pw_hash = hash_password(password)
        cur = con.execute(
            "UPDATE staff_users SET password_hash=?, updated_at=? WHERE id=?",
            (pw_hash, ts, int(user_id)),
        )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def get_staff_user_by_login(login_id: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, password_hash, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            """,
            (str(login_id or "").strip().lower(),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def count_staff_admins(*, active_only: bool = True) -> int:
    con = _connect()
    try:
        sql = "SELECT COUNT(*) AS c FROM staff_users WHERE is_admin=1"
        if active_only:
            sql += " AND is_active=1"
        row = con.execute(sql).fetchone()
        return int(row["c"] if row else 0)
    finally:
        con.close()


def count_super_admins(*, active_only: bool = True) -> int:
    con = _connect()
    try:
        sql = "SELECT COUNT(*) AS c FROM staff_users WHERE is_admin=1 AND lower(COALESCE(admin_scope,''))='super_admin'"
        if active_only:
            sql += " AND is_active=1"
        row = con.execute(sql).fetchone()
        return int(row["c"] if row else 0)
    finally:
        con.close()


def mark_staff_user_login(user_id: int) -> None:
    con = _connect()
    try:
        ts = now_iso()
        con.execute(
            "UPDATE staff_users SET last_login_at=?, updated_at=? WHERE id=?",
            (ts, ts, int(user_id)),
        )
        con.commit()
    finally:
        con.close()


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_auth_session(
    user_id: int,
    *,
    ttl_hours: int = 12,
    user_agent: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> Dict[str, Any]:
    con = _connect()
    try:
        created_at = now_iso()
        expires_at = (datetime.now() + timedelta(hours=max(1, int(ttl_hours)))).replace(microsecond=0).isoformat(
            sep=" "
        )
        raw_token = _b64u_encode(os.urandom(32))
        token_hash = _hash_session_token(raw_token)
        con.execute(
            """
            INSERT INTO auth_sessions(user_id, token_hash, created_at, expires_at, user_agent, ip_address)
            VALUES(?,?,?,?,?,?)
            """,
            (int(user_id), token_hash, created_at, expires_at, user_agent, ip_address),
        )
        con.commit()
        return {"token": raw_token, "expires_at": expires_at}
    finally:
        con.close()


def revoke_auth_session(token: str) -> bool:
    con = _connect()
    try:
        ts = now_iso()
        cur = con.execute(
            "UPDATE auth_sessions SET revoked_at=? WHERE token_hash=? AND revoked_at IS NULL",
            (ts, _hash_session_token(token)),
        )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def revoke_all_user_sessions(user_id: int) -> int:
    con = _connect()
    try:
        ts = now_iso()
        cur = con.execute(
            "UPDATE auth_sessions SET revoked_at=? WHERE user_id=? AND revoked_at IS NULL",
            (ts, int(user_id)),
        )
        con.commit()
        return int(cur.rowcount)
    finally:
        con.close()


def get_auth_user_by_token(token: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        now = now_iso()
        row = con.execute(
            """
            SELECT
              s.id AS session_id,
              s.expires_at,
              u.id,
              u.login_id,
              u.name,
              u.role,
              u.phone,
              u.note,
              u.site_code,
              u.site_name,
              u.site_id,
              u.address,
              u.office_phone,
              u.office_fax,
              u.unit_label,
              u.household_key,
              u.is_admin,
              u.is_site_admin,
              u.admin_scope,
              u.is_active,
              u.created_at,
              u.updated_at,
              u.last_login_at
            FROM auth_sessions s
            JOIN staff_users u ON u.id = s.user_id
            WHERE s.token_hash=?
              AND s.revoked_at IS NULL
              AND s.expires_at > ?
            LIMIT 1
            """,
            (_hash_session_token(token), now),
        ).fetchone()
        if not row:
            return None
        data = dict(row)
        if int(data.get("is_active") or 0) != 1:
            return None
        return data
    finally:
        con.close()


def cleanup_expired_sessions() -> int:
    con = _connect()
    try:
        now = now_iso()
        cur = con.execute(
            "DELETE FROM auth_sessions WHERE (expires_at <= ?) OR (revoked_at IS NOT NULL AND revoked_at <= ?)",
            (now, now),
        )
        con.commit()
        return int(cur.rowcount)
    finally:
        con.close()


def get_staff_user_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT id, login_id, name, role, phone, note, site_code, site_name, site_id, address, office_phone, office_fax, unit_label, household_key, password_hash, is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE phone=?
            ORDER BY is_active DESC, id ASC
            LIMIT 1
            """,
            (str(phone or "").strip(),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def create_signup_phone_verification(
    *,
    phone: str,
    code_hash: str,
    payload: Dict[str, Any],
    expires_at: str,
    request_ip: Optional[str] = None,
) -> Dict[str, Any]:
    con = _connect()
    try:
        ts = now_iso()
        payload_json = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False, separators=(",", ":"))
        con.execute(
            """
            UPDATE signup_phone_verifications
            SET consumed_at=?, updated_at=?
            WHERE phone=? AND purpose='signup' AND consumed_at IS NULL
            """,
            (ts, ts, str(phone or "").strip()),
        )
        con.execute(
            """
            INSERT INTO signup_phone_verifications(phone, code_hash, payload_json, purpose, expires_at, consumed_at, issued_login_id, attempt_count, request_ip, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(phone or "").strip(),
                str(code_hash or ""),
                payload_json,
                "signup",
                str(expires_at or ""),
                None,
                None,
                0,
                request_ip,
                ts,
                ts,
            ),
        )
        con.commit()
        row = con.execute(
            """
            SELECT id, phone, code_hash, payload_json, purpose, expires_at, consumed_at, issued_login_id, attempt_count, request_ip, created_at, updated_at
            FROM signup_phone_verifications
            WHERE phone=? AND purpose='signup'
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(phone or "").strip(),),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def get_latest_signup_phone_verification(phone: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        row = con.execute(
            """
            SELECT id, phone, code_hash, payload_json, purpose, expires_at, consumed_at, issued_login_id, attempt_count, request_ip, created_at, updated_at
            FROM signup_phone_verifications
            WHERE phone=? AND purpose='signup'
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(phone or "").strip(),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def touch_signup_phone_verification_attempt(verification_id: int, *, success: bool, issued_login_id: Optional[str] = None) -> bool:
    con = _connect()
    try:
        ts = now_iso()
        if success:
            cur = con.execute(
                """
                UPDATE signup_phone_verifications
                SET consumed_at=?, issued_login_id=?, updated_at=?
                WHERE id=? AND consumed_at IS NULL
                """,
                (ts, str(issued_login_id or "").strip() or None, ts, int(verification_id)),
            )
        else:
            cur = con.execute(
                """
                UPDATE signup_phone_verifications
                SET attempt_count=COALESCE(attempt_count,0)+1, updated_at=?
                WHERE id=?
                """,
                (ts, int(verification_id)),
            )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def count_recent_signup_phone_verifications(*, phone: str = "", request_ip: str = "", minutes: int = 15) -> int:
    window_min = max(1, min(1440, int(minutes or 0) or 15))
    cutoff = (datetime.now() - timedelta(minutes=window_min)).replace(microsecond=0).isoformat(sep=" ")
    clean_phone = str(phone or "").strip()
    clean_ip = str(request_ip or "").strip()
    con = _connect()
    try:
        sql = "SELECT COUNT(*) AS c FROM signup_phone_verifications WHERE purpose='signup' AND created_at >= ?"
        params: List[Any] = [cutoff]
        if clean_phone:
            sql += " AND phone=?"
            params.append(clean_phone)
        if clean_ip:
            sql += " AND request_ip=?"
            params.append(clean_ip)
        row = con.execute(sql, tuple(params)).fetchone()
        return int((row["c"] if row else 0) or 0)
    finally:
        con.close()
