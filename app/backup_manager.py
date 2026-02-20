from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import shutil
import threading
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
from zoneinfo import ZoneInfo

from .db import DB_PATH as FACILITY_DB_PATH

logger = logging.getLogger("ka-part.backup")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
BACKUP_ROOT = Path(os.getenv("KA_BACKUP_DIR", str(PROJECT_ROOT / "backups"))).resolve()
SERVER_BACKUP_APT_ROOT = Path(os.getenv("KA_BACKUP_APT_DIR", str(PROJECT_ROOT / "backup_APT"))).resolve()
FULL_BACKUP_DIR = BACKUP_ROOT / "full"
SITE_BACKUP_DIR = BACKUP_ROOT / "site"
TMP_BACKUP_DIR = BACKUP_ROOT / ".tmp"
RUNTIME_STATE_PATH = DATA_DIR / "backup_runtime_state.json"
MAINT_STATE_PATH = DATA_DIR / "maintenance_state.json"

BACKUP_TIMEZONE = str(os.getenv("KA_BACKUP_TIMEZONE", "Asia/Seoul") or "Asia/Seoul").strip() or "Asia/Seoul"

_RUN_LOCK = threading.Lock()
_MAINT_LOCK = threading.Lock()
_SCHED_THREAD: threading.Thread | None = None
_SCHED_STOP_EVENT = threading.Event()

try:
    _BACKUP_TZ = ZoneInfo(BACKUP_TIMEZONE)
except Exception as e:
    key = BACKUP_TIMEZONE.strip().lower()
    if key in {"asia/seoul", "kst", "utc+9", "+09:00", "gmt+9"}:
        _BACKUP_TZ = timezone(timedelta(hours=9), name="KST")
        logger.info("KA_BACKUP_TIMEZONE=%s unavailable (%s), fallback to fixed KST(+09:00)", BACKUP_TIMEZONE, e)
    else:
        _BACKUP_TZ = datetime.now().astimezone().tzinfo
        logger.warning("Invalid KA_BACKUP_TIMEZONE=%s (%s), fallback timezone=%s", BACKUP_TIMEZONE, e, _BACKUP_TZ)


def _now() -> datetime:
    if _BACKUP_TZ is None:
        return datetime.now()
    return datetime.now(_BACKUP_TZ)


def _now_iso() -> str:
    return _now().replace(microsecond=0).isoformat(sep=" ")


def backup_timezone_name() -> str:
    return BACKUP_TIMEZONE


def _sanitize_token(value: str, *, default: str = "backup") -> str:
    raw = str(value or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9_.-]+", "_", raw)
    return cleaned or default


def _ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    SERVER_BACKUP_APT_ROOT.mkdir(parents=True, exist_ok=True)
    FULL_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    SITE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    TMP_BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not path.exists():
            return dict(default)
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            merged = dict(default)
            merged.update(data)
            return merged
    except Exception:
        logger.exception("Failed to read json file: %s", path)
    return dict(default)


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    _ensure_dirs()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _maintenance_default() -> Dict[str, Any]:
    return {
        "active": False,
        "message": "",
        "reason": "",
        "started_at": "",
        "updated_at": "",
        "updated_by": "",
    }


def get_maintenance_status() -> Dict[str, Any]:
    with _MAINT_LOCK:
        return _load_json(MAINT_STATE_PATH, _maintenance_default())


def set_maintenance_mode(*, active: bool, message: str, reason: str, updated_by: str) -> Dict[str, Any]:
    with _MAINT_LOCK:
        state = _load_json(MAINT_STATE_PATH, _maintenance_default())
        now_iso = _now_iso()
        next_state = dict(state)
        next_state["active"] = bool(active)
        next_state["message"] = str(message or "").strip()
        next_state["reason"] = str(reason or "").strip()
        if bool(active):
            if not str(state.get("started_at") or "").strip():
                next_state["started_at"] = now_iso
        else:
            next_state["started_at"] = ""
        next_state["updated_at"] = now_iso
        next_state["updated_by"] = str(updated_by or "").strip() or "system"
        _save_json(MAINT_STATE_PATH, next_state)
        return dict(next_state)


def clear_maintenance_mode(updated_by: str = "system") -> Dict[str, Any]:
    return set_maintenance_mode(
        active=False,
        message="",
        reason="manual_clear",
        updated_by=updated_by,
    )


def _runtime_default() -> Dict[str, Any]:
    return {
        "full_last_date": "",
        "site_last_week": "",
        "updated_at": "",
    }


def _load_runtime_state() -> Dict[str, Any]:
    return _load_json(RUNTIME_STATE_PATH, _runtime_default())


def _save_runtime_state(state: Dict[str, Any]) -> None:
    payload = dict(_runtime_default())
    payload.update(state or {})
    payload["updated_at"] = _now_iso()
    _save_json(RUNTIME_STATE_PATH, payload)


def _resolve_parking_db_path() -> Path:
    default_path = PROJECT_ROOT / "services" / "parking" / "app" / "data" / "parking.db"
    raw = str(os.getenv("PARKING_DB_PATH") or str(default_path)).strip()
    path = Path(raw)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _backup_targets() -> List[Dict[str, Any]]:
    facility_path = Path(FACILITY_DB_PATH).resolve()
    parking_path = _resolve_parking_db_path()
    targets: List[Dict[str, Any]] = [
        {
            "key": "facility",
            "label": "시설관리 DB",
            "path": str(facility_path),
            "exists": facility_path.exists(),
            "site_scoped": True,
        },
        {
            "key": "parking",
            "label": "주차관리 DB",
            "path": str(parking_path),
            "exists": parking_path.exists(),
            "site_scoped": True,
        },
    ]
    for item in targets:
        try:
            item["size_bytes"] = int(Path(item["path"]).stat().st_size) if item["exists"] else 0
        except Exception:
            item["size_bytes"] = 0
    return targets


def list_backup_targets() -> List[Dict[str, Any]]:
    return [dict(x) for x in _backup_targets()]


def _targets_by_key() -> Dict[str, Dict[str, Any]]:
    return {str(x["key"]): x for x in _backup_targets()}


def _sqlite_backup_copy(src_path: Path, dst_path: Path) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(f"file:{src_path.as_posix()}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(str(dst_path))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def _sqlite_quick_check(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"ok": False, "detail": "missing"}
    try:
        con = sqlite3.connect(str(path))
        try:
            row = con.execute("PRAGMA quick_check").fetchone()
        finally:
            con.close()
        result = str(row[0] if row else "").strip().lower()
        if result == "ok":
            return {"ok": True, "detail": "ok"}
        return {"ok": False, "detail": result or "quick_check failed"}
    except Exception as e:
        return {"ok": False, "detail": str(e)}


def run_live_db_checks() -> Dict[str, Any]:
    targets = _backup_targets()
    checks: List[Dict[str, Any]] = []
    all_ok = True
    for t in targets:
        path = Path(str(t["path"]))
        check = _sqlite_quick_check(path)
        item = {
            "key": t["key"],
            "label": t["label"],
            "path": str(path),
            "ok": bool(check["ok"]),
            "detail": check["detail"],
        }
        checks.append(item)
        if not item["ok"]:
            all_ok = False
    return {"ok": all_ok, "checks": checks, "checked_at": _now_iso()}


def _trigger_label(trigger: str) -> str:
    t = str(trigger or "").strip().lower()
    if t == "manual":
        return "수동 실행"
    if t == "daily_0000":
        return "자동 실행(매일 00:00)"
    if t == "weekly_friday":
        return "자동 실행(매주 금요일)"
    return t or "unknown"


def _scope_label(scope: str) -> str:
    s = str(scope or "").strip().lower()
    if s == "full":
        return "전체 시스템"
    if s == "site":
        return "단지코드 범위"
    return s or "unknown"


def _sidecar_path(zip_path: Path) -> Path:
    return zip_path.with_suffix(zip_path.suffix + ".meta.json")


def _write_sidecar(zip_path: Path, metadata: Dict[str, Any]) -> None:
    sidecar = _sidecar_path(zip_path)
    payload = dict(metadata)
    payload["relative_path"] = str(zip_path.relative_to(BACKUP_ROOT)).replace("\\", "/")
    payload["file_name"] = zip_path.name
    payload["file_size_bytes"] = int(zip_path.stat().st_size) if zip_path.exists() else 0
    sidecar.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _mirror_backup_to_server_apt(zip_path: Path, metadata: Dict[str, Any]) -> None:
    if not zip_path.exists():
        return

    root_created = False
    if not SERVER_BACKUP_APT_ROOT.exists():
        SERVER_BACKUP_APT_ROOT.mkdir(parents=True, exist_ok=True)
        root_created = True

    rel = zip_path.relative_to(BACKUP_ROOT)
    mirror_zip = (SERVER_BACKUP_APT_ROOT / rel).resolve()
    mirror_zip.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(zip_path, mirror_zip)

    if root_created:
        notes = metadata.get("notes")
        if isinstance(notes, list):
            notes.append("서버 backup_APT 폴더가 없어 자동 생성 후 백업을 저장했습니다.")

    metadata["server_backup_root"] = str(SERVER_BACKUP_APT_ROOT)
    metadata["server_backup_relative_path"] = str(mirror_zip.relative_to(SERVER_BACKUP_APT_ROOT)).replace("\\", "/")
    metadata["server_backup_file_name"] = mirror_zip.name
    metadata["server_backup_saved"] = True

    # keep sidecar and mirrored sidecar in sync with latest metadata
    _write_sidecar(zip_path, metadata)
    side_src = _sidecar_path(zip_path)
    side_dst = _sidecar_path(mirror_zip)
    if side_src.exists():
        shutil.copy2(side_src, side_dst)


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return bool(row)


def _rows_to_dicts(rows: Iterable[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def _query_all(con: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    rows = con.execute(sql, params).fetchall()
    return _rows_to_dicts(rows)


def _preferred_existing_col(con: sqlite3.Connection, table: str, candidates: List[str]) -> str:
    cols = _table_columns_set(con, table)
    for name in candidates:
        key = str(name or "").strip()
        if key and key in cols:
            return key
    return ""


def _query_by_site_code(con: sqlite3.Connection, table: str, site_code: str) -> List[Dict[str, Any]]:
    if not _table_exists(con, table):
        return []
    clean = str(site_code or "").strip().upper()
    if not clean:
        return []
    code_col = _preferred_existing_col(con, table, ["site_code", "code"])
    if not code_col:
        return []
    sql = f"SELECT * FROM {table} WHERE {code_col}=?"
    rows = con.execute(sql, (clean,)).fetchall()
    return _rows_to_dicts(rows)


def _query_by_site_names(con: sqlite3.Connection, table: str, site_names: List[str]) -> List[Dict[str, Any]]:
    if not site_names or not _table_exists(con, table):
        return []
    name_col = _preferred_existing_col(con, table, ["site_name", "name"])
    if not name_col:
        return []
    ph = ",".join(["?"] * len(site_names))
    sql = f"SELECT * FROM {table} WHERE {name_col} IN ({ph})"
    rows = con.execute(sql, tuple(site_names)).fetchall()
    return _rows_to_dicts(rows)


def _query_by_int_ids(con: sqlite3.Connection, table: str, id_col: str, ids: List[int]) -> List[Dict[str, Any]]:
    if not ids or not _table_exists(con, table):
        return []
    norm_ids = [int(x) for x in ids if int(x or 0) > 0]
    if not norm_ids:
        return []
    ph = ",".join(["?"] * len(norm_ids))
    sql = f"SELECT * FROM {table} WHERE {id_col} IN ({ph})"
    rows = con.execute(sql, tuple(norm_ids)).fetchall()
    return _rows_to_dicts(rows)


def _table_columns_set(con: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(con, table):
        return set()
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    cols: set[str] = set()
    for row in rows:
        try:
            cols.add(str(row["name"]))
        except Exception:
            try:
                cols.add(str(row[1]))
            except Exception:
                continue
    return cols


def _exec_delete_by_site_code(con: sqlite3.Connection, table: str, site_code: str) -> int:
    code_col = _preferred_existing_col(con, table, ["site_code", "code"])
    if not code_col:
        return 0
    cur = con.execute(f"DELETE FROM {table} WHERE {code_col}=?", (str(site_code or "").strip().upper(),))
    return int(cur.rowcount or 0)


def _exec_delete_by_site_names(con: sqlite3.Connection, table: str, site_names: List[str]) -> int:
    names = [str(x or "").strip() for x in site_names if str(x or "").strip()]
    if not names:
        return 0
    name_col = _preferred_existing_col(con, table, ["site_name", "name"])
    if not name_col:
        return 0
    ph = ",".join(["?"] * len(names))
    cur = con.execute(f"DELETE FROM {table} WHERE {name_col} IN ({ph})", tuple(names))
    return int(cur.rowcount or 0)


def _exec_delete_by_int_ids(con: sqlite3.Connection, table: str, id_col: str, ids: List[int]) -> int:
    norm_ids = [int(x) for x in ids if int(x or 0) > 0]
    if not norm_ids or not _table_exists(con, table):
        return 0
    cols = _table_columns_set(con, table)
    if id_col not in cols:
        return 0
    ph = ",".join(["?"] * len(norm_ids))
    cur = con.execute(f"DELETE FROM {table} WHERE {id_col} IN ({ph})", tuple(norm_ids))
    return int(cur.rowcount or 0)


def _exec_upsert_rows(con: sqlite3.Connection, table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows or not _table_exists(con, table):
        return 0
    cols = _table_columns_set(con, table)
    if not cols:
        return 0
    inserted = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        keys = [k for k in row.keys() if k in cols]
        if not keys:
            continue
        placeholders = ",".join(["?"] * len(keys))
        col_sql = ",".join(keys)
        values = [row.get(k) for k in keys]
        con.execute(f"INSERT OR REPLACE INTO {table} ({col_sql}) VALUES ({placeholders})", tuple(values))
        inserted += 1
    return inserted


def _collect_site_names(con: sqlite3.Connection, site_code: str, fallback_site_name: str = "") -> List[str]:
    names: set[str] = set()
    clean_code = str(site_code or "").strip().upper()
    for table in ["site_registry", "staff_users", "site_env_configs"]:
        if not _table_exists(con, table):
            continue
        code_col = _preferred_existing_col(con, table, ["site_code", "code"])
        name_col = _preferred_existing_col(con, table, ["site_name", "name"])
        if not code_col or not name_col:
            continue
        rows = con.execute(f"SELECT {name_col} FROM {table} WHERE {code_col}=?", (clean_code,)).fetchall()
        for row in rows:
            value = str(row[0] or "").strip()
            if value:
                names.add(value)
    fallback = str(fallback_site_name or "").strip()
    if fallback:
        names.add(fallback)
    return sorted(names)


def _export_facility_site_data(site_code: str, site_name: str = "", include_user_tables: bool = True) -> Dict[str, Any]:
    db_path = Path(FACILITY_DB_PATH).resolve()
    out: Dict[str, Any] = {
        "db_key": "facility",
        "db_label": "시설관리 DB",
        "db_path": str(db_path),
        "site_code": site_code,
        "site_name": site_name,
        "generated_at": _now_iso(),
        "tables": {},
    }
    if not db_path.exists():
        out["ok"] = False
        out["detail"] = "시설관리 DB 파일이 없습니다."
        return out

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        names = _collect_site_names(con, site_code, site_name)
        out["site_names"] = names

        tables: Dict[str, Any] = {}
        tables["site_registry"] = _query_by_site_code(con, "site_registry", site_code)
        tables["site_env_configs"] = _query_by_site_code(con, "site_env_configs", site_code)
        if include_user_tables:
            tables["staff_users"] = _query_by_site_code(con, "staff_users", site_code)

        sites_rows: List[Dict[str, Any]] = []
        if names and _table_exists(con, "sites"):
            ph = ",".join(["?"] * len(names))
            sites_rows = _query_all(
                con,
                f"SELECT * FROM sites WHERE name IN ({ph})",
                tuple(names),
            )
        tables["sites"] = sites_rows
        site_ids = [int(r.get("id") or 0) for r in sites_rows if int(r.get("id") or 0) > 0]
        entry_rows: List[Dict[str, Any]] = []
        if site_ids and _table_exists(con, "entries"):
            ph = ",".join(["?"] * len(site_ids))
            entry_rows = _query_all(
                con,
                f"SELECT * FROM entries WHERE site_id IN ({ph})",
                tuple(site_ids),
            )
        tables["entries"] = entry_rows

        entry_ids = [int(r.get("id") or 0) for r in entry_rows if int(r.get("id") or 0) > 0]
        entry_value_rows: List[Dict[str, Any]] = []
        if entry_ids and _table_exists(con, "entry_values"):
            ph = ",".join(["?"] * len(entry_ids))
            entry_value_rows = _query_all(
                con,
                f"SELECT * FROM entry_values WHERE entry_id IN ({ph})",
                tuple(entry_ids),
            )
        tables["entry_values"] = entry_value_rows

        for tab_table in [
            "transformer_450_reads",
            "transformer_400_reads",
            "power_meter_reads",
            "facility_checks",
            "facility_subtasks",
        ]:
            tables[tab_table] = _query_by_site_names(con, tab_table, names)

        # Safety inspection module tables (site-scoped backup)
        inspection_targets = _query_by_site_code(con, "inspection_targets", site_code)
        tables["inspection_targets"] = inspection_targets
        target_ids = [int(r.get("id") or 0) for r in inspection_targets if int(r.get("id") or 0) > 0]

        inspection_regulations = _query_by_int_ids(con, "inspection_regulations", "target_id", target_ids)
        tables["inspection_regulations"] = inspection_regulations

        inspection_templates = _query_by_site_code(con, "inspection_templates", site_code)
        tables["inspection_templates"] = inspection_templates
        template_ids = [int(r.get("id") or 0) for r in inspection_templates if int(r.get("id") or 0) > 0]

        tables["inspection_template_items"] = _query_by_int_ids(
            con,
            "inspection_template_items",
            "template_id",
            template_ids,
        )
        tables["inspection_template_backups"] = _query_by_site_code(con, "inspection_template_backups", site_code)

        inspection_runs = _query_by_site_code(con, "inspection_runs", site_code)
        tables["inspection_runs"] = inspection_runs
        run_ids = [int(r.get("id") or 0) for r in inspection_runs if int(r.get("id") or 0) > 0]

        tables["inspection_run_items"] = _query_by_int_ids(
            con,
            "inspection_run_items",
            "run_id",
            run_ids,
        )
        tables["inspection_approvals"] = _query_by_int_ids(
            con,
            "inspection_approvals",
            "run_id",
            run_ids,
        )
        tables["inspection_archives"] = _query_by_site_code(con, "inspection_archives", site_code)

        # Complaints module tables (site-scoped backup)
        complaint_rows = _query_by_site_code(con, "complaints", site_code)
        tables["complaints"] = complaint_rows
        complaint_ids = [int(r.get("id") or 0) for r in complaint_rows if int(r.get("id") or 0) > 0]

        tables["complaint_attachments"] = _query_by_int_ids(
            con,
            "complaint_attachments",
            "complaint_id",
            complaint_ids,
        )
        tables["complaint_status_history"] = _query_by_int_ids(
            con,
            "complaint_status_history",
            "complaint_id",
            complaint_ids,
        )
        tables["complaint_comments"] = _query_by_int_ids(
            con,
            "complaint_comments",
            "complaint_id",
            complaint_ids,
        )
        tables["complaint_work_orders"] = _query_by_int_ids(
            con,
            "complaint_work_orders",
            "complaint_id",
            complaint_ids,
        )
        tables["complaint_visit_logs"] = _query_by_int_ids(
            con,
            "complaint_visit_logs",
            "complaint_id",
            complaint_ids,
        )
        # Shared lookups/templates used by complaints module
        tables["complaint_categories"] = _query_all(
            con,
            "SELECT * FROM complaint_categories",
            (),
        ) if _table_exists(con, "complaint_categories") else []
        tables["complaint_guidance_templates"] = _query_all(
            con,
            "SELECT * FROM complaint_guidance_templates",
            (),
        ) if _table_exists(con, "complaint_guidance_templates") else []
        tables["complaint_notices"] = _query_all(
            con,
            "SELECT * FROM complaint_notices",
            (),
        ) if _table_exists(con, "complaint_notices") else []
        tables["complaint_faqs"] = _query_all(
            con,
            "SELECT * FROM complaint_faqs",
            (),
        ) if _table_exists(con, "complaint_faqs") else []

        row_counts: Dict[str, int] = {}
        total_rows = 0
        for name, rows in tables.items():
            count = len(rows) if isinstance(rows, list) else 0
            row_counts[name] = count
            total_rows += count
        out["tables"] = tables
        out["row_counts"] = row_counts
        out["total_rows"] = total_rows
        out["ok"] = True
        out["detail"] = "ok"
        return out
    finally:
        con.close()


def _export_parking_site_data(site_code: str, site_name: str = "") -> Dict[str, Any]:
    db_path = _resolve_parking_db_path()
    out: Dict[str, Any] = {
        "db_key": "parking",
        "db_label": "주차관리 DB",
        "db_path": str(db_path),
        "site_code": site_code,
        "site_name": site_name,
        "generated_at": _now_iso(),
        "tables": {},
    }
    if not db_path.exists():
        out["ok"] = False
        out["detail"] = "주차관리 DB 파일이 없습니다."
        return out

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        tables: Dict[str, Any] = {}
        if _table_exists(con, "vehicles"):
            rows = _query_by_site_code(con, "vehicles", site_code)
            rows.sort(key=lambda x: str(x.get("plate") or ""))
            tables["vehicles"] = rows
        if _table_exists(con, "violations"):
            rows = _query_by_site_code(con, "violations", site_code)
            rows.sort(key=lambda x: (str(x.get("created_at") or ""), int(x.get("id") or 0)), reverse=True)
            tables["violations"] = rows
        row_counts: Dict[str, int] = {}
        total_rows = 0
        for name, rows in tables.items():
            count = len(rows) if isinstance(rows, list) else 0
            row_counts[name] = count
            total_rows += count
        out["tables"] = tables
        out["row_counts"] = row_counts
        out["total_rows"] = total_rows
        out["ok"] = True
        out["detail"] = "ok"
        return out
    finally:
        con.close()


def _resolve_selected_targets(target_keys: Iterable[str] | None, scope: str) -> List[Dict[str, Any]]:
    catalog = _targets_by_key()
    keys = [str(x or "").strip().lower() for x in (target_keys or []) if str(x or "").strip()]
    if not keys:
        if scope == "site":
            keys = [k for k, v in catalog.items() if bool(v.get("site_scoped"))]
        else:
            keys = list(catalog.keys())
    selected: List[Dict[str, Any]] = []
    for key in keys:
        item = catalog.get(key)
        if not item:
            continue
        if scope == "site" and not bool(item.get("site_scoped")):
            continue
        selected.append(dict(item))
    dedup: Dict[str, Dict[str, Any]] = {str(x["key"]): x for x in selected}
    return [dict(v) for v in dedup.values()]


def _metadata_base(
    *,
    scope: str,
    trigger: str,
    actor: str,
    target_items: List[Dict[str, Any]],
    site_id: int = 0,
    site_code: str = "",
    site_name: str = "",
    maintenance_enabled: bool = False,
    contains_user_data: bool = False,
) -> Dict[str, Any]:
    created_at = _now_iso()
    return {
        "ok": True,
        "timezone": backup_timezone_name(),
        "scope": scope,
        "scope_label": _scope_label(scope),
        "trigger": trigger,
        "trigger_label": _trigger_label(trigger),
        "actor": actor,
        "created_at": created_at,
        "site_id": int(site_id or 0),
        "site_code": site_code,
        "site_name": site_name,
        "target_keys": [str(x["key"]) for x in target_items],
        "target_labels": [str(x["label"]) for x in target_items],
        "maintenance_enabled": bool(maintenance_enabled),
        "contains_user_data": bool(contains_user_data),
        "notes": [],
        "checks": [],
    }


def _enrich_history_item(meta: Dict[str, Any], zip_path: Path) -> Dict[str, Any]:
    item = dict(meta)
    item["relative_path"] = str(zip_path.relative_to(BACKUP_ROOT)).replace("\\", "/")
    item["file_name"] = zip_path.name
    item["file_size_bytes"] = int(zip_path.stat().st_size) if zip_path.exists() else 0
    item["download_name"] = zip_path.name
    return item


def _run_full_backup(
    *,
    target_items: List[Dict[str, Any]],
    trigger: str,
    actor: str,
    maintenance_enabled: bool,
) -> Dict[str, Any]:
    ts = _now().strftime("%Y%m%d_%H%M%S")
    safe_trigger = _sanitize_token(trigger, default="manual")
    out_dir = FULL_BACKUP_DIR / _now().strftime("%Y%m%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"full_{ts}_{safe_trigger}.zip"
    tmp_dir = TMP_BACKUP_DIR / f"full_{ts}_{uuid.uuid4().hex[:8]}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    meta = _metadata_base(
        scope="full",
        trigger=trigger,
        actor=actor,
        target_items=target_items,
        maintenance_enabled=maintenance_enabled,
        contains_user_data=True,
    )
    maintenance_released = False
    if maintenance_enabled:
        set_maintenance_mode(
            active=True,
            message="서버 점검 중입니다. 전체 DB 백업이 진행 중입니다. 잠시 후 자동 복구됩니다.",
            reason="daily_full_backup",
            updated_by=actor,
        )

    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for item in target_items:
                label = str(item.get("label") or item.get("key"))
                src = Path(str(item.get("path") or ""))
                if not src.exists():
                    note = f"{label}: DB 파일이 없어 제외했습니다."
                    meta["notes"].append(note)
                    continue
                copied = tmp_dir / f"{item['key']}.db"
                _sqlite_backup_copy(src, copied)
                check = _sqlite_quick_check(copied)
                check_item = {
                    "key": item["key"],
                    "label": label,
                    "ok": bool(check["ok"]),
                    "detail": check["detail"],
                }
                meta["checks"].append(check_item)
                if not check_item["ok"]:
                    raise RuntimeError(f"{label} 백업본 점검 실패: {check_item['detail']}")
                zf.write(copied, arcname=f"db/{item['key']}.db")

            zf.writestr("manifest.json", json.dumps(meta, ensure_ascii=False, indent=2))

        live_checks = run_live_db_checks()
        meta["post_backup_live_checks"] = live_checks
        if maintenance_enabled:
            if bool(live_checks.get("ok")):
                clear_maintenance_mode(updated_by=actor)
                maintenance_released = True
            else:
                set_maintenance_mode(
                    active=True,
                    message="백업 후 DB 점검 실패로 점검모드를 유지합니다. 관리자 확인이 필요합니다.",
                    reason="post_backup_check_failed",
                    updated_by=actor,
                )
        meta["maintenance_released"] = maintenance_released
        _write_sidecar(zip_path, meta)
        try:
            _mirror_backup_to_server_apt(zip_path, meta)
        except Exception as e:
            meta["server_backup_saved"] = False
            meta["notes"].append(f"서버 backup_APT 저장 실패: {e}")
            _write_sidecar(zip_path, meta)
            logger.exception("Failed to mirror backup to server backup_APT: %s", zip_path)
        cleanup_old_backups()
        return _enrich_history_item(meta, zip_path)
    except Exception:
        meta["ok"] = False
        meta["notes"].append("백업 실패")
        if maintenance_enabled:
            set_maintenance_mode(
                active=True,
                message="DB 백업 실패로 점검모드를 유지합니다. 관리자 확인이 필요합니다.",
                reason="full_backup_failed",
                updated_by=actor,
            )
        if zip_path.exists():
            try:
                zip_path.unlink()
            except Exception:
                logger.exception("Failed to remove failed backup zip: %s", zip_path)
        raise
    finally:
        try:
            for p in tmp_dir.glob("*"):
                p.unlink(missing_ok=True)
            tmp_dir.rmdir()
        except Exception:
            logger.exception("Failed to cleanup backup temp dir: %s", tmp_dir)


def _run_site_backup(
    *,
    target_items: List[Dict[str, Any]],
    trigger: str,
    actor: str,
    site_id: int,
    site_code: str,
    site_name: str,
    include_user_tables: bool,
) -> Dict[str, Any]:
    ts = _now().strftime("%Y%m%d_%H%M%S")
    safe_trigger = _sanitize_token(trigger, default="manual")
    clean_code = str(site_code or "").strip().upper()
    clean_name = str(site_name or "").strip()
    out_dir = SITE_BACKUP_DIR / (clean_code or "UNKNOWN")
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"site_{clean_code}_{ts}_{safe_trigger}.zip"

    meta = _metadata_base(
        scope="site",
        trigger=trigger,
        actor=actor,
        target_items=target_items,
        site_id=int(site_id or 0),
        site_code=clean_code,
        site_name=clean_name,
        maintenance_enabled=False,
        contains_user_data=bool(include_user_tables and any(str(x.get("key") or "") == "facility" for x in target_items)),
    )

    payloads: Dict[str, Dict[str, Any]] = {}
    for item in target_items:
        key = str(item.get("key") or "")
        if key == "facility":
            payloads[key] = _export_facility_site_data(clean_code, clean_name, include_user_tables=include_user_tables)
        elif key == "parking":
            payloads[key] = _export_parking_site_data(clean_code, clean_name)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for key, data in payloads.items():
            zf.writestr(f"site_data/{key}.json", json.dumps(data, ensure_ascii=False, indent=2))
            meta["checks"].append(
                {
                    "key": key,
                    "label": next((x["label"] for x in target_items if x["key"] == key), key),
                    "ok": bool(data.get("ok")),
                    "detail": str(data.get("detail") or ""),
                    "rows": int(data.get("total_rows") or 0),
                }
            )
        zf.writestr("manifest.json", json.dumps(meta, ensure_ascii=False, indent=2))

    _write_sidecar(zip_path, meta)
    try:
        _mirror_backup_to_server_apt(zip_path, meta)
    except Exception as e:
        meta["server_backup_saved"] = False
        meta["notes"].append(f"서버 backup_APT 저장 실패: {e}")
        _write_sidecar(zip_path, meta)
        logger.exception("Failed to mirror site backup to server backup_APT: %s", zip_path)
    cleanup_old_backups()
    return _enrich_history_item(meta, zip_path)


def run_manual_backup(
    *,
    actor: str,
    trigger: str = "manual",
    target_keys: Iterable[str] | None = None,
    scope: str = "full",
    site_id: int = 0,
    site_code: str = "",
    site_name: str = "",
    with_maintenance: bool = False,
    include_user_tables: bool = True,
) -> Dict[str, Any]:
    clean_scope = str(scope or "").strip().lower()
    if clean_scope not in {"full", "site"}:
        raise ValueError("scope must be 'full' or 'site'")
    if clean_scope == "site":
        try:
            clean_site_id = int(site_id or 0)
        except Exception as e:
            raise ValueError("site_id must be integer") from e
        if clean_site_id < 0:
            raise ValueError("site_id must be positive")
        clean_site_code = str(site_code or "").strip().upper()
        if not clean_site_code:
            raise ValueError("site_code is required for site backup")
    else:
        clean_site_id = 0
        clean_site_code = ""

    selected = _resolve_selected_targets(target_keys, clean_scope)
    if not selected:
        raise ValueError("선택 가능한 백업 대상이 없습니다.")

    acquired = _RUN_LOCK.acquire(blocking=False)
    if not acquired:
        raise RuntimeError("이미 다른 백업 작업이 실행 중입니다.")
    try:
        if clean_scope == "full":
            return _run_full_backup(
                target_items=selected,
                trigger=trigger,
                actor=actor,
                maintenance_enabled=bool(with_maintenance),
            )
        return _run_site_backup(
            target_items=selected,
            trigger=trigger,
            actor=actor,
            site_id=clean_site_id,
            site_code=clean_site_code,
            site_name=str(site_name or "").strip(),
            include_user_tables=bool(include_user_tables),
        )
    finally:
        _RUN_LOCK.release()


def _read_manifest_from_zip(zip_path: Path) -> Dict[str, Any]:
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            if "manifest.json" not in zf.namelist():
                return {}
            with zf.open("manifest.json", "r") as fp:
                raw = json.loads(fp.read().decode("utf-8", errors="ignore"))
                if isinstance(raw, dict):
                    return raw
    except Exception:
        logger.exception("Failed to read backup manifest: %s", zip_path)
    return {}


def _sqlite_restore_copy(src_path: Path, dst_path: Path) -> None:
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(f"file:{src_path.as_posix()}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(str(dst_path), timeout=30)
        try:
            dst.execute("PRAGMA busy_timeout=30000")
            src.backup(dst)
            dst.commit()
        finally:
            dst.close()
    finally:
        src.close()


def _payload_table_rows(payload: Dict[str, Any], table: str) -> List[Dict[str, Any]]:
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        return []
    rows = tables.get(str(table))
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
    return out


def _rows_int_ids(rows: Iterable[Dict[str, Any]], key: str = "id") -> List[int]:
    out: List[int] = []
    for row in rows:
        try:
            value = int((row or {}).get(key) or 0)
        except Exception:
            value = 0
        if value > 0:
            out.append(value)
    return sorted(set(out))


def _restore_facility_site_payload(site_code: str, payload: Dict[str, Any], include_user_tables: bool = True) -> Dict[str, Any]:
    db_path = Path(FACILITY_DB_PATH).resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"시설관리 DB 파일이 없습니다: {db_path}")

    clean_site_code = str(site_code or "").strip().upper()
    if not clean_site_code:
        raise ValueError("site restore requires site_code")

    payload_site_code = str(payload.get("site_code") or "").strip().upper()
    if payload_site_code and payload_site_code != clean_site_code:
        raise ValueError(f"site_code mismatch: backup={payload_site_code} request={clean_site_code}")

    payload_site_name = str(payload.get("site_name") or "").strip()
    payload_site_names = [
        str(x or "").strip()
        for x in (payload.get("site_names") or [])
        if str(x or "").strip()
    ]

    deleted: Dict[str, int] = {}
    inserted: Dict[str, int] = {}

    def _add_count(bucket: Dict[str, int], table: str, count: int) -> None:
        n = int(count or 0)
        if n <= 0:
            return
        bucket[str(table)] = int(bucket.get(str(table), 0)) + n

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("BEGIN IMMEDIATE")

        live_site_names = _collect_site_names(con, clean_site_code, payload_site_name)
        scoped_site_names = sorted(set(live_site_names + payload_site_names + ([payload_site_name] if payload_site_name else [])))

        live_sites = _query_by_site_names(con, "sites", scoped_site_names)
        live_site_ids = _rows_int_ids(live_sites, "id")
        live_entries = _query_by_int_ids(con, "entries", "site_id", live_site_ids)
        live_entry_ids = _rows_int_ids(live_entries, "id")

        live_targets = _query_by_site_code(con, "inspection_targets", clean_site_code)
        live_target_ids = _rows_int_ids(live_targets, "id")

        live_templates = _query_by_site_code(con, "inspection_templates", clean_site_code)
        live_template_ids = _rows_int_ids(live_templates, "id")

        live_runs = _query_by_site_code(con, "inspection_runs", clean_site_code)
        live_run_ids = _rows_int_ids(live_runs, "id")

        live_complaints = _query_by_site_code(con, "complaints", clean_site_code)
        live_complaint_ids = _rows_int_ids(live_complaints, "id")

        _add_count(deleted, "entry_values", _exec_delete_by_int_ids(con, "entry_values", "entry_id", live_entry_ids))
        _add_count(deleted, "inspection_regulations", _exec_delete_by_int_ids(con, "inspection_regulations", "target_id", live_target_ids))
        _add_count(deleted, "inspection_template_items", _exec_delete_by_int_ids(con, "inspection_template_items", "template_id", live_template_ids))
        _add_count(deleted, "inspection_run_items", _exec_delete_by_int_ids(con, "inspection_run_items", "run_id", live_run_ids))
        _add_count(deleted, "inspection_approvals", _exec_delete_by_int_ids(con, "inspection_approvals", "run_id", live_run_ids))
        _add_count(deleted, "complaint_attachments", _exec_delete_by_int_ids(con, "complaint_attachments", "complaint_id", live_complaint_ids))
        _add_count(deleted, "complaint_status_history", _exec_delete_by_int_ids(con, "complaint_status_history", "complaint_id", live_complaint_ids))
        _add_count(deleted, "complaint_comments", _exec_delete_by_int_ids(con, "complaint_comments", "complaint_id", live_complaint_ids))
        _add_count(deleted, "complaint_work_orders", _exec_delete_by_int_ids(con, "complaint_work_orders", "complaint_id", live_complaint_ids))
        _add_count(deleted, "complaint_visit_logs", _exec_delete_by_int_ids(con, "complaint_visit_logs", "complaint_id", live_complaint_ids))

        for table in [
            "transformer_450_reads",
            "transformer_400_reads",
            "power_meter_reads",
            "facility_checks",
            "facility_subtasks",
        ]:
            _add_count(deleted, table, _exec_delete_by_site_names(con, table, scoped_site_names))

        _add_count(deleted, "entries", _exec_delete_by_int_ids(con, "entries", "site_id", live_site_ids))
        _add_count(deleted, "sites", _exec_delete_by_site_names(con, "sites", scoped_site_names))

        for table in [
            "inspection_archives",
            "inspection_runs",
            "inspection_template_backups",
            "inspection_templates",
            "inspection_targets",
            "complaints",
            "site_env_configs",
            "site_registry",
        ]:
            _add_count(deleted, table, _exec_delete_by_site_code(con, table, clean_site_code))
        if include_user_tables:
            _add_count(deleted, "staff_users", _exec_delete_by_site_code(con, "staff_users", clean_site_code))

        for table in [
            "site_registry",
            "site_env_configs",
            "sites",
            "entries",
            "entry_values",
            "transformer_450_reads",
            "transformer_400_reads",
            "power_meter_reads",
            "facility_checks",
            "facility_subtasks",
            "inspection_targets",
            "inspection_regulations",
            "inspection_templates",
            "inspection_template_items",
            "inspection_template_backups",
            "inspection_runs",
            "inspection_run_items",
            "inspection_approvals",
            "inspection_archives",
            "complaints",
            "complaint_attachments",
            "complaint_status_history",
            "complaint_comments",
            "complaint_work_orders",
            "complaint_visit_logs",
            # Shared lookup tables are upsert-only (no scoped delete)
            "complaint_categories",
            "complaint_guidance_templates",
            "complaint_notices",
            "complaint_faqs",
        ]:
            rows = _payload_table_rows(payload, table)
            _add_count(inserted, table, _exec_upsert_rows(con, table, rows))
        if include_user_tables:
            rows = _payload_table_rows(payload, "staff_users")
            _add_count(inserted, "staff_users", _exec_upsert_rows(con, "staff_users", rows))

        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        raise
    finally:
        con.close()

    return {
        "db_key": "facility",
        "site_code": clean_site_code,
        "deleted_rows": sum(int(v or 0) for v in deleted.values()),
        "inserted_rows": sum(int(v or 0) for v in inserted.values()),
        "deleted_by_table": deleted,
        "inserted_by_table": inserted,
    }


def _restore_parking_site_payload(site_code: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    db_path = _resolve_parking_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"주차관리 DB 파일이 없습니다: {db_path}")

    clean_site_code = str(site_code or "").strip().upper()
    if not clean_site_code:
        raise ValueError("site restore requires site_code")

    payload_site_code = str(payload.get("site_code") or "").strip().upper()
    if payload_site_code and payload_site_code != clean_site_code:
        raise ValueError(f"site_code mismatch: backup={payload_site_code} request={clean_site_code}")

    deleted: Dict[str, int] = {}
    inserted: Dict[str, int] = {}

    def _add_count(bucket: Dict[str, int], table: str, count: int) -> None:
        n = int(count or 0)
        if n <= 0:
            return
        bucket[str(table)] = int(bucket.get(str(table), 0)) + n

    con = sqlite3.connect(str(db_path))
    try:
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("BEGIN IMMEDIATE")

        for table in ["violations", "vehicles"]:
            _add_count(deleted, table, _exec_delete_by_site_code(con, table, clean_site_code))

        for table in ["vehicles", "violations"]:
            rows = _payload_table_rows(payload, table)
            _add_count(inserted, table, _exec_upsert_rows(con, table, rows))

        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        raise
    finally:
        con.close()

    return {
        "db_key": "parking",
        "site_code": clean_site_code,
        "deleted_rows": sum(int(v or 0) for v in deleted.values()),
        "inserted_rows": sum(int(v or 0) for v in inserted.values()),
        "deleted_by_table": deleted,
        "inserted_by_table": inserted,
    }


def restore_backup_zip(
    *,
    actor: str,
    relative_path: str,
    target_keys: Iterable[str] | None = None,
    with_maintenance: bool = True,
    include_user_tables: bool = True,
) -> Dict[str, Any]:
    zip_path = resolve_backup_file(relative_path)
    manifest = _read_manifest_from_zip(zip_path)
    scope = str(manifest.get("scope") or "").strip().lower()
    if scope and scope not in {"full", "site"}:
        raise ValueError("지원하지 않는 백업 범위입니다.")

    catalog = _targets_by_key()
    available_keys = [str(k) for k in catalog.keys()]
    selected_keys = [str(x or "").strip().lower() for x in (target_keys or []) if str(x or "").strip()]
    if not selected_keys:
        m_keys = manifest.get("target_keys")
        if isinstance(m_keys, list) and m_keys:
            selected_keys = [str(x or "").strip().lower() for x in m_keys if str(x or "").strip()]
    if not selected_keys:
        if scope == "site":
            selected_keys = [k for k, v in catalog.items() if bool(v.get("site_scoped"))]
        else:
            selected_keys = list(available_keys)

    invalid = [k for k in selected_keys if k not in available_keys]
    if invalid:
        raise ValueError(f"invalid target_keys: {', '.join(invalid)}")
    if scope == "site":
        not_scoped = [k for k in selected_keys if not bool(catalog.get(k, {}).get("site_scoped"))]
        if not_scoped:
            raise ValueError(f"site 복구에서 지원하지 않는 대상입니다: {', '.join(not_scoped)}")

    site_code = str(manifest.get("site_code") or "").strip().upper() if scope == "site" else ""
    site_name = str(manifest.get("site_name") or "").strip() if scope == "site" else ""
    if scope == "site" and not site_code:
        raise ValueError("site 백업 복구에는 manifest.site_code가 필요합니다.")

    acquired = _RUN_LOCK.acquire(blocking=False)
    if not acquired:
        raise RuntimeError("이미 다른 백업/복구 작업이 실행 중입니다.")

    ts = _now().strftime("%Y%m%d_%H%M%S")
    rollback_base = SITE_BACKUP_DIR if scope == "site" else FULL_BACKUP_DIR
    rollback_dir = rollback_base / _now().strftime("%Y%m%d")
    rollback_dir.mkdir(parents=True, exist_ok=True)
    rollback_name = f"pre_restore_site_{site_code}_{ts}.zip" if scope == "site" else f"pre_restore_{ts}.zip"
    rollback_zip = rollback_dir / rollback_name
    tmp_dir = TMP_BACKUP_DIR / f"restore_{ts}_{uuid.uuid4().hex[:8]}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    meta: Dict[str, Any] = {
        "ok": True,
        "operation": "restore",
        "scope": scope or "full",
        "timezone": backup_timezone_name(),
        "source_relative_path": str(relative_path or "").replace("\\", "/"),
        "source_file_name": zip_path.name,
        "created_at": _now_iso(),
        "actor": str(actor or "").strip() or "system",
        "target_keys": list(selected_keys),
        "target_labels": [str(catalog[k]["label"]) for k in selected_keys],
        "with_maintenance": bool(with_maintenance),
        "include_user_tables": bool(include_user_tables),
        "checks": [],
        "notes": [],
        "rollback_relative_path": "",
    }
    if scope == "site":
        meta["site_code"] = site_code
        meta["site_name"] = site_name

    maintenance_released = False
    if with_maintenance:
        set_maintenance_mode(
            active=True,
            message=(
                "서버 점검 중입니다. 단지코드 DB 복구가 진행 중입니다. 잠시 후 자동 복구됩니다."
                if scope == "site"
                else "서버 점검 중입니다. DB 복구가 진행 중입니다. 잠시 후 자동 복구됩니다."
            ),
            reason="db_restore",
            updated_by=meta["actor"],
        )

    try:
        if scope == "site":
            staged_payloads: Dict[str, Dict[str, Any]] = {}
            with zipfile.ZipFile(zip_path, "r") as zf:
                members = set(zf.namelist())
                for key in selected_keys:
                    member = f"site_data/{key}.json"
                    if member not in members:
                        raise ValueError(f"백업 파일에 '{member}' 항목이 없습니다.")
                    with zf.open(member, "r") as src_fp:
                        raw = json.loads(src_fp.read().decode("utf-8", errors="ignore"))
                    if not isinstance(raw, dict):
                        raise ValueError(f"{member} 형식이 올바르지 않습니다.")
                    staged_site_code = str(raw.get("site_code") or "").strip().upper()
                    if staged_site_code and staged_site_code != site_code:
                        raise ValueError(f"{key} site_code 불일치: {staged_site_code} != {site_code}")
                    staged_payloads[key] = raw
                    meta["checks"].append(
                        {
                            "key": key,
                            "label": str(catalog[key]["label"]),
                            "stage": "staged",
                            "ok": True,
                            "detail": "ok",
                            "rows": int(raw.get("total_rows") or 0),
                        }
                    )

            with zipfile.ZipFile(rollback_zip, "w", compression=zipfile.ZIP_DEFLATED) as rb:
                rb_meta = {
                    "ok": True,
                    "operation": "pre_restore_snapshot",
                    "scope": "site",
                    "created_at": _now_iso(),
                    "timezone": backup_timezone_name(),
                    "actor": meta["actor"],
                    "site_code": site_code,
                    "site_name": site_name,
                    "source_restore_file": meta["source_relative_path"],
                    "target_keys": list(selected_keys),
                }
                for key in selected_keys:
                    if key == "facility":
                        snap = _export_facility_site_data(site_code, site_name, include_user_tables=bool(include_user_tables))
                    elif key == "parking":
                        snap = _export_parking_site_data(site_code, site_name)
                    else:
                        continue
                    rb.writestr(f"site_data/{key}.json", json.dumps(snap, ensure_ascii=False, indent=2))
                rb.writestr("manifest.json", json.dumps(rb_meta, ensure_ascii=False, indent=2))

            for key in selected_keys:
                if key == "facility":
                    summary = _restore_facility_site_payload(
                        site_code,
                        staged_payloads.get(key, {}),
                        include_user_tables=bool(include_user_tables),
                    )
                elif key == "parking":
                    summary = _restore_parking_site_payload(site_code, staged_payloads.get(key, {}))
                else:
                    continue
                meta["checks"].append(
                    {
                        "key": key,
                        "label": str(catalog[key]["label"]),
                        "stage": "restored",
                        "ok": True,
                        "detail": "ok",
                        "deleted_rows": int(summary.get("deleted_rows") or 0),
                        "inserted_rows": int(summary.get("inserted_rows") or 0),
                    }
                )
        else:
            staged: Dict[str, Path] = {}
            with zipfile.ZipFile(zip_path, "r") as zf:
                members = set(zf.namelist())
                for key in selected_keys:
                    member = f"db/{key}.db"
                    if member not in members:
                        raise ValueError(f"백업 파일에 '{key}' DB가 없습니다.")
                    out = tmp_dir / f"restore_{key}.db"
                    with zf.open(member, "r") as src_fp, open(out, "wb") as dst_fp:
                        shutil.copyfileobj(src_fp, dst_fp)
                    check = _sqlite_quick_check(out)
                    chk = {
                        "key": key,
                        "label": str(catalog[key]["label"]),
                        "stage": "staged",
                        "ok": bool(check["ok"]),
                        "detail": str(check["detail"]),
                    }
                    meta["checks"].append(chk)
                    if not chk["ok"]:
                        raise RuntimeError(f"{chk['label']} 복구본 점검 실패: {chk['detail']}")
                    staged[key] = out

            # Keep a rollback snapshot before overwrite.
            with zipfile.ZipFile(rollback_zip, "w", compression=zipfile.ZIP_DEFLATED) as rb:
                rb_meta = {
                    "ok": True,
                    "operation": "pre_restore_snapshot",
                    "scope": "full",
                    "created_at": _now_iso(),
                    "timezone": backup_timezone_name(),
                    "actor": meta["actor"],
                    "source_restore_file": meta["source_relative_path"],
                    "target_keys": list(selected_keys),
                }
                for key in selected_keys:
                    dst_db = Path(str(catalog[key]["path"]))
                    if not dst_db.exists():
                        continue
                    snap = tmp_dir / f"before_{key}.db"
                    _sqlite_backup_copy(dst_db, snap)
                    rb.write(snap, arcname=f"db/{key}.db")
                rb.writestr("manifest.json", json.dumps(rb_meta, ensure_ascii=False, indent=2))

            for key in selected_keys:
                src_db = staged[key]
                dst_db = Path(str(catalog[key]["path"]))
                _sqlite_restore_copy(src_db, dst_db)
                check = _sqlite_quick_check(dst_db)
                chk = {
                    "key": key,
                    "label": str(catalog[key]["label"]),
                    "stage": "restored",
                    "ok": bool(check["ok"]),
                    "detail": str(check["detail"]),
                }
                meta["checks"].append(chk)
                if not chk["ok"]:
                    raise RuntimeError(f"{chk['label']} 복구 후 점검 실패: {chk['detail']}")

        rb_relative = str(rollback_zip.relative_to(BACKUP_ROOT)).replace("\\", "/")
        meta["rollback_relative_path"] = rb_relative

        live_checks = run_live_db_checks()
        meta["post_restore_live_checks"] = live_checks

        if with_maintenance:
            if bool(live_checks.get("ok")):
                clear_maintenance_mode(updated_by=meta["actor"])
                maintenance_released = True
            else:
                set_maintenance_mode(
                    active=True,
                    message="복구 후 DB 점검 실패로 점검모드를 유지합니다. 관리자 확인이 필요합니다.",
                    reason="post_restore_check_failed",
                    updated_by=meta["actor"],
                )

        meta["maintenance_released"] = maintenance_released
        meta["completed_at"] = _now_iso()
        return meta
    except Exception:
        meta["ok"] = False
        meta["notes"].append("DB 복구 실패")
        if with_maintenance:
            set_maintenance_mode(
                active=True,
                message="DB 복구 실패로 점검모드를 유지합니다. 관리자 확인이 필요합니다.",
                reason="restore_failed",
                updated_by=meta["actor"],
            )
        raise
    finally:
        try:
            for p in tmp_dir.glob("*"):
                p.unlink(missing_ok=True)
            tmp_dir.rmdir()
        except Exception:
            logger.exception("Failed to cleanup restore temp dir: %s", tmp_dir)
        _RUN_LOCK.release()


def _history_items_all() -> List[Dict[str, Any]]:
    _ensure_dirs()
    items: List[Dict[str, Any]] = []
    for sidecar in BACKUP_ROOT.rglob("*.zip.meta.json"):
        try:
            meta = json.loads(sidecar.read_text(encoding="utf-8"))
            if not isinstance(meta, dict):
                continue
            file_name = str(meta.get("file_name") or "")
            if not file_name:
                file_name = sidecar.name.replace(".meta.json", "")
            if sidecar.name.endswith(".meta.json"):
                zip_name = sidecar.name[: -len(".meta.json")]
                zip_path = sidecar.with_name(zip_name)
            else:
                zip_path = sidecar
            if not zip_path.exists():
                continue
            item = dict(meta)
            item["file_name"] = file_name
            item["relative_path"] = str(zip_path.relative_to(BACKUP_ROOT)).replace("\\", "/")
            item["file_size_bytes"] = int(zip_path.stat().st_size)
            item["download_name"] = file_name
            items.append(item)
        except Exception:
            logger.exception("Failed to parse backup sidecar: %s", sidecar)
    items.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return items


def list_backup_history(
    *,
    limit: int = 50,
    scope: str = "",
    site_code: str = "",
) -> List[Dict[str, Any]]:
    items = _history_items_all()
    clean_scope = str(scope or "").strip().lower()
    clean_code = str(site_code or "").strip().upper()
    out: List[Dict[str, Any]] = []
    for item in items:
        if clean_scope and str(item.get("scope") or "").strip().lower() != clean_scope:
            continue
        if clean_code:
            if str(item.get("site_code") or "").strip().upper() != clean_code:
                continue
        out.append(item)
    safe_limit = max(1, min(int(limit), 5000))
    return out[:safe_limit]


def resolve_backup_file(relative_path: str) -> Path:
    raw = str(relative_path or "").strip().replace("\\", "/")
    if not raw:
        raise ValueError("relative_path is required")
    target = (BACKUP_ROOT / raw).resolve()
    root = BACKUP_ROOT.resolve()
    if root not in target.parents and target != root:
        raise ValueError("invalid backup path")
    if not target.exists() or not target.is_file():
        raise FileNotFoundError(raw)
    return target


def get_backup_item(relative_path: str) -> Dict[str, Any] | None:
    try:
        path = resolve_backup_file(relative_path)
    except Exception:
        return None
    sidecar = _sidecar_path(path)
    item: Dict[str, Any] = {}
    if sidecar.exists():
        try:
            loaded = json.loads(sidecar.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                item.update(loaded)
        except Exception:
            logger.exception("Failed to parse sidecar: %s", sidecar)
    item["file_name"] = path.name
    item["relative_path"] = str(path.relative_to(BACKUP_ROOT)).replace("\\", "/")
    item["file_size_bytes"] = int(path.stat().st_size)
    item["download_name"] = path.name
    return item


def cleanup_old_backups() -> Dict[str, Any]:
    _ensure_dirs()
    keep_full_days = max(1, int(os.getenv("KA_BACKUP_KEEP_FULL_DAYS", "30")))
    keep_site_days = max(1, int(os.getenv("KA_BACKUP_KEEP_SITE_DAYS", "90")))
    now = _now()
    removed: List[str] = []

    def purge(dir_path: Path, keep_days: int) -> None:
        threshold = now - timedelta(days=keep_days)
        for zip_path in dir_path.rglob("*.zip"):
            try:
                if _BACKUP_TZ is None:
                    mtime = datetime.fromtimestamp(zip_path.stat().st_mtime)
                else:
                    mtime = datetime.fromtimestamp(zip_path.stat().st_mtime, tz=_BACKUP_TZ)
                if mtime >= threshold:
                    continue
                sidecar = _sidecar_path(zip_path)
                zip_path.unlink(missing_ok=True)
                sidecar.unlink(missing_ok=True)
                removed.append(str(zip_path))
            except Exception:
                logger.exception("Failed to purge old backup file: %s", zip_path)

    purge(FULL_BACKUP_DIR, keep_full_days)
    purge(SITE_BACKUP_DIR, keep_site_days)
    return {
        "ok": True,
        "removed_count": len(removed),
        "removed": removed,
        "checked_at": _now_iso(),
    }


def list_site_admin_sites() -> List[Dict[str, str]]:
    db_path = Path(FACILITY_DB_PATH).resolve()
    if not db_path.exists():
        return []
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        if not _table_exists(con, "staff_users"):
            return []
        rows = con.execute(
            """
            SELECT site_code, site_name
            FROM staff_users
            WHERE is_site_admin=1
              AND is_active=1
              AND site_code IS NOT NULL
              AND TRIM(site_code)<>''
            ORDER BY id ASC
            """
        ).fetchall()
        out: Dict[str, str] = {}
        for row in rows:
            code = str(row["site_code"] or "").strip().upper()
            if not code:
                continue
            if code in out:
                continue
            name = str(row["site_name"] or "").strip()
            out[code] = name
        return [{"site_code": k, "site_name": v} for k, v in out.items()]
    finally:
        con.close()


def run_scheduled_backups(now: datetime | None = None) -> Dict[str, Any]:
    current = now or _now()
    report: Dict[str, Any] = {
        "ok": True,
        "ran_full": False,
        "ran_site_weekly": False,
        "errors": [],
        "at": current.replace(microsecond=0).isoformat(sep=" "),
    }
    state = _load_runtime_state()
    changed = False

    today = current.strftime("%Y-%m-%d")
    if current.hour == 0 and 0 <= current.minute < 15 and str(state.get("full_last_date") or "") != today:
        try:
            target_keys = [x["key"] for x in list_backup_targets() if bool(x.get("exists"))]
            if target_keys:
                run_manual_backup(
                    actor="자동스케줄러",
                    trigger="daily_0000",
                    target_keys=target_keys,
                    scope="full",
                    with_maintenance=True,
                )
                state["full_last_date"] = today
                changed = True
                report["ran_full"] = True
        except Exception as e:
            report["ok"] = False
            report["errors"].append(f"full backup: {e}")
            logger.exception("Scheduled full backup failed")

    week_key = current.strftime("%G-W%V")
    if current.weekday() == 4 and current.hour == 0 and 20 <= current.minute < 50 and str(state.get("site_last_week") or "") != week_key:
        try:
            sites = list_site_admin_sites()
            target_keys = [x["key"] for x in list_backup_targets() if bool(x.get("exists")) and bool(x.get("site_scoped"))]
            for site in sites:
                run_manual_backup(
                    actor="자동스케줄러",
                    trigger="weekly_friday",
                    target_keys=target_keys,
                    scope="site",
                    site_code=str(site.get("site_code") or "").strip().upper(),
                    site_name=str(site.get("site_name") or "").strip(),
                    with_maintenance=False,
                    include_user_tables=False,
                )
            state["site_last_week"] = week_key
            changed = True
            report["ran_site_weekly"] = True
            report["site_count"] = len(sites)
        except Exception as e:
            report["ok"] = False
            report["errors"].append(f"site weekly backup: {e}")
            logger.exception("Scheduled site weekly backup failed")

    if changed:
        _save_runtime_state(state)
    try:
        cleanup_old_backups()
    except Exception:
        logger.exception("Failed to cleanup old backups")
    return report


def _scheduler_enabled() -> bool:
    raw = str(os.getenv("KA_BACKUP_SCHEDULER_ENABLED", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _scheduler_loop() -> None:
    logger.info("Backup scheduler started")
    while not _SCHED_STOP_EVENT.wait(20):
        try:
            run_scheduled_backups()
        except Exception:
            logger.exception("Backup scheduler tick failed")
    logger.info("Backup scheduler stopped")


def start_backup_scheduler() -> None:
    global _SCHED_THREAD
    if not _scheduler_enabled():
        logger.info("Backup scheduler disabled by KA_BACKUP_SCHEDULER_ENABLED")
        return
    if _SCHED_THREAD is not None and _SCHED_THREAD.is_alive():
        return
    _ensure_dirs()
    _SCHED_STOP_EVENT.clear()
    _SCHED_THREAD = threading.Thread(target=_scheduler_loop, name="ka-backup-scheduler", daemon=True)
    _SCHED_THREAD.start()


def stop_backup_scheduler() -> None:
    global _SCHED_THREAD
    if _SCHED_THREAD is None:
        return
    _SCHED_STOP_EVENT.set()
    _SCHED_THREAD.join(timeout=5)
    _SCHED_THREAD = None
