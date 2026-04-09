from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .ops_document_catalog import DOCUMENT_CATEGORY_CODES

BASE_DIR = Path(__file__).resolve().parent.parent
STORAGE_ROOT = Path(os.getenv("KA_STORAGE_ROOT") or BASE_DIR).resolve()
DATA_DIR = STORAGE_ROOT / "data"
DB_PATH = DATA_DIR / "ka.db"

_TENANT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,31}$")
_LOGIN_ID_RE = re.compile(r"^[a-z0-9._-]{2,32}$")
_DOC_NUMBERING_DATE_MODES = {"none", "yyyymm", "yyyymmdd"}
_DOC_NUMBERING_CATEGORY_DEFAULT_CODES = dict(DOCUMENT_CATEGORY_CODES)
_DOC_NUMBERING_DEFAULTS = {
    "separator": "-",
    "date_mode": "yyyymmdd",
    "sequence_digits": 3,
    "category_codes": dict(_DOC_NUMBERING_CATEGORY_DEFAULT_CODES),
}


def now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat(sep=" ")


def _prepare_storage_root() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    if STORAGE_ROOT == BASE_DIR:
        return
    canonical_db_path = (DATA_DIR / "ka.db").resolve()
    current_db_path = Path(DB_PATH).resolve()
    if current_db_path != canonical_db_path:
        return

    legacy_db = (BASE_DIR / "data" / "ka.db").resolve()
    if not current_db_path.exists() and legacy_db.exists():
        shutil.copy2(legacy_db, current_db_path)

    legacy_uploads = (BASE_DIR / "uploads").resolve()
    target_uploads = (STORAGE_ROOT / "uploads").resolve()
    if legacy_uploads.exists() and not target_uploads.exists():
        shutil.copytree(legacy_uploads, target_uploads, dirs_exist_ok=True)


def _connect() -> sqlite3.Connection:
    _prepare_storage_root()
    con = sqlite3.connect(str(DB_PATH), timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    try:
        con.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass
    return con


def _b64u_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64u_decode(value: str) -> bytes:
    raw = str(value or "").strip()
    if not raw:
        return b""
    padding = "=" * ((4 - len(raw) % 4) % 4)
    return base64.urlsafe_b64decode(f"{raw}{padding}".encode("ascii"))


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _hash_api_key(api_key: str) -> str:
    return hashlib.sha256(str(api_key or "").encode("utf-8")).hexdigest()


def _generate_api_key() -> str:
    return f"sk-ka-{secrets.token_urlsafe(24)}"


def _clean_login_id(value: Any) -> str:
    login_id = str(value or "").strip().lower()
    if not _LOGIN_ID_RE.match(login_id):
        raise ValueError("invalid login_id")
    return login_id


def _clean_tenant_id(value: Any) -> str:
    tenant_id = str(value or "").strip().lower()
    if not _TENANT_ID_RE.match(tenant_id):
        raise ValueError("invalid tenant_id")
    return tenant_id


def _clean_site_code(value: Any) -> str | None:
    raw = str(value or "").strip().upper().replace(" ", "").replace("-", "")
    return raw or None


def _clean_text(value: Any, max_len: int) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text[:max_len]


def default_document_numbering_config() -> Dict[str, Any]:
    return json.loads(json.dumps(_DOC_NUMBERING_DEFAULTS, ensure_ascii=False))


def normalize_document_numbering_config(value: Any) -> Dict[str, Any]:
    raw: Dict[str, Any]
    if isinstance(value, str):
        try:
            raw = json.loads(value) if str(value).strip() else {}
        except Exception:
            raw = {}
    elif isinstance(value, dict):
        raw = dict(value)
    else:
        raw = {}

    separator = str(raw.get("separator") or _DOC_NUMBERING_DEFAULTS["separator"]).strip()
    if len(separator) > 2 or any(ch.isalnum() for ch in separator):
        separator = _DOC_NUMBERING_DEFAULTS["separator"]

    date_mode = str(raw.get("date_mode") or _DOC_NUMBERING_DEFAULTS["date_mode"]).strip().lower()
    if date_mode not in _DOC_NUMBERING_DATE_MODES:
        date_mode = str(_DOC_NUMBERING_DEFAULTS["date_mode"])

    try:
        sequence_digits = int(raw.get("sequence_digits") or _DOC_NUMBERING_DEFAULTS["sequence_digits"])
    except Exception:
        sequence_digits = int(_DOC_NUMBERING_DEFAULTS["sequence_digits"])
    sequence_digits = max(2, min(6, sequence_digits))

    raw_codes = raw.get("category_codes") or raw.get("codes") or {}
    codes: Dict[str, str] = {}
    for category, default_code in _DOC_NUMBERING_CATEGORY_DEFAULT_CODES.items():
        candidate = str((raw_codes or {}).get(category) or default_code).strip().upper()
        candidate = re.sub(r"[^A-Z0-9]", "", candidate)[:8] or default_code
        codes[category] = candidate

    return {
        "separator": separator,
        "date_mode": date_mode,
        "sequence_digits": sequence_digits,
        "category_codes": codes,
    }


def _ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    names = {str(row["name"]) for row in rows}
    if column not in names:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS sites (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          site_code TEXT UNIQUE,
          site_name TEXT UNIQUE,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tenants (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          site_code TEXT,
          site_name TEXT,
          api_key_hash TEXT NOT NULL UNIQUE,
          ops_document_numbering_json TEXT,
          status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','inactive')),
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          last_used_at TEXT
        );

        CREATE TABLE IF NOT EXISTS staff_users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT REFERENCES tenants(id) ON DELETE SET NULL,
          login_id TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL,
          role TEXT NOT NULL,
          phone TEXT,
          note TEXT,
          site_id INTEGER REFERENCES sites(id) ON DELETE SET NULL,
          site_code TEXT,
          site_name TEXT,
          address TEXT,
          office_phone TEXT,
          office_fax TEXT,
          unit_label TEXT,
          household_key TEXT,
          password_hash TEXT,
          is_admin INTEGER NOT NULL DEFAULT 0 CHECK(is_admin IN (0,1)),
          is_site_admin INTEGER NOT NULL DEFAULT 0 CHECK(is_site_admin IN (0,1)),
          admin_scope TEXT,
          is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          last_login_at TEXT
        );

        CREATE TABLE IF NOT EXISTS auth_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL REFERENCES staff_users(id) ON DELETE CASCADE,
          token_hash TEXT NOT NULL UNIQUE,
          created_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          user_agent TEXT,
          ip_address TEXT,
          revoked_at TEXT
        );

        CREATE TABLE IF NOT EXISTS usage_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
          api_name TEXT NOT NULL,
          count INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tenant_id TEXT REFERENCES tenants(id) ON DELETE SET NULL,
          action TEXT NOT NULL,
          actor TEXT,
          data_json TEXT,
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status, id);
        CREATE INDEX IF NOT EXISTS idx_staff_users_tenant ON staff_users(tenant_id, is_active, id);
        CREATE INDEX IF NOT EXISTS idx_staff_users_site_code ON staff_users(site_code, is_active, id);
        CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id, expires_at);
        CREATE INDEX IF NOT EXISTS idx_auth_sessions_token ON auth_sessions(token_hash);
        CREATE INDEX IF NOT EXISTS idx_usage_logs_tenant_date ON usage_logs(tenant_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_date ON audit_logs(tenant_id, created_at DESC);
        """
    )
    _ensure_column(con, "staff_users", "tenant_id", "tenant_id TEXT REFERENCES tenants(id) ON DELETE SET NULL")
    _ensure_column(con, "tenants", "ops_document_numbering_json", "ops_document_numbering_json TEXT")


def init_db() -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        con.commit()
    finally:
        con.close()


def _ensure_site(
    con: sqlite3.Connection,
    *,
    site_code: Any = None,
    site_name: Any = None,
) -> Tuple[int | None, str | None, str | None]:
    clean_code = _clean_site_code(site_code)
    clean_name = _clean_text(site_name, 120)
    if not clean_code and not clean_name:
        return None, None, None

    row = None
    if clean_code:
        row = con.execute(
            "SELECT id, site_code, site_name FROM sites WHERE site_code=? LIMIT 1",
            (clean_code,),
        ).fetchone()
    if not row and clean_name:
        row = con.execute(
            "SELECT id, site_code, site_name FROM sites WHERE site_name=? LIMIT 1",
            (clean_name,),
        ).fetchone()

    ts = now_iso()
    if row:
        next_code = clean_code or str(row["site_code"] or "").strip() or None
        next_name = clean_name or str(row["site_name"] or "").strip() or None
        con.execute(
            """
            UPDATE sites
            SET site_code=?, site_name=?, updated_at=?
            WHERE id=?
            """,
            (next_code, next_name, ts, int(row["id"])),
        )
        return int(row["id"]), next_code, next_name

    cur = con.execute(
        """
        INSERT INTO sites(site_code, site_name, created_at, updated_at)
        VALUES(?,?,?,?)
        """,
        (clean_code, clean_name, ts, ts),
    )
    return int(cur.lastrowid), clean_code, clean_name


def hash_password(password: str, *, iterations: int = 310000) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, int(iterations))
    return f"pbkdf2_sha256${int(iterations)}${_b64u_encode(salt)}${_b64u_encode(digest)}"


def verify_password(password: str, password_hash: str) -> bool:
    raw = str(password_hash or "").strip()
    if not raw:
        return False
    try:
        algo, iterations_raw, salt_raw, digest_raw = raw.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iterations_raw)
        expected = _b64u_decode(digest_raw)
        actual = hashlib.pbkdf2_hmac(
            "sha256",
            str(password or "").encode("utf-8"),
            _b64u_decode(salt_raw),
            iterations,
        )
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def _tenant_row(con: sqlite3.Connection, tenant_id: str) -> Optional[sqlite3.Row]:
    return con.execute(
        """
        SELECT id, name, site_code, site_name, ops_document_numbering_json, status, created_at, updated_at, last_used_at
        FROM tenants
        WHERE id=?
        LIMIT 1
        """,
        (_clean_tenant_id(tenant_id),),
    ).fetchone()


def create_tenant(
    *,
    tenant_id: str,
    name: str,
    site_code: Optional[str] = None,
    site_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    clean_name = _clean_text(name, 120)
    if not clean_name:
        raise ValueError("name is required")
    clean_site_code = _clean_site_code(site_code)
    clean_site_name = _clean_text(site_name, 120)
    raw_api_key = str(api_key or "").strip() or _generate_api_key()
    ts = now_iso()

    con = _connect()
    try:
        _ensure_schema(con)
        con.execute(
            """
            INSERT INTO tenants(id, name, site_code, site_name, api_key_hash, status, created_at, updated_at, last_used_at)
            VALUES(?,?,?,?,?,'active',?,?,NULL)
            """,
            (
                clean_tenant_id,
                clean_name,
                clean_site_code,
                clean_site_name,
                _hash_api_key(raw_api_key),
                ts,
                ts,
            ),
        )
        if clean_site_code or clean_site_name:
            _ensure_site(con, site_code=clean_site_code, site_name=clean_site_name)
        con.commit()
        row = _tenant_row(con, clean_tenant_id)
        out = dict(row) if row else {"id": clean_tenant_id, "name": clean_name}
        out["ops_document_numbering"] = normalize_document_numbering_config(out.pop("ops_document_numbering_json", None))
        out["api_key"] = raw_api_key
        return out
    finally:
        con.close()


def ensure_bootstrap_admin(
    *,
    login_id: str,
    name: str,
    password: str,
) -> Dict[str, Any]:
    clean_login = _clean_login_id(login_id)
    clean_name = _clean_text(name, 40) or clean_login
    if len(str(password or "")) < 8:
        raise ValueError("bootstrap admin password must be at least 8 characters")

    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        row = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (clean_login,),
        ).fetchone()
        if row:
            con.execute(
                """
                UPDATE staff_users
                SET name=?, role='super_admin', password_hash=?, is_admin=1, is_site_admin=0,
                    admin_scope='super_admin', is_active=1, updated_at=?
                WHERE id=?
                """,
                (
                    clean_name,
                    hash_password(password),
                    ts,
                    int(row["id"]),
                ),
            )
        else:
            con.execute(
                """
                INSERT INTO staff_users(
                  tenant_id, login_id, name, role, phone, note, site_id, site_code, site_name,
                  address, office_phone, office_fax, unit_label, household_key, password_hash,
                  is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at
                )
                VALUES(NULL,?,?, 'super_admin', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, 1, 0, 'super_admin', 1, ?, ?)
                """,
                (
                    clean_login,
                    clean_name,
                    hash_password(password),
                    ts,
                    ts,
                ),
            )
        con.commit()
        fresh = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (clean_login,),
        ).fetchone()
        return dict(fresh) if fresh else {}
    finally:
        con.close()


def ensure_bootstrap_tenant(
    *,
    tenant_id: str,
    name: str,
    site_code: Optional[str] = None,
    site_name: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    clean_name = _clean_text(name, 120)
    if not clean_name:
        raise ValueError("name is required")
    clean_site_code = _clean_site_code(site_code)
    clean_site_name = _clean_text(site_name, 120)
    raw_api_key = str(api_key or "").strip() or _generate_api_key()

    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        row = _tenant_row(con, clean_tenant_id)
        if row:
            con.execute(
                """
                UPDATE tenants
                SET name=?, site_code=?, site_name=?, api_key_hash=?, status='active', updated_at=?
                WHERE id=?
                """,
                (
                    clean_name,
                    clean_site_code,
                    clean_site_name,
                    _hash_api_key(raw_api_key),
                    ts,
                    clean_tenant_id,
                ),
            )
        else:
            con.execute(
                """
                INSERT INTO tenants(id, name, site_code, site_name, api_key_hash, status, created_at, updated_at, last_used_at)
                VALUES(?,?,?,?,?,'active',?,?,NULL)
                """,
                (
                    clean_tenant_id,
                    clean_name,
                    clean_site_code,
                    clean_site_name,
                    _hash_api_key(raw_api_key),
                    ts,
                    ts,
                ),
            )
        if clean_site_code or clean_site_name:
            _ensure_site(con, site_code=clean_site_code, site_name=clean_site_name)
        con.commit()
        fresh = _tenant_row(con, clean_tenant_id)
        out = dict(fresh) if fresh else {"id": clean_tenant_id, "name": clean_name}
        out["ops_document_numbering"] = normalize_document_numbering_config(out.pop("ops_document_numbering_json", None))
        out["api_key"] = raw_api_key
        return out
    finally:
        con.close()


def ensure_bootstrap_user(
    *,
    tenant_id: str,
    login_id: str,
    name: str,
    password: str,
    role: str,
    is_site_admin: bool = False,
    note: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    clean_login = _clean_login_id(login_id)
    clean_name = _clean_text(name, 40) or clean_login
    clean_role = _clean_text(role, 40) or "staff"
    clean_note = _clean_text(note, 2000)
    if len(str(password or "")) < 8:
        raise ValueError("bootstrap user password must be at least 8 characters")

    con = _connect()
    try:
        _ensure_schema(con)
        tenant_row = _tenant_row(con, clean_tenant_id)
        if not tenant_row:
            raise ValueError("tenant not found")
        site_row_id, clean_site_code, clean_site_name = _ensure_site(
            con,
            site_code=tenant_row["site_code"],
            site_name=tenant_row["site_name"],
        )
        ts = now_iso()
        row = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (clean_login,),
        ).fetchone()
        if row:
            con.execute(
                """
                UPDATE staff_users
                SET tenant_id=?, name=?, role=?, note=?, site_id=?, site_code=?, site_name=?, password_hash=?,
                    is_admin=0, is_site_admin=?, admin_scope=NULL, is_active=1, updated_at=?
                WHERE id=?
                """,
                (
                    clean_tenant_id,
                    clean_name,
                    clean_role,
                    clean_note,
                    site_row_id,
                    clean_site_code,
                    clean_site_name,
                    hash_password(password),
                    1 if is_site_admin else 0,
                    ts,
                    int(row["id"]),
                ),
            )
        else:
            con.execute(
                """
                INSERT INTO staff_users(
                  tenant_id, login_id, name, role, phone, note, site_id, site_code, site_name,
                  address, office_phone, office_fax, unit_label, household_key, password_hash,
                  is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    clean_tenant_id,
                    clean_login,
                    clean_name,
                    clean_role,
                    None,
                    clean_note,
                    site_row_id,
                    clean_site_code,
                    clean_site_name,
                    None,
                    None,
                    None,
                    None,
                    None,
                    hash_password(password),
                    0,
                    1 if is_site_admin else 0,
                    None,
                    1,
                    ts,
                    ts,
                ),
            )
        con.commit()
        fresh = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (clean_login,),
        ).fetchone()
        return dict(fresh) if fresh else {}
    finally:
        con.close()


def bootstrap_from_env() -> Dict[str, Any]:
    admin_login = str(os.getenv("KA_BOOTSTRAP_ADMIN_LOGIN") or "").strip().lower()
    admin_password = str(os.getenv("KA_BOOTSTRAP_ADMIN_PASSWORD") or "")
    admin_name = str(os.getenv("KA_BOOTSTRAP_ADMIN_NAME") or admin_login or "초기 관리자").strip()
    tenant_id = str(os.getenv("KA_BOOTSTRAP_TENANT_ID") or "").strip().lower()
    tenant_name = str(os.getenv("KA_BOOTSTRAP_TENANT_NAME") or tenant_id or "").strip()
    tenant_site_code = str(os.getenv("KA_BOOTSTRAP_TENANT_SITE_CODE") or "").strip()
    tenant_site_name = str(os.getenv("KA_BOOTSTRAP_TENANT_SITE_NAME") or tenant_name or "").strip()
    tenant_api_key = str(os.getenv("KA_BOOTSTRAP_TENANT_API_KEY") or "").strip()

    created: Dict[str, Any] = {"admin": None, "tenant": None, "users": []}
    if admin_login and admin_password:
        created["admin"] = ensure_bootstrap_admin(login_id=admin_login, name=admin_name, password=admin_password)

    if tenant_id and tenant_name:
        created["tenant"] = ensure_bootstrap_tenant(
            tenant_id=tenant_id,
            name=tenant_name,
            site_code=tenant_site_code,
            site_name=tenant_site_name,
            api_key=tenant_api_key or None,
        )

    for prefix, role, is_site_admin, default_name in (
        ("KA_BOOTSTRAP_MANAGER", "manager", True, "운영담당"),
        ("KA_BOOTSTRAP_DESK", "staff", False, "민원접수"),
    ):
        login_id = str(os.getenv(f"{prefix}_LOGIN") or "").strip().lower()
        password = str(os.getenv(f"{prefix}_PASSWORD") or "")
        name = str(os.getenv(f"{prefix}_NAME") or default_name).strip()
        if tenant_id and login_id and password:
            created["users"].append(
                ensure_bootstrap_user(
                    tenant_id=tenant_id,
                    login_id=login_id,
                    name=name,
                    password=password,
                    role=role,
                    is_site_admin=is_site_admin,
                    note="bootstrap seeded account",
                )
            )

    return created


def list_tenants(*, active_only: bool = False) -> List[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT id, name, site_code, site_name, ops_document_numbering_json, status, created_at, updated_at, last_used_at
            FROM tenants
        """
        if active_only:
            sql += " WHERE status='active'"
        sql += " ORDER BY name ASC, id ASC"
        rows = con.execute(sql).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["ops_document_numbering"] = normalize_document_numbering_config(item.pop("ops_document_numbering_json", None))
        return items
    finally:
        con.close()


def get_tenant(tenant_id: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        row = _tenant_row(con, tenant_id)
        if not row:
            return None
        item = dict(row)
        item["ops_document_numbering"] = normalize_document_numbering_config(item.pop("ops_document_numbering_json", None))
        return item
    finally:
        con.close()


def rotate_tenant_api_key(tenant_id: str) -> Dict[str, Any]:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    raw_api_key = _generate_api_key()
    con = _connect()
    try:
        _ensure_schema(con)
        cur = con.execute(
            "UPDATE tenants SET api_key_hash=?, updated_at=? WHERE id=?",
            (_hash_api_key(raw_api_key), now_iso(), clean_tenant_id),
        )
        if cur.rowcount <= 0:
            raise ValueError("tenant not found")
        con.commit()
        row = _tenant_row(con, clean_tenant_id)
        out = dict(row) if row else {"id": clean_tenant_id}
        out["ops_document_numbering"] = normalize_document_numbering_config(out.pop("ops_document_numbering_json", None))
        out["api_key"] = raw_api_key
        return out
    finally:
        con.close()


def set_tenant_status(tenant_id: str, status: str) -> bool:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    clean_status = str(status or "").strip().lower()
    if clean_status not in {"active", "inactive"}:
        raise ValueError("invalid tenant status")
    con = _connect()
    try:
        _ensure_schema(con)
        cur = con.execute(
            "UPDATE tenants SET status=?, updated_at=? WHERE id=?",
            (clean_status, now_iso(), clean_tenant_id),
        )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def get_tenant_by_api_key(api_key: str) -> Optional[Dict[str, Any]]:
    digest = _hash_api_key(api_key)
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT id, name, site_code, site_name, ops_document_numbering_json, status, created_at, updated_at, last_used_at
            FROM tenants
            WHERE api_key_hash=? AND status='active'
            LIMIT 1
            """,
            (digest,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        item["ops_document_numbering"] = normalize_document_numbering_config(item.pop("ops_document_numbering_json", None))
        return item
    finally:
        con.close()


def get_tenant_document_numbering_config(tenant_id: str) -> Dict[str, Any]:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            "SELECT ops_document_numbering_json FROM tenants WHERE id=? LIMIT 1",
            (clean_tenant_id,),
        ).fetchone()
        if not row:
            raise ValueError("tenant not found")
        return normalize_document_numbering_config(row["ops_document_numbering_json"])
    finally:
        con.close()


def update_tenant_document_numbering_config(tenant_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    normalized = normalize_document_numbering_config(config)
    con = _connect()
    try:
        _ensure_schema(con)
        cur = con.execute(
            "UPDATE tenants SET ops_document_numbering_json=?, updated_at=? WHERE id=?",
            (json.dumps(normalized, ensure_ascii=False, separators=(",", ":")), now_iso(), clean_tenant_id),
        )
        if cur.rowcount <= 0:
            raise ValueError("tenant not found")
        con.commit()
        return normalized
    finally:
        con.close()


def mark_tenant_used(tenant_id: str) -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        con.execute(
            "UPDATE tenants SET last_used_at=?, updated_at=? WHERE id=?",
            (now_iso(), now_iso(), _clean_tenant_id(tenant_id)),
        )
        con.commit()
    finally:
        con.close()


def log_usage(tenant_id: str, api_name: str, *, count: int = 1) -> None:
    clean_api_name = str(api_name or "").strip()
    if not clean_api_name:
        return
    con = _connect()
    try:
        _ensure_schema(con)
        con.execute(
            """
            INSERT INTO usage_logs(tenant_id, api_name, count, created_at)
            VALUES(?,?,?,?)
            """,
            (_clean_tenant_id(tenant_id), clean_api_name[:120], max(1, int(count)), now_iso()),
        )
        con.commit()
    finally:
        con.close()


def append_audit_log(tenant_id: Optional[str], action: str, actor: str, data: Optional[Dict[str, Any]] = None) -> None:
    clean_action = str(action or "").strip()
    if not clean_action:
        return
    payload = json.dumps(data or {}, ensure_ascii=False, separators=(",", ":"))
    con = _connect()
    try:
        _ensure_schema(con)
        con.execute(
            """
            INSERT INTO audit_logs(tenant_id, action, actor, data_json, created_at)
            VALUES(?,?,?,?,?)
            """,
            (
                _clean_tenant_id(tenant_id) if tenant_id else None,
                clean_action[:120],
                str(actor or "").strip()[:120] or None,
                payload,
                now_iso(),
            ),
        )
        con.commit()
    finally:
        con.close()


def list_usage_logs(*, tenant_id: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        lim = max(1, min(500, int(limit)))
        sql = """
            SELECT id, tenant_id, api_name, count, created_at
            FROM usage_logs
        """
        params: List[Any] = []
        clean_tenant_id = str(tenant_id or "").strip().lower()
        if clean_tenant_id:
            sql += " WHERE tenant_id=?"
            params.append(_clean_tenant_id(clean_tenant_id))
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(lim)
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def list_audit_logs(*, tenant_id: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        lim = max(1, min(500, int(limit)))
        sql = """
            SELECT id, tenant_id, action, actor, data_json, created_at
            FROM audit_logs
        """
        params: List[Any] = []
        clean_tenant_id = str(tenant_id or "").strip().lower()
        if clean_tenant_id:
            sql += " WHERE tenant_id=?"
            params.append(_clean_tenant_id(clean_tenant_id))
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(lim)
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def create_staff_user(
    *,
    login_id: str,
    name: str,
    role: str,
    phone: Optional[str] = None,
    tenant_id: Optional[str] = None,
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
        _ensure_schema(con)
        clean_login = _clean_login_id(login_id)
        clean_tenant_id = _clean_tenant_id(tenant_id) if tenant_id else None
        site_row_id = int(site_id or 0) or None
        clean_site_code = _clean_site_code(site_code)
        clean_site_name = _clean_text(site_name, 120)
        if clean_tenant_id:
            tenant_row = _tenant_row(con, clean_tenant_id)
            if not tenant_row:
                raise ValueError("tenant not found")
            clean_site_code = clean_site_code or _clean_site_code(tenant_row["site_code"])
            clean_site_name = clean_site_name or _clean_text(tenant_row["site_name"], 120)
        if site_row_id:
            row = con.execute(
                "SELECT id, site_code, site_name FROM sites WHERE id=? LIMIT 1",
                (int(site_row_id),),
            ).fetchone()
            if row:
                site_row_id = int(row["id"])
                clean_site_code = clean_site_code or _clean_site_code(row["site_code"])
                clean_site_name = clean_site_name or _clean_text(row["site_name"], 120)
        if not site_row_id and (clean_site_code or clean_site_name):
            site_row_id, clean_site_code, clean_site_name = _ensure_site(
                con,
                site_code=clean_site_code,
                site_name=clean_site_name,
            )
        ts = now_iso()
        clean_unit_label = _clean_text(unit_label, 80)
        clean_household_key = _clean_text(household_key, 80)
        if clean_unit_label and not clean_household_key:
            clean_household_key = clean_unit_label.upper()
        if clean_household_key and not clean_unit_label:
            clean_unit_label = clean_household_key
        con.execute(
            """
            INSERT INTO staff_users(
              tenant_id, login_id, name, role, phone, note, site_id, site_code, site_name,
              address, office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                clean_login,
                str(name or "").strip(),
                str(role or "").strip() or "staff",
                _clean_text(phone, 40),
                _clean_text(note, 2000),
                site_row_id,
                clean_site_code,
                clean_site_name,
                _clean_text(address, 200),
                _clean_text(office_phone, 40),
                _clean_text(office_fax, 40),
                clean_unit_label,
                clean_household_key,
                _clean_text(password_hash, 500),
                1 if is_admin else 0,
                1 if is_site_admin else 0,
                _clean_text(admin_scope, 40),
                1 if is_active else 0,
                ts,
                ts,
            ),
        )
        con.commit()
        row = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (clean_login,),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def get_staff_user(user_id: int) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE id=?
            LIMIT 1
            """,
            (int(user_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def get_staff_user_by_login(login_id: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (_clean_login_id(login_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def list_staff_users(*, active_only: bool = False, tenant_id: str = "") -> List[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        sql = """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE 1=1
        """
        params: List[Any] = []
        if active_only:
            sql += " AND is_active=1"
        clean_tenant_id = str(tenant_id or "").strip().lower()
        if clean_tenant_id:
            sql += " AND tenant_id=?"
            params.append(_clean_tenant_id(clean_tenant_id))
        sql += " ORDER BY is_admin DESC, is_site_admin DESC, name ASC, id ASC"
        rows = con.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def update_staff_user(
    user_id: int,
    *,
    name: Optional[str] = None,
    role: Optional[str] = None,
    phone: Optional[str] = None,
    note: Optional[str] = None,
    is_site_admin: Optional[bool] = None,
    is_active: Optional[bool] = None,
) -> Dict[str, Any]:
    con = _connect()
    try:
        _ensure_schema(con)
        current = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE id=?
            LIMIT 1
            """,
            (int(user_id),),
        ).fetchone()
        if not current:
            raise ValueError("user not found")

        next_name = _clean_text(name, 40) if name is not None else str(current["name"] or "").strip()
        if not next_name:
            raise ValueError("name is required")
        next_role = _clean_text(role, 40) if role is not None else str(current["role"] or "").strip() or "staff"
        next_phone = _clean_text(phone, 40) if phone is not None else current["phone"]
        next_note = _clean_text(note, 2000) if note is not None else current["note"]
        next_is_site_admin = int(current["is_site_admin"] or 0) if is_site_admin is None else (1 if is_site_admin else 0)
        next_is_active = int(current["is_active"] or 0) if is_active is None else (1 if is_active else 0)

        con.execute(
            """
            UPDATE staff_users
            SET name=?, role=?, phone=?, note=?, is_site_admin=?, is_active=?, updated_at=?
            WHERE id=?
            """,
            (
                next_name,
                next_role,
                next_phone,
                next_note,
                next_is_site_admin,
                next_is_active,
                now_iso(),
                int(user_id),
            ),
        )
        con.commit()
        fresh = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE id=?
            LIMIT 1
            """,
            (int(user_id),),
        ).fetchone()
        return dict(fresh) if fresh else {}
    finally:
        con.close()


def delete_staff_user(user_id: int) -> Dict[str, Any]:
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE id=?
            LIMIT 1
            """,
            (int(user_id),),
        ).fetchone()
        if not row:
            raise ValueError("user not found")
        out = dict(row)
        con.execute("DELETE FROM staff_users WHERE id=?", (int(user_id),))
        con.commit()
        return out
    finally:
        con.close()


def count_staff_admins(*, active_only: bool = True) -> int:
    con = _connect()
    try:
        _ensure_schema(con)
        sql = "SELECT COUNT(*) AS c FROM staff_users WHERE is_admin=1"
        if active_only:
            sql += " AND is_active=1"
        row = con.execute(sql).fetchone()
        return int(row["c"] if row else 0)
    finally:
        con.close()


def set_staff_user_password(user_id: int, password: str) -> bool:
    con = _connect()
    try:
        _ensure_schema(con)
        cur = con.execute(
            "UPDATE staff_users SET password_hash=?, updated_at=? WHERE id=?",
            (hash_password(password), now_iso(), int(user_id)),
        )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def mark_staff_user_login(user_id: int) -> None:
    con = _connect()
    try:
        _ensure_schema(con)
        ts = now_iso()
        con.execute(
            "UPDATE staff_users SET last_login_at=?, updated_at=? WHERE id=?",
            (ts, ts, int(user_id)),
        )
        con.commit()
    finally:
        con.close()


def ensure_service_user(tenant_id: str) -> Dict[str, Any]:
    clean_tenant_id = _clean_tenant_id(tenant_id)
    login_seed = f"svc.{clean_tenant_id}".lower()
    login_id = re.sub(r"[^a-z0-9._-]", "-", login_seed)[:32]
    con = _connect()
    try:
        _ensure_schema(con)
        existing = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (login_id,),
        ).fetchone()
        if existing:
            return dict(existing)
        tenant_row = _tenant_row(con, clean_tenant_id)
        if not tenant_row:
            raise ValueError("tenant not found")
        site_row_id, clean_site_code, clean_site_name = _ensure_site(
            con,
            site_code=tenant_row["site_code"],
            site_name=tenant_row["site_name"],
        )
        ts = now_iso()
        con.execute(
            """
            INSERT INTO staff_users(
              tenant_id, login_id, name, role, phone, note, site_id, site_code, site_name,
              address, office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                clean_tenant_id,
                login_id,
                f"{tenant_row['name']} API 연동",
                "integration",
                None,
                "system integration account",
                site_row_id,
                clean_site_code,
                clean_site_name,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                0,
                None,
                1,
                ts,
                ts,
            ),
        )
        con.commit()
        row = con.execute(
            """
            SELECT
              id, tenant_id, login_id, name, role, phone, note, site_code, site_name, site_id, address,
              office_phone, office_fax, unit_label, household_key, password_hash,
              is_admin, is_site_admin, admin_scope, is_active, created_at, updated_at, last_login_at
            FROM staff_users
            WHERE login_id=?
            LIMIT 1
            """,
            (login_id,),
        ).fetchone()
        return dict(row) if row else {}
    finally:
        con.close()


def create_auth_session(
    user_id: int,
    *,
    ttl_hours: int = 12,
    user_agent: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> Dict[str, Any]:
    con = _connect()
    try:
        _ensure_schema(con)
        created_at = now_iso()
        expires_at = (datetime.now() + timedelta(hours=max(1, int(ttl_hours)))).replace(microsecond=0).isoformat(sep=" ")
        raw_token = _b64u_encode(os.urandom(32))
        con.execute(
            """
            INSERT INTO auth_sessions(user_id, token_hash, created_at, expires_at, user_agent, ip_address, revoked_at)
            VALUES(?,?,?,?,?,?,NULL)
            """,
            (
                int(user_id),
                _hash_session_token(raw_token),
                created_at,
                expires_at,
                _clean_text(user_agent, 500),
                _clean_text(ip_address, 80),
            ),
        )
        con.commit()
        return {"token": raw_token, "expires_at": expires_at}
    finally:
        con.close()


def revoke_auth_session(token: str) -> bool:
    con = _connect()
    try:
        _ensure_schema(con)
        cur = con.execute(
            """
            UPDATE auth_sessions
            SET revoked_at=?
            WHERE token_hash=? AND revoked_at IS NULL
            """,
            (now_iso(), _hash_session_token(token)),
        )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def revoke_all_user_sessions(user_id: int) -> int:
    con = _connect()
    try:
        _ensure_schema(con)
        cur = con.execute(
            """
            UPDATE auth_sessions
            SET revoked_at=?
            WHERE user_id=? AND revoked_at IS NULL
            """,
            (now_iso(), int(user_id)),
        )
        con.commit()
        return int(cur.rowcount)
    finally:
        con.close()


def get_auth_user_by_token(token: str) -> Optional[Dict[str, Any]]:
    con = _connect()
    try:
        _ensure_schema(con)
        row = con.execute(
            """
            SELECT
              s.id AS session_id,
              s.expires_at,
              u.id,
              u.tenant_id,
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
            (_hash_session_token(token), now_iso()),
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
        _ensure_schema(con)
        cur = con.execute(
            "DELETE FROM auth_sessions WHERE expires_at<=? OR revoked_at IS NOT NULL",
            (now_iso(),),
        )
        con.commit()
        return int(cur.rowcount)
    finally:
        con.close()
