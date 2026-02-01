import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse

from app.routers import (
    me, works, ui, events, reports, search, attachments, procurement, complaints, admin,
)
from app.db import DB_PATH
import sqlite3

def env_flag(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "on")

# 프로젝트 루트(ka-part) 기준 경로 고정
BASE_DIR = Path(__file__).resolve().parents[1]          # .../ka-part
STATIC_DIR = BASE_DIR / "static"
FAVICON_PATH = STATIC_DIR / "favicon.ico"              # 있으면 제공

app = FastAPI(title="ka-part", version="0.1.0")

# static (절대경로로 고정해서 작업폴더가 달라도 안전)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- 소음 제거/운영 편의 ---
@app.get("/", include_in_schema=False)
def root():
    # 기본 진입은 UI works로
    return RedirectResponse(url="/ui/works?login=admin")

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    # 없으면 204로 조용히 끝냄
    if FAVICON_PATH.exists():
        return FileResponse(str(FAVICON_PATH))
    return ("", 204)

# --- core routers (항상 ON) ---
app.include_router(me.router)
app.include_router(works.router)
app.include_router(events.router)
app.include_router(reports.router)
app.include_router(search.router)
app.include_router(attachments.router)
app.include_router(procurement.router)
app.include_router(ui.router)
app.include_router(complaints.router)
app.include_router(admin.router)

# --- optional routers (기본 OFF) ---
# 점검/검침은 준비될 때까지 기본적으로 import조차 하지 않음(사고 예방)
if env_flag("FEATURE_INSPECTIONS", "0"):
    from app.routers import inspections
    app.include_router(inspections.router)

if env_flag("FEATURE_METERS", "0"):
    from app.routers import meters
    app.include_router(meters.router)


def _run_sql_file(conn: sqlite3.Connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    conn.executescript(sql)


def _safe_migrate(conn: sqlite3.Connection, path: Path) -> None:
    try:
        _run_sql_file(conn, path)
    except sqlite3.OperationalError as e:
        # ignore idempotent migration errors
        msg = str(e).lower()
        if "duplicate column name" in msg or "already exists" in msg:
            return
        raise


def _ensure_bootstrap(conn: sqlite3.Connection) -> None:
    # Idempotent bootstrap for default roles/users.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS roles (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_roles (
          user_id INTEGER NOT NULL,
          role_id INTEGER NOT NULL,
          PRIMARY KEY (user_id, role_id)
        )
        """
    )

    roles = [
        ("CHIEF", "관리소장"),
        ("MANAGER", "시설과장"),
        ("STAFF", "담당자"),
        ("TECH", "시설기사"),
        ("RESIDENT", "입주민"),
        ("VENDOR", "외주업체"),
        ("ADMIN", "관리자"),
    ]
    for code, name in roles:
        conn.execute(
            "INSERT OR IGNORE INTO roles(code, name) VALUES(?, ?)",
            (code, name),
        )

    # Ensure default users
    conn.execute(
        """
        INSERT OR IGNORE INTO users(name, login, phone, is_active, created_at, updated_at)
        VALUES('관리자', 'admin', NULL, 1, datetime('now'), datetime('now'))
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO users(name, login, phone, is_active, created_at, updated_at)
        VALUES('담당자', 'user1', NULL, 1, datetime('now'), datetime('now'))
        """
    )

    admin_id = conn.execute("SELECT id FROM users WHERE login='admin'").fetchone()
    user1_id = conn.execute("SELECT id FROM users WHERE login='user1'").fetchone()
    role_id = lambda code: conn.execute("SELECT id FROM roles WHERE code=?", (code,)).fetchone()

    def _row_id(row):
        if not row:
            return None
        # sqlite3 without row_factory returns tuples
        return row[0] if isinstance(row, (tuple, list)) else row.get("id")

    admin_uid = _row_id(admin_id)
    if admin_uid is not None:
        for rc in ("ADMIN", "CHIEF", "MANAGER"):
            r = role_id(rc)
            rid = _row_id(r)
            if rid is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES(?, ?)",
                    (admin_uid, rid),
                )

    user1_uid = _row_id(user1_id)
    if user1_uid is not None:
        for rc in ("STAFF", "TECH"):
            r = role_id(rc)
            rid = _row_id(r)
            if rid is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES(?, ?)",
                    (user1_uid, rid),
                )


@app.on_event("startup")
def init_db_if_missing():
    """
    Ensure core schema exists on first boot (Render fresh disk).
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users';"
        )
        has_users = cur.fetchone() is not None
        if not has_users:
            base_dir = Path(__file__).resolve().parents[1]
            schema_path = base_dir / "sql" / "schema.sql"
            seed_path = base_dir / "sql" / "seed.sql"
            if schema_path.exists():
                _run_sql_file(conn, schema_path)
            if seed_path.exists():
                _run_sql_file(conn, seed_path)

            # apply migrations (best-effort)
            mig_dir = base_dir / "sql" / "migrations"
            if mig_dir.exists():
                for p in sorted(mig_dir.glob("*.sql")):
                    _safe_migrate(conn, p)
            conn.commit()
        # Always ensure default roles/users exist
        _ensure_bootstrap(conn)
        conn.commit()
    finally:
        conn.close()
