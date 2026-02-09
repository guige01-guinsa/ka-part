import os
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("PARKING_DB_PATH", str(BASE_DIR / "data" / "parking.db")))

def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def init_db() -> None:
    schema_path = BASE_DIR / "schema.sql"
    with connect() as con:
        con.executescript(schema_path.read_text(encoding="utf-8"))

def seed_demo() -> None:
    demo_rows = [
        ("12가3456", "101-1203", "홍길동", "active", "2026-01-01", "2027-12-31", "상시등록"),
        ("34나5678", "102-803",  "김영희", "blocked", None, None, "차단차량"),
        ("123다4567","103-1502", "이철수", "temp", "2026-02-01", "2026-02-28", "임시등록"),
    ]
    with connect() as con:
        for r in demo_rows:
            con.execute(
                "INSERT OR IGNORE INTO vehicles (plate, unit, owner_name, status, valid_from, valid_to, note) VALUES (?,?,?,?,?,?,?)",
                r
            )
        con.commit()

def seed_users() -> None:
    from .auth import pbkdf2_hash
    with connect() as con:
        con.execute("INSERT OR IGNORE INTO users(username,pw_hash,role) VALUES (?,?,?)", ("admin", pbkdf2_hash("admin1234"), "admin"))
        con.execute("INSERT OR IGNORE INTO users(username,pw_hash,role) VALUES (?,?,?)", ("guard", pbkdf2_hash("guard1234"), "guard"))
        con.execute("INSERT OR IGNORE INTO users(username,pw_hash,role) VALUES (?,?,?)", ("viewer", pbkdf2_hash("viewer1234"), "viewer"))
        con.commit()
