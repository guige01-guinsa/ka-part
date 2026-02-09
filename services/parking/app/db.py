import os
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("PARKING_DB_PATH", str(BASE_DIR / "data" / "parking.db")))
DEFAULT_SITE_CODE = (os.getenv("PARKING_DEFAULT_SITE_CODE", "COMMON").strip().upper() or "COMMON")


def normalize_site_code(value: str | None) -> str:
    txt = str(value or "").strip().upper()
    return txt or DEFAULT_SITE_CODE


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _table_columns(con: sqlite3.Connection, table: str) -> list[sqlite3.Row]:
    return con.execute(f"PRAGMA table_info({table})").fetchall()


def _has_column(con: sqlite3.Connection, table: str, name: str) -> bool:
    cols = _table_columns(con, table)
    return any(str(c["name"]).lower() == name.lower() for c in cols)


def _is_vehicles_composite_pk(con: sqlite3.Connection) -> bool:
    cols = _table_columns(con, "vehicles")
    pk_map: dict[int, str] = {}
    for c in cols:
        pk_pos = int(c["pk"] or 0)
        if pk_pos > 0:
            pk_map[pk_pos] = str(c["name"]).lower()
    return pk_map.get(1) == "site_code" and pk_map.get(2) == "plate"


def _migrate_vehicles_schema(con: sqlite3.Connection) -> None:
    if _is_vehicles_composite_pk(con):
        return

    has_site_code = _has_column(con, "vehicles", "site_code")
    con.execute("ALTER TABLE vehicles RENAME TO vehicles_legacy")
    con.executescript(
        """
        CREATE TABLE vehicles (
          site_code TEXT NOT NULL,
          plate TEXT NOT NULL,
          unit TEXT,
          owner_name TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          valid_from TEXT,
          valid_to TEXT,
          note TEXT,
          updated_at TEXT NOT NULL DEFAULT (datetime('now')),
          PRIMARY KEY (site_code, plate)
        );
        """
    )

    if has_site_code:
        con.execute(
            """
            INSERT OR IGNORE INTO vehicles(site_code, plate, unit, owner_name, status, valid_from, valid_to, note, updated_at)
            SELECT
              CASE
                WHEN site_code IS NULL OR TRIM(site_code) = '' THEN ?
                ELSE UPPER(TRIM(site_code))
              END,
              plate, unit, owner_name, status, valid_from, valid_to, note, updated_at
            FROM vehicles_legacy
            """,
            (DEFAULT_SITE_CODE,),
        )
    else:
        con.execute(
            """
            INSERT OR IGNORE INTO vehicles(site_code, plate, unit, owner_name, status, valid_from, valid_to, note, updated_at)
            SELECT ?, plate, unit, owner_name, status, valid_from, valid_to, note, updated_at
            FROM vehicles_legacy
            """,
            (DEFAULT_SITE_CODE,),
        )

    con.execute("DROP TABLE vehicles_legacy")
    con.execute("CREATE INDEX IF NOT EXISTS idx_vehicles_site_plate ON vehicles(site_code, plate)")


def _ensure_violations_site_code(con: sqlite3.Connection) -> None:
    if not _has_column(con, "violations", "site_code"):
        con.execute(f"ALTER TABLE violations ADD COLUMN site_code TEXT NOT NULL DEFAULT '{DEFAULT_SITE_CODE}'")
    con.execute(
        "UPDATE violations SET site_code=? WHERE site_code IS NULL OR TRIM(site_code)=''",
        (DEFAULT_SITE_CODE,),
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_violations_site_created_at ON violations(site_code, created_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_violations_site_plate ON violations(site_code, plate)")


def _ensure_users_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
          username TEXT PRIMARY KEY,
          pw_hash TEXT NOT NULL,
          role TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )


def _ensure_indexes(con: sqlite3.Connection) -> None:
    con.execute("CREATE INDEX IF NOT EXISTS idx_vehicles_site_plate ON vehicles(site_code, plate)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_violations_site_plate ON violations(site_code, plate)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_violations_site_created_at ON violations(site_code, created_at)")


def init_db() -> None:
    schema_path = BASE_DIR / "schema.sql"
    with connect() as con:
        schema_sql = schema_path.read_text(encoding="utf-8")
        try:
            con.executescript(schema_sql)
        except sqlite3.OperationalError as exc:
            # Legacy DBs may have vehicles without site_code; index creation can fail
            # before migration. Continue and fix schema in migration helpers below.
            if "site_code" not in str(exc).lower():
                raise
        _migrate_vehicles_schema(con)
        _ensure_violations_site_code(con)
        _ensure_users_table(con)
        _ensure_indexes(con)
        con.commit()


def seed_demo() -> None:
    site_code = normalize_site_code(DEFAULT_SITE_CODE)
    demo_rows = [
        (site_code, "12가3456", "101-1203", "홍길동", "active", "2026-01-01", "2027-12-31", "상시등록"),
        (site_code, "34나5678", "102-803", "김영희", "blocked", None, None, "차단차량"),
        (site_code, "123다4567", "103-1502", "이철수", "temp", "2026-02-01", "2026-02-28", "임시등록"),
    ]
    with connect() as con:
        for r in demo_rows:
            con.execute(
                """
                INSERT OR IGNORE INTO vehicles
                (site_code, plate, unit, owner_name, status, valid_from, valid_to, note)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                r,
            )
        con.commit()


def seed_users() -> None:
    from .auth import pbkdf2_hash

    with connect() as con:
        con.execute(
            "INSERT OR IGNORE INTO users(username,pw_hash,role) VALUES (?,?,?)",
            ("admin", pbkdf2_hash("admin1234"), "admin"),
        )
        con.execute(
            "INSERT OR IGNORE INTO users(username,pw_hash,role) VALUES (?,?,?)",
            ("guard", pbkdf2_hash("guard1234"), "guard"),
        )
        con.execute(
            "INSERT OR IGNORE INTO users(username,pw_hash,role) VALUES (?,?,?)",
            ("viewer", pbkdf2_hash("viewer1234"), "viewer"),
        )
        con.commit()
