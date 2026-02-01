# tools/migrate_inspection_meter.py
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "ka.db"


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    )
    return cur.fetchone() is not None


def cols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {r[1] for r in cur.fetchall()}  # (cid, name, type, notnull, dflt_value, pk)


def add_col_if_missing(conn: sqlite3.Connection, table: str, ddl: str) -> None:
    # ddl 예: "category TEXT", "status TEXT DEFAULT 'OPEN'"
    col_name = ddl.split()[0].strip().strip("`").strip('"')
    if col_name in cols(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys=ON;")

        # -----------------------------
        # meters
        # -----------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meters (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              meter_code    TEXT UNIQUE,
              name          TEXT NOT NULL,
              location_id   INTEGER,
              category      TEXT,
              unit          TEXT,
              digits        INTEGER DEFAULT 0,
              note          TEXT DEFAULT '',
              created_at    TEXT DEFAULT (datetime('now')),
              updated_at    TEXT DEFAULT (datetime('now'))
            );
            """
        )

        for ddl in [
            "meter_code TEXT",
            "name TEXT",
            "location_id INTEGER",
            "category TEXT",
            "unit TEXT",
            "digits INTEGER DEFAULT 0",
            "note TEXT DEFAULT ''",
            "created_at TEXT",
            "updated_at TEXT",
        ]:
            add_col_if_missing(conn, "meters", ddl)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_meters_location_id ON meters(location_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_meters_category ON meters(category);")

        # -----------------------------
        # meter_reads
        # -----------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meter_reads (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              meter_id      INTEGER NOT NULL,
              read_date     TEXT NOT NULL,
              raw_value     TEXT NOT NULL,
              value         REAL,
              reader_login  TEXT,
              note          TEXT DEFAULT '',
              created_at    TEXT DEFAULT (datetime('now')),
              UNIQUE(meter_id, read_date),
              FOREIGN KEY (meter_id) REFERENCES meters(id) ON DELETE CASCADE
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_meter_reads_meter_date ON meter_reads(meter_id, read_date);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_meter_reads_date ON meter_reads(read_date);")

        # -----------------------------
        # inspections
        # -----------------------------
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inspections (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              insp_code     TEXT UNIQUE,
              title         TEXT NOT NULL,
              location_id   INTEGER,
              category      TEXT,
              status        TEXT DEFAULT 'OPEN',
              urgent        INTEGER DEFAULT 0,
              result_note   TEXT DEFAULT '',
              created_at    TEXT DEFAULT (datetime('now')),
              updated_at    TEXT DEFAULT (datetime('now'))
            );
            """
        )

        for ddl in [
            "insp_code TEXT",
            "title TEXT",
            "location_id INTEGER",
            "category TEXT",
            "status TEXT DEFAULT 'OPEN'",
            "urgent INTEGER DEFAULT 0",
            "result_note TEXT DEFAULT ''",
            "created_at TEXT",
            "updated_at TEXT",
        ]:
            add_col_if_missing(conn, "inspections", ddl)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_inspections_status ON inspections(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_inspections_created ON inspections(created_at);")

        # 샘플 (안전하게)
        conn.execute("INSERT OR IGNORE INTO meters (id, name) VALUES (1, '전기실 메인 계량기');")

        conn.commit()

        # 결과 출력
        def dump_cols(t: str) -> None:
            c = cols(conn, t)
            print(f"[OK] {t} columns: {', '.join(sorted(c))}")

        dump_cols("meters")
        dump_cols("meter_reads")
        dump_cols("inspections")

        print("[DONE] migration ok")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
