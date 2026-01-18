# db.py
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "tools.db"

def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tools (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        purpose TEXT NOT NULL,
        location TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT '정상',
        qty INTEGER NOT NULL DEFAULT 1,
        purchase_amount INTEGER NOT NULL DEFAULT 0,

        -- ✅ 분류(대/중/소)
        cat_l TEXT NOT NULL DEFAULT '',
        cat_m TEXT NOT NULL DEFAULT '',
        cat_s TEXT NOT NULL DEFAULT '',

        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tool_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_id INTEGER NOT NULL,
        image_path TEXT NOT NULL,
        ahash TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY(tool_id) REFERENCES tools(id) ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tool_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        person TEXT NOT NULL DEFAULT '',
        note TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
        FOREIGN KEY(tool_id) REFERENCES tools(id) ON DELETE CASCADE
    )
    """)

    # 검색 속도용 인덱스(현장에선 체감됩니다)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tools_location ON tools(location)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tools_status   ON tools(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tools_cat_l    ON tools(cat_l)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tools_cat_m    ON tools(cat_m)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tools_cat_s    ON tools(cat_s)")

    conn.commit()
    conn.close()