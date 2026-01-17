import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).with_name("tools.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
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
    created_at TEXT DEFAULT (datetime('now'))
)
""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS tool_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_id INTEGER NOT NULL,
        image_path TEXT NOT NULL,
        ahash TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(tool_id) REFERENCES tools(id)
    )
    """)

    # 반출/반납 로그 (선택 기능이지만 현장에선 이게 ‘돈’입니다)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tool_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,   -- '반출' or '반납' or '점검'
        person TEXT DEFAULT '',
        note TEXT DEFAULT '',
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(tool_id) REFERENCES tools(id)
    )
    """)

    conn.commit()
    conn.close()