from datetime import datetime
from fastapi import HTTPException
from app.db import db_conn

def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

async def next_code(prefix: str, ym: str, width: int = 6) -> str:
    key = f"{prefix}-{ym}"
    async with db_conn() as db:
        await db.execute(
            "INSERT OR IGNORE INTO code_sequences(key,last_seq,updated_at) VALUES(?,0,datetime('now'))",
            (key,),
        )
        row = await db.execute_fetchone("SELECT last_seq FROM code_sequences WHERE key=?", (key,))
        last_seq = int(row["last_seq"]) if row else 0
        new_seq = last_seq + 1
        await db.execute(
            "UPDATE code_sequences SET last_seq=?, updated_at=datetime('now') WHERE key=?",
            (new_seq, key),
        )
    return f"{prefix}-{ym}-{new_seq:0{width}d}"

async def add_event(entity_type: str, entity_id: int, event_type: str, actor_id: int,
                    from_status: str | None = None, to_status: str | None = None, note: str | None = None):
    async with db_conn() as db:
        await db.execute(
            """
            INSERT INTO events(entity_type, entity_id, event_type, actor_id, from_status, to_status, note, created_at)
            VALUES(?,?,?,?,?,?,?,datetime('now'))
            """,
            (entity_type, entity_id, event_type, actor_id, from_status, to_status, note),
        )

async def require_attachments(entity_type: str, entity_id: int, min_count: int = 1):
    async with db_conn() as db:
        row = await db.execute_fetchone(
            "SELECT COUNT(*) AS cnt FROM attachments WHERE entity_type=? AND entity_id=?",
            (entity_type, entity_id),
        )
        cnt = int(row["cnt"]) if row else 0
        if cnt < min_count:
            raise HTTPException(status_code=400, detail=f"Need at least {min_count} attachment(s)")
