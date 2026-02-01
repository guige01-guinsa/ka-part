from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.db import db_conn
from app.notify import notify_kakao_event

router = APIRouter(prefix="/api/complaints", tags=["complaints"])


class ComplaintCreateIn(BaseModel):
    title: str = Field(min_length=2, max_length=120)
    description: Optional[str] = None
    category_id: int
    location_id: int
    priority: int = Field(default=3, ge=1, le=5)
    is_emergency: bool = False


def _next_work_code(db) -> str:
    y = date.today().strftime("%Y")
    key = f"WO-{y}"
    db.execute(
        "INSERT OR IGNORE INTO code_sequences(key,last_seq,updated_at) VALUES(?,0,datetime('now'))",
        (key,),
    )
    row = db.execute("SELECT last_seq FROM code_sequences WHERE key=?", (key,)).fetchone()
    last_seq = int(row["last_seq"]) if row else 0
    new_seq = last_seq + 1
    db.execute(
        "UPDATE code_sequences SET last_seq=?, updated_at=datetime('now') WHERE key=?",
        (new_seq, key),
    )
    return f"WO-{y}-{new_seq:06d}"


@router.post("")
def complaint_create(request: Request, body: ComplaintCreateIn):
    user = get_current_user(request)

    if not body.title.strip():
        raise HTTPException(status_code=400, detail="title required")

    with db_conn() as db:
        work_code = _next_work_code(db)

        cols = [
            "work_code",
            "source_type",
            "category_id",
            "location_id",
            "title",
            "description",
            "priority",
            "is_emergency",
            "status",
            "requested_by",
        ]
        vals = [
            work_code,
            "COMPLAINT",
            body.category_id,
            body.location_id,
            body.title.strip(),
            (body.description or "").strip(),
            int(body.priority),
            1 if body.is_emergency else 0,
            "NEW",
            int(user["id"]),
        ]

        sql_cols = cols + ["created_at", "updated_at"]
        sql_vals = ["?"] * len(cols) + ["datetime('now')", "datetime('now')"]

        has_urgent = any(c["name"] == "urgent" for c in db.execute("PRAGMA table_info(work_orders)").fetchall())
        if has_urgent:
            sql_cols.append("urgent")
            sql_vals.append("?")
            vals.append(1 if body.is_emergency else 0)

        sql = f"INSERT INTO work_orders ({', '.join(sql_cols)}) VALUES ({', '.join(sql_vals)})"
        db.execute(sql, tuple(vals))
        cur = db.execute("SELECT last_insert_rowid() AS id")
        work_id = int(cur.fetchone()["id"])

        if db.execute("PRAGMA table_info(events)").fetchone():
            cols = db.execute("PRAGMA table_info(events)").fetchall()
            has_actor_login = any(c["name"] == "actor_login" for c in cols)
            if has_actor_login:
                db.execute(
                    """
                    INSERT INTO events(entity_type, entity_id, event_type, actor_id, actor_login, created_at, note)
                    VALUES('WORK_ORDER', ?, 'CREATE', ?, ?, datetime('now'), ?)
                    """,
                    (work_id, int(user["id"]), user.get("login"), "COMPLAINT"),
                )
            else:
                db.execute(
                    """
                    INSERT INTO events(entity_type, entity_id, event_type, actor_id, created_at, note)
                    VALUES('WORK_ORDER', ?, 'CREATE', ?, datetime('now'), ?)
                    """,
                    (work_id, int(user["id"]), "COMPLAINT"),
                )

        db.commit()

    notify_kakao_event(
        event="COMPLAINT_NEW",
        work_id=work_id,
        title=body.title.strip(),
        message="New complaint received",
    )

    return {"ok": True, "work_id": work_id, "work_code": work_code}
