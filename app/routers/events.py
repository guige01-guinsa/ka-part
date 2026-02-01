# app/routers/events.py
from fastapi import APIRouter, Request, HTTPException
from typing import Optional

from app.db import db_conn
from app.auth import get_current_user

router = APIRouter(prefix="/api/events", tags=["events"])

ALLOWED_ENTITY_TYPES = {"WORK_ORDER", "INSPECTION", "MONTHLY_REPORT"}

@router.get("")
async def events_list(
    request: Request,
    entity_type: str,
    entity_id: int,
    limit: int = 200,
):
    """
    공통 이벤트 목록
    - /api/events?entity_type=WORK_ORDER&entity_id=1
    """
    _ = get_current_user(request)

    et = (entity_type or "").strip().upper()
    if et not in ALLOWED_ENTITY_TYPES:
        raise HTTPException(status_code=400, detail="Invalid entity_type")

    if limit < 1 or limit > 500:
        limit = 200

    with db_conn() as db:
        cur = await db.execute(
            """
            SELECT id, entity_type, entity_id, event_type,
                   from_status, to_status, note, actor_login, created_at
            FROM events
            WHERE entity_type=? AND entity_id=?
            ORDER BY id ASC
            LIMIT ?
            """,
            (et, entity_id, limit),
        )
        rows = await cur.fetchall()

    return {"ok": True, "items": [dict(r) for r in rows]}
