# app/routers/meters.py
from fastapi import APIRouter
from ..db import get_db

router = APIRouter(prefix="/api", tags=["meters"])

@router.get("/meters")
def list_meters(q: str = "", meter_type: str = ""):
    """
    최소 동작: meters 테이블이 있으면 최근 100건.
    없으면 빈 목록.
    """
    try:
        with get_db() as conn:
            cur = conn.execute("""
              SELECT id, meter_code, name, meter_type, unit, location_id, updated_at
              FROM meters
              ORDER BY id DESC
              LIMIT 100
            """)
            items = [dict(r) for r in cur.fetchall()]
    except Exception:
        items = []

    if q:
        qq = q.strip().lower()
        items = [it for it in items if qq in str(it.get("meter_code","")).lower() or qq in str(it.get("name","")).lower()]
    if meter_type:
        mt = meter_type.strip().upper()
        items = [it for it in items if str(it.get("meter_type","")).upper() == mt]

    return {"ok": True, "items": items}
