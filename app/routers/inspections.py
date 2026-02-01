# app/routers/inspections.py
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from ..db import get_db

router = APIRouter(prefix="/api", tags=["inspections"])

@router.get("/inspections")
def list_inspections(q: str = "", status: str = ""):
    """
    최소 동작: 현재는 DB 스키마가 확정되기 전이라, 우선 빈 목록 또는 간단 조회만 제공합니다.
    UI가 404 대신 "살아있게" 만들기 위한 뼈대입니다.
    """
    # 테이블이 없거나 컬럼이 달라도 죽지 않도록 방어적으로 처리
    try:
        with get_db() as conn:
            # inspections 테이블이 존재하면 최근 50건만
            cur = conn.execute("""
              SELECT id, insp_code, title, status, performed_at, created_at
              FROM inspections
              ORDER BY id DESC
              LIMIT 50
            """)
            items = [dict(r) for r in cur.fetchall()]
    except Exception:
        items = []

    # 간단 필터(파이썬 레벨)
    if q:
        qq = q.strip().lower()
        items = [it for it in items if qq in str(it.get("insp_code","")).lower() or qq in str(it.get("title","")).lower()]
    if status:
        ss = status.strip().upper()
        items = [it for it in items if str(it.get("status","")).upper() == ss]

    return {"ok": True, "items": items}
