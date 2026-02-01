from fastapi import APIRouter, Depends
from app.auth import get_current_user
from app.db import db_conn
from app.schemas import SearchResult

router = APIRouter(prefix="/api/search", tags=["search"])

@router.get("", response_model=list[SearchResult])
async def search(q: str, user=Depends(get_current_user)):
    q = (q or "").strip()
    if not q:
        return []

    results: list[SearchResult] = []

    with db_conn() as db:
        # 1) work_code 우선
        if q.upper().startswith("WO-"):
            rows = await (await db.execute(
                """
                SELECT id, work_code, title, status
                FROM work_orders
                WHERE work_code LIKE ?
                ORDER BY id DESC LIMIT 20
                """, (f"%{q}%",)
            )).fetchall()
            for r in rows:
                results.append(SearchResult(
                    type="WORK",
                    id=r["id"],
                    title=f"{r['work_code']} · {r['title']}",
                    subtitle=f"상태: {r['status']}"
                ))
            return results

        # 2) locations
        rows = await (await db.execute(
            """
            SELECT id, name, code, type, building, floor, unit
            FROM locations
            WHERE name LIKE ? OR code LIKE ?
            ORDER BY type DESC, id DESC LIMIT 20
            """, (f"%{q}%", f"%{q}%")
        )).fetchall()
        for r in rows:
            subtitle = r["code"]
            results.append(SearchResult(type="LOCATION", id=r["id"], title=r["name"], subtitle=subtitle))

        # 3) assets
        rows = await (await db.execute(
            """
            SELECT a.id, a.asset_code, a.name, l.name AS loc
            FROM assets a
            LEFT JOIN locations l ON l.id=a.location_id
            WHERE a.name LIKE ? OR a.asset_code LIKE ?
            ORDER BY a.id DESC LIMIT 20
            """, (f"%{q}%", f"%{q}%")
        )).fetchall()
        for r in rows:
            results.append(SearchResult(type="ASSET", id=r["id"], title=f"{r['asset_code']} · {r['name']}", subtitle=r["loc"] or ""))

        # 4) works title
        rows = await (await db.execute(
            """
            SELECT id, work_code, title, status
            FROM work_orders
            WHERE title LIKE ?
            ORDER BY id DESC LIMIT 20
            """, (f"%{q}%",)
        )).fetchall()
        for r in rows:
            results.append(SearchResult(type="WORK", id=r["id"], title=f"{r['work_code']} · {r['title']}", subtitle=f"상태: {r['status']}"))

        # 5) inspections summary
        rows = await (await db.execute(
            """
            SELECT i.id, i.overall_result, i.performed_at, COALESCE(a.name, l.name, '-') AS target
            FROM inspections i
            LEFT JOIN assets a ON a.id=i.asset_id
            LEFT JOIN locations l ON l.id=i.location_id
            WHERE i.summary_note LIKE ?
            ORDER BY i.id DESC LIMIT 20
            """, (f"%{q}%",)
        )).fetchall()
        for r in rows:
            results.append(SearchResult(type="INSPECTION", id=r["id"], title=f"{r['performed_at']} · {r['target']}", subtitle=f"결과: {r['overall_result']}"))

    return results[:40]
