from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user, require_role
from app.db import db_conn
from app.schemas import PRCreate, PRStatus
from app.utils import next_code, add_event

router = APIRouter(prefix="/api/proc", tags=["procurement"])

@router.get("/pr")
async def list_pr(status: str | None = None, user=Depends(get_current_user)):
    sql = """
    SELECT pr.id, pr.pr_code, pr.status, pr.need_by, pr.created_at, w.work_code
    FROM purchase_requests pr
    LEFT JOIN work_orders w ON w.id=pr.work_order_id
    WHERE 1=1
    """
    params = []
    if status:
        sql += " AND pr.status=?"
        params.append(status)
    sql += " ORDER BY pr.id DESC LIMIT 200"

    with db_conn() as db:
        rows = await (await db.execute(sql, tuple(params))).fetchall()
        return [dict(r) for r in rows]

@router.get("/pr/{pr_id}")
async def get_pr(pr_id: int, user=Depends(get_current_user)):
    with db_conn() as db:
        pr = await db.execute_fetchone("SELECT * FROM purchase_requests WHERE id=?", (pr_id,))
        if not pr:
            raise HTTPException(404, "Not found")
        lines = await (await db.execute("SELECT * FROM purchase_request_lines WHERE pr_id=? ORDER BY id ASC", (pr_id,))).fetchall()
        return {"pr": dict(pr), "lines": [dict(r) for r in lines]}

@router.post("/pr")
async def create_pr(payload: PRCreate, user=Depends(get_current_user)):
    year = str(datetime.now().year)
    pr_code = await next_code("PR", year, 6)

    with db_conn() as db:
        cur = await db.execute(
            """
            INSERT INTO purchase_requests(pr_code, work_order_id, requested_by, status, need_by, note)
            VALUES(?,?,?,?,?,?)
            """,
            (pr_code, payload.work_order_id, user["id"], "DRAFT", payload.need_by, payload.note),
        )
        pr_id = cur.lastrowid

        for ln in payload.lines:
            await db.execute(
                """
                INSERT INTO purchase_request_lines(pr_id, item_id, item_name, qty, unit, target_price, spec_note)
                VALUES(?,?,?,?,?,?,?)
                """,
                (pr_id, ln.item_id, ln.item_name, ln.qty, ln.unit, ln.target_price, ln.spec_note),
            )

    await add_event("PR", pr_id, "CREATE", user["id"], note="PR created")
    return {"id": pr_id, "pr_code": pr_code}

@router.post("/pr/{pr_id}/transition")
async def pr_transition(pr_id: int, to_status: PRStatus, note: str | None = None, user=Depends(get_current_user)):
    with db_conn() as db:
        pr = await db.execute_fetchone("SELECT id, status FROM purchase_requests WHERE id=?", (pr_id,))
        if not pr:
            raise HTTPException(404, "Not found")

    frm = pr["status"]
    allowed = {
        "DRAFT": {"REVIEW", "CANCELED"},
        "REVIEW": {"APPROVED", "REJECTED"},
        "REJECTED": {"DRAFT"},
        "APPROVED": {"ORDERED"},
        "ORDERED": set(),
        "CANCELED": set(),
    }
    if to_status not in allowed.get(frm, set()):
        raise HTTPException(400, f"Invalid transition {frm}->{to_status}")

    if to_status in ("APPROVED", "REJECTED", "ORDERED"):
        await require_role(user["id"], {"LEAD", "MANAGER"})

    with db_conn() as db:
        await db.execute(
            "UPDATE purchase_requests SET status=?, updated_at=datetime('now') WHERE id=?",
            (to_status, pr_id),
        )

    await add_event("PR", pr_id, "STATUS_CHANGE", user["id"], from_status=frm, to_status=to_status, note=note)
    return {"ok": True, "from": frm, "to": to_status}
