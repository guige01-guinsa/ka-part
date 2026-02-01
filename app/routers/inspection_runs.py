# app/routers/inspection_runs.py
from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.db import db_conn
from app.auth import get_current_user

router = APIRouter(prefix="/api", tags=["inspection_runs"])


class InspectionRunCreateIn(BaseModel):
    asset_id: Optional[int] = None
    template_id: Optional[int] = None
    run_date: str  # YYYY-MM-DD
    status: str = "OK"  # OK | ABNORMAL
    note: Optional[str] = None
    items: List[Dict[str, Any]] = []  # [{key,label,type,ok,value_text,value_num,note}, ...]


def _upper(s: str) -> str:
    return (s or "").strip().upper()


async def _next_run_code(db) -> str:
    # IR-YYYY-000001
    y = date.today().strftime("%Y")
    cur = await db.execute(
        "SELECT run_code FROM inspection_runs WHERE run_code LIKE ? ORDER BY id DESC LIMIT 1",
        (f"IR-{y}-%",),
    )
    r = await cur.fetchone()
    if not r:
        return f"IR-{y}-000001"
    last = r["run_code"]
    try:
        n = int(str(last).split("-")[-1]) + 1
    except Exception:
        n = 1
    return f"IR-{y}-{n:06d}"


async def _load_asset_context(db, asset_id: int) -> Dict[str, Any]:
    cur = await db.execute("SELECT * FROM assets WHERE id=?", (asset_id,))
    a = await cur.fetchone()
    if not a:
        return {}
    return dict(a)


async def _create_work_from_abnormal(
    db,
    user_login: str,
    run_id: int,
    run_code: str,
    asset: Dict[str, Any],
    run_date: str,
    note: str,
):
    """
    work_orders 스키마는 이미 프로젝트에 존재.
    가능한 컬럼만 넣는 방식이 베스트지만,
    현재 출력에 asset_name/source_type가 보이므로 우선 활용.
    """
    title = f"[ 점검이상] {asset.get('name') or '설비'} 점검 이상 ({run_code})"
    location_id = asset.get("location_id")
    category_id = asset.get("category_id")
    asset_name = asset.get("name")

    # work_code는 기존 규칙(예: WO-2026-000001)과 충돌 없이 생성:
    # 기존 work_code 생성 로직이 DB 트리거/앱에 있을 수도 있으니,
    # 여기서는 "work_code=NULL 허용"이 아니면 실패할 수 있음.
    # 그래서 안전하게 "WO-YYYY-<timestamp>" 임시를 발급(원하면 정식 넘버링으로 통일해드림).
    y = date.today().strftime("%Y")
    wo_code = f"WO-{y}-IR-{run_id:06d}"

    # 존재 컬럼을 가정하고 INSERT (schema.sql에 이미 있을 확률이 높음)
    await db.execute(
        """
        INSERT INTO work_orders
          (work_code, status, title, urgent, created_at, updated_at, due_date,
           result_note, location_id, category_id, asset_name, source_type)
        VALUES
          (?, 'ASSIGNED', ?, 1, datetime('now'), datetime('now'), ?, ?, ?, ?, ?, 'INSPECTION')
        """,
        (
            wo_code,
            title,
            run_date,
            note or "",
            location_id,
            category_id,
            asset_name,
        ),
    )


@router.get("/inspection/templates")
async def inspection_templates_list(
    request: Request,
    asset_type: str = Query("", description="PANEL/PUMP/etc"),
    active: int = Query(1, ge=0, le=1),
):
    _ = get_current_user(request)
    asset_type_u = _upper(asset_type)

    clauses = ["active = ?"]
    params: List[Any] = [active]
    if asset_type_u:
        clauses.append("upper(asset_type) = ?")
        params.append(asset_type_u)

    where = " WHERE " + " AND ".join(clauses)

    async with db_conn() as db:
        cur = await db.execute(
            f"SELECT * FROM inspection_templates {where} ORDER BY id DESC",
            tuple(params),
        )
        rows = await cur.fetchall()

    out = []
    for r in rows:
        d = dict(r)
        try:
            d["items"] = json.loads(d.get("items_json") or "[]")
        except Exception:
            d["items"] = []
        out.append(d)

    return {"ok": True, "items": out}


@router.post("/inspection/runs")
async def inspection_run_create(request: Request, body: InspectionRunCreateIn):
    user = get_current_user(request)
    login = user.get("login") or "unknown"

    status_u = _upper(body.status)
    if status_u not in {"OK", "ABNORMAL"}:
        raise HTTPException(status_code=400, detail="status must be OK or ABNORMAL")

    async with db_conn() as db:
        run_code = await _next_run_code(db)

        # 템플릿 메타
        tpl_items: List[Dict[str, Any]] = []
        if body.template_id:
            cur = await db.execute("SELECT items_json FROM inspection_templates WHERE id=?", (body.template_id,))
            tr = await cur.fetchone()
            if tr:
                try:
                    tpl_items = json.loads(tr["items_json"] or "[]")
                except Exception:
                    tpl_items = []

        await db.execute(
            """
            INSERT INTO inspection_runs (run_code, asset_id, template_id, inspector, run_date, status, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_code,
                body.asset_id,
                body.template_id,
                login,
                body.run_date,
                status_u,
                body.note,
            ),
        )
        cur = await db.execute("SELECT last_insert_rowid() AS id")
        rid = int((await cur.fetchone())["id"])

        # items 저장: body.items가 있으면 그걸 우선, 없으면 tpl_items 기반 빈 폼을 저장하지 않고 UI에서만 사용
        for it in (body.items or []):
            await db.execute(
                """
                INSERT INTO inspection_run_items (run_id, item_key, item_label, value_text, value_num, ok, note)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rid,
                    str(it.get("key") or ""),
                    str(it.get("label") or it.get("key") or ""),
                    it.get("value_text"),
                    it.get("value_num"),
                    None if it.get("ok") is None else (1 if bool(it.get("ok")) else 0),
                    it.get("note"),
                ),
            )

        # ABNORMAL이면 Work 자동 생성
        if status_u == "ABNORMAL":
            asset = await _load_asset_context(db, body.asset_id) if body.asset_id else {}
            await _create_work_from_abnormal(
                db=db,
                user_login=login,
                run_id=rid,
                run_code=run_code,
                asset=asset,
                run_date=body.run_date,
                note=body.note or "",
            )

        await db.commit()

    return {"ok": True, "run_id": rid, "run_code": run_code, "status": status_u}


@router.get("/inspection/runs")
async def inspection_runs_list(
    request: Request,
    date_from: str = Query("", description="YYYY-MM-DD"),
    date_to: str = Query("", description="YYYY-MM-DD"),
    status: str = Query("", description="OK/ABNORMAL"),
    limit: int = Query(200, ge=1, le=2000),
):
    _ = get_current_user(request)

    clauses: List[str] = ["1=1"]
    params: List[Any] = []
    if date_from:
        clauses.append("ir.run_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("ir.run_date <= ?")
        params.append(date_to)
    if status:
        clauses.append("upper(ir.status) = ?")
        params.append(_upper(status))

    where = " WHERE " + " AND ".join(clauses)

    async with db_conn() as db:
        cur = await db.execute(
            f"""
            SELECT ir.*,
                   a.name AS asset_name
            FROM inspection_runs ir
            LEFT JOIN assets a ON a.id = ir.asset_id
            {where}
            ORDER BY ir.id DESC
            LIMIT ?
            """,
            tuple(params + [limit]),
        )
        rows = await cur.fetchall()

    return {"ok": True, "items": [dict(r) for r in rows]}
