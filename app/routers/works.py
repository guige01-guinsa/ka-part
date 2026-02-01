# app/routers/works.py
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.db import db_conn
from app.auth import get_current_user
from app.notify import notify_kakao_event

router = APIRouter(prefix="/api", tags=["works"])

# ---------------------------------------------------------------------
# Role helpers
# ---------------------------------------------------------------------

ROLE_RESIDENT = {"RESIDENT", "입주민"}
ROLE_VENDOR = {"VENDOR", "외주업체"}
ROLE_STAFF = {"STAFF", "TECH", "담당자", "시설기사"}
ROLE_MANAGER = {"FACILITY_MANAGER", "LEAD", "시설과장"}
ROLE_CHIEF = {"CHIEF", "MANAGER", "관리소장"}


def _has_any(user: Dict[str, Any], role_set: set[str]) -> bool:
    roles = set(user.get("roles") or [])
    return bool(roles & role_set)


def _is_resident(user: Dict[str, Any]) -> bool:
    return _has_any(user, ROLE_RESIDENT)


def _is_vendor(user: Dict[str, Any]) -> bool:
    return _has_any(user, ROLE_VENDOR)


def _is_staff(user: Dict[str, Any]) -> bool:
    return _has_any(user, ROLE_STAFF | ROLE_MANAGER | ROLE_CHIEF)


def _is_manager(user: Dict[str, Any]) -> bool:
    return _has_any(user, ROLE_MANAGER | ROLE_CHIEF)


def _is_chief(user: Dict[str, Any]) -> bool:
    return _has_any(user, ROLE_CHIEF)


# ---------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------


def _table_exists(db, table: str) -> bool:
    cur = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return (cur.fetchone()) is not None


def _has_col(db, table: str, col: str) -> bool:
    cur = db.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    cols = {r[1] for r in rows}
    return col in cols


def _insert_event(
    db,
    entity_type: str,
    entity_id: int,
    event_type: str,
    actor_id: int,
    actor_login: Optional[str] = None,
    from_status: Optional[str] = None,
    to_status: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    cols = ["entity_type", "entity_id", "event_type", "actor_id", "from_status", "to_status", "note"]
    vals = [entity_type, entity_id, event_type, actor_id, from_status, to_status, note]

    if _has_col(db, "events", "actor_login"):
        cols.append("actor_login")
        vals.append(actor_login)

    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO events({', '.join(cols)}, created_at) VALUES({placeholders}, datetime('now'))"
    db.execute(sql, tuple(vals))


# ---------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------


def _select_work_base(db) -> str:
    if not _table_exists(db, "work_orders"):
        raise HTTPException(
            status_code=500,
            detail="DB schema mismatch: work_orders table not found. (init_db.py 실행/DB 경로 확인 필요)",
        )

    cols: List[str] = []
    cols += ["wo.id", "wo.work_code", "wo.status"]

    for c in [
        "title",
        "created_at",
        "updated_at",
        "due_at",
        "urgent",
        "result_note",
        "location_id",
        "category_id",
        "requested_by",
        "assigned_to",
        "vendor_id",
        "outsourcing_mode",
    ]:
        if _has_col(db, "work_orders", c):
            cols.append(f"wo.{c}")
        else:
            if c in ("urgent",):
                cols.append("0 AS urgent")
            elif c in ("title", "result_note", "outsourcing_mode"):
                cols.append("'' AS " + c)
            else:
                cols.append("NULL AS " + c)

    join_sql = ""

    if _table_exists(db, "locations"):
        join_sql += " LEFT JOIN locations l ON l.id = wo.location_id "
        cols.append("l.name AS location_name")
    else:
        cols.append("NULL AS location_name")

    if _table_exists(db, "categories"):
        join_sql += " LEFT JOIN categories c ON c.id = wo.category_id "
        cols.append("c.name AS category_name")
    else:
        cols.append("NULL AS category_name")

    if _has_col(db, "work_orders", "asset_name"):
        cols.append("wo.asset_name")
    else:
        cols.append("NULL AS asset_name")

    if _has_col(db, "work_orders", "source_type"):
        cols.append("wo.source_type")
    else:
        cols.append("NULL AS source_type")

    select_sql = f"SELECT {', '.join(cols)} FROM work_orders wo {join_sql}"
    return select_sql


def _build_where(db, mode: str, q: str, user: Dict[str, Any]) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []

    if mode == "done":
        clauses.append("wo.status = 'DONE'")
    elif mode == "open":
        clauses.append("wo.status NOT IN ('DONE','CANCELED')")
    elif mode == "urgent":
        if _has_col(db, "work_orders", "urgent"):
            clauses.append("wo.urgent = 1")
        else:
            clauses.append("1=0")
    elif mode == "today":
        today = date.today().isoformat()
        if _has_col(db, "work_orders", "due_at"):
            clauses.append("substr(wo.due_at,1,10) = ?")
            params.append(today)
        elif _has_col(db, "work_orders", "created_at"):
            clauses.append("substr(wo.created_at,1,10) = ?")
            params.append(today)
        else:
            clauses.append("1=0")

    q = (q or "").strip()
    if q:
        like = f"%{q}%"
        ors: List[str] = ["wo.work_code LIKE ?"]
        params.append(like)

        if _has_col(db, "work_orders", "title"):
            ors.append("wo.title LIKE ?")
            params.append(like)

        if _table_exists(db, "locations"):
            ors.append("l.name LIKE ?")
            params.append(like)

        if _table_exists(db, "categories"):
            ors.append("c.name LIKE ?")
            params.append(like)

        if _has_col(db, "work_orders", "asset_name"):
            ors.append("wo.asset_name LIKE ?")
            params.append(like)

        if _has_col(db, "work_orders", "source_type"):
            ors.append("wo.source_type LIKE ?")
            params.append(like)

        clauses.append("(" + " OR ".join(ors) + ")")

    # Role-based visibility
    if _is_resident(user):
        if _has_col(db, "work_orders", "requested_by"):
            clauses.append("wo.requested_by = ?")
            params.append(int(user["id"]))
        else:
            clauses.append("1=0")
    elif _is_vendor(user):
        if _has_col(db, "work_orders", "vendor_id") and user.get("vendor_id"):
            clauses.append("wo.vendor_id = ?")
            params.append(int(user["vendor_id"]))
        else:
            clauses.append("1=0")

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def _list_works(db, mode: str, q: str, limit: int, user: Dict[str, Any]) -> List[Dict[str, Any]]:
    select_sql = _select_work_base(db)
    where, params = _build_where(db, mode=mode, q=q, user=user)

    order = "wo.id DESC"
    if _has_col(db, "work_orders", "created_at"):
        order = "wo.created_at DESC, wo.id DESC"

    sql = select_sql + where + f" ORDER BY {order} LIMIT ?"
    params2 = list(params) + [int(limit)]

    cur = db.execute(sql, tuple(params2))
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def _count_works(db, mode: str, user: Dict[str, Any]) -> int:
    select_sql = _select_work_base(db)
    where, params = _build_where(db, mode=mode, q="", user=user)
    sql = f"SELECT COUNT(1) AS n FROM ({select_sql} {where}) t"
    cur = db.execute(sql, tuple(params))
    r = cur.fetchone()
    return int(r["n"] if r and "n" in r.keys() else 0)


def _get_work_row(db, work_id: int) -> Optional[Dict[str, Any]]:
    select_sql = _select_work_base(db)
    sql = select_sql + " WHERE wo.id=?"
    cur = db.execute(sql, (work_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def _can_view_work(user: Dict[str, Any], row: Dict[str, Any]) -> bool:
    if _is_staff(user):
        return True
    if _is_resident(user):
        return int(row.get("requested_by") or -1) == int(user["id"])
    if _is_vendor(user):
        return row.get("vendor_id") and user.get("vendor_id") and int(row["vendor_id"]) == int(user["vendor_id"])
    return False


# ---------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------


class WorkPatchIn(BaseModel):
    result_note: Optional[str] = None
    urgent: Optional[bool] = None


class WorkCommentIn(BaseModel):
    note: str


class WorkOutsourceIn(BaseModel):
    mode: str  # INHOUSE | OUTSOURCE
    vendor_id: Optional[int] = None
    note: Optional[str] = None


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------


@router.get("/works")
def works_list(
    request: Request,
    mode: str = Query("all", pattern="^(all|today|open|urgent|done)$"),
    q: str = Query("", description="search keyword"),
    limit: int = Query(200, ge=1, le=1000),
):
    user = get_current_user(request)

    with db_conn() as db:
        items = _list_works(db, mode=mode, q=q, limit=limit, user=user)

        counts = {
            "today": _count_works(db, "today", user),
            "open": _count_works(db, "open", user),
            "urgent": _count_works(db, "urgent", user),
            "done": _count_works(db, "done", user),
            "all": _count_works(db, "all", user),
        }

    return {
        "ok": True,
        "mode": mode,
        "q": q,
        "items": items,
        "counts": counts,
    }


@router.get("/works/{work_id}")
def work_get(request: Request, work_id: int):
    user = get_current_user(request)

    with db_conn() as db:
        row = _get_work_row(db, work_id)
        if not row:
            raise HTTPException(status_code=404, detail="Work not found")
        if not _can_view_work(user, row):
            raise HTTPException(status_code=403, detail="forbidden")

    return {"ok": True, "work": row}


@router.patch("/works/{work_id}")
def work_patch(request: Request, work_id: int, body: WorkPatchIn):
    user = get_current_user(request)
    if not _is_staff(user):
        raise HTTPException(status_code=403, detail="forbidden")

    with db_conn() as db:
        row = _get_work_row(db, work_id)
        if not row:
            raise HTTPException(status_code=404, detail="Work not found")

        if (row.get("status") or "").upper() == "DONE" and not _is_manager(user):
            raise HTTPException(status_code=403, detail="DONE 상태 작업은 관리자만 수정 가능")

        sets: List[str] = []
        params: List[Any] = []

        if body.result_note is not None and _has_col(db, "work_orders", "result_note"):
            sets.append("result_note=?")
            params.append(body.result_note)

        if body.urgent is not None and _has_col(db, "work_orders", "urgent"):
            sets.append("urgent=?")
            params.append(1 if body.urgent else 0)

        if not sets:
            return {"ok": True, "updated": False}

        if _has_col(db, "work_orders", "updated_at"):
            sets.append("updated_at=datetime('now')")

        params.append(work_id)
        sql = "UPDATE work_orders SET " + ", ".join(sets) + " WHERE id=?"
        db.execute(sql, tuple(params))
        db.commit()

    return {"ok": True, "updated": True}


@router.post("/works/{work_id}/status")
def work_status_change(request: Request, work_id: int, to_status: Dict[str, str]):
    user = get_current_user(request)

    next_status = (to_status.get("to_status") or "").strip().upper()
    allowed_status = {
        "NEW",
        "ASSIGNED",
        "IN_PROGRESS",
        "REVIEW",
        "APPROVED",
        "DONE",
        "HOLD",
        "REJECTED",
        "CANCELED",
    }
    if next_status not in allowed_status:
        raise HTTPException(status_code=400, detail="Invalid to_status")

    if _is_resident(user) or _is_vendor(user):
        raise HTTPException(status_code=403, detail="No permission")

    if next_status in {"APPROVED", "REJECTED"} and not _is_manager(user):
        raise HTTPException(status_code=403, detail="APPROVED/REJECTED 권한 부족")

    with db_conn() as db:
        row = _get_work_row(db, work_id)
        if not row:
            raise HTTPException(status_code=404, detail="Work not found")
        if not _can_view_work(user, row):
            raise HTTPException(status_code=403, detail="forbidden")

        cur_status = (row.get("status") or "").upper()

        transitions = {
            "NEW": {"ASSIGNED", "IN_PROGRESS", "HOLD", "CANCELED"},
            "ASSIGNED": {"IN_PROGRESS", "REVIEW", "HOLD", "CANCELED"},
            "IN_PROGRESS": {"REVIEW", "HOLD", "CANCELED"},
            "REVIEW": {"APPROVED", "REJECTED", "IN_PROGRESS"},
            "APPROVED": {"DONE", "REJECTED"},
            "REJECTED": {"IN_PROGRESS", "CANCELED"},
            "HOLD": {"IN_PROGRESS", "CANCELED"},
            "DONE": set(),
            "CANCELED": set(),
        }

        if next_status not in transitions.get(cur_status, set()):
            raise HTTPException(status_code=400, detail=f"Invalid transition {cur_status} -> {next_status}")

        sets = ["status=?", "updated_at=datetime('now')"]
        params: List[Any] = [next_status]

        if next_status == "IN_PROGRESS" and _has_col(db, "work_orders", "started_at"):
            sets.append("started_at=datetime('now')")
        if next_status == "DONE" and _has_col(db, "work_orders", "completed_at"):
            sets.append("completed_at=datetime('now')")

        params.append(work_id)
        db.execute(
            "UPDATE work_orders SET " + ", ".join(sets) + " WHERE id=?",
            tuple(params),
        )

        _insert_event(
            db,
            entity_type="WORK_ORDER",
            entity_id=work_id,
            event_type="STATUS_CHANGE",
            actor_id=int(user["id"]),
            actor_login=user.get("login"),
            from_status=cur_status,
            to_status=next_status,
        )

        db.commit()

    notify_kakao_event(
        event="WORK_STATUS",
        work_id=work_id,
        title=row.get("title") if row else "",
        message=f"{cur_status} -> {next_status}",
        vendor_id=row.get("vendor_id") if row else None,
    )

    return {"ok": True, "from": cur_status, "to": next_status}


@router.post("/works/{work_id}/comment")
def work_comment(request: Request, work_id: int, body: WorkCommentIn):
    user = get_current_user(request)
    if not (_is_staff(user) or _is_vendor(user)):
        raise HTTPException(status_code=403, detail="forbidden")

    note = (body.note or "").strip()
    if not note:
        raise HTTPException(status_code=400, detail="note required")

    with db_conn() as db:
        row = _get_work_row(db, work_id)
        if not row:
            raise HTTPException(status_code=404, detail="Work not found")
        if not _can_view_work(user, row):
            raise HTTPException(status_code=403, detail="forbidden")

        _insert_event(
            db,
            entity_type="WORK_ORDER",
            entity_id=work_id,
            event_type="COMMENT",
            actor_id=int(user["id"]),
            actor_login=user.get("login"),
            note=note,
        )
        db.commit()

    return {"ok": True}


@router.post("/works/{work_id}/outsourcing")
def work_outsourcing(request: Request, work_id: int, body: WorkOutsourceIn):
    user = get_current_user(request)
    if not _is_manager(user):
        raise HTTPException(status_code=403, detail="forbidden")

    mode = (body.mode or "").strip().upper()
    if mode not in {"INHOUSE", "OUTSOURCE"}:
        raise HTTPException(status_code=400, detail="mode must be INHOUSE or OUTSOURCE")

    with db_conn() as db:
        row = _get_work_row(db, work_id)
        if not row:
            raise HTTPException(status_code=404, detail="Work not found")

        if not _has_col(db, "work_orders", "outsourcing_mode"):
            raise HTTPException(status_code=500, detail="outsourcing columns missing (run migration)")

        sets = ["outsourcing_mode=?", "updated_at=datetime('now')"]
        params: List[Any] = [mode]

        if _has_col(db, "work_orders", "vendor_id"):
            sets.append("vendor_id=?")
            params.append(body.vendor_id)

        if _has_col(db, "work_orders", "outsourcing_note"):
            sets.append("outsourcing_note=?")
            params.append(body.note or "")

        if _has_col(db, "work_orders", "outsourcing_decided_by"):
            sets.append("outsourcing_decided_by=?")
            params.append(int(user["id"]))

        if _has_col(db, "work_orders", "outsourcing_decided_at"):
            sets.append("outsourcing_decided_at=datetime('now')")

        params.append(work_id)
        db.execute("UPDATE work_orders SET " + ", ".join(sets) + " WHERE id=?", tuple(params))

        _insert_event(
            db,
            entity_type="WORK_ORDER",
            entity_id=work_id,
            event_type="OUTSOURCING_DECISION",
            actor_id=int(user["id"]),
            actor_login=user.get("login"),
            note=f"{mode} vendor_id={body.vendor_id or ''} {body.note or ''}".strip(),
        )

        db.commit()

    notify_kakao_event(
        event="WORK_OUTSOURCING",
        work_id=work_id,
        title=row.get("title") if row else "",
        message=f"{mode} vendor_id={body.vendor_id or '-'}",
        vendor_id=body.vendor_id,
    )

    return {"ok": True, "mode": mode}
