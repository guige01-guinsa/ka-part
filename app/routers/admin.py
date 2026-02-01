from __future__ import annotations

from typing import List, Optional
import json

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.db import db_conn
from app.notify import send_kakao_payload

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _require_admin(user: dict) -> None:
    roles = set(user.get("roles") or [])
    if not (user.get("is_admin") or ("CHIEF" in roles) or ("MANAGER" in roles) or ("관리소장" in roles)):
        raise HTTPException(status_code=403, detail="forbidden")


class UserCreateIn(BaseModel):
    login: str = Field(min_length=2, max_length=40)
    name: str = Field(min_length=1, max_length=60)
    phone: Optional[str] = None
    is_active: int = 1
    role_codes: List[str] = []
    vendor_id: Optional[int] = None


class UserPatchIn(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    is_active: Optional[int] = None
    role_codes: Optional[List[str]] = None
    vendor_id: Optional[int] = None


class VendorCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    phone: Optional[str] = None
    email: Optional[str] = None
    is_active: int = 1


@router.get("/roles")
def roles_list(request: Request):
    user = get_current_user(request)
    _require_admin(user)

    with db_conn() as db:
        rows = db.execute("SELECT id, code, name FROM roles ORDER BY id ASC").fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


@router.get("/users")
def users_list(request: Request, q: str = "", role: str = ""):
    user = get_current_user(request)
    _require_admin(user)

    q = (q or "").strip()
    role = (role or "").strip().upper()
    params: list = []

    sql = """
    SELECT u.id, u.login, u.name, u.phone, u.is_active, u.vendor_id
    FROM users u
    WHERE 1=1
    """
    if q:
        sql += " AND (u.login LIKE ? OR u.name LIKE ? OR u.phone LIKE ?)"
        like = f"%{q}%"
        params += [like, like, like]

    sql += " ORDER BY u.id DESC LIMIT 500"

    with db_conn() as db:
        cur = db.execute(sql, tuple(params))
        rows = cur.fetchall()

        user_ids = [r["id"] for r in rows]
        roles_map = {}
        if user_ids:
            cur = db.execute(
                """
                SELECT ur.user_id, r.code, r.name
                FROM user_roles ur
                JOIN roles r ON r.id = ur.role_id
                WHERE ur.user_id IN (%s)
                """ % ",".join(["?"] * len(user_ids)),
                tuple(user_ids),
            )
            for r in cur.fetchall():
                roles_map.setdefault(r["user_id"], []).append({"code": r["code"], "name": r["name"]})

    items = []
    for r in rows:
        item = dict(r)
        item["roles"] = roles_map.get(r["id"], [])
        items.append(item)

    if role:
        items = [
            it for it in items
            if any((rr.get("code") or "").upper() == role for rr in it.get("roles", []))
        ]

    return {"ok": True, "items": items}


@router.post("/users")
def user_create(request: Request, body: UserCreateIn):
    user = get_current_user(request)
    _require_admin(user)

    with db_conn() as db:
        cur = db.execute("SELECT id FROM users WHERE login=?", (body.login.strip(),))
        if cur.fetchone():
            raise HTTPException(status_code=409, detail="login already exists")

        db.execute(
            """
            INSERT INTO users(name, login, phone, is_active, vendor_id, created_at, updated_at)
            VALUES(?,?,?,?,?,datetime('now'),datetime('now'))
            """,
            (body.name.strip(), body.login.strip(), body.phone, int(body.is_active), body.vendor_id),
        )
        uid = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        if body.role_codes:
            for code in body.role_codes:
                code = (code or "").strip().upper()
                if not code:
                    continue
                r = db.execute("SELECT id FROM roles WHERE upper(code)=?", (code,)).fetchone()
                if r:
                    db.execute(
                        "INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES(?,?)",
                        (uid, r["id"]),
                    )

        db.commit()

    return {"ok": True, "id": uid}


@router.patch("/users/{user_id}")
def user_patch(request: Request, user_id: int, body: UserPatchIn):
    user = get_current_user(request)
    _require_admin(user)

    sets = []
    params: list = []
    if body.name is not None:
        sets.append("name=?")
        params.append(body.name.strip())
    if body.phone is not None:
        sets.append("phone=?")
        params.append(body.phone)
    if body.is_active is not None:
        sets.append("is_active=?")
        params.append(int(body.is_active))
    if body.vendor_id is not None:
        sets.append("vendor_id=?")
        params.append(body.vendor_id)

    with db_conn() as db:
        if sets:
            sets.append("updated_at=datetime('now')")
            params.append(user_id)
            db.execute("UPDATE users SET " + ", ".join(sets) + " WHERE id=?", tuple(params))

        if body.role_codes is not None:
            db.execute("DELETE FROM user_roles WHERE user_id=?", (user_id,))
            for code in body.role_codes:
                code = (code or "").strip().upper()
                if not code:
                    continue
                r = db.execute("SELECT id FROM roles WHERE upper(code)=?", (code,)).fetchone()
                if r:
                    db.execute(
                        "INSERT OR IGNORE INTO user_roles(user_id, role_id) VALUES(?,?)",
                        (user_id, r["id"]),
                    )

        db.commit()

    return {"ok": True}


@router.get("/vendors")
def vendors_list(request: Request):
    user = get_current_user(request)
    _require_admin(user)
    with db_conn() as db:
        rows = db.execute("SELECT id, name, phone, email, is_active FROM vendors ORDER BY id DESC").fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


@router.post("/vendors")
def vendor_create(request: Request, body: VendorCreateIn):
    user = get_current_user(request)
    _require_admin(user)
    with db_conn() as db:
        db.execute(
            """
            INSERT INTO vendors(name, phone, email, is_active)
            VALUES(?,?,?,?)
            """,
            (body.name.strip(), body.phone, body.email, int(body.is_active)),
        )
        vid = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        db.commit()
    return {"ok": True, "id": vid}


@router.get("/notification-templates")
def templates_list(request: Request):
    user = get_current_user(request)
    _require_admin(user)
    with db_conn() as db:
        rows = db.execute(
            "SELECT id, event_key, template_code, enabled, message_format FROM notification_templates ORDER BY id DESC"
        ).fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


class TemplateUpsertIn(BaseModel):
    event_key: str
    template_code: str
    enabled: int = 1
    message_format: Optional[str] = None


@router.post("/notification-templates")
def template_upsert(request: Request, body: TemplateUpsertIn):
    user = get_current_user(request)
    _require_admin(user)

    ek = (body.event_key or "").strip().upper()
    if not ek:
        raise HTTPException(status_code=400, detail="event_key required")

    with db_conn() as db:
        cur = db.execute(
            "SELECT id FROM notification_templates WHERE event_key=?",
            (ek,),
        ).fetchone()
        if cur:
            db.execute(
                """
                UPDATE notification_templates
                SET template_code=?, enabled=?, message_format=?
                WHERE event_key=?
                """,
                (body.template_code, int(body.enabled), body.message_format, ek),
            )
            tid = cur["id"]
        else:
            db.execute(
                """
                INSERT INTO notification_templates(event_key, template_code, enabled, message_format)
                VALUES(?,?,?,?)
                """,
                (ek, body.template_code, int(body.enabled), body.message_format),
            )
            tid = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        db.commit()

    return {"ok": True, "id": tid}


@router.get("/notification-queue")
def notification_queue(request: Request, limit: int = 200, status: str = "", q: str = ""):
    user = get_current_user(request)
    _require_admin(user)

    status = (status or "").strip().upper()
    q = (q or "").strip()
    clauses = ["1=1"]
    params: list = []
    if status:
        clauses.append("status=?")
        params.append(status)
    if q:
        clauses.append("(recipient LIKE ? OR error LIKE ?)")
        like = f"%{q}%"
        params += [like, like]

    where = " AND ".join(clauses)
    sql = f"""
        SELECT id, channel, recipient, payload_json, status, created_at, sent_at, error
        FROM notification_queue
        WHERE {where}
        ORDER BY id DESC
        LIMIT ?
    """
    with db_conn() as db:
        rows = db.execute(sql, (*params, limit)).fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


@router.post("/notification-queue/{queue_id}/resend")
def notification_resend(request: Request, queue_id: int):
    user = get_current_user(request)
    _require_admin(user)

    with db_conn() as db:
        row = db.execute(
            "SELECT id, payload_json FROM notification_queue WHERE id=?",
            (queue_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="queue item not found")

        payload = row["payload_json"]
        try:
            payload_dict = json.loads(payload)
        except Exception:
            raise HTTPException(status_code=400, detail="invalid payload_json")

        ok, err = send_kakao_payload(payload_dict)
        if ok:
            db.execute(
                "UPDATE notification_queue SET status='SENT', sent_at=datetime('now'), error=NULL WHERE id=?",
                (queue_id,),
            )
        else:
            db.execute(
                "UPDATE notification_queue SET status='ERROR', error=? WHERE id=?",
                (err, queue_id),
            )
        db.commit()

    return {"ok": ok, "error": err}
