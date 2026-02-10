import os
import re
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.db import db_conn
from app.parking_token import create_parking_token, token_ttl_seconds, verify_parking_token

templates = Jinja2Templates(directory="templates")
router = APIRouter(tags=["parking"])

BASE_DIR = Path(__file__).resolve().parents[2]
STATIC_DIR = BASE_DIR / "static"

_PLATE_KEEP_RE = re.compile(r"[0-9A-Za-z가-힣]")
_HOST_PORT_RE = re.compile(r"^[A-Za-z0-9.-]+(?::\d{1,5})?$")


def _normalize_base_url(raw: str) -> str:
    value = (raw or "").strip().rstrip("/")
    if not value:
        return ""

    candidate = value if "://" in value else f"https://{value}"
    try:
        parsed = urlparse(candidate)
    except Exception:
        return ""

    scheme = (parsed.scheme or "").lower()
    host_port = (parsed.netloc or "").strip()
    if scheme not in ("http", "https"):
        return ""
    if not host_port or not _HOST_PORT_RE.fullmatch(host_port):
        return ""
    host = host_port.split(":", 1)[0]
    if "_" in host:
        return ""
    return f"{scheme}://{host_port}"


def _is_local_host(host_port: str) -> bool:
    host = (host_port or "").split(":", 1)[0].strip().lower()
    return host in ("127.0.0.1", "0.0.0.0", "localhost", "[::1]", "::1")


def _parking_base_url(request: Request) -> str:
    for env_name in ("KA_PART_PUBLIC_BASE_URL", "PARKING_PUBLIC_BASE_URL"):
        env_url = _normalize_base_url(os.getenv(env_name, ""))
        if env_url:
            return env_url

    host = (request.headers.get("host") or "").strip()
    if host and not _is_local_host(host):
        scheme = request.headers.get("x-forwarded-proto") or request.url.scheme or "https"
        req_url = _normalize_base_url(f"{scheme}://{host}")
        if req_url:
            return req_url
    return "https://ka-part.com"


def _normalize_plate(raw: str) -> str:
    if not raw:
        return ""
    cleaned = "".join(_PLATE_KEEP_RE.findall(raw)).upper().strip()
    return cleaned


def _load_role_codes(db: sqlite3.Connection, user_id: int) -> list[str]:
    rows = db.execute(
        """
        SELECT r.code
        FROM user_roles ur
        JOIN roles r ON r.id = ur.role_id
        WHERE ur.user_id = ?
        ORDER BY r.code ASC
        """,
        (user_id,),
    ).fetchall()
    codes = [str(r["code"]).strip() for r in rows if r["code"]]
    return sorted(set(codes))


def _ensure_default_complex(db: sqlite3.Connection) -> dict[str, Any]:
    db.execute(
        """
        INSERT OR IGNORE INTO complexes(code, name, is_active, created_at, updated_at)
        VALUES('KA-DEFAULT', 'ka-part 아파트', 1, datetime('now'), datetime('now'))
        """
    )
    row = db.execute(
        """
        SELECT id, code, name
        FROM complexes
        WHERE is_active = 1
        ORDER BY CASE WHEN code='KA-DEFAULT' THEN 0 ELSE 1 END, id ASC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="complex setup error")

    db.execute(
        """
        INSERT OR IGNORE INTO user_complexes(user_id, complex_id, is_primary, created_at)
        SELECT u.id, ?, 1, datetime('now')
        FROM users u
        WHERE NOT EXISTS (
          SELECT 1
          FROM user_complexes uc
          WHERE uc.user_id = u.id
        )
        """,
        (row["id"],),
    )
    return {"id": row["id"], "code": row["code"], "name": row["name"]}


def _complex_for_user(db: sqlite3.Connection, user_id: int) -> dict[str, Any]:
    row = db.execute(
        """
        SELECT c.id, c.code, c.name
        FROM user_complexes uc
        JOIN complexes c ON c.id = uc.complex_id
        WHERE uc.user_id = ? AND c.is_active = 1
        ORDER BY uc.is_primary DESC, uc.created_at ASC, c.id ASC
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()
    if row:
        return {"id": row["id"], "code": row["code"], "name": row["name"]}

    fallback = _ensure_default_complex(db)
    db.execute(
        """
        INSERT OR IGNORE INTO user_complexes(user_id, complex_id, is_primary, created_at)
        VALUES(?, ?, 1, datetime('now'))
        """,
        (user_id, fallback["id"]),
    )
    return fallback


def _context_for_login(db: sqlite3.Connection, login: str) -> dict[str, Any]:
    u = db.execute(
        "SELECT id, login, name FROM users WHERE login = ?",
        ((login or "").strip(),),
    ).fetchone()
    if not u:
        raise HTTPException(status_code=401, detail=f"unknown user: {login}")

    _ensure_default_complex(db)
    cx = _complex_for_user(db, int(u["id"]))
    roles = _load_role_codes(db, int(u["id"]))
    return {
        "user": {
            "id": int(u["id"]),
            "login": u["login"],
            "name": u["name"],
            "roles": roles,
        },
        "complex": {
            "id": int(cx["id"]),
            "code": cx["code"],
            "name": cx["name"],
        },
    }


def _context_from_token(token: str) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = verify_parking_token((token or "").strip())
    context = {
        "user": {
            "id": int(payload.get("uid", 0)),
            "login": payload.get("login", ""),
            "name": payload.get("name", ""),
            "roles": list(payload.get("roles") or []),
        },
        "complex": {
            "id": int(payload.get("complex_id", 0)),
            "code": payload.get("complex_code", ""),
            "name": payload.get("complex_name", ""),
        },
        "expires_at": int(payload.get("exp") or 0),
    }
    if not context["user"]["id"] or not context["complex"]["id"]:
        raise HTTPException(status_code=401, detail="parking token missing claims")
    return payload, context


def _token_from_context(ctx: dict[str, Any]) -> str:
    return create_parking_token(
        {
            "uid": int(ctx["user"]["id"]),
            "login": ctx["user"]["login"],
            "name": ctx["user"]["name"],
            "roles": list(ctx["user"].get("roles") or []),
            "complex_id": int(ctx["complex"]["id"]),
            "complex_code": ctx["complex"]["code"],
            "complex_name": ctx["complex"]["name"],
        },
        ttl_seconds=token_ttl_seconds(),
    )


class ParkingCheckIn(BaseModel):
    token: str = Field(min_length=20)
    plate: str = Field(min_length=1, max_length=32)
    source: str = Field(default="MANUAL")


class IllegalVehicleCreateIn(BaseModel):
    token: str = Field(min_length=20)
    plate_number: str = Field(min_length=1, max_length=32)
    reason: str = Field(default="미등록/불법 주차 차량", max_length=120)
    memo: str | None = Field(default=None, max_length=400)


class IllegalVehicleClearIn(BaseModel):
    token: str = Field(min_length=20)


@router.get("/api/parking/launch-url")
def parking_launch_url(request: Request):
    user = get_current_user(request)
    with db_conn() as db:
        ctx = _context_for_login(db, str(user.get("login") or ""))

    token = _token_from_context(ctx)
    target = f"{_parking_base_url(request)}/parking/app?token={quote(token, safe='')}"
    return {
        "ok": True,
        "url": target,
        "token_ttl_seconds": token_ttl_seconds(),
        "context": ctx,
    }


@router.get("/api/parking/context")
def parking_context(token: str = Query(..., min_length=20)):
    _, ctx = _context_from_token(token)
    return {"ok": True, "context": ctx}


@router.post("/api/parking/check")
def parking_check(body: ParkingCheckIn):
    _, ctx = _context_from_token(body.token)
    normalized = _normalize_plate(body.plate)
    if len(normalized) < 7:
        verdict = "UNKNOWN"
        illegal_row = None
    else:
        verdict = "CLEAR"
        illegal_row = None

    source = (body.source or "MANUAL").strip().upper()
    if source not in ("MANUAL", "OCR"):
        source = "MANUAL"

    with db_conn() as db:
        if verdict != "UNKNOWN":
            illegal_row = db.execute(
                """
                SELECT piv.id, piv.plate_number, piv.plate_normalized, piv.reason, piv.memo,
                       piv.status, piv.updated_at, u.name AS reported_by_name
                FROM parking_illegal_vehicles piv
                LEFT JOIN users u ON u.id = piv.reported_by_user_id
                WHERE piv.complex_id = ? AND piv.plate_normalized = ? AND piv.status = 'ACTIVE'
                ORDER BY piv.updated_at DESC, piv.id DESC
                LIMIT 1
                """,
                (ctx["complex"]["id"], normalized),
            ).fetchone()
            if illegal_row:
                verdict = "ILLEGAL"

        db.execute(
            """
            INSERT INTO parking_scan_logs(
              complex_id, scanned_by_user_id, plate_input, plate_normalized, source,
              verdict, illegal_vehicle_id, scanned_at
            )
            VALUES(?,?,?,?,?,?,?,datetime('now'))
            """,
            (
                ctx["complex"]["id"],
                ctx["user"]["id"],
                body.plate,
                normalized,
                source,
                verdict,
                illegal_row["id"] if illegal_row else None,
            ),
        )
        scan_id = int(db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    return {
        "ok": True,
        "scan_id": scan_id,
        "verdict": verdict,
        "plate_input": body.plate,
        "plate_normalized": normalized,
        "illegal_vehicle": dict(illegal_row) if illegal_row else None,
    }


@router.get("/api/parking/illegal-vehicles")
def illegal_vehicle_list(
    token: str = Query(..., min_length=20),
    status: str = Query("ACTIVE", min_length=3, max_length=12),
    q: str = "",
    limit: int = 100,
):
    _, ctx = _context_from_token(token)
    status = (status or "ACTIVE").strip().upper()
    if status not in ("ACTIVE", "CLEARED", "ALL"):
        status = "ACTIVE"

    limit = max(1, min(int(limit), 200))
    qv = _normalize_plate(q or "")

    clauses = ["piv.complex_id = ?"]
    params: list[Any] = [ctx["complex"]["id"]]
    if status != "ALL":
        clauses.append("piv.status = ?")
        params.append(status)
    if qv:
        clauses.append("piv.plate_normalized LIKE ?")
        params.append(f"%{qv}%")

    where_sql = " AND ".join(clauses)
    sql = f"""
        SELECT piv.id, piv.plate_number, piv.plate_normalized, piv.reason, piv.memo,
               piv.status, piv.created_at, piv.updated_at, piv.cleared_at,
               ru.name AS reported_by_name, cu.name AS cleared_by_name
        FROM parking_illegal_vehicles piv
        LEFT JOIN users ru ON ru.id = piv.reported_by_user_id
        LEFT JOIN users cu ON cu.id = piv.cleared_by_user_id
        WHERE {where_sql}
        ORDER BY piv.updated_at DESC, piv.id DESC
        LIMIT ?
    """
    params.append(limit)

    with db_conn() as db:
        rows = db.execute(sql, tuple(params)).fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


@router.post("/api/parking/illegal-vehicles")
def illegal_vehicle_create(body: IllegalVehicleCreateIn):
    _, ctx = _context_from_token(body.token)
    plate_normalized = _normalize_plate(body.plate_number)
    if len(plate_normalized) < 7:
        raise HTTPException(status_code=400, detail="plate_number is too short")

    reason = (body.reason or "").strip() or "미등록/불법 주차 차량"

    with db_conn() as db:
        existing = db.execute(
            """
            SELECT id
            FROM parking_illegal_vehicles
            WHERE complex_id = ? AND plate_normalized = ? AND status = 'ACTIVE'
            ORDER BY id DESC
            LIMIT 1
            """,
            (ctx["complex"]["id"], plate_normalized),
        ).fetchone()
        if existing:
            db.execute(
                """
                UPDATE parking_illegal_vehicles
                SET plate_number = ?, reason = ?, memo = ?, reported_by_user_id = ?,
                    updated_at = datetime('now')
                WHERE id = ?
                """,
                (
                    body.plate_number.strip(),
                    reason,
                    body.memo,
                    ctx["user"]["id"],
                    existing["id"],
                ),
            )
            item_id = int(existing["id"])
        else:
            db.execute(
                """
                INSERT INTO parking_illegal_vehicles(
                  complex_id, plate_number, plate_normalized, reason, memo,
                  status, reported_by_user_id, created_at, updated_at
                )
                VALUES(?,?,?,?,?,'ACTIVE',?,datetime('now'),datetime('now'))
                """,
                (
                    ctx["complex"]["id"],
                    body.plate_number.strip(),
                    plate_normalized,
                    reason,
                    body.memo,
                    ctx["user"]["id"],
                ),
            )
            item_id = int(db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

        row = db.execute(
            """
            SELECT id, plate_number, plate_normalized, reason, memo, status,
                   created_at, updated_at
            FROM parking_illegal_vehicles
            WHERE id = ?
            """,
            (item_id,),
        ).fetchone()
    return {"ok": True, "item": dict(row) if row else None}


@router.post("/api/parking/illegal-vehicles/{vehicle_id}/clear")
def illegal_vehicle_clear(vehicle_id: int, body: IllegalVehicleClearIn):
    _, ctx = _context_from_token(body.token)
    with db_conn() as db:
        row = db.execute(
            """
            SELECT id
            FROM parking_illegal_vehicles
            WHERE id = ? AND complex_id = ? AND status = 'ACTIVE'
            """,
            (vehicle_id, ctx["complex"]["id"]),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="active illegal vehicle not found")

        db.execute(
            """
            UPDATE parking_illegal_vehicles
            SET status = 'CLEARED',
                cleared_at = datetime('now'),
                cleared_by_user_id = ?,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (ctx["user"]["id"], vehicle_id),
        )
    return {"ok": True, "id": vehicle_id}


@router.get("/api/parking/scans/recent")
def parking_recent_scans(token: str = Query(..., min_length=20), limit: int = 20):
    _, ctx = _context_from_token(token)
    limit = max(1, min(int(limit), 100))
    with db_conn() as db:
        rows = db.execute(
            """
            SELECT psl.id, psl.plate_input, psl.plate_normalized, psl.source, psl.verdict, psl.scanned_at,
                   piv.reason AS illegal_reason
            FROM parking_scan_logs psl
            LEFT JOIN parking_illegal_vehicles piv ON piv.id = psl.illegal_vehicle_id
            WHERE psl.complex_id = ?
            ORDER BY psl.id DESC
            LIMIT ?
            """,
            (ctx["complex"]["id"], limit),
        ).fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}


@router.get("/parking/app", response_class=HTMLResponse)
def parking_app(request: Request):
    q_token = (request.query_params.get("token") or "").strip()
    login = (request.query_params.get("login") or "").strip()

    token = q_token
    context: dict[str, Any] | None = None
    bootstrap_error = ""

    if token:
        try:
            _, context = _context_from_token(token)
        except HTTPException as e:
            bootstrap_error = str(e.detail)
    elif login:
        with db_conn() as db:
            ctx = _context_for_login(db, login)
        token = _token_from_context(ctx)
        _, context = _context_from_token(token)

    return templates.TemplateResponse(
        "parking/app.html",
        {
            "request": request,
            "title": "주차관리 PWA",
            "parking_token": token,
            "parking_context": context,
            "bootstrap_error": bootstrap_error,
            "token_ttl_seconds": token_ttl_seconds(),
        },
    )


@router.get("/parking/sw.js", include_in_schema=False)
def parking_sw():
    path = STATIC_DIR / "parking_sw.js"
    if not path.exists():
        raise HTTPException(status_code=404, detail="service worker not found")
    return FileResponse(str(path), media_type="application/javascript")


@router.get("/parking/manifest.webmanifest", include_in_schema=False)
def parking_manifest():
    path = STATIC_DIR / "parking_manifest.webmanifest"
    if not path.exists():
        raise HTTPException(status_code=404, detail="manifest not found")
    return FileResponse(str(path), media_type="application/manifest+json")
