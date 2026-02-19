from __future__ import annotations

import os
import re
import secrets
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

from ..db import _connect, get_auth_user_by_token, now_iso

router = APIRouter()

AUTH_COOKIE_NAME = (os.getenv("KA_AUTH_COOKIE_NAME") or "ka_part_auth_token").strip()
ALLOW_QUERY_ACCESS_TOKEN = (os.getenv("KA_ALLOW_QUERY_ACCESS_TOKEN") or "").strip().lower() in {"1", "true", "yes", "on"}
SECURITY_ROLE_KEYWORDS = ("보안", "경비")
PUBLIC_ACCESS_LOGIN_ID = ((os.getenv("KA_PUBLIC_FULL_ACCESS_LOGIN_ID") or "public_guest").strip().lower() or "public_guest")

ROOT_DIR = Path(__file__).resolve().parents[2]
PDF_FONT_PATH = ROOT_DIR / "fonts" / "NotoSansKR-Regular.ttf"
PDF_FONT_NAME = "NotoSansKR"
_PDF_FONT_READY = False


class IncidentCreatePayload(BaseModel):
    site_code: str = ""
    site_name: str = ""
    location: str = Field(default="", max_length=120)
    title: str = Field(default="누설전류 점검", min_length=2, max_length=160)
    insulation_mohm: float = Field(..., ge=0.0)
    ground_ohm: float = Field(..., ge=0.0)
    leakage_ma: float = Field(..., ge=0.0)
    note: str = Field(default="", max_length=1200)


class NotificationAckPayload(BaseModel):
    token: str = ""


class EscalationRunPayload(BaseModel):
    site_code: str = ""
    limit: int = Field(default=100, ge=1, le=1000)


class RulesUpdatePayload(BaseModel):
    caution_leakage_ma: float | None = Field(default=None, ge=0.0)
    danger_leakage_ma: float | None = Field(default=None, ge=0.0)
    caution_insulation_mohm: float | None = Field(default=None, ge=0.0)
    danger_insulation_mohm: float | None = Field(default=None, ge=0.0)
    caution_ground_ohm: float | None = Field(default=None, ge=0.0)
    danger_ground_ohm: float | None = Field(default=None, ge=0.0)
    ack_timeout_minutes: int | None = Field(default=None, ge=5, le=1440)
    trend_lookback_count: int | None = Field(default=None, ge=3, le=10)
    trend_prealert_enabled: bool | None = None


class DutyScheduleUpdatePayload(BaseModel):
    day_user_key: str = Field(..., min_length=1, max_length=80)
    night_user_key: str = Field(..., min_length=1, max_length=80)
    day_start_hhmm: str = Field(default="06:00", pattern=r"^\d{2}:\d{2}$")
    day_end_hhmm: str = Field(default="18:00", pattern=r"^\d{2}:\d{2}$")
    night_start_hhmm: str = Field(default="18:00", pattern=r"^\d{2}:\d{2}$")
    night_end_hhmm: str = Field(default="06:00", pattern=r"^\d{2}:\d{2}$")


def _now_ts() -> str:
    return now_iso()


def _clean_site_code(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    return re.sub(r"[\s-]+", "", raw)


def _clean_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return re.sub(r"[^a-z0-9_./:-]+", "_", raw)


def _is_security_role(value: Any) -> bool:
    role = str(value or "").strip()
    if not role:
        return False
    compact = role.replace(" ", "")
    if compact == "보안/경비":
        return True
    return any(token in role for token in SECURITY_ROLE_KEYWORDS)


def _is_resident_or_board_role(value: Any) -> bool:
    role = str(value or "").strip()
    return role in {"입주민", "주민", "세대주민", "입대의", "입주자대표", "입주자대표회의"}


def _is_public_access_user(user: Dict[str, Any]) -> bool:
    if bool(user.get("is_public_access")):
        return True
    login_id = str(user.get("login_id") or "").strip().lower()
    return bool(login_id) and login_id == PUBLIC_ACCESS_LOGIN_ID


def _is_admin(user: Dict[str, Any]) -> bool:
    return int(user.get("is_admin") or 0) == 1


def _is_site_admin(user: Dict[str, Any]) -> bool:
    return int(user.get("is_site_admin") or 0) == 1


def _extract_access_token(request: Request) -> str:
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            return token
    cookie_token = (request.cookies.get(AUTH_COOKIE_NAME) or "").strip()
    if cookie_token:
        return cookie_token
    if ALLOW_QUERY_ACCESS_TOKEN:
        token = (request.query_params.get("access_token") or "").strip()
        if token:
            return token
    raise HTTPException(status_code=401, detail="로그인이 필요합니다.")


def _require_auth(request: Request) -> Tuple[Dict[str, Any], str]:
    token = _extract_access_token(request)
    user = get_auth_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다. (세션 만료)")
    return user, token


def _scope_site_code(user: Dict[str, Any], requested_site_code: Any = "") -> str:
    requested = _clean_site_code(requested_site_code)
    user_site = _clean_site_code(user.get("site_code"))
    if _is_admin(user):
        if requested:
            return requested
        if user_site:
            return user_site
        raise HTTPException(status_code=400, detail="site_code가 필요합니다.")
    if not user_site:
        raise HTTPException(status_code=403, detail="계정에 site_code가 없습니다. 관리자에게 문의하세요.")
    if requested and requested != user_site:
        raise HTTPException(status_code=403, detail="소속 단지(site_code) 데이터만 접근할 수 있습니다.")
    return user_site


def _require_elec_user(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if _is_public_access_user(user):
        raise HTTPException(status_code=403, detail="로그인 없이 사용자는 전기AI 모듈을 사용할 수 없습니다. 신규가입 후 사용해 주세요.")
    if _is_security_role(user.get("role")):
        raise HTTPException(status_code=403, detail="보안/경비 계정은 주차관리 모듈만 사용할 수 있습니다.")
    if _is_resident_or_board_role(user.get("role")):
        raise HTTPException(status_code=403, detail="입주민/입대의 계정은 전기AI 모듈을 사용할 수 없습니다.")
    return user, token


def _require_manager_user(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_elec_user(request)
    if not (_is_admin(user) or _is_site_admin(user)):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return user, token


def _ensure_hhmm(raw: str, field: str) -> str:
    txt = str(raw or "").strip()
    if not re.fullmatch(r"\d{2}:\d{2}", txt):
        raise HTTPException(status_code=400, detail=f"{field} 형식은 HH:MM 입니다.")
    hh = int(txt[:2])
    mm = int(txt[3:])
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise HTTPException(status_code=400, detail=f"{field} 시간 값이 유효하지 않습니다.")
    return f"{hh:02d}:{mm:02d}"


def _hhmm_to_minutes(value: str) -> int:
    return int(value[:2]) * 60 + int(value[3:])


def _rows(rows: Any) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows or []]


def _event_level(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw not in {"ok", "prealert", "danger"}:
        raise HTTPException(status_code=400, detail="event_level must be one of: ok/prealert/danger")
    return raw


def _ensure_default_settings(con, site_code: str, actor_login: str = "system") -> None:
    ts = _now_ts()
    con.execute(
        """
        INSERT INTO elec_rules(
          site_code,caution_leakage_ma,danger_leakage_ma,caution_insulation_mohm,danger_insulation_mohm,
          caution_ground_ohm,danger_ground_ohm,ack_timeout_minutes,trend_lookback_count,trend_prealert_enabled,
          created_by,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(site_code) DO NOTHING
        """,
        (site_code, 15.0, 30.0, 1.0, 0.5, 15.0, 30.0, 30, 3, 1, actor_login, ts, ts),
    )
    for event_level, key in [("prealert", "duty"), ("danger", "electric_mgr"), ("danger", "chief")]:
        con.execute(
            """
            INSERT INTO elec_notify_routes(site_code,event_level,recipient_key,channel,is_active,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(site_code,event_level,recipient_key,channel) DO NOTHING
            """,
            (site_code, event_level, key, "kakao", 1, ts, ts),
        )
    for shift, s, e, key in [("DAY", "06:00", "18:00", "duty_day"), ("NIGHT", "18:00", "06:00", "duty_night")]:
        con.execute(
            """
            INSERT INTO elec_duty_schedule(site_code,shift_code,start_hhmm,end_hhmm,user_key,is_active,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(site_code,shift_code) DO NOTHING
            """,
            (site_code, shift, s, e, key, 1, ts, ts),
        )
    for event_level, source_key, target_key, delay in [("prealert", "duty", "electric_mgr", 30), ("danger", "electric_mgr", "chief", 15)]:
        con.execute(
            """
            INSERT INTO elec_escalation_routes(site_code,event_level,source_recipient_key,target_recipient_key,delay_minutes,is_active,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(site_code,event_level,source_recipient_key,target_recipient_key) DO NOTHING
            """,
            (site_code, event_level, source_key, target_key, delay, 1, ts, ts),
        )


def _get_rules(con, site_code: str) -> Dict[str, Any]:
    row = con.execute("SELECT * FROM elec_rules WHERE site_code=? LIMIT 1", (site_code,)).fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="전기AI 규칙을 불러오지 못했습니다.")
    return dict(row)


def _list_settings(con, site_code: str) -> Dict[str, Any]:
    return {
        "rules": _get_rules(con, site_code),
        "notify_routes": _rows(con.execute("SELECT * FROM elec_notify_routes WHERE site_code=? ORDER BY event_level,id", (site_code,)).fetchall()),
        "duty_schedule": _rows(con.execute("SELECT * FROM elec_duty_schedule WHERE site_code=? ORDER BY shift_code,id", (site_code,)).fetchall()),
        "escalation_routes": _rows(con.execute("SELECT * FROM elec_escalation_routes WHERE site_code=? ORDER BY event_level,delay_minutes,id", (site_code,)).fetchall()),
    }


def _resolve_duty_user_key(con, site_code: str, now_dt: datetime | None = None) -> str:
    rows = con.execute(
        "SELECT start_hhmm,end_hhmm,user_key FROM elec_duty_schedule WHERE site_code=? AND is_active=1 ORDER BY shift_code,id",
        (site_code,),
    ).fetchall()
    if not rows:
        return ""
    dt = now_dt or datetime.now()
    now_min = dt.hour * 60 + dt.minute
    for row in rows:
        try:
            start = _hhmm_to_minutes(_ensure_hhmm(str(row["start_hhmm"] or "00:00"), "start_hhmm"))
            end = _hhmm_to_minutes(_ensure_hhmm(str(row["end_hhmm"] or "00:00"), "end_hhmm"))
        except HTTPException:
            continue
        if start == end:
            return str(row["user_key"] or "").strip()
        if start < end and start <= now_min < end:
            return str(row["user_key"] or "").strip()
        if start > end and (now_min >= start or now_min < end):
            return str(row["user_key"] or "").strip()
    return str(rows[0]["user_key"] or "").strip()


def _risk_eval(insulation_mohm: float, ground_ohm: float, leakage_ma: float, rules: Dict[str, Any]) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    danger = False
    caution = False
    if leakage_ma >= float(rules["danger_leakage_ma"]):
        danger = True
        reasons.append("누설전류 위험")
    elif leakage_ma >= float(rules["caution_leakage_ma"]):
        caution = True
        reasons.append("누설전류 주의")
    if insulation_mohm <= float(rules["danger_insulation_mohm"]):
        danger = True
        reasons.append("절연저항 위험")
    elif insulation_mohm <= float(rules["caution_insulation_mohm"]):
        caution = True
        reasons.append("절연저항 주의")
    if ground_ohm >= float(rules["danger_ground_ohm"]):
        danger = True
        reasons.append("접지저항 위험")
    elif ground_ohm >= float(rules["caution_ground_ohm"]):
        caution = True
        reasons.append("접지저항 주의")
    if danger:
        return "danger", reasons
    if caution:
        return "caution", reasons
    return "ok", ["정상 범위"]


def _trend_eval(con, site_code: str, location: str, rules: Dict[str, Any], insulation_mohm: float, ground_ohm: float, leakage_ma: float) -> Tuple[str, bool, List[str]]:
    lookback = max(3, int(rules.get("trend_lookback_count") or 3))
    sql = "SELECT insulation_mohm,ground_ohm,leakage_ma FROM elec_incidents WHERE site_code=?"
    args: List[Any] = [site_code]
    if str(location or "").strip():
        sql += " AND COALESCE(location,'')=?"
        args.append(str(location or "").strip())
    sql += " ORDER BY id DESC LIMIT ?"
    args.append(max(2, lookback - 1))
    hist = list(reversed(_rows(con.execute(sql, tuple(args)).fetchall())))
    hist.append({"insulation_mohm": insulation_mohm, "ground_ohm": ground_ohm, "leakage_ma": leakage_ma})
    if len(hist) < 3:
        return "stable", False, ["추세 데이터 부족(최소 3회)"]
    last = hist[-3:]
    reason: List[str] = []
    l = [float(x["leakage_ma"]) for x in last]
    i = [float(x["insulation_mohm"]) for x in last]
    g = [float(x["ground_ohm"]) for x in last]
    if l[0] < l[1] < l[2]:
        reason.append("최근 3회 누설전류 연속 상승")
    if i[0] > i[1] > i[2]:
        reason.append("최근 3회 절연저항 연속 하락")
    if g[0] < g[1] < g[2]:
        reason.append("최근 3회 접지저항 연속 상승")
    worsening = bool(reason)
    prealert = worsening and int(rules.get("trend_prealert_enabled") or 0) == 1
    return ("worsening" if worsening else "stable"), prealert, (reason or ["악화 추세 없음"])


def _event_from(risk_level: str, prealert: bool) -> str:
    if risk_level == "danger":
        return "danger"
    if risk_level == "caution" or prealert:
        return "prealert"
    return "ok"


def _notification_public(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    nid = int(out.get("id") or 0)
    token = str(out.get("ack_token") or "").strip()
    out["ack_url"] = f"/api/elec/notifications/{nid}/ack?token={token}" if nid > 0 and token else ""
    return out


def _create_notifications(con, incident_id: int, site_code: str, event_level: str, ack_timeout_minutes: int) -> List[Dict[str, Any]]:
    if event_level not in {"prealert", "danger"}:
        return []
    now_dt = datetime.now()
    ts = _now_ts()
    due_at = (now_dt + timedelta(minutes=max(1, int(ack_timeout_minutes)))).replace(microsecond=0).isoformat(sep=" ")
    duty = _clean_key(_resolve_duty_user_key(con, site_code, now_dt)) or "duty_unassigned"
    rows = con.execute(
        "SELECT * FROM elec_notify_routes WHERE site_code=? AND event_level=? AND is_active=1 ORDER BY id",
        (site_code, event_level),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for route in rows:
        raw_key = _clean_key(route["recipient_key"])
        resolved = duty if raw_key == "duty" else raw_key
        token = secrets.token_urlsafe(24)
        con.execute(
            """
            INSERT INTO elec_notifications(
              incident_id,site_code,event_level,route_recipient_key,recipient_key,channel,status,ack_token,sent_at,ack_due_at,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (incident_id, site_code, event_level, raw_key, resolved, str(route["channel"] or "kakao"), "sent", token, ts, due_at, ts, ts),
        )
        nid = int(con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        row = con.execute("SELECT * FROM elec_notifications WHERE id=? LIMIT 1", (nid,)).fetchone()
        if row:
            out.append(_notification_public(dict(row)))
    return out


def _run_escalation(con, site_code: str, limit: int) -> Dict[str, Any]:
    ts = _now_ts()
    now_dt = datetime.now()
    due = con.execute(
        """
        SELECT * FROM elec_notifications
        WHERE site_code=? AND status='sent' AND acked_at IS NULL AND ack_due_at IS NOT NULL AND ack_due_at<=?
        ORDER BY ack_due_at,id LIMIT ?
        """,
        (site_code, ts, max(1, min(1000, int(limit)))),
    ).fetchall()
    escalated = 0
    for raw in due:
        row = dict(raw)
        source = _clean_key(row.get("route_recipient_key"))
        event_level = _event_level(row.get("event_level"))
        er = con.execute(
            """
            SELECT * FROM elec_escalation_routes
            WHERE site_code=? AND event_level=? AND is_active=1 AND (source_recipient_key=? OR source_recipient_key='*')
            ORDER BY CASE WHEN source_recipient_key=? THEN 0 ELSE 1 END, delay_minutes,id
            LIMIT 1
            """,
            (site_code, event_level, source, source),
        ).fetchone()
        if not er:
            continue
        route = dict(er)
        target_raw = _clean_key(route.get("target_recipient_key"))
        target = (_clean_key(_resolve_duty_user_key(con, site_code, now_dt)) or "duty_unassigned") if target_raw == "duty" else target_raw
        delay = max(1, int(route.get("delay_minutes") or 30))
        due_at = (now_dt + timedelta(minutes=delay)).replace(microsecond=0).isoformat(sep=" ")
        token = secrets.token_urlsafe(24)
        con.execute(
            """
            INSERT INTO elec_notifications(
              incident_id,site_code,event_level,route_recipient_key,recipient_key,channel,status,ack_token,sent_at,ack_due_at,escalated_from_notification_id,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (int(row["incident_id"]), site_code, event_level, target_raw, target, str(route.get("channel") or row.get("channel") or "kakao"), "sent", token, ts, due_at, int(row["id"]), ts, ts),
        )
        con.execute(
            "UPDATE elec_notifications SET status='escalated', escalation_count=COALESCE(escalation_count,0)+1, last_escalated_at=?, updated_at=? WHERE id=?",
            (ts, ts, int(row["id"])),
        )
        escalated += 1
    return {"checked": len(due), "escalated": escalated}


def _ensure_pdf_font() -> None:
    global _PDF_FONT_READY
    if _PDF_FONT_READY:
        return
    if PDF_FONT_PATH.exists():
        pdfmetrics.registerFont(TTFont(PDF_FONT_NAME, str(PDF_FONT_PATH)))
        _PDF_FONT_READY = True


def _pdf_font_name() -> str:
    return PDF_FONT_NAME if _PDF_FONT_READY else "Helvetica"


def _pdf_incident(incident: Dict[str, Any], notifications: List[Dict[str, Any]], ack_logs: List[Dict[str, Any]]) -> bytes:
    _ensure_pdf_font()
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    x = 36
    y = height - 40
    line_h = 14
    c.setFont(_pdf_font_name(), 15)
    c.drawString(x, y, f"전기AI 사건 보고서 #{int(incident.get('id') or 0)}")
    y -= 22
    c.setFont(_pdf_font_name(), 10)
    lines = [
        f"단지코드: {incident.get('site_code') or '-'}",
        f"단지명: {incident.get('site_name') or '-'}",
        f"위치: {incident.get('location') or '-'}",
        f"제목: {incident.get('title') or '-'}",
        f"판정: {incident.get('risk_level') or '-'} / 이벤트: {incident.get('event_level') or '-'} / 추세: {incident.get('trend_state') or '-'}",
        f"절연저항(MOhm): {float(incident.get('insulation_mohm') or 0.0):.3f}",
        f"접지저항(Ohm): {float(incident.get('ground_ohm') or 0.0):.2f}",
        f"누설전류(mA): {float(incident.get('leakage_ma') or 0.0):.2f}",
        f"위험근거: {incident.get('risk_reason') or '-'}",
        f"추세근거: {incident.get('trend_reason') or '-'}",
        f"기록자: {incident.get('reported_by_name') or incident.get('reported_by_login') or '-'}",
        f"기록시각: {incident.get('created_at') or '-'}",
        "",
        f"알림이력: {len(notifications)}건",
    ]
    for n in notifications:
        lines.append(f"- #{int(n.get('id') or 0)} {n.get('event_level')} {n.get('recipient_key')} {n.get('status')} due={n.get('ack_due_at')}")
    lines.append("")
    lines.append(f"ACK로그: {len(ack_logs)}건")
    for a in ack_logs:
        lines.append(f"- #{int(a.get('id') or 0)} notif={int(a.get('notification_id') or 0)} by={a.get('acked_by_name') or a.get('acked_by_login') or '-'} at={a.get('created_at')}")
    for line in lines:
        if y < 45:
            c.showPage()
            y = height - 40
            c.setFont(_pdf_font_name(), 10)
        c.drawString(x, y, str(line))
        y -= line_h
    c.save()
    return buf.getvalue()


@router.get("/elec/bootstrap")
def elec_bootstrap(request: Request, site_code: str = Query(default=""), limit: int = Query(default=30, ge=1, le=200)):
    user, _token = _require_elec_user(request)
    scoped = _scope_site_code(user, site_code)
    con = _connect()
    try:
        _ensure_default_settings(con, scoped, actor_login=str(user.get("login_id") or "system"))
        settings = _list_settings(con, scoped)
        incidents = _rows(con.execute("SELECT * FROM elec_incidents WHERE site_code=? ORDER BY id DESC LIMIT ?", (scoped, limit)).fetchall())
        notifications = _rows(
            con.execute(
                "SELECT * FROM elec_notifications WHERE site_code=? AND status='sent' ORDER BY ack_due_at,id LIMIT ?",
                (scoped, limit),
            ).fetchall()
        )
        con.commit()
        return {
            "ok": True,
            "site_code": scoped,
            "site_name": str(user.get("site_name") or "").strip(),
            "active_duty_user_key": _resolve_duty_user_key(con, scoped),
            "user": {
                "id": int(user.get("id") or 0),
                "login_id": user.get("login_id"),
                "name": user.get("name"),
                "role": user.get("role"),
                "is_admin": bool(user.get("is_admin")),
                "is_site_admin": bool(user.get("is_site_admin")),
            },
            "settings": settings,
            "incidents": incidents,
            "pending_notifications": [_notification_public(r) for r in notifications],
        }
    finally:
        con.close()


@router.put("/elec/settings/rules")
def elec_update_rules(request: Request, payload: RulesUpdatePayload = Body(...), site_code: str = Query(default="")):
    user, _token = _require_manager_user(request)
    scoped = _scope_site_code(user, site_code)
    con = _connect()
    try:
        _ensure_default_settings(con, scoped, actor_login=str(user.get("login_id") or "system"))
        cur = _get_rules(con, scoped)
        caution_leakage = float(payload.caution_leakage_ma if payload.caution_leakage_ma is not None else cur["caution_leakage_ma"])
        danger_leakage = float(payload.danger_leakage_ma if payload.danger_leakage_ma is not None else cur["danger_leakage_ma"])
        caution_insulation = float(payload.caution_insulation_mohm if payload.caution_insulation_mohm is not None else cur["caution_insulation_mohm"])
        danger_insulation = float(payload.danger_insulation_mohm if payload.danger_insulation_mohm is not None else cur["danger_insulation_mohm"])
        caution_ground = float(payload.caution_ground_ohm if payload.caution_ground_ohm is not None else cur["caution_ground_ohm"])
        danger_ground = float(payload.danger_ground_ohm if payload.danger_ground_ohm is not None else cur["danger_ground_ohm"])
        ack_timeout = int(payload.ack_timeout_minutes if payload.ack_timeout_minutes is not None else cur["ack_timeout_minutes"])
        trend_lookback = int(payload.trend_lookback_count if payload.trend_lookback_count is not None else cur["trend_lookback_count"])
        trend_enabled = int(payload.trend_prealert_enabled) if payload.trend_prealert_enabled is not None else int(cur["trend_prealert_enabled"])
        if danger_leakage < caution_leakage:
            raise HTTPException(status_code=400, detail="danger_leakage_ma must be >= caution_leakage_ma")
        if danger_insulation > caution_insulation:
            raise HTTPException(status_code=400, detail="danger_insulation_mohm must be <= caution_insulation_mohm")
        if danger_ground < caution_ground:
            raise HTTPException(status_code=400, detail="danger_ground_ohm must be >= caution_ground_ohm")
        ts = _now_ts()
        con.execute(
            """
            UPDATE elec_rules
            SET caution_leakage_ma=?,danger_leakage_ma=?,caution_insulation_mohm=?,danger_insulation_mohm=?,
                caution_ground_ohm=?,danger_ground_ohm=?,ack_timeout_minutes=?,trend_lookback_count=?,trend_prealert_enabled=?,updated_at=?
            WHERE site_code=?
            """,
            (caution_leakage, danger_leakage, caution_insulation, danger_insulation, caution_ground, danger_ground, ack_timeout, trend_lookback, trend_enabled, ts, scoped),
        )
        out = _get_rules(con, scoped)
        con.commit()
        return {"ok": True, "rules": out}
    finally:
        con.close()


@router.put("/elec/settings/duty")
def elec_update_duty_schedule(request: Request, payload: DutyScheduleUpdatePayload = Body(...), site_code: str = Query(default="")):
    user, _token = _require_manager_user(request)
    scoped = _scope_site_code(user, site_code)
    day_key = _clean_key(payload.day_user_key)
    night_key = _clean_key(payload.night_user_key)
    if not day_key or not night_key:
        raise HTTPException(status_code=400, detail="day_user_key/night_user_key는 필수입니다.")
    day_start = _ensure_hhmm(payload.day_start_hhmm, "day_start_hhmm")
    day_end = _ensure_hhmm(payload.day_end_hhmm, "day_end_hhmm")
    night_start = _ensure_hhmm(payload.night_start_hhmm, "night_start_hhmm")
    night_end = _ensure_hhmm(payload.night_end_hhmm, "night_end_hhmm")
    con = _connect()
    try:
        _ensure_default_settings(con, scoped, actor_login=str(user.get("login_id") or "system"))
        ts = _now_ts()
        for shift, s, e, key in [("DAY", day_start, day_end, day_key), ("NIGHT", night_start, night_end, night_key)]:
            con.execute(
                """
                INSERT INTO elec_duty_schedule(site_code,shift_code,start_hhmm,end_hhmm,user_key,is_active,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(site_code,shift_code) DO UPDATE SET
                    start_hhmm=excluded.start_hhmm,
                    end_hhmm=excluded.end_hhmm,
                    user_key=excluded.user_key,
                    is_active=excluded.is_active,
                    updated_at=excluded.updated_at
                """,
                (scoped, shift, s, e, key, 1, ts, ts),
            )
        rows = _rows(con.execute("SELECT * FROM elec_duty_schedule WHERE site_code=? ORDER BY shift_code,id", (scoped,)).fetchall())
        con.commit()
        return {"ok": True, "duty_schedule": rows, "active_duty_user_key": _resolve_duty_user_key(con, scoped)}
    finally:
        con.close()


@router.get("/elec/incidents")
def elec_incidents_list(
    request: Request,
    site_code: str = Query(default=""),
    risk_level: str = Query(default=""),
    event_level: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
):
    user, _token = _require_elec_user(request)
    scoped = _scope_site_code(user, site_code)
    sql = "SELECT * FROM elec_incidents WHERE site_code=?"
    args: List[Any] = [scoped]
    risk = str(risk_level or "").strip().lower()
    if risk:
        if risk not in {"ok", "caution", "danger"}:
            raise HTTPException(status_code=400, detail="risk_level must be one of: ok/caution/danger")
        sql += " AND risk_level=?"
        args.append(risk)
    event = str(event_level or "").strip().lower()
    if event:
        event = _event_level(event)
        sql += " AND event_level=?"
        args.append(event)
    sql += " ORDER BY id DESC LIMIT ?"
    args.append(max(1, min(500, int(limit))))
    con = _connect()
    try:
        items = _rows(con.execute(sql, tuple(args)).fetchall())
        return {"ok": True, "site_code": scoped, "items": items}
    finally:
        con.close()


@router.post("/elec/incidents")
def elec_incident_create(request: Request, payload: IncidentCreatePayload = Body(...)):
    user, _token = _require_elec_user(request)
    scoped = _scope_site_code(user, payload.site_code)
    con = _connect()
    try:
        _ensure_default_settings(con, scoped, actor_login=str(user.get("login_id") or "system"))
        rules = _get_rules(con, scoped)
        risk_level, risk_reasons = _risk_eval(payload.insulation_mohm, payload.ground_ohm, payload.leakage_ma, rules)
        trend_state, trend_prealert, trend_reasons = _trend_eval(
            con, scoped, str(payload.location or "").strip(), rules, payload.insulation_mohm, payload.ground_ohm, payload.leakage_ma
        )
        event_level = _event_from(risk_level, trend_prealert)
        ts = _now_ts()
        con.execute(
            """
            INSERT INTO elec_incidents(
              site_code,site_name,location,title,insulation_mohm,ground_ohm,leakage_ma,
              risk_level,trend_state,event_level,risk_reason,trend_reason,note,
              reported_by_user_id,reported_by_login,reported_by_name,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                scoped,
                str(payload.site_name or user.get("site_name") or "").strip(),
                str(payload.location or "").strip(),
                str(payload.title or "").strip(),
                float(payload.insulation_mohm),
                float(payload.ground_ohm),
                float(payload.leakage_ma),
                risk_level,
                trend_state,
                event_level,
                "; ".join(risk_reasons),
                "; ".join(trend_reasons),
                str(payload.note or "").strip(),
                int(user.get("id") or 0) or None,
                str(user.get("login_id") or "").strip().lower(),
                str(user.get("name") or "").strip(),
                ts,
                ts,
            ),
        )
        iid = int(con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        incident = dict(con.execute("SELECT * FROM elec_incidents WHERE id=? LIMIT 1", (iid,)).fetchone())
        notifications = _create_notifications(con, iid, scoped, event_level, int(rules.get("ack_timeout_minutes") or 30))
        con.commit()
        return {
            "ok": True,
            "incident": incident,
            "rules": rules,
            "risk_reasons": risk_reasons,
            "trend_reasons": trend_reasons,
            "notifications": notifications,
        }
    finally:
        con.close()


@router.post("/elec/notifications/{notification_id}/ack")
def elec_notification_ack(
    notification_id: int,
    request: Request,
    payload: NotificationAckPayload = Body(default={"token": ""}),
    token: str = Query(default=""),
):
    user, _token = _require_elec_user(request)
    scoped = _scope_site_code(user, request.query_params.get("site_code") or "")
    provided = str(token or payload.token or "").strip()
    con = _connect()
    try:
        row = con.execute("SELECT * FROM elec_notifications WHERE id=? AND site_code=? LIMIT 1", (int(notification_id), scoped)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="notification not found")
        cur = dict(row)
        if cur.get("status") == "acked" and cur.get("acked_at"):
            return {"ok": True, "already_acked": True, "notification": _notification_public(cur)}
        row_token = str(cur.get("ack_token") or "").strip()
        if provided and row_token and provided != row_token:
            raise HTTPException(status_code=403, detail="ack token mismatch")
        ts = _now_ts()
        con.execute(
            """
            UPDATE elec_notifications
            SET status='acked',acked_at=?,acked_by_user_id=?,acked_by_login=?,acked_by_name=?,updated_at=?
            WHERE id=?
            """,
            (
                ts,
                int(user.get("id") or 0) or None,
                str(user.get("login_id") or "").strip().lower(),
                str(user.get("name") or "").strip(),
                ts,
                int(notification_id),
            ),
        )
        con.execute(
            """
            INSERT INTO elec_notify_ack(notification_id,incident_id,site_code,ack_token,acked_by_user_id,acked_by_login,acked_by_name,via,created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                int(notification_id),
                int(cur.get("incident_id") or 0),
                scoped,
                row_token,
                int(user.get("id") or 0) or None,
                str(user.get("login_id") or "").strip().lower(),
                str(user.get("name") or "").strip(),
                "api",
                ts,
            ),
        )
        out = dict(con.execute("SELECT * FROM elec_notifications WHERE id=? LIMIT 1", (int(notification_id),)).fetchone())
        con.commit()
        return {"ok": True, "notification": _notification_public(out)}
    finally:
        con.close()


@router.post("/elec/escalations/run")
def elec_escalation_run(request: Request, payload: EscalationRunPayload = Body(default={"limit": 100})):
    user, _token = _require_manager_user(request)
    scoped = _scope_site_code(user, payload.site_code)
    con = _connect()
    try:
        _ensure_default_settings(con, scoped, actor_login=str(user.get("login_id") or "system"))
        result = _run_escalation(con, scoped, int(payload.limit))
        pending = _rows(
            con.execute(
                "SELECT * FROM elec_notifications WHERE site_code=? AND status='sent' ORDER BY ack_due_at,id LIMIT 100",
                (scoped,),
            ).fetchall()
        )
        con.commit()
        return {"ok": True, "site_code": scoped, **result, "pending_notifications": [_notification_public(r) for r in pending]}
    finally:
        con.close()


@router.get("/elec/incidents/{incident_id}/report.pdf")
def elec_incident_report_pdf(incident_id: int, request: Request, site_code: str = Query(default="")):
    user, _token = _require_elec_user(request)
    scoped = _scope_site_code(user, site_code)
    con = _connect()
    try:
        incident_row = con.execute("SELECT * FROM elec_incidents WHERE id=? AND site_code=? LIMIT 1", (int(incident_id), scoped)).fetchone()
        if not incident_row:
            raise HTTPException(status_code=404, detail="incident not found")
        incident = dict(incident_row)
        notifications = _rows(con.execute("SELECT * FROM elec_notifications WHERE incident_id=? AND site_code=? ORDER BY id", (int(incident_id), scoped)).fetchall())
        ack_logs = _rows(con.execute("SELECT * FROM elec_notify_ack WHERE incident_id=? AND site_code=? ORDER BY id", (int(incident_id), scoped)).fetchall())
        data = _pdf_incident(incident, notifications, ack_logs)
        return Response(
            content=data,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename=elec_incident_{int(incident_id)}.pdf"},
        )
    finally:
        con.close()
