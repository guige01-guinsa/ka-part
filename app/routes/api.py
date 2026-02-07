from __future__ import annotations

import io
import re
from typing import Any, Dict, List

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..db import (
    delete_entry,
    delete_staff_user,
    ensure_site,
    get_staff_user,
    list_entries,
    list_staff_users,
    load_entry,
    load_entry_by_id,
    save_tab_values,
    schema_alignment_report,
    create_staff_user,
    upsert_entry,
    upsert_tab_domain_data,
    update_staff_user,
)
from ..schema_defs import SCHEMA_DEFS, normalize_tabs_payload
from ..utils import build_excel, build_pdf, safe_ymd, today_ymd

router = APIRouter()

VALID_USER_ROLES = ["관리소장", "과장", "주임", "기사", "행정", "경비", "미화", "기타"]


def _clean_login_id(value: Any) -> str:
    login_id = (str(value or "")).strip().lower()
    if not re.match(r"^[a-z0-9._-]{2,32}$", login_id):
        raise HTTPException(status_code=400, detail="login_id must match ^[a-z0-9._-]{2,32}$")
    return login_id


def _clean_name(value: Any) -> str:
    name = (str(value or "")).strip()
    if len(name) < 2 or len(name) > 40:
        raise HTTPException(status_code=400, detail="name length must be 2..40")
    return name


def _clean_role(value: Any) -> str:
    role = (str(value or "")).strip()
    if len(role) < 1 or len(role) > 20:
        raise HTTPException(status_code=400, detail="role length must be 1..20")
    return role


def _clean_optional_text(value: Any, max_len: int) -> str | None:
    txt = (str(value or "")).strip()
    if not txt:
        return None
    if len(txt) > max_len:
        raise HTTPException(status_code=400, detail=f"text length must be <= {max_len}")
    return txt


@router.get("/schema")
def api_schema():
    return {"schema": SCHEMA_DEFS}


@router.get("/health")
def health():
    report = schema_alignment_report()
    return {"ok": True, "version": "2.7.0", "schema_alignment_ok": report.get("ok", False)}


@router.get("/schema_alignment")
def api_schema_alignment():
    return schema_alignment_report()


@router.get("/user_roles")
def api_user_roles():
    roles = ["관리소장", "과장", "주임", "기사", "행정", "경비", "미화", "기타"]
    return {"ok": True, "roles": roles, "recommended_staff_count": 9}


@router.get("/users")
def api_users(active_only: int = Query(default=0)):
    users = list_staff_users(active_only=bool(active_only))
    return {"ok": True, "recommended_staff_count": 9, "count": len(users), "users": users}


@router.post("/users")
def api_users_create(payload: Dict[str, Any] = Body(...)):
    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name"))
    role = _clean_role(payload.get("role"))
    phone = _clean_optional_text(payload.get("phone"), 40)
    note = _clean_optional_text(payload.get("note"), 200)
    is_active = 1 if bool(payload.get("is_active", True)) else 0
    try:
        user = create_staff_user(
            login_id=login_id,
            name=name,
            role=role,
            phone=phone,
            note=note,
            is_active=is_active,
        )
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            raise HTTPException(status_code=409, detail="login_id already exists")
        raise
    return {"ok": True, "user": user}


@router.patch("/users/{user_id}")
def api_users_patch(user_id: int, payload: Dict[str, Any] = Body(...)):
    current = get_staff_user(user_id)
    if not current:
        raise HTTPException(status_code=404, detail="user not found")
    login_id = _clean_login_id(payload.get("login_id", current.get("login_id")))
    name = _clean_name(payload.get("name", current.get("name")))
    role = _clean_role(payload.get("role", current.get("role")))
    phone = _clean_optional_text(
        payload["phone"] if "phone" in payload else current.get("phone"),
        40,
    )
    note = _clean_optional_text(
        payload["note"] if "note" in payload else current.get("note"),
        200,
    )
    is_active_raw = payload["is_active"] if "is_active" in payload else current.get("is_active", 1)
    is_active = 1 if bool(is_active_raw) else 0
    try:
        user = update_staff_user(
            user_id,
            login_id=login_id,
            name=name,
            role=role,
            phone=phone,
            note=note,
            is_active=is_active,
        )
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            raise HTTPException(status_code=409, detail="login_id already exists")
        raise
    if not user:
        raise HTTPException(status_code=404, detail="user not found")
    return {"ok": True, "user": user}


@router.delete("/users/{user_id}")
def api_users_delete(user_id: int):
    ok = delete_staff_user(user_id)
    return {"ok": ok}


@router.post("/save")
def api_save(payload: Dict[str, Any] = Body(...)):
    """
    {
      "site_name": "단지명",
      "date": "YYYY-MM-DD",
      "tabs": { "<tab_key>": { "<field_key>": "value" } }
    }
    """
    site_name = (payload.get("site_name") or "").strip() or "미지정단지"
    entry_date = safe_ymd(payload.get("date") or "")

    raw_tabs = payload.get("tabs") or {}
    if not isinstance(raw_tabs, dict):
        raise HTTPException(status_code=400, detail="tabs must be object")

    tabs = normalize_tabs_payload(raw_tabs)
    ignored_tabs = sorted(set(str(k) for k in raw_tabs.keys()) - set(tabs.keys()))

    site_id = ensure_site(site_name)
    entry_id = upsert_entry(site_id, entry_date)

    for tab_key, fields in tabs.items():
        save_tab_values(entry_id, tab_key, fields)
        upsert_tab_domain_data(site_name, entry_date, tab_key, fields)

    return {
        "ok": True,
        "site_name": site_name,
        "date": entry_date,
        "saved_tabs": sorted(tabs.keys()),
        "ignored_tabs": ignored_tabs,
    }


@router.get("/load")
def api_load(site_name: str = Query(...), date: str = Query(...)):
    site_name = (site_name or "").strip() or "미지정단지"
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    tabs = load_entry(site_id, entry_date)
    return {"ok": True, "site_name": site_name, "date": entry_date, "tabs": tabs}


@router.delete("/delete")
def api_delete(site_name: str = Query(...), date: str = Query(...)):
    site_name = (site_name or "").strip() or "미지정단지"
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    ok = delete_entry(site_id, entry_date)
    return {"ok": ok}


@router.get("/list_range")
def api_list_range(
    site_name: str = Query(...),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    site_name = (site_name or "").strip() or "미지정단지"
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df
    site_id = ensure_site(site_name)
    entries = list_entries(site_id, df, dt)
    dates = [e["entry_date"] for e in entries]
    return {"ok": True, "site_name": site_name, "date_from": df, "date_to": dt, "dates": dates}


@router.get("/export")
def api_export(
    site_name: str = Query(...),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    site_name = (site_name or "").strip() or "미지정단지"
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df

    site_id = ensure_site(site_name)
    entries = list_entries(site_id, df, dt)
    rows: List[Dict[str, Any]] = []
    for e in entries:
        rows.append({"entry_date": e["entry_date"], "tabs": load_entry_by_id(int(e["id"]))})

    xbytes = build_excel(site_name, df, dt, rows)
    filename = f"점검일지_{site_name}_{df}~{dt}.xlsx"

    from urllib.parse import quote

    ascii_fallback = "export.xlsx"
    cd = f"attachment; filename={ascii_fallback}; filename*=UTF-8''{quote(filename)}"
    return StreamingResponse(
        io.BytesIO(xbytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": cd},
    )


@router.get("/pdf")
def api_pdf(site_name: str = Query(...), date: str = Query(...)):
    site_name = (site_name or "").strip() or "미지정단지"
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    tabs = load_entry(site_id, entry_date)

    pbytes = build_pdf(site_name, entry_date, tabs)
    filename = f"점검일지_{site_name}_{entry_date}.pdf"

    from urllib.parse import quote

    ascii_fallback = "report.pdf"
    cd = f"attachment; filename={ascii_fallback}; filename*=UTF-8''{quote(filename)}"
    return StreamingResponse(
        io.BytesIO(pbytes),
        media_type="application/pdf",
        headers={"Content-Disposition": cd},
    )
