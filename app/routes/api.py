from __future__ import annotations

import io
import re
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from ..db import (
    cleanup_expired_sessions,
    count_staff_admins,
    create_auth_session,
    create_staff_user,
    delete_entry,
    delete_staff_user,
    ensure_site,
    get_auth_user_by_token,
    get_site_env_config,
    get_staff_user,
    get_staff_user_by_login,
    hash_password,
    list_entries,
    list_site_env_configs,
    list_staff_users,
    load_entry,
    load_entry_by_id,
    mark_staff_user_login,
    revoke_all_user_sessions,
    revoke_auth_session,
    save_tab_values,
    schema_alignment_report,
    set_staff_user_password,
    upsert_site_env_config,
    delete_site_env_config,
    update_staff_user,
    upsert_entry,
    upsert_tab_domain_data,
    verify_password,
)
from ..schema_defs import (
    SCHEMA_DEFS,
    build_effective_schema,
    default_site_env_config,
    merge_site_env_configs,
    normalize_site_env_config,
    normalize_tabs_payload,
    schema_field_keys,
    site_env_template,
    site_env_templates,
)
from ..utils import build_excel, build_pdf, safe_ymd, today_ymd

router = APIRouter()

VALID_USER_ROLES = ["관리소장", "과장", "주임", "기사", "행정", "경비", "미화", "기타"]
DEFAULT_SITE_NAME = "미지정단지"


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


def _clean_site_name(value: Any, *, required: bool = False) -> str:
    site_name = (str(value or "")).strip()
    if not site_name:
        if required:
            raise HTTPException(status_code=400, detail="site_name is required")
        return DEFAULT_SITE_NAME
    if len(site_name) > 80:
        raise HTTPException(status_code=400, detail="site_name length must be <= 80")
    return site_name


def _clean_password(value: Any, *, required: bool = True) -> str | None:
    txt = (str(value or "")).strip()
    if not txt:
        if required:
            raise HTTPException(status_code=400, detail="password is required")
        return None
    if len(txt) < 8 or len(txt) > 128:
        raise HTTPException(status_code=400, detail="password length must be 8..128")
    return txt


def _clean_optional_text(value: Any, max_len: int) -> str | None:
    txt = (str(value or "")).strip()
    if not txt:
        return None
    if len(txt) > max_len:
        raise HTTPException(status_code=400, detail=f"text length must be <= {max_len}")
    return txt


def _clean_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    return s in ("1", "true", "y", "yes", "on")


def _public_user(user: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(user.get("id")),
        "login_id": user.get("login_id"),
        "name": user.get("name"),
        "role": user.get("role"),
        "phone": user.get("phone"),
        "note": user.get("note"),
        "is_admin": bool(user.get("is_admin")),
        "is_active": bool(user.get("is_active")),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
        "last_login_at": user.get("last_login_at"),
    }


def _site_schema_and_env(site_name: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    raw = get_site_env_config(site_name)
    if raw is None:
        env_cfg = default_site_env_config()
    else:
        env_cfg = normalize_site_env_config(raw)
    effective_cfg = merge_site_env_configs(default_site_env_config(), env_cfg)
    schema = build_effective_schema(base_schema=SCHEMA_DEFS, site_env_config=effective_cfg)
    return schema, env_cfg


def _schema_allowed_keys(schema: Dict[str, Dict[str, Any]]) -> Dict[str, set[str]]:
    out: Dict[str, set[str]] = {}
    for tab_key in schema.keys():
        out[tab_key] = set(schema_field_keys(tab_key, schema_defs=schema))
    return out


def _extract_access_token(request: Request) -> str:
    auth = (request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            return token
    token = (request.query_params.get("access_token") or "").strip()
    if token:
        return token
    raise HTTPException(status_code=401, detail="auth required")


def _require_auth(request: Request) -> Tuple[Dict[str, Any], str]:
    token = _extract_access_token(request)
    user = get_auth_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid or expired session")
    return user, token


def _require_admin(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if int(user.get("is_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="admin only")
    return user, token


@router.get("/health")
def health():
    report = schema_alignment_report()
    return {"ok": True, "version": "2.9.0", "schema_alignment_ok": report.get("ok", False)}


@router.get("/schema")
def api_schema(request: Request, site_name: str = Query(default=DEFAULT_SITE_NAME)):
    _require_auth(request)
    clean_site_name = _clean_site_name(site_name, required=False)
    schema, env_cfg = _site_schema_and_env(clean_site_name)
    return {"schema": schema, "site_name": clean_site_name, "site_env_config": env_cfg}


@router.get("/schema_alignment")
def api_schema_alignment(request: Request):
    _require_admin(request)
    return schema_alignment_report()


@router.get("/site_env_template")
def api_site_env_template(request: Request):
    _require_admin(request)
    return {"ok": True, "template": site_env_template()}


@router.get("/site_env_templates")
def api_site_env_templates(request: Request):
    _require_admin(request)
    templates = site_env_templates()
    items = [
        {
            "key": k,
            "name": v.get("name"),
            "description": v.get("description"),
            "config": v.get("config") or {},
        }
        for k, v in templates.items()
    ]
    return {"ok": True, "count": len(items), "items": items}


@router.get("/base_schema")
def api_base_schema(request: Request):
    _require_admin(request)
    return {"ok": True, "schema": SCHEMA_DEFS}


@router.get("/site_env")
def api_site_env(request: Request, site_name: str = Query(...)):
    _require_admin(request)
    clean_site_name = _clean_site_name(site_name, required=True)
    schema, env_cfg = _site_schema_and_env(clean_site_name)
    return {"ok": True, "site_name": clean_site_name, "config": env_cfg, "schema": schema}


@router.put("/site_env")
def api_site_env_upsert(request: Request, payload: Dict[str, Any] = Body(...)):
    _require_admin(request)
    clean_site_name = _clean_site_name(payload.get("site_name"), required=True)
    raw_cfg = payload.get("config", payload.get("env", payload if isinstance(payload, dict) else {}))
    cfg = normalize_site_env_config(raw_cfg if isinstance(raw_cfg, dict) else {})
    row = upsert_site_env_config(clean_site_name, cfg)
    schema, _env_cfg = _site_schema_and_env(clean_site_name)
    return {
        "ok": True,
        "site_name": clean_site_name,
        "config": cfg,
        "schema": schema,
        "updated_at": row.get("updated_at"),
    }


@router.delete("/site_env")
def api_site_env_delete(request: Request, site_name: str = Query(...)):
    _require_admin(request)
    clean_site_name = _clean_site_name(site_name, required=True)
    ok = delete_site_env_config(clean_site_name)
    return {"ok": ok, "site_name": clean_site_name}


@router.get("/site_env_list")
def api_site_env_list(request: Request):
    _require_admin(request)
    rows = list_site_env_configs()
    return {
        "ok": True,
        "count": len(rows),
        "items": [{"site_name": r.get("site_name"), "updated_at": r.get("updated_at")} for r in rows],
    }


@router.get("/auth/bootstrap_status")
def auth_bootstrap_status():
    return {"ok": True, "required": count_staff_admins(active_only=True) == 0}


@router.post("/auth/bootstrap")
def auth_bootstrap(request: Request, payload: Dict[str, Any] = Body(...)):
    if count_staff_admins(active_only=True) > 0:
        raise HTTPException(status_code=409, detail="bootstrap is already completed")

    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name") or payload.get("login_id"))
    role = _clean_role(payload.get("role") or "관리소장")
    password = _clean_password(payload.get("password"), required=True)

    existing = get_staff_user_by_login(login_id)
    if existing:
        user = update_staff_user(
            int(existing["id"]),
            login_id=login_id,
            name=name,
            role=role,
            phone=existing.get("phone"),
            note=existing.get("note"),
            is_admin=1,
            is_active=1,
        )
        set_staff_user_password(int(existing["id"]), password)
        user = get_staff_user(int(existing["id"])) if user else None
    else:
        user = create_staff_user(
            login_id=login_id,
            name=name,
            role=role,
            password_hash=hash_password(password),
            is_admin=1,
            is_active=1,
        )
    if not user:
        raise HTTPException(status_code=500, detail="failed to create bootstrap admin")

    mark_staff_user_login(int(user["id"]))
    cleanup_expired_sessions()
    session = create_auth_session(
        int(user["id"]),
        ttl_hours=12,
        user_agent=request.headers.get("user-agent"),
        ip_address=(request.client.host if request.client else None),
    )
    return {"ok": True, "token": session["token"], "expires_at": session["expires_at"], "user": _public_user(user)}


@router.post("/auth/login")
def auth_login(request: Request, payload: Dict[str, Any] = Body(...)):
    login_id = _clean_login_id(payload.get("login_id"))
    password = _clean_password(payload.get("password"), required=True)

    user = get_staff_user_by_login(login_id)
    if not user or int(user.get("is_active") or 0) != 1:
        raise HTTPException(status_code=401, detail="invalid credentials")
    password_hash = user.get("password_hash")
    if not password_hash:
        raise HTTPException(status_code=403, detail="password is not set for this account")
    if not verify_password(password, str(password_hash)):
        raise HTTPException(status_code=401, detail="invalid credentials")

    mark_staff_user_login(int(user["id"]))
    cleanup_expired_sessions()
    session = create_auth_session(
        int(user["id"]),
        ttl_hours=12,
        user_agent=request.headers.get("user-agent"),
        ip_address=(request.client.host if request.client else None),
    )
    fresh = get_staff_user(int(user["id"])) or user
    return {"ok": True, "token": session["token"], "expires_at": session["expires_at"], "user": _public_user(fresh)}


@router.post("/auth/logout")
def auth_logout(request: Request):
    _user, token = _require_auth(request)
    revoke_auth_session(token)
    return {"ok": True}


@router.get("/auth/me")
def auth_me(request: Request):
    user, _token = _require_auth(request)
    return {"ok": True, "user": _public_user(user), "session_expires_at": user.get("expires_at")}


@router.post("/auth/change_password")
def auth_change_password(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_auth(request)
    old_password = _clean_password(payload.get("old_password"), required=True)
    new_password = _clean_password(payload.get("new_password"), required=True)
    db_user = get_staff_user_by_login(str(user.get("login_id") or ""))
    if not db_user or not db_user.get("password_hash"):
        raise HTTPException(status_code=404, detail="user not found")
    if not verify_password(old_password, str(db_user.get("password_hash"))):
        raise HTTPException(status_code=401, detail="old password is incorrect")
    set_staff_user_password(int(user["id"]), new_password)
    revoke_all_user_sessions(int(user["id"]))
    session = create_auth_session(
        int(user["id"]),
        ttl_hours=12,
        user_agent=request.headers.get("user-agent"),
        ip_address=(request.client.host if request.client else None),
    )
    fresh = get_staff_user(int(user["id"])) or db_user
    return {"ok": True, "token": session["token"], "expires_at": session["expires_at"], "user": _public_user(fresh)}


@router.get("/user_roles")
def api_user_roles(request: Request):
    _require_auth(request)
    return {"ok": True, "roles": VALID_USER_ROLES, "recommended_staff_count": 9}


@router.get("/users")
def api_users(request: Request, active_only: int = Query(default=0)):
    _require_admin(request)
    users = [_public_user(x) for x in list_staff_users(active_only=bool(active_only))]
    return {"ok": True, "recommended_staff_count": 9, "count": len(users), "users": users}


@router.post("/users")
def api_users_create(request: Request, payload: Dict[str, Any] = Body(...)):
    _actor, _token = _require_admin(request)
    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name"))
    role = _clean_role(payload.get("role"))
    phone = _clean_optional_text(payload.get("phone"), 40)
    note = _clean_optional_text(payload.get("note"), 200)
    password = _clean_password(payload.get("password"), required=True)
    is_admin = 1 if _clean_bool(payload.get("is_admin"), default=False) else 0
    is_active = 1 if _clean_bool(payload.get("is_active"), default=True) else 0
    try:
        user = create_staff_user(
            login_id=login_id,
            name=name,
            role=role,
            phone=phone,
            note=note,
            password_hash=hash_password(password),
            is_admin=is_admin,
            is_active=is_active,
        )
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            raise HTTPException(status_code=409, detail="login_id already exists")
        raise
    return {"ok": True, "user": _public_user(user)}


@router.patch("/users/{user_id}")
def api_users_patch(request: Request, user_id: int, payload: Dict[str, Any] = Body(...)):
    actor, _token = _require_admin(request)
    current = get_staff_user(user_id)
    if not current:
        raise HTTPException(status_code=404, detail="user not found")

    login_id = _clean_login_id(payload.get("login_id", current.get("login_id")))
    name = _clean_name(payload.get("name", current.get("name")))
    role = _clean_role(payload.get("role", current.get("role")))
    phone = _clean_optional_text(payload["phone"] if "phone" in payload else current.get("phone"), 40)
    note = _clean_optional_text(payload["note"] if "note" in payload else current.get("note"), 200)

    is_admin = _clean_bool(payload["is_admin"], default=bool(current.get("is_admin"))) if "is_admin" in payload else bool(
        current.get("is_admin")
    )
    is_active = (
        _clean_bool(payload["is_active"], default=bool(current.get("is_active")))
        if "is_active" in payload
        else bool(current.get("is_active"))
    )

    if int(actor["id"]) == int(user_id) and not is_admin:
        raise HTTPException(status_code=400, detail="cannot remove your own admin permission")
    if int(actor["id"]) == int(user_id) and not is_active:
        raise HTTPException(status_code=400, detail="cannot deactivate your own account")

    if bool(current.get("is_admin")) and not is_admin and bool(current.get("is_active")) and count_staff_admins(active_only=True) <= 1:
        raise HTTPException(status_code=400, detail="at least one active admin is required")

    try:
        user = update_staff_user(
            user_id,
            login_id=login_id,
            name=name,
            role=role,
            phone=phone,
            note=note,
            is_admin=1 if is_admin else 0,
            is_active=1 if is_active else 0,
        )
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            raise HTTPException(status_code=409, detail="login_id already exists")
        raise
    if not user:
        raise HTTPException(status_code=404, detail="user not found")

    new_password = _clean_password(payload.get("password"), required=False) if "password" in payload else None
    if new_password:
        set_staff_user_password(int(user_id), new_password)
        if int(actor["id"]) == int(user_id):
            revoke_all_user_sessions(int(user_id))

    if not is_active:
        revoke_all_user_sessions(int(user_id))

    fresh = get_staff_user(int(user_id)) or user
    return {"ok": True, "user": _public_user(fresh)}


@router.delete("/users/{user_id}")
def api_users_delete(request: Request, user_id: int):
    actor, _token = _require_admin(request)
    if int(actor["id"]) == int(user_id):
        raise HTTPException(status_code=400, detail="cannot delete your own account")

    target = get_staff_user(user_id)
    if not target:
        return {"ok": False}
    if bool(target.get("is_admin")) and bool(target.get("is_active")) and count_staff_admins(active_only=True) <= 1:
        raise HTTPException(status_code=400, detail="at least one active admin is required")

    revoke_all_user_sessions(int(user_id))
    ok = delete_staff_user(user_id)
    return {"ok": ok}


@router.post("/save")
def api_save(request: Request, payload: Dict[str, Any] = Body(...)):
    _require_auth(request)
    site_name = _clean_site_name(payload.get("site_name"), required=False)
    entry_date = safe_ymd(payload.get("date") or "")

    raw_tabs = payload.get("tabs") or {}
    if not isinstance(raw_tabs, dict):
        raise HTTPException(status_code=400, detail="tabs must be object")

    schema, _env_cfg = _site_schema_and_env(site_name)
    tabs = normalize_tabs_payload(raw_tabs, schema_defs=schema)
    ignored_tabs = sorted(set(str(k) for k in raw_tabs.keys()) - set(tabs.keys()))

    site_id = ensure_site(site_name)
    entry_id = upsert_entry(site_id, entry_date)

    for tab_key, fields in tabs.items():
        save_tab_values(entry_id, tab_key, fields, schema_defs=schema)
        upsert_tab_domain_data(site_name, entry_date, tab_key, fields)

    return {
        "ok": True,
        "site_name": site_name,
        "date": entry_date,
        "saved_tabs": sorted(tabs.keys()),
        "ignored_tabs": ignored_tabs,
    }


@router.get("/load")
def api_load(request: Request, site_name: str = Query(...), date: str = Query(...)):
    _require_auth(request)
    site_name = _clean_site_name(site_name, required=False)
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    schema, _env_cfg = _site_schema_and_env(site_name)
    tabs = load_entry(site_id, entry_date, allowed_keys_by_tab=_schema_allowed_keys(schema))
    return {"ok": True, "site_name": site_name, "date": entry_date, "tabs": tabs}


@router.delete("/delete")
def api_delete(request: Request, site_name: str = Query(...), date: str = Query(...)):
    _require_auth(request)
    site_name = _clean_site_name(site_name, required=False)
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    ok = delete_entry(site_id, entry_date)
    return {"ok": ok}


@router.get("/list_range")
def api_list_range(
    request: Request,
    site_name: str = Query(...),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    _require_auth(request)
    site_name = _clean_site_name(site_name, required=False)
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
    request: Request,
    site_name: str = Query(...),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    _require_auth(request)
    site_name = _clean_site_name(site_name, required=False)
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df

    site_id = ensure_site(site_name)
    schema, _env_cfg = _site_schema_and_env(site_name)
    allowed = _schema_allowed_keys(schema)
    entries = list_entries(site_id, df, dt)
    rows: List[Dict[str, Any]] = []
    for e in entries:
        rows.append({"entry_date": e["entry_date"], "tabs": load_entry_by_id(int(e["id"]), allowed_keys_by_tab=allowed)})

    xbytes = build_excel(site_name, df, dt, rows, schema_defs=schema)
    from urllib.parse import quote

    filename = f"전기일지_{site_name}_{df}~{dt}.xlsx"
    ascii_fallback = "export.xlsx"
    cd = f"attachment; filename={ascii_fallback}; filename*=UTF-8''{quote(filename)}"
    return StreamingResponse(
        io.BytesIO(xbytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": cd},
    )


@router.get("/pdf")
def api_pdf(request: Request, site_name: str = Query(...), date: str = Query(...)):
    _require_auth(request)
    site_name = _clean_site_name(site_name, required=False)
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    schema, _env_cfg = _site_schema_and_env(site_name)
    tabs = load_entry(site_id, entry_date, allowed_keys_by_tab=_schema_allowed_keys(schema))

    pbytes = build_pdf(site_name, entry_date, tabs, schema_defs=schema)
    from urllib.parse import quote

    filename = f"전기일지_{site_name}_{entry_date}.pdf"
    ascii_fallback = "report.pdf"
    cd = f"attachment; filename={ascii_fallback}; filename*=UTF-8''{quote(filename)}"
    return StreamingResponse(
        io.BytesIO(pbytes),
        media_type="application/pdf",
        headers={"Content-Disposition": cd},
    )
