from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ..db import (
    append_audit_log,
    cleanup_expired_sessions,
    count_staff_admins,
    create_auth_session,
    create_staff_user,
    create_tenant,
    get_auth_user_by_token,
    get_staff_user,
    get_staff_user_by_login,
    get_tenant,
    hash_password,
    list_audit_logs,
    list_staff_users,
    list_tenants,
    list_usage_logs,
    mark_staff_user_login,
    revoke_all_user_sessions,
    revoke_auth_session,
    rotate_tenant_api_key,
    set_staff_user_password,
    set_tenant_status,
    verify_password,
)

router = APIRouter()

AUTH_COOKIE_NAME = (os.getenv("KA_AUTH_COOKIE_NAME") or "ka_part_auth_token").strip()
AUTH_COOKIE_SAMESITE = (os.getenv("KA_AUTH_COOKIE_SAMESITE") or "lax").strip().lower() or "lax"
if AUTH_COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    AUTH_COOKIE_SAMESITE = "lax"
ALLOW_INSECURE_DEFAULTS = str(os.getenv("ALLOW_INSECURE_DEFAULTS") or "").strip().lower() in {"1", "true", "yes", "on"}
AUTH_COOKIE_SECURE = str(os.getenv("KA_AUTH_COOKIE_SECURE") or ("0" if ALLOW_INSECURE_DEFAULTS else "1")).strip().lower() in {"1", "true", "yes", "on"}
AUTH_COOKIE_MAX_AGE = max(300, int(os.getenv("KA_AUTH_COOKIE_MAX_AGE") or "43200"))

VALID_LOGIN_RE = re.compile(r"^[a-z0-9._-]{2,32}$")


def _client_ip(request: Request) -> str:
    xff = str(request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        first = xff.split(",", 1)[0].strip()
        if first:
            return first
    xri = str(request.headers.get("x-real-ip") or "").strip()
    if xri:
        return xri
    return request.client.host if request.client else ""


def _clean_login_id(value: Any) -> str:
    login_id = str(value or "").strip().lower()
    if not VALID_LOGIN_RE.match(login_id):
        raise HTTPException(status_code=400, detail="아이디 형식이 올바르지 않습니다.")
    return login_id


def _clean_name(value: Any) -> str:
    name = str(value or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="이름을 입력하세요.")
    if len(name) > 40:
        raise HTTPException(status_code=400, detail="이름은 40자 이하여야 합니다.")
    return name


def _clean_password(value: Any, *, field_name: str = "비밀번호") -> str:
    password = str(value or "")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail=f"{field_name}는 8자 이상이어야 합니다.")
    if len(password) > 72:
        raise HTTPException(status_code=400, detail=f"{field_name}는 72자 이하여야 합니다.")
    return password


def _public_user(user: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(user)
    out.pop("password_hash", None)
    return out


def _cookie_secure(request: Request) -> bool:
    forced = os.getenv("KA_AUTH_COOKIE_SECURE")
    if forced is not None and str(forced).strip():
        return str(forced).strip().lower() in {"1", "true", "yes", "on"}
    proto = str(request.headers.get("x-forwarded-proto") or request.url.scheme or "").strip().lower()
    return proto == "https"


def _set_auth_cookie(request: Request, resp: JSONResponse, token: str) -> None:
    resp.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=token,
        max_age=AUTH_COOKIE_MAX_AGE,
        httponly=True,
        secure=_cookie_secure(request),
        samesite=AUTH_COOKIE_SAMESITE,
        path="/",
    )


def _clear_auth_cookie(request: Request, resp: JSONResponse) -> None:
    resp.delete_cookie(
        key=AUTH_COOKIE_NAME,
        httponly=True,
        secure=_cookie_secure(request),
        samesite=AUTH_COOKIE_SAMESITE,
        path="/",
    )


def _extract_access_token(request: Request) -> str:
    auth = str(request.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            return token
    token = str(request.cookies.get(AUTH_COOKIE_NAME) or "").strip()
    if token:
        return token
    raise HTTPException(status_code=401, detail="로그인이 필요합니다.")


def _require_auth(request: Request) -> Tuple[Dict[str, Any], str]:
    token = _extract_access_token(request)
    cleanup_expired_sessions()
    user = get_auth_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user, token


def _require_admin(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if int(user.get("is_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="최고관리자만 사용할 수 있습니다.")
    return user, token


@router.api_route("/health", methods=["GET", "HEAD"])
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "ka-part-complaint-engine"}


@router.get("/auth/bootstrap_status")
def auth_bootstrap_status() -> Dict[str, Any]:
    cleanup_expired_sessions()
    return {"ok": True, "needs_bootstrap": count_staff_admins(active_only=True) == 0}


@router.post("/auth/bootstrap")
def auth_bootstrap(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    cleanup_expired_sessions()
    if count_staff_admins(active_only=True) > 0:
        raise HTTPException(status_code=409, detail="초기 관리자 생성은 이미 완료되었습니다.")

    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name") or login_id)
    password = _clean_password(payload.get("password"))

    if get_staff_user_by_login(login_id):
        raise HTTPException(status_code=409, detail="이미 존재하는 아이디입니다.")

    user = create_staff_user(
        login_id=login_id,
        name=name,
        role="super_admin",
        password_hash=hash_password(password),
        is_admin=1,
        admin_scope="super_admin",
        is_active=1,
    )
    mark_staff_user_login(int(user["id"]))
    session = create_auth_session(
        int(user["id"]),
        user_agent=str(request.headers.get("user-agent") or "").strip(),
        ip_address=_client_ip(request),
    )
    fresh = get_staff_user(int(user["id"])) or user
    append_audit_log(None, "bootstrap_admin", login_id, {"user_id": int(user["id"])})
    resp = JSONResponse(
        {
            "ok": True,
            "user": _public_user(fresh),
            "landing_path": "/pwa/",
            "session_expires_at": session["expires_at"],
        }
    )
    _set_auth_cookie(request, resp, str(session["token"]))
    return resp


@router.post("/auth/login")
def auth_login(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    cleanup_expired_sessions()
    login_id = _clean_login_id(payload.get("login_id"))
    password = _clean_password(payload.get("password"))

    user = get_staff_user_by_login(login_id)
    if not user or int(user.get("is_active") or 0) != 1:
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 올바르지 않습니다.")
    if not verify_password(password, str(user.get("password_hash") or "")):
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 올바르지 않습니다.")

    mark_staff_user_login(int(user["id"]))
    session = create_auth_session(
        int(user["id"]),
        user_agent=str(request.headers.get("user-agent") or "").strip(),
        ip_address=_client_ip(request),
    )
    fresh = get_staff_user(int(user["id"])) or user
    resp = JSONResponse(
        {
            "ok": True,
            "user": _public_user(fresh),
            "landing_path": "/pwa/",
            "session_expires_at": session["expires_at"],
        }
    )
    _set_auth_cookie(request, resp, str(session["token"]))
    return resp


@router.post("/auth/logout")
def auth_logout(request: Request) -> JSONResponse:
    try:
        user, token = _require_auth(request)
        revoke_auth_session(token)
        append_audit_log(user.get("tenant_id"), "logout", str(user.get("login_id") or ""), {})
    except HTTPException:
        pass
    resp = JSONResponse({"ok": True})
    _clear_auth_cookie(request, resp)
    return resp


@router.get("/auth/me")
def auth_me(request: Request) -> Dict[str, Any]:
    user, _token = _require_auth(request)
    tenant = get_tenant(str(user.get("tenant_id") or "")) if user.get("tenant_id") else None
    return {
        "ok": True,
        "user": _public_user(user),
        "tenant": tenant,
        "session_expires_at": user.get("expires_at"),
        "landing_path": "/pwa/",
    }


@router.post("/auth/change_password")
def auth_change_password(request: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    user, _token = _require_auth(request)
    current_password = _clean_password(payload.get("current_password"), field_name="현재 비밀번호")
    new_password = _clean_password(payload.get("new_password"), field_name="새 비밀번호")

    db_user = get_staff_user_by_login(str(user.get("login_id") or ""))
    if not db_user or not verify_password(current_password, str(db_user.get("password_hash") or "")):
        raise HTTPException(status_code=400, detail="현재 비밀번호가 올바르지 않습니다.")
    if current_password == new_password:
        raise HTTPException(status_code=400, detail="새 비밀번호가 현재 비밀번호와 같습니다.")

    ok = set_staff_user_password(int(user["id"]), new_password)
    if not ok:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")

    revoke_all_user_sessions(int(user["id"]))
    mark_staff_user_login(int(user["id"]))
    session = create_auth_session(
        int(user["id"]),
        user_agent=str(request.headers.get("user-agent") or "").strip(),
        ip_address=_client_ip(request),
    )
    fresh = get_staff_user(int(user["id"])) or user
    append_audit_log(user.get("tenant_id"), "change_password", str(user.get("login_id") or ""), {"user_id": int(user["id"])})
    resp = JSONResponse(
        {
            "ok": True,
            "user": _public_user(fresh),
            "landing_path": "/pwa/",
            "session_expires_at": session["expires_at"],
        }
    )
    _set_auth_cookie(request, resp, str(session["token"]))
    return resp


@router.get("/modules/contracts")
def modules_contracts(request: Request) -> Dict[str, Any]:
    _user, _token = _require_auth(request)
    contracts = [
        {
            "module_key": "complaint_engine",
            "module_name": "AI 민원처리 엔진",
            "ui_path": "/pwa/",
            "api_prefix": "/api",
            "auth_modes": ["session", "api_key"],
        }
    ]
    return {
        "ok": True,
        "allowed_modules": ["complaint_engine"],
        "contracts": contracts,
    }


@router.get("/users")
def users_list(
    request: Request,
    active_only: bool = Query(default=True),
    tenant_id: str = Query(default=""),
) -> Dict[str, Any]:
    user, _token = _require_auth(request)
    if int(user.get("is_admin") or 0) == 1:
        rows = list_staff_users(active_only=bool(active_only), tenant_id=tenant_id)
    else:
        rows = list_staff_users(active_only=bool(active_only), tenant_id=str(user.get("tenant_id") or ""))
    items = []
    for row in rows:
        item = dict(row)
        item.pop("password_hash", None)
        items.append(item)
    return {"ok": True, "items": items}


@router.post("/users")
def users_create(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, _token = _require_auth(request)
    if int(user.get("is_admin") or 0) == 1:
        tenant_id = str(payload.get("tenant_id") or "").strip().lower()
    else:
        tenant_id = str(user.get("tenant_id") or "").strip().lower()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id가 필요합니다.")
    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name"))
    password = _clean_password(payload.get("password"))
    role = str(payload.get("role") or "staff").strip() or "staff"
    if get_staff_user_by_login(login_id):
        raise HTTPException(status_code=409, detail="이미 존재하는 아이디입니다.")
    created = create_staff_user(
        tenant_id=tenant_id,
        login_id=login_id,
        name=name,
        role=role,
        password_hash=hash_password(password),
        site_code=payload.get("site_code"),
        site_name=payload.get("site_name"),
        note=payload.get("note"),
        is_site_admin=1 if payload.get("is_site_admin") else 0,
        is_active=1,
    )
    created.pop("password_hash", None)
    append_audit_log(tenant_id, "create_user", str(user.get("login_id") or ""), {"login_id": login_id})
    return {"ok": True, "item": created}


@router.get("/admin/tenants")
def admin_tenants(request: Request) -> Dict[str, Any]:
    _user, _token = _require_admin(request)
    return {"ok": True, "items": list_tenants(active_only=False)}


@router.post("/admin/tenants")
def admin_create_tenant(request: Request, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, _token = _require_admin(request)
    try:
        item = create_tenant(
            tenant_id=payload.get("tenant_id"),
            name=payload.get("name"),
            site_code=payload.get("site_code"),
            site_name=payload.get("site_name"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    append_audit_log(item.get("id"), "create_tenant", str(user.get("login_id") or ""), {"tenant_id": item.get("id")})
    return {"ok": True, "item": item}


@router.post("/admin/tenants/{tenant_id}/rotate_key")
def admin_rotate_tenant_key(request: Request, tenant_id: str) -> Dict[str, Any]:
    user, _token = _require_admin(request)
    try:
        item = rotate_tenant_api_key(tenant_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    append_audit_log(item.get("id"), "rotate_api_key", str(user.get("login_id") or ""), {"tenant_id": item.get("id")})
    return {"ok": True, "item": item}


@router.patch("/admin/tenants/{tenant_id}")
def admin_patch_tenant(request: Request, tenant_id: str, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, _token = _require_admin(request)
    try:
        ok = set_tenant_status(tenant_id, payload.get("status"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not ok:
        raise HTTPException(status_code=404, detail="tenant not found")
    append_audit_log(tenant_id, "set_tenant_status", str(user.get("login_id") or ""), {"status": payload.get("status")})
    return {"ok": True, "item": get_tenant(tenant_id)}


@router.get("/admin/usage")
def admin_usage(request: Request, tenant_id: str = Query(default=""), limit: int = Query(default=100, ge=1, le=500)) -> Dict[str, Any]:
    _user, _token = _require_admin(request)
    return {"ok": True, "items": list_usage_logs(tenant_id=tenant_id, limit=limit)}


@router.get("/admin/audit")
def admin_audit(request: Request, tenant_id: str = Query(default=""), limit: int = Query(default=100, ge=1, le=500)) -> Dict[str, Any]:
    _user, _token = _require_admin(request)
    return {"ok": True, "items": list_audit_logs(tenant_id=tenant_id, limit=limit)}
