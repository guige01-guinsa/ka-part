from __future__ import annotations

import os
import re
import tempfile
from typing import Any, Dict, Tuple

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from ..db import (
    append_audit_log,
    cleanup_expired_sessions,
    count_staff_admins,
    create_auth_session,
    create_staff_user,
    create_tenant,
    delete_staff_user,
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
    update_staff_user,
    verify_password,
)
from ..legacy_import import import_legacy_source

router = APIRouter()

AUTH_COOKIE_NAME = (os.getenv("KA_AUTH_COOKIE_NAME") or "ka_part_auth_token").strip()
AUTH_COOKIE_SAMESITE = (os.getenv("KA_AUTH_COOKIE_SAMESITE") or "lax").strip().lower() or "lax"
if AUTH_COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    AUTH_COOKIE_SAMESITE = "lax"
ALLOW_INSECURE_DEFAULTS = str(os.getenv("ALLOW_INSECURE_DEFAULTS") or "").strip().lower() in {"1", "true", "yes", "on"}
AUTH_COOKIE_SECURE = str(os.getenv("KA_AUTH_COOKIE_SECURE") or ("0" if ALLOW_INSECURE_DEFAULTS else "1")).strip().lower() in {"1", "true", "yes", "on"}
AUTH_COOKIE_MAX_AGE = max(300, int(os.getenv("KA_AUTH_COOKIE_MAX_AGE") or "43200"))

VALID_LOGIN_RE = re.compile(r"^[a-z0-9._-]{2,32}$")
USER_ROLE_VALUES = ("staff", "desk", "manager", "vendor", "reader", "integration")


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


def _clean_role(value: Any, *, allow_super_admin: bool = False) -> str:
    role = str(value or "staff").strip().lower() or "staff"
    allowed = set(USER_ROLE_VALUES)
    if allow_super_admin:
        allowed.add("super_admin")
    if role not in allowed:
        raise HTTPException(status_code=400, detail="지원하지 않는 역할입니다.")
    return role


def _clean_optional_text(value: Any, *, field_name: str, max_len: int) -> str:
    text = str(value or "").strip()
    if len(text) > max_len:
        raise HTTPException(status_code=400, detail=f"{field_name}는 {max_len}자 이하여야 합니다.")
    return text


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


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


def _can_manage_users(user: Dict[str, Any]) -> bool:
    return int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1


def _require_user_manager(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if not _can_manage_users(user):
        raise HTTPException(status_code=403, detail="사용자 관리 권한이 없습니다.")
    return user, token


def _managed_tenant_id(user: Dict[str, Any], payload: Dict[str, Any]) -> str:
    if int(user.get("is_admin") or 0) == 1:
        tenant_id = str(payload.get("tenant_id") or "").strip().lower()
        if not tenant_id:
            raise HTTPException(status_code=400, detail="tenant_id가 필요합니다.")
        return tenant_id
    tenant_id = str(user.get("tenant_id") or "").strip().lower()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="계정에 tenant_id가 연결되어 있지 않습니다.")
    return tenant_id


def _assert_manageable_target(manager: Dict[str, Any], target: Dict[str, Any], *, allow_view_only: bool = False) -> None:
    if int(manager.get("is_admin") or 0) == 1:
        return
    manager_tenant_id = str(manager.get("tenant_id") or "").strip().lower()
    target_tenant_id = str(target.get("tenant_id") or "").strip().lower()
    if not manager_tenant_id or manager_tenant_id != target_tenant_id:
        raise HTTPException(status_code=403, detail="다른 테넌트 사용자는 관리할 수 없습니다.")
    if not allow_view_only and (int(target.get("is_admin") or 0) == 1 or int(target.get("is_site_admin") or 0) == 1):
        raise HTTPException(status_code=403, detail="현장관리자는 관리자 계정을 수정할 수 없습니다.")


def _assert_admin_guard(target: Dict[str, Any], *, deactivating: bool = False, deleting: bool = False) -> None:
    if int(target.get("is_admin") or 0) != 1 or int(target.get("is_active") or 0) != 1:
        return
    if not deactivating and not deleting:
        return
    if count_staff_admins(active_only=True) <= 1:
        raise HTTPException(status_code=400, detail="마지막 활성 최고관리자는 비활성화하거나 삭제할 수 없습니다.")


@router.api_route("/health", methods=["GET", "HEAD"])
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "ka-part-complaint-engine"}


@router.get("/auth/bootstrap_status")
def auth_bootstrap_status() -> Dict[str, Any]:
    cleanup_expired_sessions()
    return {"ok": True, "needs_bootstrap": count_staff_admins(active_only=True) == 0}


@router.get("/auth/register_options")
def auth_register_options() -> Dict[str, Any]:
    cleanup_expired_sessions()
    items = [
        {
            "id": str(row.get("id") or ""),
            "name": str(row.get("name") or ""),
            "site_code": str(row.get("site_code") or ""),
            "site_name": str(row.get("site_name") or ""),
        }
        for row in list_tenants(active_only=True)
    ]
    return {"ok": True, "enabled": bool(items), "items": items}


@router.post("/auth/register")
def auth_register(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    cleanup_expired_sessions()
    tenant_id = str(payload.get("tenant_id") or "").strip().lower()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="단지를 선택하세요.")
    tenant = get_tenant(tenant_id)
    if not tenant or str(tenant.get("status") or "") != "active":
        raise HTTPException(status_code=404, detail="가입 가능한 단지를 찾을 수 없습니다.")

    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name"))
    password = _clean_password(payload.get("password"))
    if get_staff_user_by_login(login_id):
        raise HTTPException(status_code=409, detail="이미 존재하는 아이디입니다.")

    phone = _clean_optional_text(payload.get("phone"), field_name="연락처", max_len=40)
    note = _clean_optional_text(payload.get("note"), field_name="메모", max_len=2000)
    created = create_staff_user(
        tenant_id=tenant_id,
        login_id=login_id,
        name=name,
        role="staff",
        phone=phone,
        note=(f"[self-register] {note}".strip() if note else "[self-register] 로그인 화면 회원등록"),
        password_hash=hash_password(password),
        is_active=0,
    )
    created.pop("password_hash", None)
    append_audit_log(tenant_id, "self_register", login_id, {"user_id": int(created["id"])})
    return {
        "ok": True,
        "message": "회원등록 요청이 접수되었습니다. 관리자 승인 후 로그인할 수 있습니다.",
        "item": _public_user(created),
        "tenant": tenant,
    }


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
        },
        {
            "module_key": "operations_admin",
            "module_name": "행정업무 모듈",
            "ui_path": "/pwa/",
            "api_prefix": "/api/ops",
            "auth_modes": ["session"],
        },
        {
            "module_key": "facility_ops",
            "module_name": "시설운영 모듈",
            "ui_path": "/pwa/",
            "api_prefix": "/api/facility",
            "auth_modes": ["session"],
        },
    ]
    return {
        "ok": True,
        "allowed_modules": ["complaint_engine", "operations_admin", "facility_ops"],
        "contracts": contracts,
    }


@router.get("/users")
def users_list(
    request: Request,
    active_only: bool = Query(default=True),
    tenant_id: str = Query(default=""),
) -> Dict[str, Any]:
    user, _token = _require_user_manager(request)
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
    user, _token = _require_user_manager(request)
    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name"))
    password = _clean_password(payload.get("password"))
    if get_staff_user_by_login(login_id):
        raise HTTPException(status_code=409, detail="이미 존재하는 아이디입니다.")

    is_super_admin = int(user.get("is_admin") or 0) == 1
    make_admin = is_super_admin and _truthy(payload.get("is_admin"))
    if not is_super_admin and (_truthy(payload.get("is_admin")) or _truthy(payload.get("is_site_admin"))):
        raise HTTPException(status_code=403, detail="현장관리자는 관리자 계정을 생성할 수 없습니다.")

    tenant_id = None if make_admin else _managed_tenant_id(user, payload)
    role = "super_admin" if make_admin else _clean_role(payload.get("role") or ("manager" if _truthy(payload.get("is_site_admin")) else "staff"))
    is_site_admin = 1 if (is_super_admin and not make_admin and _truthy(payload.get("is_site_admin"))) else 0
    created = create_staff_user(
        tenant_id=tenant_id,
        login_id=login_id,
        name=name,
        role=role,
        phone=_clean_optional_text(payload.get("phone"), field_name="연락처", max_len=40),
        password_hash=hash_password(password),
        note=_clean_optional_text(payload.get("note"), field_name="메모", max_len=2000),
        is_admin=1 if make_admin else 0,
        admin_scope="super_admin" if make_admin else None,
        is_site_admin=is_site_admin,
        is_active=1,
    )
    created.pop("password_hash", None)
    append_audit_log(str(created.get("tenant_id") or user.get("tenant_id") or ""), "create_user", str(user.get("login_id") or ""), {"login_id": login_id})
    return {"ok": True, "item": created}


@router.get("/users/{user_id}")
def users_get(request: Request, user_id: int) -> Dict[str, Any]:
    user, _token = _require_user_manager(request)
    item = get_staff_user(int(user_id))
    if not item:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    _assert_manageable_target(user, item, allow_view_only=True)
    return {"ok": True, "item": _public_user(item)}


@router.patch("/users/{user_id}")
def users_update(request: Request, user_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, _token = _require_user_manager(request)
    target = get_staff_user(int(user_id))
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    if int(target.get("id") or 0) == int(user.get("id") or 0):
        raise HTTPException(status_code=400, detail="본인 계정은 사용자관리 화면에서 수정할 수 없습니다.")
    _assert_manageable_target(user, target, allow_view_only=False)

    if int(user.get("is_admin") or 0) != 1 and "is_site_admin" in payload:
        raise HTTPException(status_code=403, detail="현장관리자는 관리자 권한을 변경할 수 없습니다.")

    next_active = payload.get("is_active")
    if next_active is not None:
        _assert_admin_guard(target, deactivating=not _truthy(next_active))

    item = update_staff_user(
        int(user_id),
        name=_clean_name(payload.get("name")) if "name" in payload else None,
        role=_clean_role(payload.get("role")) if "role" in payload else None,
        phone=_clean_optional_text(payload.get("phone"), field_name="연락처", max_len=40) if "phone" in payload else None,
        note=_clean_optional_text(payload.get("note"), field_name="메모", max_len=2000) if "note" in payload else None,
        is_site_admin=_truthy(payload.get("is_site_admin")) if "is_site_admin" in payload and int(user.get("is_admin") or 0) == 1 else None,
        is_active=_truthy(next_active) if next_active is not None else None,
    )
    if int(item.get("is_active") or 0) != 1:
        revoke_all_user_sessions(int(user_id))
    item.pop("password_hash", None)
    append_audit_log(
        str(item.get("tenant_id") or user.get("tenant_id") or ""),
        "update_user",
        str(user.get("login_id") or ""),
        {"user_id": int(user_id)},
    )
    return {"ok": True, "item": item}


@router.post("/users/{user_id}/approve")
def users_approve(request: Request, user_id: int) -> Dict[str, Any]:
    user, _token = _require_user_manager(request)
    target = get_staff_user(int(user_id))
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    if int(target.get("id") or 0) == int(user.get("id") or 0):
        raise HTTPException(status_code=400, detail="본인 계정은 승인할 수 없습니다.")
    _assert_manageable_target(user, target, allow_view_only=False)

    item = target
    if int(target.get("is_active") or 0) != 1:
        item = update_staff_user(
            int(user_id),
            name=str(target.get("name") or "").strip(),
            role=str(target.get("role") or "staff").strip() or "staff",
            phone=str(target.get("phone") or "").strip(),
            note=str(target.get("note") or "").strip(),
            is_site_admin=bool(target.get("is_site_admin")),
            is_active=True,
        )
    item.pop("password_hash", None)
    append_audit_log(
        str(item.get("tenant_id") or user.get("tenant_id") or ""),
        "approve_user",
        str(user.get("login_id") or ""),
        {"user_id": int(user_id), "login_id": item.get("login_id")},
    )
    return {"ok": True, "item": item}


@router.post("/users/{user_id}/reset_password")
def users_reset_password(request: Request, user_id: int, payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    user, _token = _require_user_manager(request)
    target = get_staff_user(int(user_id))
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    if int(target.get("id") or 0) == int(user.get("id") or 0):
        raise HTTPException(status_code=400, detail="본인 비밀번호는 비밀번호 변경 메뉴를 사용하세요.")
    _assert_manageable_target(user, target, allow_view_only=False)
    new_password = _clean_password(payload.get("password"), field_name="초기화 비밀번호")
    ok = set_staff_user_password(int(user_id), new_password)
    if not ok:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    revoke_all_user_sessions(int(user_id))
    append_audit_log(
        str(target.get("tenant_id") or user.get("tenant_id") or ""),
        "reset_user_password",
        str(user.get("login_id") or ""),
        {"user_id": int(user_id)},
    )
    fresh = get_staff_user(int(user_id)) or target
    return {"ok": True, "item": _public_user(fresh)}


@router.delete("/users/{user_id}")
def users_delete(request: Request, user_id: int) -> Dict[str, Any]:
    user, _token = _require_user_manager(request)
    target = get_staff_user(int(user_id))
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    if int(target.get("id") or 0) == int(user.get("id") or 0):
        raise HTTPException(status_code=400, detail="본인 계정은 삭제할 수 없습니다.")
    _assert_manageable_target(user, target, allow_view_only=False)
    _assert_admin_guard(target, deleting=True)
    revoke_all_user_sessions(int(user_id))
    deleted = delete_staff_user(int(user_id))
    deleted.pop("password_hash", None)
    append_audit_log(
        str(deleted.get("tenant_id") or user.get("tenant_id") or ""),
        "delete_user",
        str(user.get("login_id") or ""),
        {"user_id": int(user_id), "login_id": deleted.get("login_id")},
    )
    return {"ok": True, "item": deleted}


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


@router.post("/admin/legacy/import")
async def admin_legacy_import(
    request: Request,
    source_file: UploadFile = File(...),
    tenant_id: str = Form(...),
    tenant_name: str = Form(...),
    site_code: str = Form(default=""),
    site_name: str = Form(default=""),
    default_user_password: str = Form(default="ChangeMe123!"),
    dry_run: bool = Form(default=False),
) -> Dict[str, Any]:
    user, _token = _require_admin(request)
    filename = str(source_file.filename or "").strip()
    suffix = os.path.splitext(filename)[1].lower()
    if suffix not in {".db", ".sqlite", ".sqlite3", ".json"}:
        raise HTTPException(status_code=400, detail="지원하지 않는 이관 파일 형식입니다.")

    temp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            temp_path = handle.name
            while True:
                chunk = await source_file.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)

        item = import_legacy_source(
            source_path=temp_path,
            tenant_id=str(tenant_id or "").strip().lower(),
            tenant_name=str(tenant_name or "").strip(),
            site_code=str(site_code or "").strip(),
            site_name=str(site_name or "").strip(),
            default_user_password=str(default_user_password or "").strip() or "ChangeMe123!",
            dry_run=bool(dry_run),
        )
        append_audit_log(
            str(item.get("tenant_id") or "").strip().lower() or None,
            "admin_legacy_import",
            str(user.get("login_id") or user.get("name") or "admin"),
            {
                "source_filename": filename,
                "dry_run": bool(dry_run),
                "summary": item,
            },
        )
        return {"ok": True, "item": item}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"legacy import failed: {exc}") from exc
    finally:
        try:
            await source_file.close()
        except Exception:
            pass
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
