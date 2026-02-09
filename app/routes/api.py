from __future__ import annotations

import io
import json
import os
import re
import secrets
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from itsdangerous import URLSafeTimedSerializer

from ..db import (
    cleanup_expired_sessions,
    count_staff_admins,
    count_staff_users_for_site,
    create_signup_phone_verification,
    create_auth_session,
    create_staff_user,
    delete_entry,
    delete_staff_user,
    ensure_site,
    get_auth_user_by_token,
    get_latest_signup_phone_verification,
    get_site_env_config,
    get_site_env_record,
    get_first_staff_user_for_site,
    get_latest_home_complex_name,
    get_staff_user_by_phone,
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
    resolve_or_create_site_code,
    save_tab_values,
    schema_alignment_report,
    set_staff_user_site_code,
    set_staff_user_password,
    touch_signup_phone_verification_attempt,
    upsert_site_env_config,
    delete_site_env_config,
    update_staff_user,
    upsert_entry,
    upsert_tab_domain_data,
    verify_password,
)
from ..schema_defs import (
    SCHEMA_DEFS,
    SCHEMA_TAB_ORDER,
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
VALID_PERMISSION_LEVELS = ["admin", "site_admin", "user"]
DEFAULT_SITE_NAME = "미지정단지"
PHONE_VERIFY_TTL_MINUTES = 5
PHONE_VERIFY_MAX_ATTEMPTS = 5
PARKING_CONTEXT_SECRET = (
    os.getenv("PARKING_CONTEXT_SECRET")
    or os.getenv("PARKING_SECRET_KEY")
    or os.getenv("KA_PHONE_VERIFY_SECRET", "ka-part-dev-secret")
)
PARKING_CONTEXT_MAX_AGE = int(os.getenv("PARKING_CONTEXT_MAX_AGE", "300"))
PARKING_BASE_URL = (os.getenv("PARKING_BASE_URL") or "").strip()
PARKING_SSO_PATH = (os.getenv("PARKING_SSO_PATH") or "/parking/sso").strip()
if not PARKING_SSO_PATH.startswith("/"):
    PARKING_SSO_PATH = f"/{PARKING_SSO_PATH}"
_embed_raw = os.getenv("ENABLE_PARKING_EMBED")
if _embed_raw is None:
    PARKING_EMBED_ENABLED = (PARKING_BASE_URL == "")
else:
    PARKING_EMBED_ENABLED = _embed_raw.strip().lower() not in ("0", "false", "no", "off")
_parking_ctx_ser = URLSafeTimedSerializer(PARKING_CONTEXT_SECRET, salt="parking-context")


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


def _clean_site_code(value: Any, *, required: bool = False) -> str:
    site_code = (str(value or "")).strip().upper()
    if not site_code:
        if required:
            raise HTTPException(status_code=400, detail="site_code is required")
        return ""
    if len(site_code) > 32:
        raise HTTPException(status_code=400, detail="site_code length must be <= 32")
    if not re.match(r"^[A-Z0-9][A-Z0-9._-]{0,31}$", site_code):
        raise HTTPException(status_code=400, detail="site_code must match ^[A-Z0-9][A-Z0-9._-]{0,31}$")
    return site_code


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


def _clean_required_text(value: Any, max_len: int, field_name: str) -> str:
    txt = (str(value or "")).strip()
    if not txt:
        raise HTTPException(status_code=400, detail=f"{field_name} is required")
    if len(txt) > max_len:
        raise HTTPException(status_code=400, detail=f"{field_name} length must be <= {max_len}")
    return txt


def _format_phone_digits(digits: str) -> str:
    if len(digits) == 11:
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if len(digits) == 10 and digits.startswith("02"):
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 9 and digits.startswith("02"):
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
    return digits


def _normalize_phone(value: Any, *, required: bool, field_name: str) -> str | None:
    raw = (str(value or "")).strip()
    if not raw:
        if required:
            raise HTTPException(status_code=400, detail=f"{field_name} is required")
        return None
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("82") and len(digits) >= 11:
        digits = "0" + digits[2:]
    if len(digits) < 9 or len(digits) > 11:
        raise HTTPException(status_code=400, detail=f"{field_name} format is invalid")
    return _format_phone_digits(digits)


def _phone_digits(formatted_phone: str) -> str:
    return re.sub(r"\D", "", str(formatted_phone or ""))


def _phone_code_hash(phone: str, code: str) -> str:
    secret = os.getenv("KA_PHONE_VERIFY_SECRET", "ka-part-dev-secret")
    src = f"{secret}|{phone}|{code}"
    import hashlib

    return hashlib.sha256(src.encode("utf-8")).hexdigest()


def _send_sms_verification(phone: str, code: str) -> Dict[str, Any]:
    message = f"[아파트 시설관리] 인증번호 {code} (유효 {PHONE_VERIFY_TTL_MINUTES}분)"
    webhook = (os.getenv("KA_SMS_WEBHOOK_URL") or "").strip()
    if not webhook:
        return {
            "delivery": "mock",
            "debug_code": code,
            "message": "KA_SMS_WEBHOOK_URL 미설정 상태입니다. 현재는 화면에 인증번호를 표시합니다.",
        }

    payload = json.dumps({"to": phone, "message": message}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            if int(getattr(resp, "status", 500)) >= 300:
                raise HTTPException(status_code=502, detail="sms delivery failed")
    except urllib.error.URLError as e:
        raise HTTPException(status_code=502, detail=f"sms delivery failed: {e.reason}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"sms delivery failed: {e}") from e
    return {"delivery": "sms"}


def _generate_login_id_from_phone(phone: str) -> str:
    digits = _phone_digits(phone)
    base = f"u{digits[-8:]}" if digits else "user"
    base = re.sub(r"[^a-zA-Z0-9._-]", "", base).lower()
    if len(base) < 2:
        base = "user00"
    base = base[:24]
    candidate = base
    seq = 0
    while get_staff_user_by_login(candidate):
        seq += 1
        suffix = f".{seq}"
        candidate = f"{base[: max(2, 32 - len(suffix))]}{suffix}"
    return candidate


def _generate_temp_password(length: int = 10) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    n = max(8, int(length))
    return "".join(secrets.choice(alphabet) for _ in range(n))


def _clean_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    return s in ("1", "true", "y", "yes", "on")


def _permission_level_from_user(user: Dict[str, Any]) -> str:
    if int(user.get("is_admin") or 0) == 1:
        return "admin"
    if int(user.get("is_site_admin") or 0) == 1:
        return "site_admin"
    return "user"


def _clean_permission_level(value: Any, *, required: bool = False) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        if required:
            raise HTTPException(status_code=400, detail="permission_level is required")
        return ""
    if raw not in VALID_PERMISSION_LEVELS:
        raise HTTPException(status_code=400, detail=f"permission_level must be one of: {', '.join(VALID_PERMISSION_LEVELS)}")
    return raw


def _resolve_permission_flags(payload: Dict[str, Any], *, default_admin: bool = False, default_site_admin: bool = False) -> Tuple[int, int, str]:
    if "permission_level" in payload:
        level = _clean_permission_level(payload.get("permission_level"), required=True)
        if level == "admin":
            return 1, 0, level
        if level == "site_admin":
            return 0, 1, level
        return 0, 0, level

    is_admin = _clean_bool(payload.get("is_admin"), default=default_admin)
    is_site_admin = _clean_bool(payload.get("is_site_admin"), default=default_site_admin)
    if is_admin:
        is_site_admin = False
    level = "admin" if is_admin else ("site_admin" if is_site_admin else "user")
    return (1 if is_admin else 0), (1 if is_site_admin else 0), level


def _resolve_site_code_for_site(site_name: str, site_code: str = "") -> str:
    try:
        return resolve_or_create_site_code(site_name, preferred_code=(site_code or None))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"site_code resolve failed: {e}") from e


def _bind_user_site_code_if_missing(user: Dict[str, Any]) -> Dict[str, Any]:
    if not user:
        return user
    if int(user.get("is_admin") or 0) == 1:
        return user
    current_code = _clean_site_code(user.get("site_code"), required=False)
    if current_code:
        return user
    site_name = str(user.get("site_name") or "").strip()
    if not site_name:
        return user

    resolved = _resolve_site_code_for_site(site_name, "")
    if not resolved:
        return user
    uid = int(user.get("id") or 0)
    if uid > 0:
        set_staff_user_site_code(uid, resolved)
        fresh = get_staff_user(uid)
        if fresh:
            merged = dict(user)
            merged.update(fresh)
            return merged
    return user


def _public_user(user: Dict[str, Any]) -> Dict[str, Any]:
    permission_level = _permission_level_from_user(user)
    return {
        "id": int(user.get("id")),
        "login_id": user.get("login_id"),
        "name": user.get("name"),
        "role": user.get("role"),
        "phone": user.get("phone"),
        "site_code": user.get("site_code"),
        "site_name": user.get("site_name"),
        "address": user.get("address"),
        "office_phone": user.get("office_phone"),
        "office_fax": user.get("office_fax"),
        "note": user.get("note"),
        "is_admin": bool(user.get("is_admin")),
        "is_site_admin": bool(user.get("is_site_admin")),
        "permission_level": permission_level,
        "is_active": bool(user.get("is_active")),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
        "last_login_at": user.get("last_login_at"),
    }


def _site_schema_and_env(site_name: str, site_code: str = "") -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    raw = get_site_env_config(site_name, site_code=site_code or None)
    if raw is None:
        env_cfg = default_site_env_config()
    else:
        env_cfg = normalize_site_env_config(raw)
    effective_cfg = merge_site_env_configs(default_site_env_config(), env_cfg)
    schema = build_effective_schema(base_schema=SCHEMA_DEFS, site_env_config=effective_cfg)
    return schema, env_cfg


def _resolve_allowed_site_code(user: Dict[str, Any], requested_site_code: Any) -> str:
    requested = _clean_site_code(requested_site_code, required=False)
    if int(user.get("is_admin") or 0) == 1:
        return requested

    assigned = _clean_site_code(user.get("site_code"), required=False)
    if assigned:
        if requested and requested != assigned:
            raise HTTPException(status_code=403, detail="소속 단지코드 데이터만 접근할 수 있습니다.")
        return assigned
    if requested:
        raise HTTPException(status_code=403, detail="소속 단지코드(site_code)가 지정되지 않았습니다. 관리자에게 문의하세요.")
    return ""


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
    user = _bind_user_site_code_if_missing(user)
    return user, token


def _require_admin(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if int(user.get("is_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="admin only")
    return user, token


def _can_manage_site_env(user: Dict[str, Any]) -> bool:
    return int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1


def _require_site_env_manager(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if not _can_manage_site_env(user):
        raise HTTPException(status_code=403, detail="site env manager only")
    return user, token


def _home_only_site_env_config() -> Dict[str, Any]:
    hide_tabs = [tab for tab in SCHEMA_TAB_ORDER if tab != "home"]
    return normalize_site_env_config({"hide_tabs": hide_tabs, "tabs": {"home": {"title": "홈"}}})


def _verify_first_site_registrant_for_spec_env(user: Dict[str, Any], site_name: str, site_code: str = "") -> Dict[str, Any]:
    if int(user.get("is_admin") or 0) == 1:
        return {"verified": True, "home_site_name": site_name, "first_user_id": None}

    if int(user.get("is_site_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="제원설정 권한이 없습니다.")

    assigned_site_name = _normalized_assigned_site_name(user)
    assigned_site_code = _clean_site_code(user.get("site_code"), required=False)
    target_site_name = _clean_site_name(site_name or assigned_site_name, required=True)
    target_site_code = _clean_site_code(site_code or assigned_site_code, required=False)

    home_complex_name = _clean_site_name(get_latest_home_complex_name(target_site_name) or target_site_name, required=True)
    if home_complex_name != assigned_site_name:
        raise HTTPException(
            status_code=403,
            detail="홈 탭 단지명 재확인에 실패했습니다. 소속 단지와 홈 단지명이 일치할 때만 제원설정을 사용할 수 있습니다.",
        )

    first_user = get_first_staff_user_for_site(home_complex_name, site_code=target_site_code or assigned_site_code or None)
    if not first_user:
        raise HTTPException(status_code=403, detail="최초가입자 정보를 찾을 수 없습니다. 관리자에게 문의하세요.")
    if int(first_user.get("id") or 0) != int(user.get("id") or 0):
        raise HTTPException(status_code=403, detail="해당 단지의 최초가입자만 제원설정을 사용할 수 있습니다.")

    return {
        "verified": True,
        "home_site_name": home_complex_name,
        "first_user_id": int(first_user.get("id") or 0),
        "first_login_id": str(first_user.get("login_id") or ""),
    }


def _is_first_site_registrant(user: Dict[str, Any], site_name: str, site_code: str = "") -> bool:
    if int(user.get("is_admin") or 0) == 1:
        return False
    uid = int(user.get("id") or 0)
    if uid <= 0:
        return False

    clean_site_name = _clean_site_name(site_name, required=True)
    clean_site_code = _clean_site_code(site_code, required=False)
    first_user = get_first_staff_user_for_site(clean_site_name, site_code=clean_site_code or None)
    if not first_user:
        return False
    return int(first_user.get("id") or 0) == uid


def _normalized_assigned_site_name(user: Dict[str, Any]) -> str:
    raw_site = (str(user.get("site_name") or "")).strip()
    if not raw_site:
        raise HTTPException(status_code=403, detail="소속 단지(site_name)가 지정되지 않았습니다. 관리자에게 문의하세요.")
    return _clean_site_name(raw_site, required=True)


def _resolve_allowed_site_name(user: Dict[str, Any], requested_site_name: Any, *, required: bool = False) -> str:
    if int(user.get("is_admin") or 0) == 1:
        return _clean_site_name(requested_site_name, required=required)

    assigned_site = _normalized_assigned_site_name(user)
    raw_requested = (str(requested_site_name or "")).strip()
    if not raw_requested:
        return assigned_site

    requested = _clean_site_name(raw_requested, required=required)
    if requested != assigned_site:
        raise HTTPException(status_code=403, detail="소속 단지 데이터만 접근할 수 있습니다.")
    return assigned_site


def _require_site_access(request: Request, requested_site_name: Any, *, required: bool = False) -> Tuple[Dict[str, Any], str, str]:
    user, token = _require_auth(request)
    allowed_site_name = _resolve_allowed_site_name(user, requested_site_name, required=required)
    return user, token, allowed_site_name


@router.get("/health")
def health():
    report = schema_alignment_report()
    return {"ok": True, "version": "2.9.0", "schema_alignment_ok": report.get("ok", False)}


@router.get("/parking/context")
def parking_context(request: Request):
    user, _token = _require_auth(request)
    permission_level = _permission_level_from_user(user)
    site_code = _clean_site_code(user.get("site_code"), required=False)
    if not site_code:
        site_name = _resolve_allowed_site_name(user, "", required=False)
        site_code = _resolve_site_code_for_site(site_name, "")
        if int(user.get("id") or 0) > 0 and site_code:
            set_staff_user_site_code(int(user["id"]), site_code)
    if not site_code:
        raise HTTPException(status_code=403, detail="site_code is required for parking access")
    if (not PARKING_EMBED_ENABLED) and (not PARKING_BASE_URL):
        raise HTTPException(status_code=503, detail="parking gateway misconfigured: PARKING_BASE_URL is required")

    ctx = _parking_ctx_ser.dumps({"site_code": site_code, "permission_level": permission_level})
    rel = f"{PARKING_SSO_PATH}?ctx={urllib.parse.quote(ctx, safe='')}"
    parking_url = rel if not PARKING_BASE_URL else f"{PARKING_BASE_URL.rstrip('/')}{rel}"
    return {
        "ok": True,
        "url": parking_url,
        "site_code": site_code,
        "permission_level": permission_level,
        "expires_in": PARKING_CONTEXT_MAX_AGE,
    }


@router.get("/schema")
def api_schema(request: Request, site_name: str = Query(default=""), site_code: str = Query(default="")):
    user, _token, clean_site_name = _require_site_access(request, site_name, required=False)
    clean_site_code = _resolve_allowed_site_code(user, site_code)
    resolved_site_code = clean_site_code
    if not resolved_site_code:
        row = get_site_env_record(clean_site_name)
        if row:
            resolved_site_code = _clean_site_code(row.get("site_code"), required=False)
    schema, env_cfg = _site_schema_and_env(clean_site_name, resolved_site_code)

    # On first login for the first site registrant, show only the Home tab
    # until the site env is explicitly saved.
    if int(user.get("is_site_admin") or 0) == 1 and _is_first_site_registrant(user, clean_site_name, resolved_site_code):
        row = get_site_env_record(clean_site_name, site_code=resolved_site_code or None)
        if row is None:
            env_cfg = _home_only_site_env_config()
            schema = build_effective_schema(
                base_schema=SCHEMA_DEFS,
                site_env_config=merge_site_env_configs(default_site_env_config(), env_cfg),
            )

    return {"schema": schema, "site_name": clean_site_name, "site_code": resolved_site_code, "site_env_config": env_cfg}


@router.get("/schema_alignment")
def api_schema_alignment(request: Request):
    _require_admin(request)
    return schema_alignment_report()


@router.get("/site_env_template")
def api_site_env_template(request: Request):
    _require_site_env_manager(request)
    return {"ok": True, "template": site_env_template()}


@router.get("/site_env_templates")
def api_site_env_templates(request: Request):
    _require_site_env_manager(request)
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
    _require_site_env_manager(request)
    return {"ok": True, "schema": SCHEMA_DEFS}


@router.get("/site_env")
def api_site_env(request: Request, site_name: str = Query(default=""), site_code: str = Query(default="")):
    user, _token = _require_site_env_manager(request)
    if int(user.get("is_admin") or 0) == 1:
        clean_site_name = _clean_site_name(site_name, required=True)
        clean_site_code = _clean_site_code(site_code, required=False)
    else:
        clean_site_name = _resolve_allowed_site_name(user, site_name, required=False)
        clean_site_code = _resolve_allowed_site_code(user, site_code)
    verify = _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)

    row = get_site_env_record(clean_site_name, site_code=clean_site_code or None)
    resolved_site_code = _clean_site_code((row or {}).get("site_code"), required=False) or clean_site_code
    schema, env_cfg = _site_schema_and_env(clean_site_name, resolved_site_code)
    if row is None and int(user.get("is_site_admin") or 0) == 1 and int(user.get("is_admin") or 0) != 1:
        env_cfg = _home_only_site_env_config()
        schema = build_effective_schema(
            base_schema=SCHEMA_DEFS,
            site_env_config=merge_site_env_configs(default_site_env_config(), env_cfg),
        )
    return {
        "ok": True,
        "site_name": clean_site_name,
        "site_code": resolved_site_code,
        "config": env_cfg,
        "schema": schema,
        "spec_access": verify,
    }


@router.put("/site_env")
def api_site_env_upsert(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_site_env_manager(request)
    if int(user.get("is_admin") or 0) == 1:
        clean_site_name = _clean_site_name(payload.get("site_name"), required=True)
        clean_site_code = _clean_site_code(payload.get("site_code"), required=False)
    else:
        clean_site_name = _resolve_allowed_site_name(user, payload.get("site_name"), required=False)
        clean_site_code = _resolve_allowed_site_code(user, payload.get("site_code"))
    verify = _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)
    clean_site_code = _resolve_site_code_for_site(clean_site_name, clean_site_code)
    raw_cfg = payload.get("config", payload.get("env", payload if isinstance(payload, dict) else {}))
    cfg = normalize_site_env_config(raw_cfg if isinstance(raw_cfg, dict) else {})
    row = upsert_site_env_config(clean_site_name, cfg, site_code=clean_site_code or None)
    resolved_site_code = _clean_site_code(row.get("site_code"), required=False)
    schema, _env_cfg = _site_schema_and_env(clean_site_name, resolved_site_code)
    return {
        "ok": True,
        "site_name": clean_site_name,
        "site_code": resolved_site_code,
        "config": cfg,
        "schema": schema,
        "updated_at": row.get("updated_at"),
        "spec_access": verify,
    }


@router.delete("/site_env")
def api_site_env_delete(request: Request, site_name: str = Query(default=""), site_code: str = Query(default="")):
    user, _token = _require_site_env_manager(request)
    if int(user.get("is_admin") or 0) == 1:
        raw_site_name = (str(site_name or "")).strip()
        clean_site_name = _clean_site_name(raw_site_name, required=True) if raw_site_name else ""
        clean_site_code = _clean_site_code(site_code, required=False)
    else:
        clean_site_name = _resolve_allowed_site_name(user, site_name, required=False)
        clean_site_code = _resolve_allowed_site_code(user, site_code)
    _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)
    if not clean_site_name and not clean_site_code:
        raise HTTPException(status_code=400, detail="site_name or site_code is required")
    ok = delete_site_env_config(clean_site_name, site_code=clean_site_code or None)
    return {"ok": ok, "site_name": clean_site_name, "site_code": clean_site_code}


@router.get("/site_env_list")
def api_site_env_list(request: Request):
    user, _token = _require_site_env_manager(request)
    rows = list_site_env_configs()
    if int(user.get("is_admin") or 0) != 1:
        assigned_name = _normalized_assigned_site_name(user)
        assigned_code = _clean_site_code(user.get("site_code"), required=False)
        _verify_first_site_registrant_for_spec_env(user, assigned_name, assigned_code)
        filtered: List[Dict[str, Any]] = []
        for r in rows:
            row_code = _clean_site_code(r.get("site_code"), required=False)
            row_name = _clean_site_name(r.get("site_name"), required=False)
            if assigned_code:
                if row_code == assigned_code:
                    filtered.append(r)
            elif row_name == assigned_name:
                filtered.append(r)
        rows = filtered
    return {
        "ok": True,
        "count": len(rows),
        "items": [
            {
                "site_name": r.get("site_name"),
                "site_code": r.get("site_code"),
                "updated_at": r.get("updated_at"),
            }
            for r in rows
        ],
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
            site_code=existing.get("site_code"),
            site_name=existing.get("site_name"),
            address=existing.get("address"),
            office_phone=existing.get("office_phone"),
            office_fax=existing.get("office_fax"),
            note=existing.get("note"),
            is_admin=1,
            is_site_admin=0,
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
            is_site_admin=0,
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


@router.post("/auth/signup/request_phone_verification")
def auth_signup_request_phone_verification(request: Request, payload: Dict[str, Any] = Body(...)):
    name = _clean_name(payload.get("name"))
    phone = _normalize_phone(payload.get("phone"), required=True, field_name="phone")
    site_code = _clean_site_code(payload.get("site_code"), required=False)
    site_name = _clean_required_text(payload.get("site_name"), 80, "site_name")
    role = _clean_role(payload.get("role"))
    address = _clean_required_text(payload.get("address"), 200, "address")
    office_phone = _normalize_phone(payload.get("office_phone"), required=True, field_name="office_phone")
    office_fax = _normalize_phone(payload.get("office_fax"), required=True, field_name="office_fax")

    code = f"{secrets.randbelow(1000000):06d}"
    code_hash = _phone_code_hash(phone, code)
    expires_at = (datetime.now() + timedelta(minutes=PHONE_VERIFY_TTL_MINUTES)).replace(microsecond=0).isoformat(sep=" ")
    profile = {
        "name": name,
        "phone": phone,
        "site_code": site_code,
        "site_name": site_name,
        "role": role,
        "address": address,
        "office_phone": office_phone,
        "office_fax": office_fax,
    }

    create_signup_phone_verification(
        phone=phone,
        code_hash=code_hash,
        payload=profile,
        expires_at=expires_at,
        request_ip=(request.client.host if request.client else None),
    )
    delivery = _send_sms_verification(phone, code)
    out = {
        "ok": True,
        "phone": phone,
        "expires_at": expires_at,
        "expires_in_sec": PHONE_VERIFY_TTL_MINUTES * 60,
        "delivery": delivery.get("delivery") or "sms",
        "message": "인증번호를 전송했습니다.",
    }
    if delivery.get("message"):
        out["message"] = str(delivery["message"])
    if delivery.get("debug_code"):
        out["debug_code"] = str(delivery["debug_code"])
    return out


@router.post("/auth/signup/verify_phone_and_issue_id")
def auth_signup_verify_phone_and_issue_id(payload: Dict[str, Any] = Body(...)):
    phone = _normalize_phone(payload.get("phone"), required=True, field_name="phone")
    code = str(payload.get("code") or "").strip()
    if not re.match(r"^\d{6}$", code):
        raise HTTPException(status_code=400, detail="code must be 6 digits")

    row = get_latest_signup_phone_verification(phone)
    if not row:
        raise HTTPException(status_code=404, detail="verification request not found")
    if row.get("consumed_at"):
        raise HTTPException(status_code=409, detail="verification already used; request a new code")
    if str(row.get("expires_at") or "") <= datetime.now().replace(microsecond=0).isoformat(sep=" "):
        raise HTTPException(status_code=410, detail="verification expired; request a new code")

    attempt_count = int(row.get("attempt_count") or 0)
    if attempt_count >= PHONE_VERIFY_MAX_ATTEMPTS:
        raise HTTPException(status_code=429, detail="too many attempts; request a new code")

    expected = _phone_code_hash(phone, code)
    if str(row.get("code_hash") or "") != expected:
        touch_signup_phone_verification_attempt(int(row["id"]), success=False)
        remain = max(0, PHONE_VERIFY_MAX_ATTEMPTS - (attempt_count + 1))
        raise HTTPException(status_code=401, detail=f"invalid code; remaining attempts: {remain}")

    existing = get_staff_user_by_phone(phone)
    if existing:
        existing_site_name = _clean_required_text(existing.get("site_name"), 80, "site_name")
        existing_site_code = _clean_site_code(existing.get("site_code"), required=False)
        if not existing_site_code:
            resolved_code = _resolve_site_code_for_site(existing_site_name, "")
            set_staff_user_site_code(int(existing["id"]), resolved_code)
            existing = get_staff_user(int(existing["id"])) or existing
        touch_signup_phone_verification_attempt(int(row["id"]), success=True, issued_login_id=str(existing.get("login_id") or ""))
        return {
            "ok": True,
            "already_registered": True,
            "login_id": existing.get("login_id"),
            "temporary_password": None,
            "user": _public_user(existing),
            "message": "이미 등록된 휴대폰번호입니다. 기존 아이디를 안내합니다.",
        }

    profile = {}
    try:
        profile = json.loads(str(row.get("payload_json") or "{}"))
    except Exception:
        profile = {}

    name = _clean_name(profile.get("name"))
    role = _clean_role(profile.get("role"))
    site_code = _clean_site_code(profile.get("site_code"), required=False)
    site_name = _clean_required_text(profile.get("site_name"), 80, "site_name")
    address = _clean_required_text(profile.get("address"), 200, "address")
    office_phone = _normalize_phone(profile.get("office_phone"), required=True, field_name="office_phone")
    office_fax = _normalize_phone(profile.get("office_fax"), required=True, field_name="office_fax")

    resolved_site_code = _resolve_site_code_for_site(site_name, site_code)
    existing_site_user_count = count_staff_users_for_site(site_name, site_code=resolved_site_code)
    login_id = _generate_login_id_from_phone(phone)
    temp_password = _generate_temp_password(10)
    user = create_staff_user(
        login_id=login_id,
        name=name,
        role=role,
        phone=phone,
        site_code=resolved_site_code,
        site_name=site_name,
        address=address,
        office_phone=office_phone,
        office_fax=office_fax,
        note="자가가입(휴대폰 인증)",
        password_hash=hash_password(temp_password),
        is_admin=0,
        is_site_admin=1 if existing_site_user_count == 0 else 0,
        is_active=1,
    )
    touch_signup_phone_verification_attempt(int(row["id"]), success=True, issued_login_id=login_id)
    return {
        "ok": True,
        "already_registered": False,
        "login_id": login_id,
        "temporary_password": temp_password,
        "user": _public_user(user),
        "must_change_password": True,
        "message": "가입이 완료되었습니다. 발급된 아이디/임시비밀번호로 로그인 후 비밀번호를 변경하세요.",
    }


@router.post("/auth/login")
def auth_login(request: Request, payload: Dict[str, Any] = Body(...)):
    login_id = _clean_login_id(payload.get("login_id"))
    password = _clean_password(payload.get("password"), required=True)

    user = get_staff_user_by_login(login_id)
    if not user or int(user.get("is_active") or 0) != 1:
        raise HTTPException(status_code=401, detail="invalid credentials")
    user = _bind_user_site_code_if_missing(user)
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
    return {
        "ok": True,
        "roles": VALID_USER_ROLES,
        "permission_levels": [
            {"key": "admin", "label": "관리자"},
            {"key": "site_admin", "label": "단지관리자"},
            {"key": "user", "label": "사용자"},
        ],
        "recommended_staff_count": 9,
    }


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
    phone = _normalize_phone(payload.get("phone"), required=False, field_name="phone")
    site_code = _clean_site_code(payload.get("site_code"), required=False)
    site_name = _clean_optional_text(payload.get("site_name"), 80)
    if site_name:
        site_code = _resolve_site_code_for_site(site_name, site_code)
    address = _clean_optional_text(payload.get("address"), 200)
    office_phone = _normalize_phone(payload.get("office_phone"), required=False, field_name="office_phone")
    office_fax = _normalize_phone(payload.get("office_fax"), required=False, field_name="office_fax")
    note = _clean_optional_text(payload.get("note"), 200)
    password = _clean_password(payload.get("password"), required=True)
    is_admin, is_site_admin, _permission_level = _resolve_permission_flags(payload, default_admin=False, default_site_admin=False)
    is_active = 1 if _clean_bool(payload.get("is_active"), default=True) else 0
    try:
        user = create_staff_user(
            login_id=login_id,
            name=name,
            role=role,
            phone=phone,
            site_code=site_code,
            site_name=site_name,
            address=address,
            office_phone=office_phone,
            office_fax=office_fax,
            note=note,
            password_hash=hash_password(password),
            is_admin=is_admin,
            is_site_admin=is_site_admin,
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
    phone = _normalize_phone(payload["phone"] if "phone" in payload else current.get("phone"), required=False, field_name="phone")
    site_code = _clean_site_code(payload["site_code"] if "site_code" in payload else current.get("site_code"), required=False)
    site_name = _clean_optional_text(payload["site_name"] if "site_name" in payload else current.get("site_name"), 80)
    if site_name:
        site_code = _resolve_site_code_for_site(site_name, site_code)
    address = _clean_optional_text(payload["address"] if "address" in payload else current.get("address"), 200)
    office_phone = _normalize_phone(
        payload["office_phone"] if "office_phone" in payload else current.get("office_phone"),
        required=False,
        field_name="office_phone",
    )
    office_fax = _normalize_phone(
        payload["office_fax"] if "office_fax" in payload else current.get("office_fax"),
        required=False,
        field_name="office_fax",
    )
    note = _clean_optional_text(payload["note"] if "note" in payload else current.get("note"), 200)

    is_admin, is_site_admin, _permission_level = _resolve_permission_flags(
        payload,
        default_admin=bool(current.get("is_admin")),
        default_site_admin=bool(current.get("is_site_admin")),
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
            site_code=site_code,
            site_name=site_name,
            address=address,
            office_phone=office_phone,
            office_fax=office_fax,
            note=note,
            is_admin=1 if is_admin else 0,
            is_site_admin=1 if is_site_admin else 0,
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
    _user, _token, site_name = _require_site_access(request, payload.get("site_name"), required=False)
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
    _user, _token, site_name = _require_site_access(request, site_name, required=True)
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    schema, _env_cfg = _site_schema_and_env(site_name)
    tabs = load_entry(site_id, entry_date, allowed_keys_by_tab=_schema_allowed_keys(schema))
    return {"ok": True, "site_name": site_name, "date": entry_date, "tabs": tabs}


@router.delete("/delete")
def api_delete(request: Request, site_name: str = Query(...), date: str = Query(...)):
    _user, _token, site_name = _require_site_access(request, site_name, required=True)
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
    _user, _token, site_name = _require_site_access(request, site_name, required=True)
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
    _user, _token, site_name = _require_site_access(request, site_name, required=True)
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
    _user, _token, site_name = _require_site_access(request, site_name, required=True)
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
