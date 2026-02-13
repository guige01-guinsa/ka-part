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
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from itsdangerous import URLSafeTimedSerializer

from ..backup_manager import (
    backup_timezone_name,
    clear_maintenance_mode,
    get_backup_item,
    get_maintenance_status,
    list_backup_history,
    list_backup_targets,
    resolve_backup_file,
    restore_backup_zip,
    run_manual_backup,
)
from ..db import (
    cleanup_expired_sessions,
    count_active_resident_household_users,
    count_staff_admins,
    count_super_admins,
    count_staff_users_for_site,
    create_signup_phone_verification,
    create_auth_session,
    create_privileged_change_request,
    create_staff_user,
    delete_entry,
    delete_site_env_config,
    delete_staff_user,
    ensure_site,
    get_auth_user_by_token,
    get_privileged_change_request,
    get_latest_signup_phone_verification,
    find_site_code_by_name,
    find_site_name_by_code,
    get_site_env_config,
    get_site_env_record,
    get_site_env_config_version,
    get_first_staff_user_for_site,
    get_staff_user_by_phone,
    get_staff_user,
    get_staff_user_by_login,
    hash_password,
    list_entries,
    list_privileged_change_requests,
    list_security_audit_logs,
    list_site_env_config_versions,
    list_site_env_configs,
    list_staff_users,
    load_entry,
    load_entry_by_id,
    mark_staff_user_login,
    mark_privileged_change_request_executed,
    migrate_site_code,
    approve_privileged_change_request,
    revoke_all_user_sessions,
    revoke_auth_session,
    resolve_or_create_site_code,
    rollback_site_env_config_version,
    save_tab_values,
    schema_alignment_report,
    set_staff_user_site_code,
    set_staff_user_password,
    touch_signup_phone_verification_attempt,
    upsert_site_env_config,
    update_staff_user,
    upsert_entry,
    upsert_tab_domain_data,
    verify_password,
    write_security_audit_log,
)
from ..schema_defs import (
    DEFAULT_INITIAL_VISIBLE_TAB_KEYS,
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

VALID_USER_ROLES = [
    "서버관리자",
    "관리소장",
    "부장",
    "과장",
    "대리",
    "주임",
    "경리",
    "보안/경비",
    "미화원",
    "세대주민",
    "입대의",
]
VALID_PERMISSION_LEVELS = ["admin", "site_admin", "user", "security_guard", "resident", "board_member"]
VALID_ADMIN_SCOPES = ["super_admin", "ops_admin"]
ADMIN_SCOPE_LABELS = {
    "super_admin": "최고관리자",
    "ops_admin": "운영관리자",
}
RESIDENT_ROLE_SET = {"입주민", "주민", "세대주민"}
BOARD_ROLE_SET = {"입대의", "입주자대표", "입주자대표회의"}
SECURITY_ROLE_KEYWORDS = ("보안", "경비")
DEFAULT_SITE_NAME = "미지정단지"
PHONE_VERIFY_TTL_MINUTES = 5
PHONE_VERIFY_MAX_ATTEMPTS = 5
PARKING_CONTEXT_MAX_AGE = int(os.getenv("PARKING_CONTEXT_MAX_AGE", "300"))
PARKING_BASE_URL = (os.getenv("PARKING_BASE_URL") or "").strip()
_embed_raw = os.getenv("ENABLE_PARKING_EMBED")
if _embed_raw is None:
    PARKING_EMBED_ENABLED = True
else:
    PARKING_EMBED_ENABLED = _embed_raw.strip().lower() not in ("0", "false", "no", "off")
_raw_sso_path = (os.getenv("PARKING_SSO_PATH") or "").strip()
if not _raw_sso_path:
    # Default differs by runtime mode:
    # - embedded parking in ka-part: /parking/sso
    # - external parking_man gateway: /sso
    _raw_sso_path = "/parking/sso" if PARKING_EMBED_ENABLED else "/sso"
PARKING_SSO_PATH = _raw_sso_path if _raw_sso_path.startswith("/") else f"/{_raw_sso_path}"
# Safety guard: embedded mode cannot use root /sso on ka-part host.
if PARKING_EMBED_ENABLED and PARKING_SSO_PATH == "/sso":
    PARKING_SSO_PATH = "/parking/sso"

_TRUTHY = {"1", "true", "yes", "on"}
_WEAK_SECRET_MARKERS = {
    "change-me",
    "change-this-secret",
    "ka-part-dev-secret",
    "parking-dev-secret-change-me",
}
AUTH_COOKIE_NAME = (os.getenv("KA_AUTH_COOKIE_NAME") or "ka_part_auth_token").strip()
AUTH_COOKIE_SAMESITE = (os.getenv("KA_AUTH_COOKIE_SAMESITE") or "lax").strip().lower() or "lax"
if AUTH_COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    AUTH_COOKIE_SAMESITE = "lax"


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in _TRUTHY


def _safe_int_env(name: str, default: int, minimum: int) -> int:
    raw = (os.getenv(name) or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except Exception:
        value = int(default)
    return max(minimum, value)


ALLOW_INSECURE_DEFAULTS = _env_enabled("ALLOW_INSECURE_DEFAULTS", False)
AUTH_COOKIE_SECURE = _env_enabled("KA_AUTH_COOKIE_SECURE", True)
AUTH_COOKIE_MAX_AGE = _safe_int_env("KA_AUTH_COOKIE_MAX_AGE", 43200, 300)


def _require_secret_env(
    names: Tuple[str, ...],
    label: str,
    *,
    min_len: int = 16,
) -> str:
    for key in names:
        value = (os.getenv(key) or "").strip()
        if not value:
            continue
        lowered = value.lower()
        if lowered in _WEAK_SECRET_MARKERS and not ALLOW_INSECURE_DEFAULTS:
            raise RuntimeError(f"{label} uses an insecure default-like value ({key})")
        if len(value) < min_len and not ALLOW_INSECURE_DEFAULTS:
            raise RuntimeError(f"{label} must be at least {min_len} characters ({key})")
        return value
    generated = secrets.token_urlsafe(max(24, min_len))
    if names:
        os.environ.setdefault(names[0], generated)
    return generated


PARKING_CONTEXT_SECRET_VALUE = _require_secret_env(
    ("PARKING_CONTEXT_SECRET", "PARKING_SECRET_KEY", "KA_PHONE_VERIFY_SECRET"),
    "PARKING_CONTEXT_SECRET",
    min_len=24,
)
PHONE_VERIFY_SECRET_VALUE = _require_secret_env(
    ("KA_PHONE_VERIFY_SECRET", "PARKING_CONTEXT_SECRET", "PARKING_SECRET_KEY"),
    "KA_PHONE_VERIFY_SECRET",
    min_len=24,
)


def _parking_context_secret() -> str:
    return PARKING_CONTEXT_SECRET_VALUE


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


def _normalized_role_text(value: Any) -> str:
    return str(value or "").strip()


def _is_resident_role(value: Any) -> bool:
    return _normalized_role_text(value) in RESIDENT_ROLE_SET


def _is_board_role(value: Any) -> bool:
    return _normalized_role_text(value) in BOARD_ROLE_SET


def _is_complaints_only_role(value: Any) -> bool:
    return _is_resident_role(value) or _is_board_role(value)


def _is_security_role(value: Any) -> bool:
    role = _normalized_role_text(value)
    if not role:
        return False
    compact = role.replace(" ", "")
    if compact == "보안/경비":
        return True
    return any(token in role for token in SECURITY_ROLE_KEYWORDS)


def _allowed_modules_for_user(user: Dict[str, Any]) -> List[str]:
    role = _normalized_role_text(user.get("role"))
    if _is_security_role(role):
        return ["parking"]
    if _is_complaints_only_role(role):
        return ["complaints"]
    return ["main", "parking", "complaints"]


def _default_landing_path_for_user(user: Dict[str, Any]) -> str:
    modules = _allowed_modules_for_user(user)
    if "parking" in modules and len(modules) == 1:
        return "/parking/admin2"
    if "complaints" in modules and len(modules) == 1:
        return "/pwa/complaints.html"
    return "/pwa/"


def _normalize_unit_label(value: Any, *, required: bool = False) -> Tuple[str | None, str | None]:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise HTTPException(status_code=400, detail="입주민 계정은 동/호(unit_label)가 필요합니다.")
        return None, None

    compact = re.sub(r"\s+", "", raw)
    m = re.match(r"^(\d{2,4})[-/](\d{3,4})$", compact)
    if not m:
        m = re.match(r"^(\d{2,4})동(\d{3,4})호?$", compact)
    if not m:
        raise HTTPException(status_code=400, detail="unit_label 형식은 예: 101-1203 또는 101동 1203호")

    dong = str(m.group(1)).strip()
    ho = str(m.group(2)).strip()
    normalized = f"{dong}-{ho}"
    return normalized, normalized


def _extract_resident_household(payload: Dict[str, Any], *, role: str, current_unit_label: Any = None) -> Tuple[str | None, str | None]:
    if _is_resident_role(role):
        source = payload.get("unit_label") if "unit_label" in payload else current_unit_label
        return _normalize_unit_label(source, required=True)

    if "unit_label" in payload:
        return _normalize_unit_label(payload.get("unit_label"), required=False)
    return None, None


def _assert_resident_household_available(
    *,
    role: str,
    site_code: str,
    household_key: str | None,
    is_active: bool,
    exclude_user_id: int | None = None,
) -> None:
    if not _is_resident_role(role):
        return
    if not is_active:
        return
    clean_site_code = _clean_site_code(site_code, required=True)
    clean_household_key = str(household_key or "").strip().upper()
    if not clean_household_key:
        raise HTTPException(status_code=400, detail="입주민 계정은 동/호(unit_label)가 필요합니다.")
    exists = count_active_resident_household_users(
        site_code=clean_site_code,
        household_key=clean_household_key,
        exclude_user_id=exclude_user_id,
    )
    if exists > 0:
        raise HTTPException(status_code=409, detail="해당 세대에는 이미 입주민 계정이 등록되어 있습니다.")


def _effective_role_for_permission_level(role: str, permission_level: str) -> str:
    level = str(permission_level or "").strip().lower()
    if level == "security_guard":
        return "보안/경비"
    if level == "resident":
        return "세대주민"
    if level == "board_member":
        return "입대의"
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
    src = f"{PHONE_VERIFY_SECRET_VALUE}|{phone}|{code}"
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


def _clean_query_text(value: Any, *, max_len: int = 80) -> str:
    txt = str(value or "").strip()
    if len(txt) > max_len:
        raise HTTPException(status_code=400, detail=f"query text length must be <= {max_len}")
    return txt


_REGION_ALIASES = {
    "서울": "서울특별시",
    "부산": "부산광역시",
    "대구": "대구광역시",
    "인천": "인천광역시",
    "광주": "광주광역시",
    "대전": "대전광역시",
    "울산": "울산광역시",
    "세종": "세종특별자치시",
    "경기": "경기도",
    "강원": "강원특별자치도",
    "충북": "충청북도",
    "충남": "충청남도",
    "전북": "전북특별자치도",
    "전남": "전라남도",
    "경북": "경상북도",
    "경남": "경상남도",
    "제주": "제주특별자치도",
}


def _normalize_region_label(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    token = re.split(r"[\s,]+", raw, maxsplit=1)[0].strip()
    if not token:
        return ""
    return _REGION_ALIASES.get(token, token)


def _region_from_address(address: Any) -> str:
    return _normalize_region_label(address)


def _permission_level_from_user(user: Dict[str, Any]) -> str:
    if int(user.get("is_admin") or 0) == 1:
        return "admin"
    if int(user.get("is_site_admin") or 0) == 1:
        return "site_admin"
    role = _normalized_role_text(user.get("role"))
    if _is_security_role(role):
        return "security_guard"
    if _is_resident_role(role):
        return "resident"
    if _is_board_role(role):
        return "board_member"
    return "user"


def _account_type_from_user(user: Dict[str, Any]) -> str:
    level = _permission_level_from_user(user)
    if level == "admin":
        return "최고/운영관리자"
    if level == "site_admin":
        return "단지관리자"
    if level == "security_guard":
        return "보안/경비"
    if level == "resident":
        return "입주민"
    if level == "board_member":
        return "입대의"
    return "사용자"


def _parking_permission_level_from_user(user: Dict[str, Any]) -> str:
    level = _permission_level_from_user(user)
    if level == "security_guard":
        return "site_admin"
    if level in {"admin", "site_admin"}:
        return level
    return "user"


def _clean_admin_scope(value: Any, *, required: bool = False) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        if required:
            raise HTTPException(status_code=400, detail="admin_scope is required for admin permission")
        return ""
    if raw not in VALID_ADMIN_SCOPES:
        raise HTTPException(status_code=400, detail=f"admin_scope must be one of: {', '.join(VALID_ADMIN_SCOPES)}")
    return raw


def _admin_scope_from_user(user: Dict[str, Any]) -> str:
    if int(user.get("is_admin") or 0) != 1:
        return ""
    raw = str(user.get("admin_scope") or "").strip().lower()
    if raw in VALID_ADMIN_SCOPES:
        return raw
    login_id = str(user.get("login_id") or "").strip().lower()
    if login_id == "admin":
        return "super_admin"
    return "ops_admin"


def _admin_scope_label(scope: str) -> str:
    return ADMIN_SCOPE_LABELS.get(str(scope or "").strip().lower(), "")


def _is_super_admin(user: Dict[str, Any]) -> bool:
    return _admin_scope_from_user(user) == "super_admin"


def _clean_permission_level(value: Any, *, required: bool = False) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        if required:
            raise HTTPException(status_code=400, detail="permission_level is required")
        return ""
    if raw not in VALID_PERMISSION_LEVELS:
        raise HTTPException(status_code=400, detail=f"permission_level must be one of: {', '.join(VALID_PERMISSION_LEVELS)}")
    return raw


def _resolve_permission_flags(
    payload: Dict[str, Any],
    *,
    default_admin: bool = False,
    default_site_admin: bool = False,
    default_admin_scope: str = "",
) -> Tuple[int, int, str, str]:
    if "permission_level" in payload:
        level = _clean_permission_level(payload.get("permission_level"), required=True)
        if level == "admin":
            scope = _clean_admin_scope(payload.get("admin_scope"), required=False) or _clean_admin_scope(
                default_admin_scope, required=False
            )
            if not scope:
                scope = "ops_admin"
            return 1, 0, level, scope
        if level == "site_admin":
            return 0, 1, level, ""
        if level in {"security_guard", "resident", "board_member"}:
            return 0, 0, level, ""
        return 0, 0, level, ""

    is_admin = _clean_bool(payload.get("is_admin"), default=default_admin)
    is_site_admin = _clean_bool(payload.get("is_site_admin"), default=default_site_admin)
    if is_admin:
        is_site_admin = False
    level = "admin" if is_admin else ("site_admin" if is_site_admin else "user")
    scope = ""
    if is_admin:
        scope = _clean_admin_scope(payload.get("admin_scope"), required=False) or _clean_admin_scope(
            default_admin_scope, required=False
        )
        if not scope:
            scope = "ops_admin"
    return (1 if is_admin else 0), (1 if is_site_admin else 0), level, scope


def _resolve_site_code_for_site(
    site_name: str,
    site_code: str = "",
    *,
    allow_create: bool = True,
    allow_remap: bool = False,
) -> str:
    try:
        return resolve_or_create_site_code(
            site_name,
            preferred_code=(site_code or None),
            allow_create=allow_create,
            allow_remap=allow_remap,
        )
    except ValueError as e:
        detail = str(e)
        status_code = 409
        if "mapping not found" in detail:
            status_code = 404
        raise HTTPException(status_code=status_code, detail=detail) from e
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

    try:
        resolved = _resolve_site_code_for_site(site_name, "", allow_create=True, allow_remap=False)
    except HTTPException:
        return user
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
    admin_scope = _admin_scope_from_user(user)
    allowed_modules = _allowed_modules_for_user(user)
    account_type = _account_type_from_user(user)
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
        "unit_label": user.get("unit_label"),
        "household_key": user.get("household_key"),
        "note": user.get("note"),
        "is_admin": bool(user.get("is_admin")),
        "is_site_admin": bool(user.get("is_site_admin")),
        "admin_scope": admin_scope,
        "admin_scope_label": _admin_scope_label(admin_scope),
        "permission_level": permission_level,
        "account_type": account_type,
        "allowed_modules": allowed_modules,
        "default_landing_path": _default_landing_path_for_user(user),
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


def _resolve_spec_env_site_target(
    user: Dict[str, Any],
    requested_site_name: Any,
    requested_site_code: Any,
    *,
    require_any: bool = True,
    for_write: bool = False,
) -> Tuple[str, str]:
    raw_site_name = str(requested_site_name or "").strip()
    raw_site_code = str(requested_site_code or "").strip().upper()

    if int(user.get("is_admin") or 0) == 1:
        is_super = _is_super_admin(user)
        clean_site_name = _clean_site_name(raw_site_name, required=True) if raw_site_name else ""
        clean_site_code = _clean_site_code(raw_site_code, required=False)

        if not clean_site_name and not clean_site_code:
            if require_any:
                raise HTTPException(status_code=400, detail="site_name 또는 site_code 중 하나를 입력하세요.")
            return "", ""

        if clean_site_code and not clean_site_name:
            resolved_name = find_site_name_by_code(clean_site_code)
            if not resolved_name:
                raise HTTPException(status_code=404, detail="입력한 site_code에 해당하는 site_name을 찾을 수 없습니다.")
            clean_site_name = _clean_site_name(resolved_name, required=True)
        if clean_site_name:
            allow_create = bool(for_write and is_super)
            try:
                clean_site_code = _resolve_site_code_for_site(
                    clean_site_name,
                    clean_site_code,
                    allow_create=allow_create,
                    allow_remap=False,
                )
            except HTTPException as e:
                if e.status_code == 404 and not is_super:
                    raise HTTPException(
                        status_code=403,
                        detail="운영관리자는 기존 단지코드만 사용할 수 있습니다. 최고관리자에게 등록 요청하세요.",
                    ) from e
                raise

        return clean_site_name, clean_site_code

    clean_site_name = _resolve_allowed_site_name(user, raw_site_name, required=False)
    clean_site_code = _resolve_allowed_site_code(user, raw_site_code)
    if not clean_site_code:
        try:
            clean_site_code = _resolve_site_code_for_site(
                clean_site_name,
                "",
                allow_create=False,
                allow_remap=False,
            )
        except HTTPException as e:
            if e.status_code == 404:
                raise HTTPException(
                    status_code=403,
                    detail="소속 단지코드가 등록되어 있지 않습니다. 최고관리자에게 등록 요청하세요.",
                ) from e
            raise
        uid = int(user.get("id") or 0)
        if uid > 0 and clean_site_code:
            set_staff_user_site_code(uid, clean_site_code)
    return clean_site_name, clean_site_code


def _resolve_site_identity_for_main(user: Dict[str, Any], requested_site_name: Any, requested_site_code: Any) -> Tuple[str, str]:
    raw_site_name = str(requested_site_name or "").strip()
    raw_site_code = str(requested_site_code or "").strip().upper()
    is_admin = int(user.get("is_admin") or 0) == 1

    if is_admin:
        is_super = _is_super_admin(user)
        clean_site_name = _clean_site_name(raw_site_name, required=True) if raw_site_name else ""
        clean_site_code = _clean_site_code(raw_site_code, required=False)

        if clean_site_code and not clean_site_name:
            resolved_name = find_site_name_by_code(clean_site_code)
            if not resolved_name:
                raise HTTPException(status_code=404, detail="입력한 site_code에 매핑된 site_name이 없습니다.")
            clean_site_name = _clean_site_name(resolved_name, required=True)

        if clean_site_name:
            try:
                clean_site_code = _resolve_site_code_for_site(
                    clean_site_name,
                    clean_site_code,
                    allow_create=is_super,
                    allow_remap=False,
                )
            except HTTPException as e:
                if e.status_code == 404 and not is_super:
                    raise HTTPException(
                        status_code=403,
                        detail="운영관리자는 기존 단지코드만 선택할 수 있습니다. 최고관리자에게 등록 요청하세요.",
                    ) from e
                raise
            return clean_site_name, clean_site_code

        fallback_name = str(user.get("site_name") or "").strip()
        fallback_code = _clean_site_code(user.get("site_code"), required=False)
        return fallback_name, fallback_code

    assigned_site_name = _normalized_assigned_site_name(user)
    assigned_site_code = _clean_site_code(user.get("site_code"), required=False)

    if raw_site_name:
        requested_name = _clean_site_name(raw_site_name, required=True)
        if requested_name != assigned_site_name:
            raise HTTPException(status_code=403, detail="단지명/단지코드 입력·수정은 관리자만 가능합니다.")
    if raw_site_code:
        requested_code = _clean_site_code(raw_site_code, required=True)
        if not assigned_site_code or requested_code != assigned_site_code:
            raise HTTPException(status_code=403, detail="단지명/단지코드 입력·수정은 관리자만 가능합니다.")

    if not assigned_site_code:
        assigned_site_code = _clean_site_code(find_site_code_by_name(assigned_site_name), required=False)

    return assigned_site_name, assigned_site_code


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
    cookie_token = (request.cookies.get(AUTH_COOKIE_NAME) or "").strip()
    if cookie_token:
        return cookie_token
    token = (request.query_params.get("access_token") or "").strip()
    if token:
        return token
    raise HTTPException(status_code=401, detail="auth required")


def _set_auth_cookie(resp: JSONResponse, token: str) -> None:
    resp.set_cookie(
        AUTH_COOKIE_NAME,
        token,
        max_age=AUTH_COOKIE_MAX_AGE,
        httponly=True,
        secure=AUTH_COOKIE_SECURE,
        samesite=AUTH_COOKIE_SAMESITE,
        path="/",
    )


def _clear_auth_cookie(resp: JSONResponse) -> None:
    resp.delete_cookie(
        AUTH_COOKIE_NAME,
        path="/",
        secure=AUTH_COOKIE_SECURE,
        httponly=True,
        samesite=AUTH_COOKIE_SAMESITE,
    )


def _enforce_api_module_scope(user: Dict[str, Any], request_path: str) -> None:
    path = str(request_path or "").split("?", 1)[0].strip()
    if not path:
        return
    role = _normalized_role_text(user.get("role"))
    if _is_security_role(role):
        allowed_paths = {"/api/auth/me", "/api/auth/logout", "/api/auth/change_password", "/api/parking/context"}
        if path in allowed_paths:
            return
        raise HTTPException(status_code=403, detail="보안/경비 계정은 주차관리 모듈만 사용할 수 있습니다.")
    if _is_complaints_only_role(role):
        allowed_paths = {"/api/auth/me", "/api/auth/logout", "/api/auth/change_password"}
        if path in allowed_paths:
            return
        raise HTTPException(status_code=403, detail="입대의/세대주민 계정은 민원 모듈만 사용할 수 있습니다.")


def _require_auth(request: Request) -> Tuple[Dict[str, Any], str]:
    token = _extract_access_token(request)
    user = get_auth_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid or expired session")
    user = _bind_user_site_code_if_missing(user)
    _enforce_api_module_scope(user, request.url.path)
    return user, token


def _require_admin(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if int(user.get("is_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="admin only")
    return user, token


def _require_super_admin(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_admin(request)
    if not _is_super_admin(user):
        raise HTTPException(status_code=403, detail="최고관리자 권한이 필요합니다.")
    return user, token


def _critical_change_window() -> Tuple[int, int]:
    start = max(0, min(23, _safe_int_env("KA_CRITICAL_CHANGE_WINDOW_START_HOUR", 6, 0)))
    end = max(1, min(24, _safe_int_env("KA_CRITICAL_CHANGE_WINDOW_END_HOUR", 23, 1)))
    return start, end


def _is_within_change_window(now: datetime | None = None) -> bool:
    start, end = _critical_change_window()
    if start == end:
        return True
    current = now or datetime.now()
    hour = int(current.hour)
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _assert_change_window(operation_label: str = "중요 변경") -> None:
    if not _env_enabled("KA_CRITICAL_CHANGE_WINDOW_ENABLED", True):
        return
    if _is_within_change_window():
        return
    start, end = _critical_change_window()
    raise HTTPException(
        status_code=403,
        detail=f"{operation_label}은(는) 허용 시간({start:02d}:00~{end:02d}:00) 내에서만 실행할 수 있습니다.",
    )


def _assert_mfa_confirmed(request: Request, payload: Dict[str, Any] | None = None, *, operation_label: str = "중요 변경") -> None:
    if not _env_enabled("KA_CRITICAL_REQUIRE_MFA", True):
        return
    body = payload if isinstance(payload, dict) else {}
    header_ok = str(request.headers.get("X-KA-MFA-VERIFIED") or "").strip().lower() in {"1", "true", "yes", "on"}
    body_ok = _clean_bool(body.get("mfa_confirmed"), default=False)
    if header_ok or body_ok:
        return
    raise HTTPException(
        status_code=403,
        detail=f"{operation_label}에는 MFA 확인이 필요합니다. 요청값 mfa_confirmed=true 또는 X-KA-MFA-VERIFIED 헤더를 전달하세요.",
    )


def _run_prechange_site_backup(user: Dict[str, Any], *, site_code: str, site_name: str, reason: str) -> Dict[str, Any]:
    if not _env_enabled("KA_PRECHANGE_BACKUP_ENABLED", True):
        return {}
    clean_site_code = _clean_site_code(site_code, required=False)
    if not clean_site_code:
        return {}
    actor_login = _clean_login_id(user.get("login_id") or "policy-backup")
    target_keys = [
        str(x.get("key") or "").strip().lower()
        for x in list_backup_targets()
        if bool(x.get("exists")) and bool(x.get("site_scoped"))
    ]
    target_keys = [x for x in target_keys if x]
    if not target_keys:
        return {}
    try:
        return run_manual_backup(
            actor=actor_login,
            trigger=f"prechange:{str(reason or 'policy').strip().lower()}",
            target_keys=target_keys,
            scope="site",
            site_code=clean_site_code,
            site_name=_clean_site_name(site_name, required=False),
            with_maintenance=False,
        )
    except Exception as e:
        raise HTTPException(status_code=409, detail=f"변경 전 자동백업 실패: {e}") from e


def _audit_security(
    *,
    user: Dict[str, Any] | None,
    event_type: str,
    severity: str = "INFO",
    outcome: str = "ok",
    target_site_code: str = "",
    target_site_name: str = "",
    request_id: int | None = None,
    detail: Dict[str, Any] | List[Any] | str | None = None,
) -> int:
    actor_id = int(user.get("id") or 0) if isinstance(user, dict) else 0
    actor_login = str(user.get("login_id") or "").strip().lower() if isinstance(user, dict) else ""
    return write_security_audit_log(
        event_type=event_type,
        severity=severity,
        outcome=outcome,
        actor_user_id=(actor_id if actor_id > 0 else None),
        actor_login=actor_login,
        target_site_code=_clean_site_code(target_site_code, required=False),
        target_site_name=str(target_site_name or "").strip(),
        request_id=request_id,
        detail=detail,
    )


def _can_manage_site_env(user: Dict[str, Any]) -> bool:
    return int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1


def _require_site_env_manager(request: Request) -> Tuple[Dict[str, Any], str]:
    user, token = _require_auth(request)
    if not _can_manage_site_env(user):
        raise HTTPException(status_code=403, detail="site env manager only")
    return user, token


def _can_manage_backup(user: Dict[str, Any]) -> bool:
    return int(user.get("is_admin") or 0) == 1 or int(user.get("is_site_admin") or 0) == 1


def _home_only_site_env_config() -> Dict[str, Any]:
    visible = set(DEFAULT_INITIAL_VISIBLE_TAB_KEYS)
    hide_tabs = [tab for tab in SCHEMA_TAB_ORDER if tab not in visible]
    return normalize_site_env_config(
        {
            "hide_tabs": hide_tabs,
            "tabs": {
                "home": {"title": "홈"},
                "notice_qna": {"title": "공지/질문"},
                "todo": {"title": "Todo 일정관리"},
            },
        }
    )


def _verify_first_site_registrant_for_spec_env(user: Dict[str, Any], site_name: str, site_code: str = "") -> Dict[str, Any]:
    target_site_name = _clean_site_name(site_name, required=True)
    target_site_code = _clean_site_code(site_code, required=False)

    if int(user.get("is_admin") or 0) == 1:
        return {"verified": True, "home_site_name": target_site_name, "first_user_id": None}

    if int(user.get("is_site_admin") or 0) != 1:
        raise HTTPException(status_code=403, detail="제원설정 권한이 없습니다.")

    assigned_site_name = _normalized_assigned_site_name(user)
    assigned_site_code = _clean_site_code(user.get("site_code"), required=False)
    if target_site_name != assigned_site_name:
        raise HTTPException(status_code=403, detail="소속 단지의 제원설정만 접근할 수 있습니다.")
    if assigned_site_code and target_site_code and target_site_code != assigned_site_code:
        raise HTTPException(status_code=403, detail="소속 단지코드의 제원설정만 접근할 수 있습니다.")
    if not target_site_code:
        target_site_code = assigned_site_code

    return {
        "verified": True,
        "home_site_name": target_site_name,
        "first_user_id": int(user.get("id") or 0),
        "first_login_id": str(user.get("login_id") or ""),
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


def _resolve_main_site_target(
    user: Dict[str, Any],
    requested_site_name: Any,
    requested_site_code: Any,
    *,
    required: bool = False,
) -> Tuple[str, str]:
    clean_site_name, clean_site_code = _resolve_site_identity_for_main(user, requested_site_name, requested_site_code)
    clean_site_name = _clean_site_name(clean_site_name, required=False)
    clean_site_code = _clean_site_code(clean_site_code, required=False)

    if clean_site_code and not clean_site_name:
        mapped_name = find_site_name_by_code(clean_site_code)
        if mapped_name:
            clean_site_name = _clean_site_name(mapped_name, required=True)

    if clean_site_name and not clean_site_code:
        allow_create = _is_super_admin(user)
        try:
            clean_site_code = _resolve_site_code_for_site(
                clean_site_name,
                "",
                allow_create=allow_create,
                allow_remap=False,
            )
        except HTTPException as e:
            if e.status_code == 404 and int(user.get("is_admin") or 0) == 1 and not _is_super_admin(user):
                raise HTTPException(
                    status_code=403,
                    detail="운영관리자는 기존 단지코드만 선택할 수 있습니다. 최고관리자에게 등록 요청하세요.",
                ) from e
            raise

    if required and not clean_site_name:
        raise HTTPException(status_code=400, detail="site_name 또는 site_code를 확인하세요.")

    return clean_site_name, clean_site_code


@router.get("/health")
def health():
    report = schema_alignment_report()
    return {"ok": True, "version": "2.9.0", "schema_alignment_ok": report.get("ok", False)}


@router.get("/parking/context")
def parking_context(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
):
    user, _token = _require_auth(request)
    permission_level = _parking_permission_level_from_user(user)
    is_admin_user = int(user.get("is_admin") or 0) == 1

    # Non-admin accounts must always use their assigned site context.
    # This avoids false "site edit" errors when stale query/local values are sent.
    requested_site_name = site_name
    requested_site_code = site_code
    if not is_admin_user:
        requested_site_name = str(user.get("site_name") or "").strip()
        requested_site_code = str(user.get("site_code") or "").strip().upper()

    clean_site_name, clean_site_code = _resolve_main_site_target(
        user,
        requested_site_name,
        requested_site_code,
        required=False,
    )
    clean_site_name = _clean_site_name(clean_site_name, required=False)
    clean_site_code = _clean_site_code(clean_site_code, required=False)

    if clean_site_code and not clean_site_name:
        mapped = find_site_name_by_code(clean_site_code)
        if mapped:
            clean_site_name = _clean_site_name(mapped, required=True)

    if clean_site_name and not clean_site_code:
        try:
            clean_site_code = _resolve_site_code_for_site(
                clean_site_name,
                "",
                allow_create=_is_super_admin(user),
                allow_remap=False,
            )
        except HTTPException as e:
            if e.status_code == 404:
                raise HTTPException(
                    status_code=403,
                    detail="단지코드가 등록되지 않았습니다. 최고관리자에게 등록을 요청하세요.",
                ) from e
            raise

    # Fallback: legacy sessions may still miss site_code.
    if not clean_site_code:
        clean_site_code = _clean_site_code(user.get("site_code"), required=False)
        if clean_site_code and not clean_site_name:
            mapped = find_site_name_by_code(clean_site_code)
            if mapped:
                clean_site_name = _clean_site_name(mapped, required=True)

    if not clean_site_code:
        raise HTTPException(status_code=403, detail="site_code is required for parking access")

    if not clean_site_name:
        clean_site_name = _clean_site_name(user.get("site_name"), required=False)

    uid = int(user.get("id") or 0)
    user_site_code = _clean_site_code(user.get("site_code"), required=False)
    if uid > 0 and not is_admin_user and not user_site_code:
        set_staff_user_site_code(uid, clean_site_code)

    serializer = URLSafeTimedSerializer(_parking_context_secret(), salt="parking-context")
    ctx = serializer.dumps(
        {
            "site_code": clean_site_code,
            "site_name": clean_site_name,
            "permission_level": permission_level,
            "login_id": _clean_login_id(user.get("login_id") or "ka-part-user"),
            "user_name": _clean_name(user.get("name") or user.get("login_id") or "사용자"),
        }
    )
    rel = f"{PARKING_SSO_PATH}?ctx={urllib.parse.quote(ctx, safe='')}"
    if PARKING_EMBED_ENABLED:
        parking_url = rel
    else:
        if not PARKING_BASE_URL:
            raise HTTPException(status_code=503, detail="parking gateway misconfigured: PARKING_BASE_URL is required")
        parking_url = f"{PARKING_BASE_URL.rstrip('/')}{rel}"
    return {
        "ok": True,
        "url": parking_url,
        "site_code": clean_site_code,
        "site_name": clean_site_name,
        "permission_level": permission_level,
        "expires_in": PARKING_CONTEXT_MAX_AGE,
    }


@router.get("/schema")
def api_schema(request: Request, site_name: str = Query(default=""), site_code: str = Query(default="")):
    user, _token = _require_auth(request)
    clean_site_name, resolved_site_code = _resolve_main_site_target(
        user, site_name, site_code, required=False
    )
    schema, env_cfg = _site_schema_and_env(clean_site_name, resolved_site_code)

    # On first login for the first site registrant, show only the Home tab
    # until the site env is explicitly saved.
    if clean_site_name and int(user.get("is_site_admin") or 0) == 1 and _is_first_site_registrant(user, clean_site_name, resolved_site_code):
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
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, site_name, site_code, require_any=True, for_write=False
    )
    verify = _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)

    row = get_site_env_record(clean_site_name, site_code=clean_site_code or None)
    resolved_site_name = str((row or {}).get("site_name") or "").strip() or clean_site_name
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
        "site_name": resolved_site_name,
        "site_code": resolved_site_code,
        "config": env_cfg,
        "schema": schema,
        "spec_access": verify,
    }


@router.get("/site_identity")
def api_site_identity(request: Request, site_name: str = Query(default=""), site_code: str = Query(default="")):
    user, _token = _require_auth(request)
    clean_site_name, clean_site_code = _resolve_site_identity_for_main(user, site_name, site_code)
    admin_scope = _admin_scope_from_user(user)
    return {
        "ok": True,
        "site_name": clean_site_name,
        "site_code": clean_site_code,
        "editable": int(user.get("is_admin") or 0) == 1,
        "site_code_editable": admin_scope == "super_admin",
        "admin_scope": admin_scope,
    }


@router.put("/site_env")
def api_site_env_upsert(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_site_env_manager(request)
    _assert_change_window("제원설정 변경")
    _assert_mfa_confirmed(request, payload, operation_label="제원설정 변경")
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, payload.get("site_name"), payload.get("site_code"), require_any=True, for_write=True
    )
    verify = _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)
    raw_cfg = payload.get("config", payload.get("env", payload if isinstance(payload, dict) else {}))
    cfg = normalize_site_env_config(raw_cfg if isinstance(raw_cfg, dict) else {})
    reason = str(payload.get("reason") or "").strip()
    actor_login = _clean_login_id(user.get("login_id") or "site-env")
    prechange_backup = _run_prechange_site_backup(
        user,
        site_code=clean_site_code,
        site_name=clean_site_name,
        reason="site_env_update",
    )
    try:
        row = upsert_site_env_config(
            clean_site_name,
            cfg,
            site_code=clean_site_code or None,
            action="update",
            actor_login=actor_login,
            reason=reason,
            record_version=True,
        )
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="site_env_update",
            severity="ERROR",
            outcome="error",
            target_site_code=clean_site_code,
            target_site_name=clean_site_name,
            detail={"reason": reason, "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    resolved_site_code = _clean_site_code(row.get("site_code"), required=False)
    schema, _env_cfg = _site_schema_and_env(clean_site_name, resolved_site_code)
    _audit_security(
        user=user,
        event_type="site_env_update",
        severity="INFO",
        outcome="ok",
        target_site_code=resolved_site_code,
        target_site_name=clean_site_name,
        detail={
            "reason": reason,
            "admin_scope": _admin_scope_from_user(user),
            "prechange_backup_scope": str((prechange_backup or {}).get("scope") or ""),
            "prechange_backup_path": str((prechange_backup or {}).get("relative_path") or ""),
        },
    )
    return {
        "ok": True,
        "site_name": clean_site_name,
        "site_code": resolved_site_code,
        "config": cfg,
        "schema": schema,
        "updated_at": row.get("updated_at"),
        "spec_access": verify,
        "prechange_backup": prechange_backup,
    }


@router.delete("/site_env")
def api_site_env_delete(request: Request, site_name: str = Query(default=""), site_code: str = Query(default="")):
    user, _token = _require_site_env_manager(request)
    _assert_change_window("제원설정 삭제")
    _assert_mfa_confirmed(request, operation_label="제원설정 삭제")
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, site_name, site_code, require_any=True, for_write=False
    )
    _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)
    prechange_backup = _run_prechange_site_backup(
        user,
        site_code=clean_site_code,
        site_name=clean_site_name,
        reason="site_env_delete",
    )
    actor_login = _clean_login_id(user.get("login_id") or "site-env")
    ok = delete_site_env_config(
        clean_site_name,
        site_code=clean_site_code or None,
        actor_login=actor_login,
        reason="delete site_env",
        record_version=True,
    )
    _audit_security(
        user=user,
        event_type="site_env_delete",
        severity="WARN",
        outcome=("ok" if ok else "noop"),
        target_site_code=clean_site_code,
        target_site_name=clean_site_name,
        detail={
            "prechange_backup_scope": str((prechange_backup or {}).get("scope") or ""),
            "prechange_backup_path": str((prechange_backup or {}).get("relative_path") or ""),
        },
    )
    return {
        "ok": ok,
        "site_name": clean_site_name,
        "site_code": clean_site_code,
        "prechange_backup": prechange_backup,
    }


@router.get("/site_env_list")
def api_site_env_list(request: Request):
    user, _token = _require_site_env_manager(request)
    rows = list_site_env_configs()
    if int(user.get("is_admin") or 0) != 1:
        assigned_name = _normalized_assigned_site_name(user)
        assigned_code = _clean_site_code(user.get("site_code"), required=False)
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


@router.get("/site_env/history")
def api_site_env_history(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    limit: int = Query(default=30),
):
    user, _token = _require_site_env_manager(request)
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, site_name, site_code, require_any=True, for_write=False
    )
    _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)
    items = list_site_env_config_versions(
        site_name=clean_site_name,
        site_code=clean_site_code or None,
        limit=max(1, min(int(limit), 200)),
    )
    return {
        "ok": True,
        "site_name": clean_site_name,
        "site_code": clean_site_code,
        "count": len(items),
        "items": items,
    }


@router.post("/site_env/rollback")
def api_site_env_rollback(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_site_env_manager(request)
    _assert_change_window("제원설정 롤백")
    _assert_mfa_confirmed(request, payload, operation_label="제원설정 롤백")
    version_id = int(payload.get("version_id") or 0)
    if version_id <= 0:
        raise HTTPException(status_code=400, detail="version_id is required")
    version = get_site_env_config_version(version_id)
    if not version:
        raise HTTPException(status_code=404, detail="site_env version not found")

    req_site_name = payload.get("site_name") or version.get("site_name")
    req_site_code = payload.get("site_code") or version.get("site_code")
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, req_site_name, req_site_code, require_any=True, for_write=True
    )
    _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)

    version_site_name = _clean_site_name(version.get("site_name"), required=True)
    version_site_code = _clean_site_code(version.get("site_code"), required=False)
    if clean_site_name != version_site_name:
        raise HTTPException(status_code=409, detail="version_id가 지정한 단지와 일치하지 않습니다.")
    if clean_site_code and version_site_code and clean_site_code != version_site_code:
        raise HTTPException(status_code=409, detail="version_id가 지정한 단지코드와 일치하지 않습니다.")

    prechange_backup = _run_prechange_site_backup(
        user,
        site_code=clean_site_code or version_site_code,
        site_name=clean_site_name,
        reason="site_env_rollback",
    )
    actor_login = _clean_login_id(user.get("login_id") or "site-env")
    reason = str(payload.get("reason") or "").strip()
    try:
        result = rollback_site_env_config_version(
            version_id=version_id,
            actor_login=actor_login,
            reason=(reason or f"rollback by version_id={version_id}"),
        )
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="site_env_rollback",
            severity="ERROR",
            outcome="error",
            target_site_code=clean_site_code or version_site_code,
            target_site_name=clean_site_name,
            detail={"version_id": version_id, "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    row = result.get("site_env") if isinstance(result, dict) else {}
    resolved_site_code = _clean_site_code((row or {}).get("site_code"), required=False) or clean_site_code
    schema, _env_cfg = _site_schema_and_env(clean_site_name, resolved_site_code)
    _audit_security(
        user=user,
        event_type="site_env_rollback",
        severity="WARN",
        outcome="ok",
        target_site_code=resolved_site_code,
        target_site_name=clean_site_name,
        detail={
            "version_id": version_id,
            "reason": reason,
            "prechange_backup_path": str((prechange_backup or {}).get("relative_path") or ""),
        },
    )
    return {
        "ok": True,
        "site_name": clean_site_name,
        "site_code": resolved_site_code,
        "config": (row or {}).get("config") or {},
        "schema": schema,
        "rollback_from_version": version_id,
        "prechange_backup": prechange_backup,
    }


@router.get("/security/audit_logs")
def api_security_audit_logs(
    request: Request,
    limit: int = Query(default=120),
    event_type: str = Query(default=""),
    outcome: str = Query(default=""),
):
    _user, _token = _require_admin(request)
    items = list_security_audit_logs(
        limit=max(1, min(int(limit), 500)),
        event_type=str(event_type or "").strip(),
        outcome=str(outcome or "").strip().lower(),
    )
    return {"ok": True, "count": len(items), "items": items}


@router.get("/site_code/migration/requests")
def api_site_code_migration_requests(
    request: Request,
    status: str = Query(default=""),
    limit: int = Query(default=100),
):
    _user, _token = _require_super_admin(request)
    items = list_privileged_change_requests(
        change_type="site_code_migration",
        status=str(status or "").strip().lower(),
        limit=max(1, min(int(limit), 300)),
    )
    return {"ok": True, "count": len(items), "items": items}


@router.post("/site_code/migration/request")
def api_site_code_migration_request(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_super_admin(request)
    _assert_change_window("단지코드 마이그레이션 요청")
    _assert_mfa_confirmed(request, payload, operation_label="단지코드 마이그레이션 요청")

    if count_super_admins(active_only=True) < 2:
        raise HTTPException(status_code=409, detail="2인 승인 정책을 위해 활성 최고관리자 계정이 2명 이상 필요합니다.")

    site_name = _clean_site_name(payload.get("site_name"), required=True)
    old_site_code = _clean_site_code(payload.get("old_site_code"), required=True)
    new_site_code = _clean_site_code(payload.get("new_site_code"), required=True)
    if old_site_code == new_site_code:
        raise HTTPException(status_code=400, detail="new_site_code는 old_site_code와 달라야 합니다.")
    reason = str(payload.get("reason") or "").strip()
    if len(reason) < 4:
        raise HTTPException(status_code=400, detail="reason(변경사유)을 4자 이상 입력하세요.")

    actor_login = _clean_login_id(user.get("login_id") or "admin")
    req = create_privileged_change_request(
        change_type="site_code_migration",
        payload={
            "site_name": site_name,
            "old_site_code": old_site_code,
            "new_site_code": new_site_code,
            "reason": reason,
        },
        requested_by_user_id=int(user.get("id") or 0),
        requested_by_login=actor_login,
        target_site_name=site_name,
        target_site_code=old_site_code,
        reason=reason,
        expires_hours=max(1, min(int(payload.get("expires_hours") or 24), 72)),
    )
    _audit_security(
        user=user,
        event_type="site_code_migration_request",
        severity="WARN",
        outcome="ok",
        target_site_code=old_site_code,
        target_site_name=site_name,
        request_id=int(req.get("id") or 0),
        detail={"new_site_code": new_site_code, "reason": reason},
    )
    return {"ok": True, "request": req}


@router.post("/site_code/migration/approve")
def api_site_code_migration_approve(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_super_admin(request)
    _assert_mfa_confirmed(request, payload, operation_label="단지코드 마이그레이션 승인")
    request_id = int(payload.get("request_id") or 0)
    if request_id <= 0:
        raise HTTPException(status_code=400, detail="request_id is required")

    actor_login = _clean_login_id(user.get("login_id") or "admin")
    try:
        item = approve_privileged_change_request(
            request_id=request_id,
            approver_user_id=int(user.get("id") or 0),
            approver_login=actor_login,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e

    _audit_security(
        user=user,
        event_type="site_code_migration_approve",
        severity="WARN",
        outcome="ok",
        target_site_code=str(item.get("target_site_code") or ""),
        target_site_name=str(item.get("target_site_name") or ""),
        request_id=request_id,
        detail={"status": item.get("status")},
    )
    return {"ok": True, "request": item}


@router.post("/site_code/migration/execute")
def api_site_code_migration_execute(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_super_admin(request)
    _assert_change_window("단지코드 마이그레이션 실행")
    _assert_mfa_confirmed(request, payload, operation_label="단지코드 마이그레이션 실행")
    request_id = int(payload.get("request_id") or 0)
    if request_id <= 0:
        raise HTTPException(status_code=400, detail="request_id is required")

    req = get_privileged_change_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    if str(req.get("change_type") or "").strip().lower() != "site_code_migration":
        raise HTTPException(status_code=400, detail="invalid request type")
    if str(req.get("status") or "").strip().lower() != "approved":
        raise HTTPException(status_code=409, detail="request is not approved")

    payload_data = req.get("payload") if isinstance(req.get("payload"), dict) else {}
    site_name = _clean_site_name(payload_data.get("site_name"), required=True)
    old_site_code = _clean_site_code(payload_data.get("old_site_code"), required=True)
    new_site_code = _clean_site_code(payload_data.get("new_site_code"), required=True)
    reason = str(payload_data.get("reason") or req.get("reason") or "").strip()

    prechange_backup = _run_prechange_site_backup(
        user,
        site_code=old_site_code,
        site_name=site_name,
        reason="site_code_migration_execute",
    )
    try:
        migration_result = migrate_site_code(
            site_name=site_name,
            old_site_code=old_site_code,
            new_site_code=new_site_code,
        )
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="site_code_migration_execute",
            severity="ERROR",
            outcome="error",
            target_site_code=old_site_code,
            target_site_name=site_name,
            request_id=request_id,
            detail={"reason": reason, "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e

    actor_login = _clean_login_id(user.get("login_id") or "admin")
    executed = mark_privileged_change_request_executed(
        request_id=request_id,
        executed_by_user_id=int(user.get("id") or 0),
        executed_by_login=actor_login,
        result={
            "reason": reason,
            "migration": migration_result,
            "prechange_backup": prechange_backup,
        },
    )
    _audit_security(
        user=user,
        event_type="site_code_migration_execute",
        severity="WARN",
        outcome="ok",
        target_site_code=new_site_code,
        target_site_name=site_name,
        request_id=request_id,
        detail={
            "old_site_code": old_site_code,
            "new_site_code": new_site_code,
            "updated_table_count": int((migration_result or {}).get("updated_table_count") or 0),
            "prechange_backup_path": str((prechange_backup or {}).get("relative_path") or ""),
        },
    )
    return {
        "ok": True,
        "request": executed,
        "migration": migration_result,
        "prechange_backup": prechange_backup,
    }


@router.get("/backup/status")
def api_backup_status(request: Request):
    user, _token = _require_auth(request)
    return {
        "ok": True,
        "timezone": backup_timezone_name(),
        "maintenance": get_maintenance_status(),
        "can_manage_backup": _can_manage_backup(user),
        "permission_level": _permission_level_from_user(user),
        "schedules": [
            {"key": "daily_full", "label": "전체 시스템 DB 자동백업", "when": f"매일 00:00 ({backup_timezone_name()})"},
            {"key": "weekly_site", "label": "단지관리자 단지코드 자동백업", "when": f"매주 금요일 00:20 ({backup_timezone_name()})"},
        ],
    }


@router.get("/backup/options")
def api_backup_options(request: Request):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    is_admin = int(user.get("is_admin") or 0) == 1
    site_code = _clean_site_code(user.get("site_code"), required=False)
    site_name = _clean_site_name(user.get("site_name"), required=False)
    targets = list_backup_targets()
    if not is_admin:
        if not site_code:
            raise HTTPException(status_code=403, detail="소속 단지코드가 없어 백업할 수 없습니다.")
        targets = [x for x in targets if bool(x.get("site_scoped"))]

    return {
        "ok": True,
        "is_admin": is_admin,
        "site_code": site_code,
        "site_name": site_name,
        "allowed_scopes": ["full", "site"] if is_admin else ["site"],
        "targets": targets,
    }


@router.post("/backup/run")
def api_backup_run(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    is_admin = int(user.get("is_admin") or 0) == 1
    target_payload = payload.get("target_keys", payload.get("targets", []))
    if isinstance(target_payload, str):
        target_keys = [target_payload]
    elif isinstance(target_payload, list):
        target_keys = [str(x or "").strip().lower() for x in target_payload if str(x or "").strip()]
    else:
        raise HTTPException(status_code=400, detail="target_keys must be list")

    scope = str(payload.get("scope") or ("full" if is_admin else "site")).strip().lower()
    if not is_admin:
        scope = "site"
    if scope not in {"full", "site"}:
        raise HTTPException(status_code=400, detail="scope must be full or site")

    available_targets = list_backup_targets()
    allowed_keys = {
        str(x["key"]).strip().lower()
        for x in available_targets
        if is_admin or bool(x.get("site_scoped"))
    }
    selected_keys = target_keys or sorted(allowed_keys)
    invalid = [k for k in selected_keys if k not in allowed_keys]
    if invalid:
        raise HTTPException(status_code=400, detail=f"invalid target_keys: {', '.join(invalid)}")
    if not selected_keys:
        raise HTTPException(status_code=400, detail="선택 가능한 백업 대상이 없습니다.")

    if scope == "site":
        if is_admin:
            site_code = _clean_site_code(payload.get("site_code"), required=True)
            site_name = _clean_site_name(payload.get("site_name"), required=False)
        else:
            site_code = _clean_site_code(user.get("site_code"), required=False)
            site_name = _clean_site_name(user.get("site_name"), required=False)
            if not site_code:
                raise HTTPException(status_code=403, detail="소속 단지코드가 없어 백업할 수 없습니다.")
    else:
        site_code = ""
        site_name = ""

    actor_login = _clean_login_id(user.get("login_id") or "backup-runner")
    try:
        result = run_manual_backup(
            actor=actor_login,
            trigger="manual",
            target_keys=selected_keys,
            scope=scope,
            site_code=site_code,
            site_name=site_name,
            with_maintenance=(scope == "full"),
        )
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"backup failed: {e}") from e

    return {"ok": True, "result": result, "maintenance": get_maintenance_status()}


@router.get("/backup/history")
def api_backup_history(
    request: Request,
    limit: int = Query(default=50),
    scope: str = Query(default=""),
    site_code: str = Query(default=""),
):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    is_admin = int(user.get("is_admin") or 0) == 1
    clean_scope = str(scope or "").strip().lower()
    if clean_scope and clean_scope not in {"full", "site"}:
        raise HTTPException(status_code=400, detail="scope must be full or site")

    if is_admin:
        clean_site_code = _clean_site_code(site_code, required=False)
    else:
        clean_scope = "site"
        clean_site_code = _clean_site_code(user.get("site_code"), required=False)
        if not clean_site_code:
            raise HTTPException(status_code=403, detail="소속 단지코드가 없어 백업이력을 조회할 수 없습니다.")

    items = list_backup_history(
        limit=max(1, min(int(limit), 200)),
        scope=clean_scope,
        site_code=clean_site_code,
    )
    return {"ok": True, "count": len(items), "items": items}


@router.get("/backup/download")
def api_backup_download(request: Request, path: str = Query(...)):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    item = get_backup_item(path)
    if not item:
        raise HTTPException(status_code=404, detail="backup file not found")

    is_admin = int(user.get("is_admin") or 0) == 1
    if not is_admin:
        assigned_code = _clean_site_code(user.get("site_code"), required=False)
        item_scope = str(item.get("scope") or "").strip().lower()
        item_code = str(item.get("site_code") or "").strip().upper()
        if not assigned_code:
            raise HTTPException(status_code=403, detail="소속 단지코드가 없어 다운로드할 수 없습니다.")
        if item_scope != "site" or item_code != assigned_code:
            raise HTTPException(status_code=403, detail="소속 단지코드 백업파일만 다운로드할 수 있습니다.")

    try:
        target = resolve_backup_file(str(item.get("relative_path") or path))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    filename = str(item.get("download_name") or target.name)
    return FileResponse(
        path=target,
        media_type="application/zip",
        filename=filename,
    )


@router.post("/backup/restore")
def api_backup_restore(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_admin(request)
    path = str(payload.get("path") or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="path is required")

    target_payload = payload.get("target_keys", payload.get("targets", []))
    if isinstance(target_payload, str):
        target_keys = [target_payload]
    elif isinstance(target_payload, list):
        target_keys = [str(x or "").strip().lower() for x in target_payload if str(x or "").strip()]
    else:
        raise HTTPException(status_code=400, detail="target_keys must be list")

    with_maintenance = _clean_bool(payload.get("with_maintenance"), default=True)
    actor_login = _clean_login_id(user.get("login_id") or "backup-restore")
    try:
        result = restore_backup_zip(
            actor=actor_login,
            relative_path=path,
            target_keys=target_keys,
            with_maintenance=with_maintenance,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"restore failed: {e}") from e

    return {"ok": True, "result": result, "maintenance": get_maintenance_status()}


@router.post("/backup/maintenance/clear")
def api_backup_maintenance_clear(request: Request):
    user, _token = _require_admin(request)
    actor_login = _clean_login_id(user.get("login_id") or "admin")
    status = clear_maintenance_mode(updated_by=actor_login)
    return {"ok": True, "maintenance": status}


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
            admin_scope="super_admin",
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
            admin_scope="super_admin",
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
    body = {
        "ok": True,
        "token": session["token"],
        "expires_at": session["expires_at"],
        "user": _public_user(user),
    }
    resp = JSONResponse(body)
    _set_auth_cookie(resp, session["token"])
    return resp


@router.post("/auth/signup/request_phone_verification")
def auth_signup_request_phone_verification(request: Request, payload: Dict[str, Any] = Body(...)):
    name = _clean_name(payload.get("name"))
    phone = _normalize_phone(payload.get("phone"), required=True, field_name="phone")
    site_code = _clean_site_code(payload.get("site_code"), required=False)
    site_name = _clean_required_text(payload.get("site_name"), 80, "site_name")
    role = _clean_role(payload.get("role"))
    unit_label, household_key = _extract_resident_household(payload, role=role)
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
        "unit_label": unit_label,
        "household_key": household_key,
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
            try:
                resolved_code = _resolve_site_code_for_site(
                    existing_site_name,
                    "",
                    allow_create=True,
                    allow_remap=False,
                )
            except HTTPException as e:
                if e.status_code == 404:
                    raise HTTPException(
                        status_code=403,
                        detail="단지코드 조회/생성에 실패했습니다. 잠시 후 다시 시도하세요.",
                    ) from e
                raise
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
    unit_label, household_key = _extract_resident_household(profile, role=role)
    site_code = _clean_site_code(profile.get("site_code"), required=False)
    site_name = _clean_required_text(profile.get("site_name"), 80, "site_name")
    address = _clean_required_text(profile.get("address"), 200, "address")
    office_phone = _normalize_phone(profile.get("office_phone"), required=True, field_name="office_phone")
    office_fax = _normalize_phone(profile.get("office_fax"), required=True, field_name="office_fax")

    try:
        resolved_site_code = _resolve_site_code_for_site(
            site_name,
            site_code,
            allow_create=True,
            allow_remap=False,
        )
    except HTTPException as e:
        if e.status_code == 404:
            raise HTTPException(
                status_code=403,
                detail="단지코드 조회/생성에 실패했습니다. 잠시 후 다시 시도하세요.",
            ) from e
        raise
    existing_site_user_count = count_staff_users_for_site(site_name, site_code=resolved_site_code)
    _assert_resident_household_available(
        role=role,
        site_code=resolved_site_code,
        household_key=household_key,
        is_active=True,
    )
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
        unit_label=unit_label,
        household_key=household_key,
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
    body = {
        "ok": True,
        "token": session["token"],
        "expires_at": session["expires_at"],
        "user": _public_user(fresh),
        "landing_path": _default_landing_path_for_user(fresh),
    }
    resp = JSONResponse(body)
    _set_auth_cookie(resp, session["token"])
    return resp


@router.post("/auth/logout")
def auth_logout(request: Request):
    try:
        _user, token = _require_auth(request)
        revoke_auth_session(token)
    except HTTPException:
        pass
    resp = JSONResponse({"ok": True})
    _clear_auth_cookie(resp)
    return resp


@router.get("/auth/me")
def auth_me(request: Request):
    user, _token = _require_auth(request)
    return {
        "ok": True,
        "user": _public_user(user),
        "session_expires_at": user.get("expires_at"),
        "landing_path": _default_landing_path_for_user(user),
    }


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
    body = {
        "ok": True,
        "token": session["token"],
        "expires_at": session["expires_at"],
        "user": _public_user(fresh),
        "landing_path": _default_landing_path_for_user(fresh),
    }
    resp = JSONResponse(body)
    _set_auth_cookie(resp, session["token"])
    return resp


@router.get("/user_roles")
def api_user_roles(request: Request):
    _require_auth(request)
    start_h, end_h = _critical_change_window()
    return {
        "ok": True,
        "roles": VALID_USER_ROLES,
        "permission_levels": [
            {"key": "admin", "label": "최고/운영관리자"},
            {"key": "site_admin", "label": "단지관리자"},
            {"key": "user", "label": "사용자"},
            {"key": "security_guard", "label": "보안/경비"},
            {"key": "resident", "label": "입주민"},
            {"key": "board_member", "label": "입대의"},
        ],
        "admin_scopes": [
            {"key": "super_admin", "label": ADMIN_SCOPE_LABELS["super_admin"]},
            {"key": "ops_admin", "label": ADMIN_SCOPE_LABELS["ops_admin"]},
        ],
        "security_policy": {
            "site_code_mutation": "super_admin_only_with_two_person_approval",
            "critical_mfa_required": _env_enabled("KA_CRITICAL_REQUIRE_MFA", True),
            "critical_change_window_enabled": _env_enabled("KA_CRITICAL_CHANGE_WINDOW_ENABLED", True),
            "critical_change_window": {"start_hour": start_h, "end_hour": end_h},
            "prechange_backup_enabled": _env_enabled("KA_PRECHANGE_BACKUP_ENABLED", True),
        },
        "recommended_staff_count": 9,
    }


@router.get("/users/me")
def api_users_me(request: Request):
    actor, _token = _require_auth(request)
    actor = _bind_user_site_code_if_missing(actor)
    return {"ok": True, "user": _public_user(actor)}


@router.patch("/users/me")
def api_users_me_patch(request: Request, payload: Dict[str, Any] = Body(...)):
    actor, _token = _require_auth(request)
    user_id = int(actor.get("id") or 0)
    current = get_staff_user(user_id)
    if not current:
        raise HTTPException(status_code=404, detail="user not found")

    restricted_keys = {
        "login_id",
        "role",
        "site_code",
        "site_name",
        "is_admin",
        "is_site_admin",
        "admin_scope",
        "permission_level",
        "is_active",
    }
    blocked = sorted([str(k) for k in payload.keys() if str(k) in restricted_keys])
    if blocked:
        raise HTTPException(
            status_code=403,
            detail=f"해당 항목은 관리자만 수정할 수 있습니다: {', '.join(blocked)}",
        )

    login_id = _clean_login_id(current.get("login_id"))
    name = _clean_name(payload.get("name", current.get("name")))
    role = _clean_role(payload.get("role", current.get("role")))
    phone = _normalize_phone(
        payload["phone"] if "phone" in payload else current.get("phone"),
        required=False,
        field_name="phone",
    )
    site_code = _clean_site_code(current.get("site_code"), required=False)
    site_name = _clean_optional_text(current.get("site_name"), 80)
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
    unit_label, household_key = _extract_resident_household(
        payload,
        role=role,
        current_unit_label=current.get("unit_label"),
    )
    is_admin = 1 if bool(current.get("is_admin")) else 0
    is_site_admin = 1 if bool(current.get("is_site_admin")) else 0
    admin_scope = _admin_scope_from_user(current)
    is_active = 1 if bool(current.get("is_active")) else 0
    _assert_resident_household_available(
        role=role,
        site_code=site_code,
        household_key=household_key,
        is_active=bool(is_active),
        exclude_user_id=user_id,
    )

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
            unit_label=unit_label,
            household_key=household_key,
            note=note,
            is_admin=is_admin,
            is_site_admin=is_site_admin,
            admin_scope=admin_scope,
            is_active=is_active,
        )
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            raise HTTPException(status_code=409, detail="login_id already exists")
        raise
    if not user:
        raise HTTPException(status_code=404, detail="user not found")

    password_changed = False
    new_password = _clean_password(payload.get("password"), required=False) if "password" in payload else None
    if new_password:
        set_staff_user_password(user_id, new_password)
        revoke_all_user_sessions(user_id)
        password_changed = True

    fresh = get_staff_user(user_id) or user
    return {"ok": True, "user": _public_user(fresh), "password_changed": password_changed}


@router.get("/users")
def api_users(
    request: Request,
    active_only: int = Query(default=0),
    site_code: str = Query(default=""),
    site_name: str = Query(default=""),
    region: str = Query(default=""),
    keyword: str = Query(default=""),
):
    _require_admin(request)
    clean_site_code = _clean_site_code(site_code, required=False)
    clean_site_name = _clean_query_text(site_name, max_len=80)
    clean_region = _normalize_region_label(_clean_query_text(region, max_len=40))
    clean_keyword = _clean_query_text(keyword, max_len=80).lower()

    source_users = [_public_user(x) for x in list_staff_users(active_only=bool(active_only))]
    for u in source_users:
        u["region"] = _region_from_address(u.get("address"))

    site_bucket: Dict[str, Dict[str, Any]] = {}
    region_bucket: Dict[str, int] = {}
    for u in source_users:
        row_code = _clean_site_code(u.get("site_code"), required=False)
        row_name = str(u.get("site_name") or "").strip()
        if row_code or row_name:
            key = f"{row_code}|{row_name}"
            item = site_bucket.get(key)
            if not item:
                item = {"site_code": row_code, "site_name": row_name, "count": 0}
                site_bucket[key] = item
            item["count"] = int(item["count"]) + 1

        row_region = _normalize_region_label(u.get("region"))
        if row_region:
            region_bucket[row_region] = int(region_bucket.get(row_region) or 0) + 1

    users = source_users
    if clean_site_code:
        users = [u for u in users if _clean_site_code(u.get("site_code"), required=False) == clean_site_code]
    if clean_site_name:
        users = [u for u in users if str(u.get("site_name") or "").strip() == clean_site_name]
    if clean_region:
        users = [u for u in users if _normalize_region_label(u.get("region")) == clean_region]
    if clean_keyword:
        def _hit(u: Dict[str, Any]) -> bool:
            hay = " ".join(
                [
                    str(u.get("login_id") or ""),
                    str(u.get("name") or ""),
                    str(u.get("role") or ""),
                    str(u.get("phone") or ""),
                    str(u.get("site_code") or ""),
                    str(u.get("site_name") or ""),
                    str(u.get("unit_label") or ""),
                    str(u.get("address") or ""),
                    str(u.get("office_phone") or ""),
                    str(u.get("office_fax") or ""),
                ]
            ).lower()
            return clean_keyword in hay

        users = [u for u in users if _hit(u)]

    sites = sorted(
        site_bucket.values(),
        key=lambda x: (
            str(x.get("site_code") or "").strip(),
            str(x.get("site_name") or "").strip(),
        ),
    )
    regions = sorted(
        [{"region": k, "count": v} for k, v in region_bucket.items()],
        key=lambda x: str(x.get("region") or ""),
    )

    return {
        "ok": True,
        "recommended_staff_count": 9,
        "count": len(users),
        "users": users,
        "filters": {
            "applied": {
                "active_only": bool(active_only),
                "site_code": clean_site_code,
                "site_name": clean_site_name,
                "region": clean_region,
                "keyword": clean_keyword,
            },
            "sites": sites,
            "regions": regions,
        },
    }


@router.post("/users")
def api_users_create(request: Request, payload: Dict[str, Any] = Body(...)):
    actor, _token = _require_admin(request)
    actor_super = _is_super_admin(actor)
    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name"))
    raw_role = _clean_role(payload.get("role"))
    is_admin, is_site_admin, permission_level, admin_scope = _resolve_permission_flags(
        payload,
        default_admin=False,
        default_site_admin=False,
        default_admin_scope="ops_admin",
    )
    role = _effective_role_for_permission_level(raw_role, permission_level)
    phone = _normalize_phone(payload.get("phone"), required=False, field_name="phone")
    site_code = _clean_site_code(payload.get("site_code"), required=False)
    site_name = _clean_optional_text(payload.get("site_name"), 80)
    allow_site_code_create = bool(not is_admin)
    if is_admin:
        allow_site_code_create = bool(actor_super)
    if site_name:
        try:
            site_code = _resolve_site_code_for_site(
                site_name,
                site_code,
                allow_create=allow_site_code_create,
                allow_remap=False,
            )
        except HTTPException as e:
            if e.status_code == 404 and not allow_site_code_create:
                raise HTTPException(
                    status_code=403,
                    detail="운영관리자는 기존 단지코드만 사용할 수 있습니다. 최고관리자에게 등록 요청하세요.",
                ) from e
            raise
    address = _clean_optional_text(payload.get("address"), 200)
    office_phone = _normalize_phone(payload.get("office_phone"), required=False, field_name="office_phone")
    office_fax = _normalize_phone(payload.get("office_fax"), required=False, field_name="office_fax")
    note = _clean_optional_text(payload.get("note"), 200)
    unit_label, household_key = _extract_resident_household(payload, role=role)
    password = _clean_password(payload.get("password"), required=True)
    if is_admin and not actor_super:
        raise HTTPException(status_code=403, detail="운영관리자는 관리자 계정을 생성할 수 없습니다.")
    if not is_admin:
        admin_scope = ""
    is_active = 1 if _clean_bool(payload.get("is_active"), default=True) else 0
    _assert_resident_household_available(
        role=role,
        site_code=site_code,
        household_key=household_key,
        is_active=bool(is_active),
    )
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
            unit_label=unit_label,
            household_key=household_key,
            note=note,
            password_hash=hash_password(password),
            is_admin=is_admin,
            is_site_admin=is_site_admin,
            admin_scope=admin_scope,
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
    actor_super = _is_super_admin(actor)
    current = get_staff_user(user_id)
    if not current:
        raise HTTPException(status_code=404, detail="user not found")
    current_admin_scope = _admin_scope_from_user(current)

    login_id = _clean_login_id(payload.get("login_id", current.get("login_id")))
    name = _clean_name(payload.get("name", current.get("name")))
    raw_role = _clean_role(payload.get("role", current.get("role")))
    is_admin, is_site_admin, permission_level, admin_scope = _resolve_permission_flags(
        payload,
        default_admin=bool(current.get("is_admin")),
        default_site_admin=bool(current.get("is_site_admin")),
        default_admin_scope=current_admin_scope,
    )
    role = _effective_role_for_permission_level(raw_role, permission_level)
    phone = _normalize_phone(payload["phone"] if "phone" in payload else current.get("phone"), required=False, field_name="phone")
    site_code = _clean_site_code(payload["site_code"] if "site_code" in payload else current.get("site_code"), required=False)
    site_name = _clean_optional_text(payload["site_name"] if "site_name" in payload else current.get("site_name"), 80)
    allow_site_code_create = bool(not is_admin)
    if is_admin:
        allow_site_code_create = bool(actor_super)
    if site_name:
        try:
            site_code = _resolve_site_code_for_site(
                site_name,
                site_code,
                allow_create=allow_site_code_create,
                allow_remap=False,
            )
        except HTTPException as e:
            if e.status_code == 404 and not allow_site_code_create:
                raise HTTPException(
                    status_code=403,
                    detail="운영관리자는 기존 단지코드만 사용할 수 있습니다. 최고관리자에게 등록 요청하세요.",
                ) from e
            raise
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
    unit_label, household_key = _extract_resident_household(
        payload,
        role=role,
        current_unit_label=current.get("unit_label"),
    )
    if not is_admin:
        admin_scope = ""
    is_active = (
        _clean_bool(payload["is_active"], default=bool(current.get("is_active")))
        if "is_active" in payload
        else bool(current.get("is_active"))
    )
    _assert_resident_household_available(
        role=role,
        site_code=site_code,
        household_key=household_key,
        is_active=bool(is_active),
        exclude_user_id=int(user_id),
    )

    if not actor_super:
        if bool(current.get("is_admin")):
            raise HTTPException(status_code=403, detail="운영관리자는 관리자 계정을 수정할 수 없습니다.")
        if is_admin:
            raise HTTPException(status_code=403, detail="운영관리자는 관리자 권한을 부여할 수 없습니다.")

    if int(actor["id"]) == int(user_id) and not is_admin:
        raise HTTPException(status_code=400, detail="cannot remove your own admin permission")
    if int(actor["id"]) == int(user_id) and not is_active:
        raise HTTPException(status_code=400, detail="cannot deactivate your own account")
    if int(actor["id"]) == int(user_id) and _is_super_admin(actor) and (not is_admin or admin_scope != "super_admin"):
        raise HTTPException(status_code=400, detail="cannot remove your own super admin scope")

    if bool(current.get("is_admin")) and not is_admin and bool(current.get("is_active")) and count_staff_admins(active_only=True) <= 1:
        raise HTTPException(status_code=400, detail="at least one active admin is required")
    if (
        bool(current.get("is_admin"))
        and current_admin_scope == "super_admin"
        and bool(current.get("is_active"))
        and (not is_admin or admin_scope != "super_admin")
        and count_super_admins(active_only=True) <= 1
    ):
        raise HTTPException(status_code=400, detail="at least one active super admin is required")

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
            unit_label=unit_label,
            household_key=household_key,
            note=note,
            is_admin=1 if is_admin else 0,
            is_site_admin=1 if is_site_admin else 0,
            admin_scope=admin_scope,
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
    actor_super = _is_super_admin(actor)
    if int(actor["id"]) == int(user_id):
        raise HTTPException(status_code=400, detail="cannot delete your own account")

    target = get_staff_user(user_id)
    if not target:
        return {"ok": False}
    target_scope = _admin_scope_from_user(target)
    if bool(target.get("is_admin")) and not actor_super:
        raise HTTPException(status_code=403, detail="운영관리자는 관리자 계정을 삭제할 수 없습니다.")
    if bool(target.get("is_admin")) and bool(target.get("is_active")) and count_staff_admins(active_only=True) <= 1:
        raise HTTPException(status_code=400, detail="at least one active admin is required")
    if target_scope == "super_admin" and bool(target.get("is_active")) and count_super_admins(active_only=True) <= 1:
        raise HTTPException(status_code=400, detail="at least one active super admin is required")

    revoke_all_user_sessions(int(user_id))
    ok = delete_staff_user(user_id)
    return {"ok": ok}


@router.post("/save")
def api_save(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_auth(request)
    site_name, site_code = _resolve_main_site_target(
        user, payload.get("site_name"), payload.get("site_code"), required=True
    )
    entry_date = safe_ymd(payload.get("date") or "")

    raw_tabs = payload.get("tabs") or {}
    if not isinstance(raw_tabs, dict):
        raise HTTPException(status_code=400, detail="tabs must be object")

    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
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
        "site_code": site_code,
        "date": entry_date,
        "saved_tabs": sorted(tabs.keys()),
        "ignored_tabs": ignored_tabs,
    }


@router.get("/load")
def api_load(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date: str = Query(...),
):
    user, _token = _require_auth(request)
    site_name, site_code = _resolve_main_site_target(user, site_name, site_code, required=True)
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
    tabs = load_entry(site_id, entry_date, allowed_keys_by_tab=_schema_allowed_keys(schema))
    return {"ok": True, "site_name": site_name, "site_code": site_code, "date": entry_date, "tabs": tabs}


@router.delete("/delete")
def api_delete(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date: str = Query(...),
):
    user, _token = _require_auth(request)
    site_name, site_code = _resolve_main_site_target(user, site_name, site_code, required=True)
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    ok = delete_entry(site_id, entry_date)
    return {"ok": ok, "site_name": site_name, "site_code": site_code}


@router.get("/list_range")
def api_list_range(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    user, _token = _require_auth(request)
    site_name, site_code = _resolve_main_site_target(user, site_name, site_code, required=True)
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df
    site_id = ensure_site(site_name)
    entries = list_entries(site_id, df, dt)
    dates = [e["entry_date"] for e in entries]
    return {"ok": True, "site_name": site_name, "site_code": site_code, "date_from": df, "date_to": dt, "dates": dates}


@router.get("/export")
def api_export(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
):
    user, _token = _require_auth(request)
    site_name, site_code = _resolve_main_site_target(user, site_name, site_code, required=True)
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df

    site_id = ensure_site(site_name)
    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
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
def api_pdf(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date: str = Query(...),
):
    user, _token = _require_auth(request)
    site_name, site_code = _resolve_main_site_target(user, site_name, site_code, required=True)
    entry_date = safe_ymd(date)
    site_id = ensure_site(site_name)
    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
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
