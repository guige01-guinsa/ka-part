from __future__ import annotations

import io
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
import zipfile
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from itsdangerous import URLSafeTimedSerializer

from ..backup_manager import (
    BACKUP_ROOT,
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
from ..ops_diagnostics import (
    get_ops_diagnostics_status,
    run_ops_diagnostics,
)
from ..db import (
    apartment_profile_defaults,
    cleanup_expired_sessions,
    count_active_resident_household_users,
    count_staff_admins,
    count_super_admins,
    count_recent_signup_phone_verifications,
    create_signup_phone_verification,
    create_auth_session,
    create_privileged_change_request,
    create_staff_user,
    delete_privileged_change_request,
    delete_entry,
    delete_site_env_config,
    delete_staff_user,
    find_site_code_by_id,
    get_auth_user_by_token,
    get_privileged_change_request,
    get_latest_signup_phone_verification,
    find_site_name_by_id,
    find_site_code_by_name,
    find_site_name_by_code,
    get_site_env_config,
    get_site_env_record,
    get_site_apartment_profile_record,
    get_site_env_config_version,
    get_first_staff_user_for_site,
    get_staff_user_by_phone,
    get_staff_user,
    get_staff_user_by_login,
    hash_password,
    list_entries,
    list_module_contracts,
    list_privileged_change_requests,
    list_security_audit_logs,
    list_site_env_config_versions,
    list_site_env_configs,
    list_staff_users,
    load_entry,
    load_entry_by_id,
    mark_staff_user_login,
    mark_privileged_change_request_executed,
    normalize_staff_user_site_identity,
    normalize_work_type,
    migrate_site_code,
    approve_privileged_change_request,
    revoke_all_user_sessions,
    revoke_auth_session,
    resolve_or_create_site_code,
    resolve_site_identity,
    rollback_site_env_config_version,
    save_tab_values,
    schema_alignment_report,
    site_identity_consistency_report,
    set_staff_user_site_code,
    set_staff_user_password,
    touch_signup_phone_verification_attempt,
    upsert_site_apartment_profile,
    upsert_site_env_config,
    update_staff_user,
    update_staff_user_profile_fields,
    upsert_entry,
    upsert_tab_domain_data,
    verify_password,
    withdraw_staff_user,
    write_security_audit_log,
    delete_site_apartment_profile,
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

VALID_USER_ROLES = [
    "최고/운영관리자",
    "단지대표자",
    "사용자",
    "보안/경비",
    "입주민",
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
SITE_ADMIN_ROLE_SET = {"단지대표자", "단지관리자"}  # accept legacy label for backward compatibility
ROLE_LABEL_BY_PERMISSION = {
    "admin": "최고/운영관리자",
    "site_admin": "단지대표자",
    "user": "사용자",
    "security_guard": "보안/경비",
    "resident": "입주민",
    "board_member": "입대의",
}
DEFAULT_GENERAL_MODULE_ORDER = ["main", "parking", "complaints", "inspection", "electrical_ai"]
SITE_REGISTRY_REQUEST_CHANGE_TYPE = "site_code_registration"
DEFAULT_SITE_NAME = "미지정단지"
PHONE_VERIFY_TTL_MINUTES = 5
PHONE_VERIFY_MAX_ATTEMPTS = 5
SIGNUP_FINALIZE_TOKEN_MAX_AGE_SEC = max(300, min(7200, int(os.getenv("KA_SIGNUP_FINALIZE_TOKEN_MAX_AGE_SEC", "900"))))
SIGNUP_PASSWORD_MIN_LENGTH = max(8, min(64, int(os.getenv("KA_SIGNUP_PASSWORD_MIN_LENGTH", "10"))))
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
BOOTSTRAP_TOKEN = (os.getenv("KA_BOOTSTRAP_TOKEN") or "").strip()
BACKUP_RESTORE_UPLOAD_MAX_BYTES = max(
    10 * 1024 * 1024,
    int(os.getenv("KA_BACKUP_RESTORE_UPLOAD_MAX_BYTES", str(1024 * 1024 * 1024))),
)
_BACKUP_DOWNLOAD_TOKEN_LOCK = threading.Lock()
_BACKUP_DOWNLOAD_TOKENS: Dict[str, Dict[str, Any]] = {}
_SPEC_ENV_MANAGE_CODE_TOKEN_LOCK = threading.Lock()
_SPEC_ENV_MANAGE_CODE_TOKENS: Dict[str, Dict[str, Any]] = {}
_SPEC_ENV_MANAGE_CODE_FAIL_LOCK = threading.Lock()
_SPEC_ENV_MANAGE_CODE_FAIL_STATE: Dict[str, deque[float]] = {}


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
SITE_CODE_AUTOCREATE_NON_ADMIN = _env_enabled("KA_SITE_CODE_AUTOCREATE_NON_ADMIN", False)
ALLOW_QUERY_ACCESS_TOKEN = _env_enabled("KA_ALLOW_QUERY_ACCESS_TOKEN", False)
BACKUP_DOWNLOAD_LINK_TTL_SEC = _safe_int_env("KA_BACKUP_DOWNLOAD_LINK_TTL_SEC", 600, 60)
BACKUP_SITE_DAILY_MAX_RUNS = _safe_int_env("KA_BACKUP_SITE_DAILY_MAX_RUNS", 5, 0)
BACKUP_SITE_DAILY_MAX_BYTES = _safe_int_env("KA_BACKUP_SITE_DAILY_MAX_BYTES", 2 * 1024 * 1024 * 1024, 0)
SPEC_ENV_MANAGE_CODE_TTL_SEC = max(60, min(3600, _safe_int_env("KA_SPEC_ENV_MANAGE_CODE_TTL_SEC", 600, 60)))
SPEC_ENV_MANAGE_CODE_WINDOW_SEC = max(30, min(3600, _safe_int_env("KA_SPEC_ENV_MANAGE_CODE_WINDOW_SEC", 300, 30)))
SPEC_ENV_MANAGE_CODE_MAX_FAILURES = max(1, min(20, _safe_int_env("KA_SPEC_ENV_MANAGE_CODE_MAX_FAILURES", 5, 1)))
if AUTH_COOKIE_SAMESITE == "none":
    # Modern browsers require Secure when SameSite=None.
    AUTH_COOKIE_SECURE = True


def _client_ip(request: Request) -> str:
    if not request:
        return ""
    xff = str(request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        first = xff.split(",", 1)[0].strip()
        if first:
            return first
    xri = str(request.headers.get("x-real-ip") or "").strip()
    if xri:
        return xri
    return request.client.host if request.client else ""


_LOGIN_FAIL_LOCK = threading.Lock()
_LOGIN_FAIL_STATE: Dict[str, deque[float]] = {}


def _login_fail_key(request: Request, login_id: str) -> str:
    ip = _client_ip(request) or "unknown"
    clean_login = str(login_id or "").strip().lower() or "-"
    return f"{ip}:{clean_login}"


def _check_login_fail_rate(request: Request, login_id: str) -> None:
    if not _env_enabled("KA_LOGIN_RATE_LIMIT_ENABLED", True):
        return
    window_sec = max(30, min(86400, _safe_int_env("KA_LOGIN_RATE_LIMIT_WINDOW_SEC", 600, 30)))
    max_failures = max(3, min(100, _safe_int_env("KA_LOGIN_RATE_LIMIT_MAX_FAILURES", 10, 3)))
    key = _login_fail_key(request, login_id)
    now = time.time()
    cutoff = now - float(window_sec)
    with _LOGIN_FAIL_LOCK:
        dq = _LOGIN_FAIL_STATE.get(key)
        if not dq:
            return
        while dq and float(dq[0]) < cutoff:
            dq.popleft()
        if len(dq) >= max_failures:
            raise HTTPException(status_code=429, detail="로그인 시도가 너무 많습니다. 잠시 후 다시 시도하세요.")


def _record_login_failure(request: Request, login_id: str) -> None:
    if not _env_enabled("KA_LOGIN_RATE_LIMIT_ENABLED", True):
        return
    window_sec = max(30, min(86400, _safe_int_env("KA_LOGIN_RATE_LIMIT_WINDOW_SEC", 600, 30)))
    key = _login_fail_key(request, login_id)
    now = time.time()
    cutoff = now - float(window_sec)
    with _LOGIN_FAIL_LOCK:
        dq = _LOGIN_FAIL_STATE.get(key)
        if dq is None:
            dq = deque()
            _LOGIN_FAIL_STATE[key] = dq
        dq.append(float(now))
        while dq and float(dq[0]) < cutoff:
            dq.popleft()


def _clear_login_failures(request: Request, login_id: str) -> None:
    key = _login_fail_key(request, login_id)
    with _LOGIN_FAIL_LOCK:
        _LOGIN_FAIL_STATE.pop(key, None)


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


def _clean_signup_login_id(value: Any) -> str:
    login_id = (str(value or "")).strip().lower()
    if not re.match(r"^[a-z0-9][a-z0-9_]{7,24}$", login_id):
        raise HTTPException(status_code=400, detail="아이디는 소문자/숫자/_만 사용하여 8~25자로 입력하세요.")
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


def _permission_level_from_role_text(value: Any, *, allow_admin_levels: bool = True) -> str:
    role = _normalized_role_text(value)
    if allow_admin_levels and role in {"최고/운영관리자", "최고관리자", "운영관리자"}:
        return "admin"
    if allow_admin_levels and role in SITE_ADMIN_ROLE_SET:
        return "site_admin"
    if _is_security_role(role):
        return "security_guard"
    if _is_resident_role(role):
        return "resident"
    if _is_board_role(role):
        return "board_member"
    return "user"


def _active_general_modules() -> List[str]:
    known = set(DEFAULT_GENERAL_MODULE_ORDER)
    try:
        contracts = list_module_contracts(active_only=True)
    except Exception:
        return list(DEFAULT_GENERAL_MODULE_ORDER)
    ordered: List[str] = []
    for item in contracts:
        key = str(item.get("module_key") or "").strip()
        if key and key in known and key not in ordered:
            ordered.append(key)
    if ordered:
        return ordered
    return list(DEFAULT_GENERAL_MODULE_ORDER)


def _allowed_modules_for_user(user: Dict[str, Any]) -> List[str]:
    role = _effective_role_for_permission_level(
        _normalized_role_text(user.get("role")),
        _permission_level_from_user(user),
    )
    if _is_security_role(role):
        return ["parking"]
    if _is_complaints_only_role(role):
        return ["complaints"]
    return _active_general_modules()


def _module_ui_path(module_key: str) -> str:
    clean_key = str(module_key or "").strip()
    fallback = {
        "main": "/pwa/",
        "parking": "/parking/admin2",
        "complaints": "/pwa/complaints.html",
        "inspection": "/pwa/inspection.html",
        "electrical_ai": "/pwa/electrical_ai.html",
    }.get(clean_key, "/pwa/")
    try:
        rows = list_module_contracts(active_only=True)
    except Exception:
        return fallback
    for item in rows:
        key = str(item.get("module_key") or "").strip()
        if key != clean_key:
            continue
        ui_path = str(item.get("ui_path") or "").strip()
        return ui_path or fallback
    return fallback


def _default_landing_path_for_user(user: Dict[str, Any]) -> str:
    modules = _allowed_modules_for_user(user)
    if len(modules) == 1:
        return _module_ui_path(modules[0])
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
    mapped = ROLE_LABEL_BY_PERMISSION.get(level)
    if mapped:
        return mapped
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
    site_code = re.sub(r"[\s-]+", "", str(value or "").strip()).upper()
    if not site_code:
        if required:
            raise HTTPException(status_code=400, detail="site_code is required")
        return ""
    if not re.match(r"^[A-Z]{3}[0-9]{5}$", site_code):
        raise HTTPException(status_code=400, detail="site_code must match ^[A-Z]{3}[0-9]{5}$")
    return site_code


def _clean_site_id(value: Any, *, required: bool = False) -> int:
    raw = str(value or "").strip()
    if not raw:
        if required:
            raise HTTPException(status_code=400, detail="site_id is required")
        return 0
    try:
        site_id = int(raw)
    except Exception as e:
        raise HTTPException(status_code=400, detail="site_id must be integer") from e
    if site_id <= 0:
        if required:
            raise HTTPException(status_code=400, detail="site_id must be positive")
        return 0
    return site_id


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


def _clean_work_type(value: Any, *, default: str = "일일") -> str:
    clean_default = str(default or "").strip() or "일일"
    return normalize_work_type(value, default=clean_default)


def _entry_work_type_from_tabs(tabs: Dict[str, Dict[str, Any]] | None, *, default: str = "일일") -> str:
    if not isinstance(tabs, dict):
        return _clean_work_type("", default=default)
    home = tabs.get("home")
    if not isinstance(home, dict):
        return _clean_work_type("", default=default)
    return _clean_work_type(home.get("work_type"), default=default)


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
    return hashlib.sha256(src.encode("utf-8")).hexdigest()


def _assert_signup_sms_rate_limit(request: Request, phone: str) -> None:
    if not _env_enabled("KA_SIGNUP_SMS_RATE_LIMIT_ENABLED", True):
        return
    window_min = _safe_int_env("KA_SIGNUP_SMS_RATE_LIMIT_WINDOW_MIN", 15, 1)
    max_per_phone = _safe_int_env("KA_SIGNUP_SMS_RATE_LIMIT_MAX_PER_PHONE", 3, 1)
    max_per_ip = _safe_int_env("KA_SIGNUP_SMS_RATE_LIMIT_MAX_PER_IP", 30, 1)

    clean_phone = str(phone or "").strip()
    ip = _client_ip(request)
    if clean_phone and count_recent_signup_phone_verifications(phone=clean_phone, minutes=window_min) >= max_per_phone:
        raise HTTPException(status_code=429, detail="인증번호 요청이 너무 많습니다. 잠시 후 다시 시도하세요.")
    if ip and count_recent_signup_phone_verifications(request_ip=ip, minutes=window_min) >= max_per_ip:
        raise HTTPException(status_code=429, detail="요청이 너무 많습니다. 잠시 후 다시 시도하세요.")


def _mock_sms_response(*, debug_code: str = "", mock_message: str = "") -> Dict[str, Any]:
    fallback = mock_message or (
        "SMS 설정이 없습니다. 현재는 화면에 인증번호를 표시합니다. "
        "(KA_SMS_WEBHOOK_URL 또는 KA_SOLAPI_API_KEY/KA_SOLAPI_API_SECRET/KA_SOLAPI_FROM 설정 필요)"
    )
    out: Dict[str, Any] = {
        "delivery": "mock",
        "message": fallback,
    }
    if debug_code:
        out["debug_code"] = str(debug_code)
    return out


def _send_sms_via_webhook(webhook: str, phone: str, message: str) -> Dict[str, Any]:
    payload = json.dumps({"to": phone, "message": str(message or "")}, ensure_ascii=False).encode("utf-8")
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
    return {"delivery": "webhook"}


def _build_solapi_authorization(*, api_key: str, api_secret: str) -> str:
    date_header = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    salt = secrets.token_hex(16)
    source = f"{date_header}{salt}"
    signature = hmac.new(
        api_secret.encode("utf-8"),
        source.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return (
        f"HMAC-SHA256 ApiKey={api_key}, "
        f"Date={date_header}, "
        f"salt={salt}, "
        f"signature={signature}"
    )


def _send_sms_via_solapi(
    *,
    phone: str,
    message: str,
    api_key: str,
    api_secret: str,
    sender: str,
) -> Dict[str, Any]:
    to_digits = _phone_digits(phone)
    from_digits = _phone_digits(sender)
    if not to_digits or not from_digits:
        raise HTTPException(status_code=500, detail="solapi phone configuration is invalid")

    payload = json.dumps(
        {
            "messages": [
                {
                    "to": to_digits,
                    "from": from_digits,
                    "text": str(message or ""),
                }
            ]
        },
        ensure_ascii=False,
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://api.solapi.com/messages/v4/send-many",
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": _build_solapi_authorization(api_key=api_key, api_secret=api_secret),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            if int(getattr(resp, "status", 500)) >= 300:
                raise HTTPException(status_code=502, detail="solapi sms delivery failed")
    except urllib.error.HTTPError as e:
        detail = "solapi sms delivery failed"
        try:
            body = e.read().decode("utf-8", "ignore").strip()
            if body:
                detail = f"{detail}: {body[:160]}"
        except Exception:
            pass
        raise HTTPException(status_code=502, detail=detail) from e
    except urllib.error.URLError as e:
        raise HTTPException(status_code=502, detail=f"solapi sms delivery failed: {e.reason}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"solapi sms delivery failed: {e}") from e
    return {"delivery": "solapi"}


def _send_sms_message(
    phone: str,
    message: str,
    *,
    debug_code: str = "",
    mock_message: str = "",
) -> Dict[str, Any]:
    provider = (os.getenv("KA_SMS_PROVIDER") or "auto").strip().lower()
    if provider not in {"auto", "webhook", "solapi", "mock"}:
        provider = "auto"
    webhook = (os.getenv("KA_SMS_WEBHOOK_URL") or "").strip()
    solapi_key = (os.getenv("KA_SOLAPI_API_KEY") or "").strip()
    solapi_secret = (os.getenv("KA_SOLAPI_API_SECRET") or "").strip()
    solapi_from = (os.getenv("KA_SOLAPI_FROM") or "").strip()

    if provider == "mock":
        return _mock_sms_response(debug_code=debug_code, mock_message=mock_message)

    if provider in {"auto", "webhook"} and webhook:
        return _send_sms_via_webhook(webhook, phone, message)
    if provider == "webhook":
        raise HTTPException(status_code=500, detail="KA_SMS_PROVIDER=webhook 이지만 KA_SMS_WEBHOOK_URL 미설정 상태입니다.")

    if provider in {"auto", "solapi"} and solapi_key and solapi_secret and solapi_from:
        return _send_sms_via_solapi(
            phone=phone,
            message=message,
            api_key=solapi_key,
            api_secret=solapi_secret,
            sender=solapi_from,
        )
    if provider == "solapi":
        raise HTTPException(
            status_code=500,
            detail=(
                "KA_SMS_PROVIDER=solapi 이지만 KA_SOLAPI_API_KEY/KA_SOLAPI_API_SECRET/KA_SOLAPI_FROM "
                "중 일부가 미설정 상태입니다."
            ),
        )

    return _mock_sms_response(debug_code=debug_code, mock_message=mock_message)


def _send_sms_verification(phone: str, code: str) -> Dict[str, Any]:
    message = f"[아파트 시설관리] 인증번호 {code} (유효 {PHONE_VERIFY_TTL_MINUTES}분)"
    return _send_sms_message(
        phone,
        message,
        debug_code=code,
        mock_message=(
            "문자 연동 미설정 상태입니다. 현재는 화면에 인증번호를 표시합니다. "
            "(KA_SMS_WEBHOOK_URL 또는 KA_SOLAPI_* 설정)"
        ),
    )


def _mask_phone(phone: str) -> str:
    digits = _phone_digits(phone)
    if len(digits) < 7:
        return str(phone or "")
    if len(digits) == 11:
        return f"{digits[:3]}-****-{digits[-4:]}"
    return f"{digits[:3]}***{digits[-4:]}"


def _signup_ready_setup_url(*, request: Request, phone: str, login_id: str = "") -> str:
    base = str(request.base_url or "").strip().rstrip("/")
    if not base:
        base = ""
    query = {"signup_ready": "1", "phone": str(phone or "").strip()}
    clean_login_id = str(login_id or "").strip().lower()
    if clean_login_id:
        query["login_id"] = clean_login_id
    return f"{base}/pwa/login.html?{urllib.parse.urlencode(query)}"


def _send_sms_signup_ready(phone: str, code: str, *, setup_url: str) -> Dict[str, Any]:
    message = (
        f"[아파트 시설관리] 단지코드 등록이 완료되었습니다. 인증번호 {code} (유효 {PHONE_VERIFY_TTL_MINUTES}분). "
        f"아래 링크에서 인증확인 후 비밀번호를 설정하세요. {setup_url}"
    )
    return _send_sms_message(
        phone,
        message,
        debug_code=code,
        mock_message=(
            "문자 연동 미설정 상태입니다. 사용자관리 요청처리 결과에 인증번호를 표시합니다. "
            "(KA_SMS_WEBHOOK_URL 또는 KA_SOLAPI_* 설정)"
        ),
    )


def _build_signup_profile_from_site_registry_payload(
    req_payload: Dict[str, Any],
    *,
    site_name: str,
    site_code: str,
) -> Tuple[Dict[str, Any] | None, str]:
    payload = req_payload if isinstance(req_payload, dict) else {}
    try:
        name = _clean_name(payload.get("signup_name", payload.get("requester_name")))
        phone = _normalize_phone(
            payload.get("signup_phone", payload.get("requester_phone")),
            required=True,
            field_name="requester_phone",
        )
        desired_raw = payload.get("signup_login_id", payload.get("requester_login_id"))
        desired_text = str(desired_raw or "").strip()
        if desired_text:
            try:
                desired_login_id = _clean_signup_login_id(desired_text)
            except HTTPException:
                desired_login_id = _generate_login_id_from_phone(phone)
        else:
            desired_login_id = _generate_login_id_from_phone(phone)

        raw_role = _clean_role(payload.get("signup_role", payload.get("requester_role")))
        signup_level = _permission_level_from_role_text(raw_role)
        if signup_level == "admin":
            raise HTTPException(status_code=403, detail="최고/운영관리자 계정은 자가가입할 수 없습니다.")
        role = _effective_role_for_permission_level(raw_role, signup_level)

        unit_source = payload.get("signup_unit_label", payload.get("requester_unit_label"))
        unit_label, household_key = _extract_resident_household({"unit_label": unit_source}, role=role)
        address = _clean_required_text(payload.get("signup_address", payload.get("requester_address")), 200, "address")
        office_phone = _normalize_phone(
            payload.get("signup_office_phone", payload.get("requester_office_phone")),
            required=True,
            field_name="office_phone",
        )
        office_fax = _normalize_phone(
            payload.get("signup_office_fax", payload.get("requester_office_fax")),
            required=True,
            field_name="office_fax",
        )
        _assert_resident_household_available(
            role=role,
            site_code=site_code,
            household_key=household_key,
            is_active=True,
        )
        existing = get_staff_user_by_phone(phone)
        if existing:
            raise HTTPException(status_code=409, detail="이미 등록된 휴대폰번호입니다.")

        profile = {
            "name": name,
            "phone": phone,
            "desired_login_id": desired_login_id,
            "site_code": site_code,
            "site_name": site_name,
            "role": role,
            "unit_label": unit_label,
            "household_key": household_key,
            "address": address,
            "office_phone": office_phone,
            "office_fax": office_fax,
        }
        return profile, ""
    except HTTPException as e:
        return None, str(e.detail or "signup_profile_invalid")
    except Exception as e:
        return None, str(e)


def _issue_site_registry_signup_ready_notice(
    *,
    request: Request,
    req_payload: Dict[str, Any],
    site_name: str,
    site_code: str,
) -> Dict[str, Any]:
    profile, reason = _build_signup_profile_from_site_registry_payload(
        req_payload,
        site_name=site_name,
        site_code=site_code,
    )
    if not profile:
        return {
            "notified": False,
            "reason": reason or "signup_profile_missing",
        }

    phone = str(profile.get("phone") or "").strip()
    desired_login_id = str(profile.get("desired_login_id") or "").strip().lower()
    code = f"{secrets.randbelow(1000000):06d}"
    code_hash = _phone_code_hash(phone, code)
    expires_at = (datetime.now() + timedelta(minutes=PHONE_VERIFY_TTL_MINUTES)).replace(microsecond=0).isoformat(sep=" ")
    create_signup_phone_verification(
        phone=phone,
        code_hash=code_hash,
        payload=profile,
        expires_at=expires_at,
        request_ip=(_client_ip(request) or None),
    )
    setup_url = _signup_ready_setup_url(request=request, phone=phone, login_id=desired_login_id)
    delivery = _send_sms_signup_ready(phone, code, setup_url=setup_url)
    out = {
        "notified": True,
        "delivery": delivery.get("delivery") or "sms",
        "phone_masked": _mask_phone(phone),
        "expires_at": expires_at,
        "setup_url": setup_url,
        "message": "등록처리 완료 안내를 발송했습니다.",
    }
    if delivery.get("message"):
        out["message"] = str(delivery.get("message"))
    if delivery.get("debug_code"):
        out["debug_code"] = str(delivery.get("debug_code"))
    return out


def _latest_executed_site_registry_request_for_phone(phone: str) -> Dict[str, Any] | None:
    target_digits = _phone_digits(phone)
    if not target_digits:
        return None
    rows = list_privileged_change_requests(
        change_type=SITE_REGISTRY_REQUEST_CHANGE_TYPE,
        status="executed",
        limit=500,
    )
    for item in rows:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        row_phone = str(payload.get("signup_phone") or payload.get("requester_phone") or "").strip()
        if not row_phone:
            continue
        if _phone_digits(row_phone) == target_digits:
            return item
    return None


def _generate_login_id_from_phone(phone: str) -> str:
    digits = _phone_digits(phone)
    base = f"u{digits[-8:]}" if digits else f"user{secrets.randbelow(10000):04d}"
    base = re.sub(r"[^a-z0-9_]", "", base.lower())
    if len(base) < 8:
        base = (base + "user0000")[:8]
    base = base[:25]
    candidate = base
    seq = 0
    while not _is_signup_login_id_available(candidate):
        seq += 1
        suffix = f"_{seq}"
        candidate = f"{base[: max(1, 25 - len(suffix))]}{suffix}"
    return candidate


def _generate_temp_password(length: int = 10) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    n = max(8, int(length))
    return "".join(secrets.choice(alphabet) for _ in range(n))


def _signup_finalize_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(PHONE_VERIFY_SECRET_VALUE, salt="signup-finalize-v1")


def _issue_signup_finalize_token(*, verification_id: int, phone: str) -> str:
    return _signup_finalize_serializer().dumps({"v": int(verification_id), "p": str(phone or "").strip()})


def _parse_signup_finalize_token(token: Any) -> Dict[str, Any]:
    raw = str(token or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="signup_token is required")
    try:
        data = _signup_finalize_serializer().loads(raw, max_age=SIGNUP_FINALIZE_TOKEN_MAX_AGE_SEC)
    except Exception:
        raise HTTPException(status_code=401, detail="invalid or expired signup token")
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="invalid signup token payload")
    try:
        verification_id = int(data.get("v") or 0)
    except Exception:
        verification_id = 0
    if verification_id <= 0:
        raise HTTPException(status_code=400, detail="invalid signup token payload")
    phone = _normalize_phone(data.get("p"), required=True, field_name="phone")
    return {"verification_id": verification_id, "phone": phone}


def _is_signup_login_id_available(login_id: str, *, phone: str = "") -> bool:
    existing = get_staff_user_by_login(login_id)
    if not existing:
        return True
    existing_phone_digits = _phone_digits(str(existing.get("phone") or ""))
    request_phone_digits = _phone_digits(str(phone or ""))
    if existing_phone_digits and request_phone_digits and existing_phone_digits == request_phone_digits:
        return True
    return False


def _signup_password_policy_meta() -> Dict[str, Any]:
    return {
        "min_length": SIGNUP_PASSWORD_MIN_LENGTH,
        "rules": [
            f"{SIGNUP_PASSWORD_MIN_LENGTH}자 이상",
            "영문 대/소문자, 숫자, 특수문자 중 3종 이상 포함",
            "아이디/휴대폰번호 포함 금지",
            "같은 문자 3회 연속 금지",
        ],
    }


def _signup_password_violations(password: str, *, login_id: str = "", phone: str = "") -> List[str]:
    pw = str(password or "")
    clean_login = str(login_id or "").strip().lower()
    digits = _phone_digits(phone)
    violations: List[str] = []

    if len(pw) < SIGNUP_PASSWORD_MIN_LENGTH:
        violations.append(f"비밀번호는 {SIGNUP_PASSWORD_MIN_LENGTH}자 이상이어야 합니다.")
    if len(pw) > 128:
        violations.append("비밀번호는 128자 이하여야 합니다.")
    if re.search(r"\s", pw):
        violations.append("비밀번호에 공백을 포함할 수 없습니다.")

    category_count = 0
    if re.search(r"[A-Z]", pw):
        category_count += 1
    if re.search(r"[a-z]", pw):
        category_count += 1
    if re.search(r"[0-9]", pw):
        category_count += 1
    if re.search(r"[^A-Za-z0-9]", pw):
        category_count += 1
    if category_count < 3:
        violations.append("영문 대/소문자, 숫자, 특수문자 중 3종 이상을 포함해야 합니다.")

    if re.search(r"(.)\1\1", pw):
        violations.append("같은 문자를 3회 이상 연속으로 사용할 수 없습니다.")

    low = pw.lower()
    if clean_login and len(clean_login) >= 3 and clean_login in low:
        violations.append("아이디를 포함한 비밀번호는 사용할 수 없습니다.")
    if digits:
        sensitive_parts = [digits[-8:], digits[-4:], digits]
        for part in sensitive_parts:
            if len(part) >= 4 and part in pw:
                violations.append("휴대폰번호를 포함한 비밀번호는 사용할 수 없습니다.")
                break

    if low in {"password", "qwer1234", "admin1234", "11111111", "12345678", "abcd1234"}:
        violations.append("추측하기 쉬운 비밀번호는 사용할 수 없습니다.")

    return violations


def _assert_signup_password_policy(password: Any, *, login_id: str = "", phone: str = "") -> str:
    pw = _clean_password(password, required=True)
    violations = _signup_password_violations(pw, login_id=login_id, phone=phone)
    if violations:
        raise HTTPException(status_code=400, detail=violations[0])
    return pw


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
    return _permission_level_from_role_text(user.get("role"), allow_admin_levels=False)


def _account_type_from_user(user: Dict[str, Any]) -> str:
    level = _permission_level_from_user(user)
    if level == "admin":
        return "최고/운영관리자"
    if level == "site_admin":
        return "단지대표자"
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

    if "role" in payload and "is_admin" not in payload and "is_site_admin" not in payload:
        inferred_level = _permission_level_from_role_text(payload.get("role"))
        if inferred_level == "admin":
            scope = _clean_admin_scope(payload.get("admin_scope"), required=False) or _clean_admin_scope(
                default_admin_scope, required=False
            )
            if not scope:
                scope = "ops_admin"
            return 1, 0, inferred_level, scope
        if inferred_level == "site_admin":
            return 0, 1, inferred_level, ""
        if inferred_level in {"security_guard", "resident", "board_member"}:
            return 0, 0, inferred_level, ""
        return 0, 0, "user", ""

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


def _bind_user_site_identity(user: Dict[str, Any]) -> Dict[str, Any]:
    if not user:
        return user
    uid = int(user.get("id") or 0)
    if uid > 0:
        normalized = normalize_staff_user_site_identity(uid)
        if normalized:
            merged = dict(user)
            merged.update(normalized)
            user = merged

    if int(user.get("is_admin") or 0) == 1:
        return user

    current_code = _clean_site_code(user.get("site_code"), required=False)
    if current_code:
        return user
    site_name = str(user.get("site_name") or "").strip()
    if not site_name:
        return user

    try:
        resolved = _resolve_site_code_for_site(
            site_name,
            "",
            allow_create=SITE_CODE_AUTOCREATE_NON_ADMIN,
            allow_remap=False,
        )
    except HTTPException:
        return user
    if not resolved:
        return user
    if uid > 0:
        set_staff_user_site_code(uid, resolved)
        fresh = normalize_staff_user_site_identity(uid) or get_staff_user(uid)
        if fresh:
            merged = dict(user)
            merged.update(fresh)
            return merged
    return user


def _is_public_access_user(user: Dict[str, Any]) -> bool:
    if not isinstance(user, dict):
        return False
    login_id = str(user.get("login_id") or "").strip().lower()
    if not login_id:
        return False
    return login_id == _public_access_login_id()


def _public_user(user: Dict[str, Any]) -> Dict[str, Any]:
    permission_level = _permission_level_from_user(user)
    admin_scope = _admin_scope_from_user(user)
    allowed_modules = _allowed_modules_for_user(user)
    account_type = _account_type_from_user(user)
    canonical_role = _effective_role_for_permission_level(_normalized_role_text(user.get("role")), permission_level)
    raw_site_id = user.get("site_id")
    site_id: int | None
    try:
        parsed_site_id = int(raw_site_id)
        site_id = parsed_site_id if parsed_site_id > 0 else None
    except Exception:
        site_id = None
    return {
        "id": int(user.get("id")),
        "login_id": user.get("login_id"),
        "name": user.get("name"),
        "role": canonical_role,
        "phone": user.get("phone"),
        "site_id": site_id,
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
        "is_public_access": _is_public_access_user(user),
        "allowed_modules": allowed_modules,
        "default_landing_path": _default_landing_path_for_user(user),
        "is_active": bool(user.get("is_active")),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
        "last_login_at": user.get("last_login_at"),
    }


def _public_access_enabled() -> bool:
    return _env_enabled("KA_PUBLIC_FULL_ACCESS_ENABLED", True)


def _public_access_login_id() -> str:
    raw = (os.getenv("KA_PUBLIC_FULL_ACCESS_LOGIN_ID") or "public_guest").strip().lower()
    try:
        return _clean_login_id(raw)
    except HTTPException:
        return _clean_login_id("public_guest")


def _public_access_name() -> str:
    raw = (os.getenv("KA_PUBLIC_FULL_ACCESS_NAME") or "로그인없이 사용자").strip()
    try:
        return _clean_name(raw)
    except HTTPException:
        return _clean_name("로그인없이 사용자")


def _public_access_site_name() -> str:
    raw = (os.getenv("KA_PUBLIC_FULL_ACCESS_SITE_NAME") or DEFAULT_SITE_NAME).strip()
    try:
        return _clean_site_name(raw, required=False)
    except HTTPException:
        return DEFAULT_SITE_NAME


def _public_access_site_code(site_name: str) -> str:
    raw = (os.getenv("KA_PUBLIC_FULL_ACCESS_SITE_CODE") or "").strip().upper()
    preferred = ""
    try:
        preferred = _clean_site_code(raw, required=False)
    except HTTPException:
        preferred = ""

    try:
        return _resolve_site_code_for_site(site_name, preferred, allow_create=True, allow_remap=False)
    except HTTPException:
        if preferred:
            return preferred
        found = _clean_site_code(find_site_code_by_name(site_name), required=False)
        if found:
            return found
        return "PUB00001"


def _ensure_public_access_user() -> Dict[str, Any]:
    login_id = _public_access_login_id()
    name = _public_access_name()
    site_name = _public_access_site_name()
    site_code = _public_access_site_code(site_name)
    role = ROLE_LABEL_BY_PERMISSION["user"]

    existing = get_staff_user_by_login(login_id)
    if existing:
        user = update_staff_user(
            int(existing["id"]),
            login_id=login_id,
            name=name,
            role=role,
            phone=existing.get("phone"),
            site_code=site_code,
            site_name=site_name,
            site_id=existing.get("site_id"),
            address=existing.get("address"),
            office_phone=existing.get("office_phone"),
            office_fax=existing.get("office_fax"),
            unit_label=existing.get("unit_label"),
            household_key=existing.get("household_key"),
            note=existing.get("note"),
            is_admin=0,
            is_site_admin=0,
            admin_scope="",
            is_active=1,
        )
        if not user:
            user = get_staff_user(int(existing["id"])) or existing
    else:
        try:
            user = create_staff_user(
                login_id=login_id,
                name=name,
                role=role,
                site_code=site_code,
                site_name=site_name,
                password_hash=hash_password(secrets.token_urlsafe(32)),
                is_admin=0,
                is_site_admin=0,
                admin_scope="",
                is_active=1,
            )
        except Exception:
            user = get_staff_user_by_login(login_id)
            if not user:
                raise

    uid = int(user.get("id") or 0)
    if uid <= 0:
        raise HTTPException(status_code=500, detail="public access user resolve failed")
    fresh = normalize_staff_user_site_identity(uid) or get_staff_user(uid)
    return fresh or user


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
    requested_site_id: Any = 0,
    *,
    require_any: bool = True,
    for_write: bool = False,
) -> Tuple[str, str]:
    raw_site_name = str(requested_site_name or "").strip()
    raw_site_code = str(requested_site_code or "").strip().upper()
    raw_site_id = _clean_site_id(requested_site_id, required=False)

    if int(user.get("is_admin") or 0) == 1:
        is_super = _is_super_admin(user)
        clean_site_name = _clean_site_name(raw_site_name, required=True) if raw_site_name else ""
        clean_site_code = _clean_site_code(raw_site_code, required=False)

        # site_id is the primary anchor for spec-env target selection.
        if raw_site_id:
            mapped_name = _clean_site_name(find_site_name_by_id(raw_site_id), required=False)
            mapped_code = _clean_site_code(find_site_code_by_id(raw_site_id), required=False)
            if mapped_name:
                clean_site_name = mapped_name
                # Ignore user-sent code when site_id is fixed.
                clean_site_code = mapped_code or ""
            elif mapped_code:
                clean_site_code = mapped_code

        if not clean_site_name and not clean_site_code:
            if require_any:
                raise HTTPException(status_code=400, detail="site_name 또는 site_code 중 하나를 입력하세요.")
            return "", ""

        if clean_site_code and not clean_site_name:
            resolved_name = find_site_name_by_code(clean_site_code)
            if not resolved_name:
                raise HTTPException(status_code=404, detail="입력한 site_code에 해당하는 site_name을 찾을 수 없습니다.")
            clean_site_name = _clean_site_name(resolved_name, required=True)
        if clean_site_code:
            mapped_name = find_site_name_by_code(clean_site_code)
            if mapped_name:
                clean_site_name = _clean_site_name(mapped_name, required=True)
        if clean_site_name:
            canonical_code = _clean_site_code(find_site_code_by_name(clean_site_name), required=False)
            if canonical_code:
                clean_site_code = canonical_code
        canonical = resolve_site_identity(
            site_id=(raw_site_id if raw_site_id > 0 else None),
            site_name=clean_site_name,
            site_code=clean_site_code,
            create_site_if_missing=False,
        )
        canonical_name = _clean_site_name(canonical.get("site_name"), required=False)
        canonical_code = _clean_site_code(canonical.get("site_code"), required=False)
        if canonical_name:
            clean_site_name = canonical_name
        if canonical_code:
            clean_site_code = canonical_code
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

    assigned_site_id = _clean_site_id(user.get("site_id"), required=False)
    clean_site_name = _resolve_allowed_site_name(user, raw_site_name, required=False)
    if assigned_site_id and not clean_site_name:
        clean_site_name = _clean_site_name(find_site_name_by_id(assigned_site_id), required=False)
    clean_site_code = _resolve_allowed_site_code(user, raw_site_code)
    if assigned_site_id and not clean_site_code:
        clean_site_code = _clean_site_code(find_site_code_by_id(assigned_site_id), required=False)
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


def _resolve_site_identity_for_main(
    user: Dict[str, Any],
    requested_site_name: Any,
    requested_site_code: Any,
    requested_site_id: Any = 0,
) -> Tuple[str, str]:
    raw_site_name = str(requested_site_name or "").strip()
    raw_site_code = str(requested_site_code or "").strip().upper()
    raw_site_id = _clean_site_id(requested_site_id, required=False)
    is_admin = int(user.get("is_admin") or 0) == 1

    if is_admin:
        is_super = _is_super_admin(user)
        clean_site_name = _clean_site_name(raw_site_name, required=True) if raw_site_name else ""
        clean_site_code = _clean_site_code(raw_site_code, required=False)
        clean_site_id = raw_site_id

        # site_id is treated as a fallback hint, not as authoritative input.
        # If name/code are provided, they should drive canonical resolution.
        if clean_site_id and not clean_site_name and not clean_site_code:
            clean_site_name = _clean_site_name(find_site_name_by_id(clean_site_id), required=False)
            clean_site_code = _clean_site_code(find_site_code_by_id(clean_site_id), required=False)

        if clean_site_code:
            mapped_name = find_site_name_by_code(clean_site_code)
            if mapped_name:
                clean_site_name = _clean_site_name(mapped_name, required=True)
        if clean_site_name:
            canonical_code = _clean_site_code(find_site_code_by_name(clean_site_name), required=False)
            if canonical_code:
                clean_site_code = canonical_code

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
        fallback_site_id = _clean_site_id(user.get("site_id"), required=False)
        if not fallback_name and fallback_site_id:
            fallback_name = _clean_site_name(find_site_name_by_id(fallback_site_id), required=False)
        if not fallback_code and fallback_site_id:
            fallback_code = _clean_site_code(find_site_code_by_id(fallback_site_id), required=False)
        if fallback_code and not fallback_name:
            fallback_name = _clean_site_name(find_site_name_by_code(fallback_code), required=False)
        return fallback_name, fallback_code

    # Non-admin accounts are always bound to assigned site identity.
    # Ignore client-sent site params to avoid stale local/query values causing conflicts.
    assigned_site_id = _clean_site_id(user.get("site_id"), required=False)
    assigned_site_name = str(user.get("site_name") or "").strip()
    if not assigned_site_name and assigned_site_id:
        assigned_site_name = _clean_site_name(find_site_name_by_id(assigned_site_id), required=False)
    if not assigned_site_name:
        assigned_site_name = _normalized_assigned_site_name(user)
    assigned_site_code = _clean_site_code(user.get("site_code"), required=False)

    if not assigned_site_code and assigned_site_id:
        assigned_site_code = _clean_site_code(find_site_code_by_id(assigned_site_id), required=False)
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
    if ALLOW_QUERY_ACCESS_TOKEN:
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
    role = _effective_role_for_permission_level(
        _normalized_role_text(user.get("role")),
        _permission_level_from_user(user),
    )
    if _is_security_role(role):
        allowed_paths = {
            "/api/auth/me",
            "/api/auth/logout",
            "/api/auth/change_password",
            "/api/modules/contracts",
            "/api/users/me",
            "/api/users/me/withdraw",
            "/api/parking/context",
        }
        if path in allowed_paths:
            return
        raise HTTPException(status_code=403, detail="보안/경비 계정은 주차관리 모듈만 사용할 수 있습니다.")
    if _is_complaints_only_role(role):
        allowed_paths = {
            "/api/auth/me",
            "/api/auth/logout",
            "/api/auth/change_password",
            "/api/modules/contracts",
            "/api/users/me",
            "/api/users/me/withdraw",
        }
        if path in allowed_paths:
            return
        raise HTTPException(status_code=403, detail="입대의/입주민 계정은 민원 모듈만 사용할 수 있습니다.")


def _require_auth(request: Request) -> Tuple[Dict[str, Any], str]:
    token = _extract_access_token(request)
    user = get_auth_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="invalid or expired session")
    user = _bind_user_site_identity(user)
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


def _spec_env_manage_code_feature_enabled() -> bool:
    if _env_enabled("KA_SPEC_ENV_MANAGE_CODE_ENABLED", False):
        return True
    for key in (
        "KA_SPEC_ENV_MANAGE_CODE_ADMIN",
        "KA_SPEC_ENV_MANAGE_CODE_SUPER_ADMIN",
        "KA_SPEC_ENV_MANAGE_CODE_SITE_ADMIN",
        "KA_SPEC_ENV_MANAGE_CODE_COMMON",
    ):
        if str(os.getenv(key) or "").strip():
            return True
    return False


def _spec_env_manage_code_bucket(user: Dict[str, Any]) -> str:
    if int(user.get("is_admin") or 0) == 1:
        return "admin"
    if int(user.get("is_site_admin") or 0) == 1:
        return "site_admin"
    return ""


def _spec_env_manage_code_role_label(bucket: str) -> str:
    clean = str(bucket or "").strip().lower()
    if clean == "admin":
        return "최고/운영관리자"
    if clean == "site_admin":
        return "단지대표자"
    return "-"


def _spec_env_manage_code_value(bucket: str) -> str:
    clean = str(bucket or "").strip().lower()
    if clean == "admin":
        raw = (
            os.getenv("KA_SPEC_ENV_MANAGE_CODE_ADMIN")
            or os.getenv("KA_SPEC_ENV_MANAGE_CODE_SUPER_ADMIN")
            or os.getenv("KA_SPEC_ENV_MANAGE_CODE_COMMON")
            or ""
        )
        return str(raw).strip()
    if clean == "site_admin":
        raw = os.getenv("KA_SPEC_ENV_MANAGE_CODE_SITE_ADMIN") or os.getenv("KA_SPEC_ENV_MANAGE_CODE_COMMON") or ""
        return str(raw).strip()
    return ""


def _spec_env_manage_code_site_admin_password_enabled() -> bool:
    # Default ON so site admins can use their account password unless explicitly disabled.
    return _env_enabled("KA_SPEC_ENV_MANAGE_CODE_SITE_ADMIN_USE_PASSWORD", True)


def _spec_env_manage_code_allow_password(bucket: str) -> bool:
    clean = str(bucket or "").strip().lower()
    return clean == "site_admin" and _spec_env_manage_code_site_admin_password_enabled()


def _spec_env_manage_code_verify_mode(bucket: str, expected_code: str) -> str:
    has_code = bool(str(expected_code or "").strip())
    allow_password = _spec_env_manage_code_allow_password(bucket)
    if has_code and allow_password:
        return "code_or_password"
    if has_code:
        return "code"
    if allow_password:
        return "password"
    return ""


def _spec_env_manage_code_policy(user: Dict[str, Any]) -> Dict[str, Any]:
    enabled = _spec_env_manage_code_feature_enabled()
    bucket = _spec_env_manage_code_bucket(user)
    role_label = _spec_env_manage_code_role_label(bucket)
    expected_code = _spec_env_manage_code_value(bucket) if enabled else ""
    verify_mode = _spec_env_manage_code_verify_mode(bucket, expected_code) if enabled else ""
    if enabled and bucket and not verify_mode:
        raise HTTPException(
            status_code=503,
            detail=(
                "제원설정 관리코드가 역할별로 설정되지 않았습니다. "
                "KA_SPEC_ENV_MANAGE_CODE_ADMIN / KA_SPEC_ENV_MANAGE_CODE_SITE_ADMIN "
                "(또는 KA_SPEC_ENV_MANAGE_CODE_COMMON) 값을 확인하거나 "
                "KA_SPEC_ENV_MANAGE_CODE_SITE_ADMIN_USE_PASSWORD=1(단지대표자 비밀번호 사용)을 설정하세요."
            ),
        )
    return {
        "enabled": bool(enabled),
        "required": bool(enabled and bucket and verify_mode),
        "role_bucket": bucket,
        "role_label": role_label,
        "verify_mode": verify_mode,
        "accept_password": bool(_spec_env_manage_code_allow_password(bucket)),
        "ttl_sec": int(SPEC_ENV_MANAGE_CODE_TTL_SEC),
        "expected_code": expected_code,
    }


def _spec_env_manage_code_fail_key(request: Request, user: Dict[str, Any]) -> str:
    ip = _client_ip(request) or "unknown"
    uid = int(user.get("id") or 0)
    login = _clean_login_id(user.get("login_id") or "")
    role = _spec_env_manage_code_bucket(user) or "-"
    return f"{ip}:{uid}:{login}:{role}"


def _check_spec_env_manage_code_rate_limit(request: Request, user: Dict[str, Any]) -> None:
    key = _spec_env_manage_code_fail_key(request, user)
    now_ts = time.time()
    cutoff = now_ts - float(SPEC_ENV_MANAGE_CODE_WINDOW_SEC)
    with _SPEC_ENV_MANAGE_CODE_FAIL_LOCK:
        dq = _SPEC_ENV_MANAGE_CODE_FAIL_STATE.get(key)
        if not dq:
            return
        while dq and float(dq[0]) < cutoff:
            dq.popleft()
        if len(dq) < int(SPEC_ENV_MANAGE_CODE_MAX_FAILURES):
            return
        wait = int(max(1, float(dq[0]) + float(SPEC_ENV_MANAGE_CODE_WINDOW_SEC) - now_ts))
    raise HTTPException(status_code=429, detail=f"관리코드 인증 시도가 너무 많습니다. {wait}초 후 다시 시도하세요.")


def _record_spec_env_manage_code_failure(request: Request, user: Dict[str, Any]) -> None:
    key = _spec_env_manage_code_fail_key(request, user)
    now_ts = time.time()
    cutoff = now_ts - float(SPEC_ENV_MANAGE_CODE_WINDOW_SEC)
    with _SPEC_ENV_MANAGE_CODE_FAIL_LOCK:
        dq = _SPEC_ENV_MANAGE_CODE_FAIL_STATE.setdefault(key, deque())
        while dq and float(dq[0]) < cutoff:
            dq.popleft()
        dq.append(now_ts)


def _clear_spec_env_manage_code_failures(request: Request, user: Dict[str, Any]) -> None:
    key = _spec_env_manage_code_fail_key(request, user)
    with _SPEC_ENV_MANAGE_CODE_FAIL_LOCK:
        _SPEC_ENV_MANAGE_CODE_FAIL_STATE.pop(key, None)


def _prune_spec_env_manage_code_tokens_locked(now_ts: float | None = None) -> None:
    cutoff = float(now_ts if now_ts is not None else time.time())
    stale = [
        token
        for token, payload in _SPEC_ENV_MANAGE_CODE_TOKENS.items()
        if float(payload.get("expires_at_ts") or 0.0) <= cutoff
    ]
    for token in stale:
        _SPEC_ENV_MANAGE_CODE_TOKENS.pop(token, None)


def _issue_spec_env_manage_code_token(*, user: Dict[str, Any], role_bucket: str) -> Dict[str, Any]:
    token = secrets.token_urlsafe(32)
    now_ts = time.time()
    expires_at_ts = now_ts + float(SPEC_ENV_MANAGE_CODE_TTL_SEC)
    with _SPEC_ENV_MANAGE_CODE_TOKEN_LOCK:
        _prune_spec_env_manage_code_tokens_locked(now_ts)
        _SPEC_ENV_MANAGE_CODE_TOKENS[token] = {
            "user_id": int(user.get("id") or 0),
            "actor_login": _clean_login_id(user.get("login_id") or ""),
            "role_bucket": str(role_bucket or "").strip().lower(),
            "issued_at_ts": now_ts,
            "expires_at_ts": expires_at_ts,
        }
    expires_at = datetime.fromtimestamp(expires_at_ts).replace(microsecond=0).isoformat(sep=" ")
    return {
        "token": token,
        "expires_in_sec": int(SPEC_ENV_MANAGE_CODE_TTL_SEC),
        "expires_at": expires_at,
    }


def _assert_spec_env_manage_code_token(*, user: Dict[str, Any], token: str, role_bucket: str, operation_label: str) -> None:
    clean_token = str(token or "").strip()
    if not clean_token:
        raise HTTPException(
            status_code=403,
            detail=f"{operation_label}에는 제원설정 관리코드 인증이 필요합니다.",
        )
    now_ts = time.time()
    with _SPEC_ENV_MANAGE_CODE_TOKEN_LOCK:
        _prune_spec_env_manage_code_tokens_locked(now_ts)
        payload = _SPEC_ENV_MANAGE_CODE_TOKENS.get(clean_token)
    if not payload:
        raise HTTPException(status_code=403, detail="제원설정 관리코드 인증이 만료되었습니다. 다시 인증하세요.")
    if float(payload.get("expires_at_ts") or 0.0) <= now_ts:
        with _SPEC_ENV_MANAGE_CODE_TOKEN_LOCK:
            _SPEC_ENV_MANAGE_CODE_TOKENS.pop(clean_token, None)
        raise HTTPException(status_code=403, detail="제원설정 관리코드 인증이 만료되었습니다. 다시 인증하세요.")

    expected_user_id = int(payload.get("user_id") or 0)
    actor_user_id = int(user.get("id") or 0)
    if expected_user_id > 0 and actor_user_id > 0 and expected_user_id != actor_user_id:
        raise HTTPException(status_code=403, detail="관리코드 인증 사용자와 요청 사용자가 일치하지 않습니다.")

    expected_login = str(payload.get("actor_login") or "").strip().lower()
    actor_login = _clean_login_id(user.get("login_id") or "")
    if expected_login and actor_login and expected_login != actor_login:
        raise HTTPException(status_code=403, detail="관리코드 인증 사용자와 요청 사용자가 일치하지 않습니다.")

    expected_bucket = str(payload.get("role_bucket") or "").strip().lower()
    clean_bucket = str(role_bucket or "").strip().lower()
    if expected_bucket and clean_bucket and expected_bucket != clean_bucket:
        raise HTTPException(status_code=403, detail="관리코드 인증 권한 범위가 올바르지 않습니다.")


def _spec_env_manage_code_matches(provided_code: str, expected_code: str) -> bool:
    provided = str(provided_code or "").strip()
    expected = str(expected_code or "").strip()
    if not provided or not expected:
        return False
    if expected.lower().startswith("sha256:"):
        expected_digest = expected.split(":", 1)[1].strip().lower()
        digest = hashlib.sha256(provided.encode("utf-8")).hexdigest().lower()
        if not expected_digest:
            return False
        return hmac.compare_digest(digest, expected_digest)
    return hmac.compare_digest(provided, expected)


def _assert_spec_env_manage_code_confirmed(
    request: Request,
    user: Dict[str, Any],
    payload: Dict[str, Any] | None = None,
    *,
    operation_label: str = "제원설정 변경",
) -> None:
    policy = _spec_env_manage_code_policy(user)
    if not bool(policy.get("required")):
        return
    body = payload if isinstance(payload, dict) else {}
    token = str(body.get("manage_code_token") or request.headers.get("X-KA-SPEC-ENV-CODE-TOKEN") or "").strip()
    _assert_spec_env_manage_code_token(
        user=user,
        token=token,
        role_bucket=str(policy.get("role_bucket") or ""),
        operation_label=operation_label,
    )


def _can_manage_backup(user: Dict[str, Any]) -> bool:
    return _permission_level_from_user(user) in {"admin", "site_admin"}


def _user_site_identity(user: Dict[str, Any]) -> Dict[str, Any]:
    clean_code = _clean_site_code(user.get("site_code"), required=False)
    clean_name = str(user.get("site_name") or "").strip()
    clean_site_id = _clean_site_id(user.get("site_id"), required=False)
    if clean_site_id <= 0 and clean_code:
        try:
            resolved = resolve_site_identity(
                site_code=clean_code,
                site_name=clean_name,
                create_site_if_missing=False,
            )
        except Exception:
            resolved = {}
        clean_site_id = _clean_site_id((resolved or {}).get("site_id"), required=False)
        if not clean_name:
            clean_name = str((resolved or {}).get("site_name") or "").strip()
    if not clean_code and clean_site_id > 0:
        clean_code = _clean_site_code(find_site_code_by_id(clean_site_id), required=False)
    return {
        "site_id": int(clean_site_id or 0),
        "site_code": clean_code,
        "site_name": clean_name,
    }


def _site_id_from_identity(site_code: Any, site_name: Any = "") -> int:
    clean_code = _clean_site_code(site_code, required=False)
    if not clean_code:
        return 0
    try:
        resolved = resolve_site_identity(
            site_code=clean_code,
            site_name=str(site_name or "").strip(),
            create_site_if_missing=False,
        )
    except Exception:
        return 0
    return _clean_site_id((resolved or {}).get("site_id"), required=False)


def _site_backup_usage_today(site_code: str) -> Dict[str, Any]:
    clean_code = _clean_site_code(site_code, required=False)
    today = datetime.now().strftime("%Y-%m-%d")
    if not clean_code:
        return {"date": today, "runs": 0, "size_bytes": 0}

    history = list_backup_history(limit=5000, scope="site", site_code=clean_code)
    runs = 0
    size_bytes = 0
    for item in history:
        created_at = str(item.get("created_at") or "").strip()
        if len(created_at) < 10 or created_at[:10] != today:
            continue
        runs += 1
        try:
            size_bytes += max(0, int(item.get("file_size_bytes") or 0))
        except Exception:
            pass
    return {"date": today, "runs": runs, "size_bytes": size_bytes}


def _enforce_site_backup_daily_limits(site_code: str) -> Dict[str, Any]:
    usage = _site_backup_usage_today(site_code)
    max_runs = int(BACKUP_SITE_DAILY_MAX_RUNS or 0)
    max_bytes = int(BACKUP_SITE_DAILY_MAX_BYTES or 0)
    if max_runs > 0 and int(usage.get("runs") or 0) >= max_runs:
        raise HTTPException(
            status_code=429,
            detail=f"일일 백업 횟수 제한({max_runs}회)에 도달했습니다. 내일 다시 시도해 주세요.",
        )
    if max_bytes > 0 and int(usage.get("size_bytes") or 0) >= max_bytes:
        raise HTTPException(
            status_code=429,
            detail=f"일일 백업 용량 제한({max_bytes} bytes)에 도달했습니다. 내일 다시 시도해 주세요.",
        )
    return usage


def _prune_backup_download_tokens_locked(now_ts: float | None = None) -> None:
    cutoff = float(now_ts if now_ts is not None else time.time())
    stale = [
        token
        for token, payload in _BACKUP_DOWNLOAD_TOKENS.items()
        if float(payload.get("expires_at_ts") or 0.0) <= cutoff
    ]
    for token in stale:
        _BACKUP_DOWNLOAD_TOKENS.pop(token, None)


def _issue_backup_download_token(*, user: Dict[str, Any], relative_path: str) -> Dict[str, Any]:
    clean_rel = str(relative_path or "").strip().replace("\\", "/")
    if not clean_rel:
        raise HTTPException(status_code=400, detail="path is required")
    now_ts = time.time()
    expires_at_ts = now_ts + float(BACKUP_DOWNLOAD_LINK_TTL_SEC)
    token = secrets.token_urlsafe(32)
    with _BACKUP_DOWNLOAD_TOKEN_LOCK:
        _prune_backup_download_tokens_locked(now_ts)
        _BACKUP_DOWNLOAD_TOKENS[token] = {
            "relative_path": clean_rel,
            "user_id": int(user.get("id") or 0),
            "actor_login": _clean_login_id(user.get("login_id") or ""),
            "issued_at_ts": now_ts,
            "expires_at_ts": expires_at_ts,
        }
    expires_at = datetime.fromtimestamp(expires_at_ts).replace(microsecond=0).isoformat(sep=" ")
    return {
        "token": token,
        "url": f"/api/backup/download?token={urllib.parse.quote(token)}",
        "expires_in_sec": int(BACKUP_DOWNLOAD_LINK_TTL_SEC),
        "expires_at": expires_at,
    }


def _consume_backup_download_token(*, user: Dict[str, Any], token: str) -> Dict[str, Any]:
    clean_token = str(token or "").strip()
    if not clean_token:
        raise HTTPException(status_code=400, detail="token is required")

    now_ts = time.time()
    with _BACKUP_DOWNLOAD_TOKEN_LOCK:
        _prune_backup_download_tokens_locked(now_ts)
        payload = _BACKUP_DOWNLOAD_TOKENS.pop(clean_token, None)

    if not payload:
        raise HTTPException(status_code=410, detail="다운로드 링크가 만료되었거나 이미 사용되었습니다.")
    if float(payload.get("expires_at_ts") or 0.0) <= now_ts:
        raise HTTPException(status_code=410, detail="다운로드 링크가 만료되었습니다.")

    expected_user_id = int(payload.get("user_id") or 0)
    if expected_user_id > 0 and expected_user_id != int(user.get("id") or 0):
        raise HTTPException(status_code=403, detail="발급받은 사용자만 다운로드할 수 있습니다.")

    expected_login = str(payload.get("actor_login") or "").strip().lower()
    actor_login = _clean_login_id(user.get("login_id") or "")
    if expected_login and actor_login and expected_login != actor_login:
        raise HTTPException(status_code=403, detail="발급받은 사용자만 다운로드할 수 있습니다.")

    return payload


def _resolve_backup_item_permission(
    *,
    user: Dict[str, Any],
    item: Dict[str, Any],
    requested_path: str = "",
) -> Dict[str, Any]:
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    rel = str(item.get("relative_path") or requested_path or "").strip()
    if not rel:
        raise HTTPException(status_code=400, detail="backup path is required")

    manifest: Dict[str, Any] = {}
    try:
        manifest = _validate_uploaded_restore_backup(rel)
    except HTTPException:
        # Sidecar metadata only file can still be downloaded by admin.
        if int(user.get("is_admin") or 0) != 1:
            raise
        manifest = {}

    item_scope = str(item.get("scope") or manifest.get("scope") or "").strip().lower()
    item_code = _clean_site_code(item.get("site_code"), required=False) or _clean_site_code(manifest.get("site_code"), required=False)
    item_name = str(item.get("site_name") or manifest.get("site_name") or "").strip()
    contains_user_data = bool(item.get("contains_user_data")) or bool(manifest.get("contains_user_data"))
    if not item_scope:
        item_scope = "site" if item_code else "full"

    is_admin = int(user.get("is_admin") or 0) == 1
    if not is_admin:
        assigned = _user_site_identity(user)
        assigned_code = str(assigned.get("site_code") or "").strip().upper()
        assigned_site_id = int(assigned.get("site_id") or 0)
        if not assigned_code:
            raise HTTPException(status_code=403, detail="소속 단지코드가 없어 다운로드할 수 없습니다.")
        if item_scope != "site":
            raise HTTPException(status_code=403, detail="소속 단지코드 백업파일만 다운로드할 수 있습니다.")
        if not item_code:
            raise HTTPException(status_code=400, detail="백업 파일에 단지코드(site_code)가 없습니다.")
        if item_code != assigned_code:
            raise HTTPException(status_code=403, detail="소속 단지코드 백업파일만 다운로드할 수 있습니다.")
        if contains_user_data:
            raise HTTPException(status_code=403, detail="사용자정보 포함 백업파일은 최고/운영관리자만 다운로드할 수 있습니다.")

        if assigned_site_id > 0:
            item_site_id = _clean_site_id(item.get("site_id"), required=False)
            if item_site_id <= 0:
                item_site_id = _clean_site_id(manifest.get("site_id"), required=False)
            if item_site_id <= 0:
                item_site_id = _site_id_from_identity(item_code, item_name)
            if item_site_id > 0 and item_site_id != assigned_site_id:
                raise HTTPException(status_code=403, detail="소속 단지(site_id) 백업파일만 다운로드할 수 있습니다.")

    return {
        "relative_path": rel,
        "scope": item_scope,
        "site_code": item_code,
        "site_name": item_name,
        "contains_user_data": contains_user_data,
        "manifest": manifest,
    }


def _store_uploaded_restore_backup(upload: UploadFile, actor_login: str) -> Dict[str, Any]:
    if upload is None:
        raise HTTPException(status_code=400, detail="backup_file is required")
    raw_name = str(getattr(upload, "filename", "") or "").strip()
    safe_name = Path(raw_name).name.strip()
    if not safe_name:
        safe_name = "mobile_upload"

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    token = secrets.token_hex(4)
    safe_actor = re.sub(r"[^a-z0-9_.-]+", "_", str(actor_login or "").strip().lower()) or "admin"
    out_dir = (Path(BACKUP_ROOT) / "imported" / datetime.now().strftime("%Y%m%d")).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_name = f"uploaded_{safe_actor}_{ts}_{token}.zip"
    out_path = (out_dir / out_name).resolve()

    total = 0
    signature = b""
    try:
        with open(out_path, "wb") as fp:
            while True:
                chunk = upload.file.read(1024 * 1024)
                if not chunk:
                    break
                if not signature:
                    signature = bytes(chunk[:4])
                total += len(chunk)
                if total > BACKUP_RESTORE_UPLOAD_MAX_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"복구 업로드 파일이 너무 큽니다. 최대 {BACKUP_RESTORE_UPLOAD_MAX_BYTES} bytes",
                    )
                fp.write(chunk)
        if total <= 0:
            raise HTTPException(status_code=400, detail="빈 파일은 복구할 수 없습니다.")
        valid_sig = signature.startswith(b"PK\x03\x04") or signature.startswith(b"PK\x05\x06") or signature.startswith(b"PK\x07\x08")
        if not valid_sig:
            raise HTTPException(status_code=400, detail="ZIP 형식 파일만 복구할 수 있습니다.")
    except HTTPException:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"복구 업로드 저장 실패: {e}") from e
    finally:
        try:
            upload.file.close()
        except Exception:
            pass

    rel = str(out_path.relative_to(Path(BACKUP_ROOT))).replace("\\", "/")
    return {
        "relative_path": rel,
        "file_name": out_name,
        "size_bytes": total,
        "original_name": safe_name,
    }


def _validate_uploaded_restore_backup(relative_path: str) -> Dict[str, Any]:
    try:
        zip_path = resolve_backup_file(relative_path)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = set(zf.namelist())
            if "manifest.json" not in names:
                raise HTTPException(status_code=400, detail="manifest.json 이 없는 복구 파일입니다.")
            with zf.open("manifest.json", "r") as fp:
                manifest = json.loads(fp.read().decode("utf-8", errors="ignore"))
    except HTTPException:
        raise
    except zipfile.BadZipFile as e:
        raise HTTPException(status_code=400, detail=f"손상된 zip 파일입니다: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"복구 파일 검증 실패: {e}") from e

    if not isinstance(manifest, dict):
        raise HTTPException(status_code=400, detail="manifest 형식이 올바르지 않습니다.")
    scope = str(manifest.get("scope") or "").strip().lower()
    if scope not in {"", "full", "site"}:
        raise HTTPException(status_code=400, detail="지원하지 않는 백업 범위(scope)입니다.")
    if not scope:
        if any(name.startswith("site_data/") and name.lower().endswith(".json") for name in names):
            scope = "site"
        else:
            scope = "full"
    manifest["scope"] = scope
    contains_user_data = bool(manifest.get("contains_user_data"))

    target_keys = [
        str(x or "").strip().lower()
        for x in (manifest.get("target_keys") or [])
        if str(x or "").strip()
    ]
    if scope == "full":
        if target_keys:
            missing = [k for k in target_keys if f"db/{k}.db" not in names]
            if missing:
                raise HTTPException(status_code=400, detail=f"복구 파일에 DB 항목이 없습니다: {', '.join(missing)}")
        else:
            db_members = [x for x in names if x.startswith("db/") and x.lower().endswith(".db")]
            if not db_members:
                raise HTTPException(status_code=400, detail="복구 가능한 DB 파일(db/*.db)이 없습니다.")
    else:
        site_code = _clean_site_code(manifest.get("site_code"), required=False)
        if not site_code:
            raise HTTPException(status_code=400, detail="단지코드(site_code)가 없는 site 백업 파일입니다.")
        if not contains_user_data and "site_data/facility.json" in names:
            try:
                with zipfile.ZipFile(zip_path, "r") as inspect_zip:
                    with inspect_zip.open("site_data/facility.json", "r") as fp:
                        facility_payload = json.loads(fp.read().decode("utf-8", errors="ignore"))
                tables = facility_payload.get("tables") if isinstance(facility_payload, dict) else {}
                if isinstance(tables, dict) and "staff_users" in tables:
                    contains_user_data = True
            except Exception:
                # keep previous detection result
                pass
        if target_keys:
            missing = [k for k in target_keys if f"site_data/{k}.json" not in names]
            if missing:
                raise HTTPException(status_code=400, detail=f"복구 파일에 site_data 항목이 없습니다: {', '.join(missing)}")
        else:
            site_members = [x for x in names if x.startswith("site_data/") and x.lower().endswith(".json")]
            if not site_members:
                raise HTTPException(status_code=400, detail="복구 가능한 site_data 파일이 없습니다.")

    manifest["contains_user_data"] = bool(contains_user_data)
    return manifest


def _enforce_restore_permission(user: Dict[str, Any], manifest: Dict[str, Any]) -> None:
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    is_admin = int(user.get("is_admin") or 0) == 1
    if is_admin:
        return

    raise HTTPException(
        status_code=403,
        detail="단지대표자는 직접 복구할 수 없습니다. 복구 요청을 등록하고 최고관리자 승인을 받으세요.",
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
    requested_site_id: Any = 0,
    *,
    required: bool = False,
) -> Tuple[str, str]:
    ident = _resolve_main_site_identity(
        user,
        requested_site_name,
        requested_site_code,
        requested_site_id,
        required=required,
    )
    return str(ident.get("site_name") or ""), str(ident.get("site_code") or "")


def _resolve_main_site_identity(
    user: Dict[str, Any],
    requested_site_name: Any,
    requested_site_code: Any,
    requested_site_id: Any = 0,
    *,
    required: bool = False,
) -> Dict[str, Any]:
    clean_site_name, clean_site_code = _resolve_site_identity_for_main(
        user,
        requested_site_name,
        requested_site_code,
        requested_site_id,
    )
    canonical_site_id = _clean_site_id(requested_site_id, required=False)
    if clean_site_name or clean_site_code:
        canonical_site_id = 0
    canonical = resolve_site_identity(
        site_id=canonical_site_id,
        site_name=clean_site_name,
        site_code=clean_site_code,
        create_site_if_missing=False,
    )
    canonical_name = _clean_site_name(canonical.get("site_name"), required=False)
    canonical_code = _clean_site_code(canonical.get("site_code"), required=False)
    if canonical_name:
        clean_site_name = canonical_name
    if canonical_code:
        clean_site_code = canonical_code
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

    allow_create_site = int(user.get("is_admin") or 0) == 1 and _is_super_admin(user)
    resolved = resolve_site_identity(
        site_name=clean_site_name,
        site_code=clean_site_code,
        create_site_if_missing=bool(allow_create_site),
    )
    resolved_site_id = _clean_site_id((resolved or {}).get("site_id"), required=False)
    resolved_site_name = _clean_site_name((resolved or {}).get("site_name"), required=False)
    resolved_site_code = _clean_site_code((resolved or {}).get("site_code"), required=False)

    if resolved_site_name:
        clean_site_name = resolved_site_name
    if resolved_site_code:
        clean_site_code = resolved_site_code

    if required and (not clean_site_name or resolved_site_id <= 0):
        raise HTTPException(status_code=404, detail="단지 식별정보(site_id/site_code)를 확인하세요.")

    return {
        "site_id": int(resolved_site_id or 0),
        "site_name": clean_site_name,
        "site_code": clean_site_code,
    }


@router.get("/health")
def health():
    report = schema_alignment_report()
    return {"ok": True, "version": "2.9.0", "schema_alignment_ok": report.get("ok", False)}


@router.get("/parking/context")
def parking_context(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    site_id: int = Query(default=0),
):
    user, _token = _require_auth(request)
    permission_level = _parking_permission_level_from_user(user)
    portal_permission_level = _permission_level_from_user(user)
    is_admin_user = int(user.get("is_admin") or 0) == 1

    # Non-admin accounts must always use their assigned site context.
    # This avoids false "site edit" errors when stale query/local values are sent.
    requested_site_name = site_name
    requested_site_code = site_code
    requested_site_id = site_id
    if not is_admin_user:
        requested_site_name = str(user.get("site_name") or "").strip()
        requested_site_code = str(user.get("site_code") or "").strip().upper()
        requested_site_id = _clean_site_id(user.get("site_id"), required=False)

    site_ident = _resolve_main_site_identity(
        user,
        requested_site_name,
        requested_site_code,
        requested_site_id,
        required=False,
    )
    clean_site_name = _clean_site_name(site_ident.get("site_name"), required=False)
    clean_site_code = _clean_site_code(site_ident.get("site_code"), required=False)
    resolved_site_id = _clean_site_id(site_ident.get("site_id"), required=False)

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

    if resolved_site_id <= 0:
        resolved_identity = resolve_site_identity(
            site_name=clean_site_name,
            site_code=clean_site_code,
            create_site_if_missing=False,
        )
        resolved_site_id = _clean_site_id(resolved_identity.get("site_id"), required=False)

    serializer = URLSafeTimedSerializer(_parking_context_secret(), salt="parking-context")
    ctx = serializer.dumps(
        {
            "site_code": clean_site_code,
            "site_name": clean_site_name,
            "permission_level": permission_level,
            "portal_permission_level": portal_permission_level,
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
        "site_id": (resolved_site_id or 0),
        "site_code": clean_site_code,
        "site_name": clean_site_name,
        "permission_level": permission_level,
        "expires_in": PARKING_CONTEXT_MAX_AGE,
    }


@router.get("/schema")
def api_schema(request: Request, site_name: str = Query(default=""), site_code: str = Query(default="")):
    user, _token = _require_auth(request)
    site_ident = _resolve_main_site_identity(
        user, site_name, site_code, required=False
    )
    clean_site_name = str(site_ident.get("site_name") or "")
    resolved_site_code = str(site_ident.get("site_code") or "")
    resolved_site_id = int(site_ident.get("site_id") or 0)
    schema, env_cfg = _site_schema_and_env(clean_site_name, resolved_site_code)

    return {
        "schema": schema,
        "site_id": resolved_site_id,
        "site_name": clean_site_name,
        "site_code": resolved_site_code,
        "site_env_config": env_cfg,
    }


@router.get("/schema_alignment")
def api_schema_alignment(request: Request):
    _require_admin(request)
    return schema_alignment_report()


@router.get("/ops/diagnostics")
def api_ops_diagnostics(request: Request):
    _require_admin(request)
    return {"ok": True, "diagnostics": get_ops_diagnostics_status()}


@router.post("/ops/diagnostics/run")
def api_ops_diagnostics_run(request: Request):
    _require_admin(request)
    return {"ok": True, "diagnostics": run_ops_diagnostics()}


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


@router.get("/site_env/manage_code/policy")
def api_site_env_manage_code_policy(request: Request):
    user, _token = _require_site_env_manager(request)
    policy = _spec_env_manage_code_policy(user)
    return {
        "ok": True,
        "enabled": bool(policy.get("enabled")),
        "required": bool(policy.get("required")),
        "role_bucket": str(policy.get("role_bucket") or ""),
        "role_label": str(policy.get("role_label") or ""),
        "verify_mode": str(policy.get("verify_mode") or ""),
        "ttl_sec": int(policy.get("ttl_sec") or SPEC_ENV_MANAGE_CODE_TTL_SEC),
    }


@router.post("/site_env/manage_code/verify")
def api_site_env_manage_code_verify(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_site_env_manager(request)
    policy = _spec_env_manage_code_policy(user)
    if not bool(policy.get("required")):
        return {
            "ok": True,
            "enabled": bool(policy.get("enabled")),
            "required": False,
            "role_bucket": str(policy.get("role_bucket") or ""),
            "role_label": str(policy.get("role_label") or ""),
            "verify_mode": str(policy.get("verify_mode") or ""),
        }

    _check_spec_env_manage_code_rate_limit(request, user)
    code = str((payload or {}).get("code") or "").strip()
    verify_mode = str(policy.get("verify_mode") or "")
    if not code:
        if verify_mode == "password":
            raise HTTPException(status_code=400, detail="비밀번호를 입력하세요.")
        if verify_mode == "code_or_password":
            raise HTTPException(status_code=400, detail="관리코드 또는 비밀번호를 입력하세요.")
        raise HTTPException(status_code=400, detail="관리코드를 입력하세요.")

    expected_code = str(policy.get("expected_code") or "").strip()
    role_bucket = str(policy.get("role_bucket") or "")
    role_label = str(policy.get("role_label") or "")
    matched = _spec_env_manage_code_matches(code, expected_code)
    match_source = "code" if matched else ""

    if (not matched) and bool(policy.get("accept_password")):
        login_id = _clean_login_id(user.get("login_id") or "")
        if login_id:
            db_user = get_staff_user_by_login(login_id)
            password_hash = str((db_user or {}).get("password_hash") or "")
            if password_hash and verify_password(code, password_hash):
                matched = True
                match_source = "password"

    if not matched:
        _record_spec_env_manage_code_failure(request, user)
        _audit_security(
            user=user,
            event_type="site_env_manage_code_verify",
            severity="WARN",
            outcome="denied",
            detail={"reason": "code_mismatch", "role_bucket": role_bucket, "verify_mode": verify_mode},
        )
        if verify_mode == "password":
            raise HTTPException(status_code=403, detail="비밀번호가 일치하지 않습니다.")
        if verify_mode == "code_or_password":
            raise HTTPException(status_code=403, detail="제원설정 관리코드 또는 비밀번호가 일치하지 않습니다.")
        raise HTTPException(status_code=403, detail="제원설정 관리코드가 일치하지 않습니다.")

    _clear_spec_env_manage_code_failures(request, user)
    ticket = _issue_spec_env_manage_code_token(user=user, role_bucket=role_bucket)
    _audit_security(
        user=user,
        event_type="site_env_manage_code_verify",
        severity="INFO",
        outcome="ok",
        detail={"role_bucket": role_bucket, "verify_mode": verify_mode, "match_source": match_source},
    )
    return {
        "ok": True,
        "enabled": True,
        "required": True,
        "role_bucket": role_bucket,
        "role_label": role_label,
        "verify_mode": verify_mode,
        "token": str(ticket.get("token") or ""),
        "expires_in_sec": int(ticket.get("expires_in_sec") or SPEC_ENV_MANAGE_CODE_TTL_SEC),
        "expires_at": str(ticket.get("expires_at") or ""),
    }


@router.get("/site_env")
def api_site_env(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    site_id: int = Query(default=0),
):
    user, _token = _require_site_env_manager(request)
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, site_name, site_code, site_id, require_any=True, for_write=False
    )
    verify = _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)

    row = get_site_env_record(clean_site_name, site_code=clean_site_code or None)
    resolved_site_name = str((row or {}).get("site_name") or "").strip() or clean_site_name
    resolved_site_code = _clean_site_code((row or {}).get("site_code"), required=False) or clean_site_code
    schema, env_cfg = _site_schema_and_env(resolved_site_name, resolved_site_code)
    resolved_identity = resolve_site_identity(
        site_id=_clean_site_id(site_id, required=False),
        site_name=resolved_site_name,
        site_code=resolved_site_code,
        create_site_if_missing=False,
    )
    resolved_site_id = _clean_site_id(resolved_identity.get("site_id"), required=False)
    return {
        "ok": True,
        "site_id": resolved_site_id,
        "site_name": resolved_site_name,
        "site_code": resolved_site_code,
        "config": env_cfg,
        "schema": schema,
        "spec_access": verify,
    }


@router.get("/site_identity")
def api_site_identity(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    site_id: int = Query(default=0),
):
    user, _token = _require_auth(request)
    ident = _resolve_main_site_identity(user, site_name, site_code, site_id, required=False)
    clean_site_name = str(ident.get("site_name") or "")
    clean_site_code = str(ident.get("site_code") or "")
    resolved_site_id = int(ident.get("site_id") or 0)
    admin_scope = _admin_scope_from_user(user)
    if resolved_site_id <= 0:
        try:
            parsed = int(user.get("site_id") or 0)
            resolved_site_id = parsed if parsed > 0 else 0
        except Exception:
            resolved_site_id = 0
    return {
        "ok": True,
        "site_id": resolved_site_id,
        "site_name": clean_site_name,
        "site_code": clean_site_code,
        "editable": int(user.get("is_admin") or 0) == 1,
        "site_code_editable": admin_scope == "super_admin",
        "admin_scope": admin_scope,
    }


@router.get("/site_identity/diagnostics")
def api_site_identity_diagnostics(
    request: Request,
    limit: int = Query(default=200),
):
    _require_admin(request)
    report = site_identity_consistency_report(limit=max(10, min(int(limit), 2000)))
    return {"ok": True, "report": report}


def _site_registry_request_status_label(status: str) -> str:
    clean = str(status or "").strip().lower()
    if clean == "pending":
        return "대기"
    if clean == "approved":
        return "승인"
    if clean == "executed":
        return "처리완료"
    if clean:
        return clean
    return "-"


def _site_registry_request_public(item: Dict[str, Any]) -> Dict[str, Any]:
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    result = item.get("result") if isinstance(item.get("result"), dict) else {}
    signup_ready = result.get("signup_ready") if isinstance(result.get("signup_ready"), dict) else {}
    requested_site_code = str(payload.get("requested_site_code") or item.get("target_site_code") or "").strip().upper()
    resolved_site_code = str(result.get("site_code") or "").strip().upper()
    resolved_site_name = str(result.get("site_name") or "").strip()
    site_name = str(payload.get("site_name") or item.get("target_site_name") or "").strip()
    status = str(item.get("status") or "").strip().lower()
    return {
        "id": int(item.get("id") or 0),
        "status": status,
        "status_label": _site_registry_request_status_label(status),
        "site_name": site_name,
        "requested_site_code": requested_site_code,
        "resolved_site_name": resolved_site_name,
        "resolved_site_code": resolved_site_code,
        "requester_name": str(payload.get("requester_name") or "").strip(),
        "requester_login_id": str(payload.get("requester_login_id") or "").strip().lower(),
        "requester_phone": str(payload.get("requester_phone") or "").strip(),
        "requester_role": str(payload.get("requester_role") or "").strip(),
        "requester_unit_label": str(payload.get("requester_unit_label") or "").strip(),
        "requester_note": str(payload.get("requester_note") or "").strip(),
        "requested_by_login": str(item.get("requested_by_login") or "").strip(),
        "processed_by_login": str(item.get("executed_by_login") or "").strip(),
        "created_at": str(item.get("created_at") or "").strip(),
        "approved_at": str(item.get("approved_at") or "").strip(),
        "processed_at": str(item.get("executed_at") or "").strip(),
        "expires_at": str(item.get("expires_at") or "").strip(),
        "signup_ready_notified": bool(signup_ready.get("notified")),
        "signup_ready_delivery": str(signup_ready.get("delivery") or "").strip(),
        "signup_ready_phone_masked": str(signup_ready.get("phone_masked") or "").strip(),
        "signup_ready_expires_at": str(signup_ready.get("expires_at") or "").strip(),
        "signup_ready_setup_url": str(signup_ready.get("setup_url") or "").strip(),
        "signup_ready_message": str(signup_ready.get("message") or signup_ready.get("reason") or "").strip(),
        "signup_ready_debug_code": (
            str(signup_ready.get("debug_code") or "").strip()
            if str(signup_ready.get("delivery") or "").strip().lower() == "mock"
            else ""
        ),
    }


def _site_registry_auto_actor() -> Dict[str, Any]:
    try:
        users = list_staff_users(active_only=True)
    except Exception:
        users = []
    for row in users:
        try:
            if int(row.get("is_admin") or 0) != 1:
                continue
            if not _is_super_admin(row):
                continue
            actor_id = int(row.get("id") or 0)
            actor_login = _clean_login_id(row.get("login_id") or "")
            if actor_id > 0 and actor_login:
                return {"id": actor_id, "login_id": actor_login}
        except Exception:
            continue
    return {"id": 1, "login_id": "auto.site_registry"}


def _site_registry_actor_identity(actor: Dict[str, Any] | None) -> Tuple[int, str]:
    actor_id = 0
    actor_login = ""
    if isinstance(actor, dict):
        try:
            actor_id = int(actor.get("id") or 0)
        except Exception:
            actor_id = 0
        actor_login = str(actor.get("login_id") or "").strip().lower()
    if actor_id <= 0:
        actor_id = 1
    try:
        actor_login = _clean_login_id(actor_login or "auto.site_registry")
    except HTTPException:
        actor_login = "auto.site_registry"
    return actor_id, actor_login


def _execute_site_registry_request(
    *,
    request: Request,
    request_id: int,
    actor: Dict[str, Any] | None,
    payload: Dict[str, Any] | None = None,
    item: Dict[str, Any] | None = None,
    auto_processed: bool = False,
    audit_event_type: str = "site_registry_request_execute",
) -> Dict[str, Any]:
    target = item if isinstance(item, dict) else get_privileged_change_request(int(request_id))
    if not target:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없습니다.")
    if str(target.get("change_type") or "").strip().lower() != SITE_REGISTRY_REQUEST_CHANGE_TYPE:
        raise HTTPException(status_code=400, detail="지원하지 않는 요청 유형입니다.")

    status = str(target.get("status") or "").strip().lower()
    if status == "executed":
        existing_result = target.get("result") if isinstance(target.get("result"), dict) else {}
        existing_signup_ready = existing_result.get("signup_ready") if isinstance(existing_result.get("signup_ready"), dict) else {}
        return {
            "ok": True,
            "already_processed": True,
            "auto_processed": bool(existing_result.get("auto_processed")),
            "signup_ready": existing_signup_ready,
            "request": _site_registry_request_public(target),
            "message": "이미 처리된 요청입니다.",
        }
    if status not in {"pending", "approved"}:
        raise HTTPException(status_code=409, detail="요청 상태가 처리 불가능합니다.")

    actor_id, actor_login = _site_registry_actor_identity(actor)
    body = payload if isinstance(payload, dict) else {}
    req_payload = target.get("payload") if isinstance(target.get("payload"), dict) else {}
    site_name = _clean_site_name(
        body.get("site_name", req_payload.get("site_name") or target.get("target_site_name")),
        required=True,
    )
    requested_site_code = _clean_site_code(
        body.get("site_code", req_payload.get("requested_site_code") or target.get("target_site_code")),
        required=False,
    )
    process_note = _clean_optional_text(body.get("process_note"), 200) or ""

    if status == "pending":
        try:
            target = approve_privileged_change_request(
                request_id=int(request_id),
                approver_user_id=actor_id,
                approver_login=actor_login,
            )
            status = str(target.get("status") or "").strip().lower()
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e

    if status != "approved":
        raise HTTPException(status_code=409, detail="요청이 승인 상태가 아닙니다.")

    resolved_site_code = _resolve_site_code_for_site(
        site_name,
        requested_site_code,
        allow_create=True,
        allow_remap=False,
    )
    signup_ready: Dict[str, Any]
    try:
        signup_ready = _issue_site_registry_signup_ready_notice(
            request=request,
            req_payload=req_payload,
            site_name=site_name,
            site_code=resolved_site_code,
        )
    except Exception as e:
        signup_ready = {"notified": False, "reason": str(e)}
    result = {
        "site_name": site_name,
        "site_code": resolved_site_code,
        "requested_site_code": requested_site_code,
        "process_note": process_note,
        "signup_ready": signup_ready,
    }
    if auto_processed:
        result["auto_processed"] = True
    try:
        executed = mark_privileged_change_request_executed(
            request_id=int(request_id),
            executed_by_user_id=actor_id,
            executed_by_login=actor_login,
            result=result,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e

    _audit_security(
        user={"id": actor_id, "login_id": actor_login},
        event_type=audit_event_type,
        severity="INFO",
        outcome="ok",
        target_site_code=resolved_site_code,
        target_site_name=site_name,
        request_id=int(request_id),
        detail={
            "requested_site_code": requested_site_code,
            "process_note": process_note,
            "auto_processed": bool(auto_processed),
        },
    )
    return {
        "ok": True,
        "already_processed": False,
        "auto_processed": bool(auto_processed),
        "site_name": site_name,
        "site_code": resolved_site_code,
        "signup_ready": signup_ready,
        "request": _site_registry_request_public(executed),
        "message": "요청을 자동 처리하여 단지코드를 등록했습니다." if auto_processed else "요청을 처리하여 단지코드를 등록했습니다.",
    }


@router.post("/site_registry/request")
def api_site_registry_request(request: Request, payload: Dict[str, Any] = Body(...)):
    site_name = _clean_site_name(payload.get("site_name"), required=True)
    requested_site_code = _clean_site_code(payload.get("site_code"), required=False)
    requester_name = _clean_optional_text(payload.get("requester_name", payload.get("name")), 40) or ""
    requester_login_id = _clean_optional_text(payload.get("requester_login_id", payload.get("login_id")), 32) or ""
    requester_role = _clean_optional_text(payload.get("requester_role", payload.get("role")), 20) or ""
    requester_unit_label = _clean_optional_text(payload.get("requester_unit_label", payload.get("unit_label")), 20) or ""
    requester_note = _clean_optional_text(payload.get("requester_note", payload.get("note")), 200) or ""
    requester_phone = ""
    try:
        requester_phone = _normalize_phone(
            payload.get("requester_phone", payload.get("phone")),
            required=False,
            field_name="requester_phone",
        ) or ""
    except HTTPException:
        requester_phone = ""
    signup_name = _clean_optional_text(payload.get("signup_name", requester_name), 40) or requester_name
    signup_login_id = _clean_optional_text(payload.get("signup_login_id", requester_login_id), 32) or requester_login_id.lower()
    signup_role = _clean_optional_text(payload.get("signup_role", requester_role), 20) or requester_role
    signup_unit_label = _clean_optional_text(payload.get("signup_unit_label", requester_unit_label), 20) or requester_unit_label
    signup_address = _clean_optional_text(payload.get("signup_address", payload.get("requester_address")), 200) or ""
    signup_office_phone = ""
    try:
        signup_office_phone = _normalize_phone(
            payload.get("signup_office_phone", payload.get("requester_office_phone")),
            required=False,
            field_name="signup_office_phone",
        ) or ""
    except HTTPException:
        signup_office_phone = ""
    signup_office_fax = ""
    try:
        signup_office_fax = _normalize_phone(
            payload.get("signup_office_fax", payload.get("requester_office_fax")),
            required=False,
            field_name="signup_office_fax",
        ) or ""
    except HTTPException:
        signup_office_fax = ""
    phone_digits = _phone_digits(requester_phone)
    requested_by_login = f"signup-{phone_digits[-4:]}" if phone_digits else "signup-request"

    req = create_privileged_change_request(
        change_type=SITE_REGISTRY_REQUEST_CHANGE_TYPE,
        payload={
            "site_name": site_name,
            "requested_site_code": requested_site_code,
            "requester_name": requester_name,
            "requester_login_id": requester_login_id.lower(),
            "requester_phone": requester_phone,
            "requester_role": requester_role,
            "requester_unit_label": requester_unit_label,
            "requester_note": requester_note,
            "signup_name": signup_name,
            "signup_phone": requester_phone,
            "signup_login_id": signup_login_id.lower(),
            "signup_role": signup_role,
            "signup_unit_label": signup_unit_label,
            "signup_address": signup_address,
            "signup_office_phone": signup_office_phone,
            "signup_office_fax": signup_office_fax,
            "request_ip": _client_ip(request),
        },
        requested_by_user_id=0,
        requested_by_login=requested_by_login,
        target_site_name=site_name,
        target_site_code=requested_site_code,
        reason="signup_site_code_missing",
        expires_hours=24 * 30,
    )
    _audit_security(
        user=None,
        event_type="site_registry_request_create",
        severity="INFO",
        outcome="ok",
        target_site_code=requested_site_code,
        target_site_name=site_name,
        request_id=int(req.get("id") or 0),
        detail={"requester_role": requester_role, "requester_phone": requester_phone},
    )
    should_auto_process = _clean_bool(payload.get("auto_process"), default=False)
    if not should_auto_process:
        requester_level = _permission_level_from_role_text(requester_role, allow_admin_levels=False)
        signup_level = _permission_level_from_role_text(signup_role, allow_admin_levels=False)
        should_auto_process = requester_level == "site_admin" or signup_level == "site_admin"

    auto_error = ""
    execute_result: Dict[str, Any] | None = None
    if should_auto_process:
        try:
            execute_result = _execute_site_registry_request(
                request=request,
                request_id=int(req.get("id") or 0),
                actor=_site_registry_auto_actor(),
                payload={
                    "site_name": site_name,
                    "site_code": requested_site_code,
                    "process_note": "signup_auto_process",
                },
                item=req,
                auto_processed=True,
                audit_event_type="site_registry_request_auto_execute",
            )
            req = get_privileged_change_request(int(req.get("id") or 0)) or req
        except HTTPException as e:
            auto_error = str(e.detail or "auto process failed")
            _audit_security(
                user=None,
                event_type="site_registry_request_auto_execute",
                severity="WARNING",
                outcome="failed",
                target_site_code=requested_site_code,
                target_site_name=site_name,
                request_id=int(req.get("id") or 0),
                detail={"error": auto_error, "requester_role": requester_role},
            )
        except Exception as e:
            auto_error = str(e)
            _audit_security(
                user=None,
                event_type="site_registry_request_auto_execute",
                severity="WARNING",
                outcome="failed",
                target_site_code=requested_site_code,
                target_site_name=site_name,
                request_id=int(req.get("id") or 0),
                detail={"error": auto_error, "requester_role": requester_role},
            )

    status_value = str(req.get("status") or "pending")
    out: Dict[str, Any] = {
        "ok": True,
        "request_id": int(req.get("id") or 0),
        "status": status_value,
        "status_label": _site_registry_request_status_label(status_value),
        "site_name": site_name,
        "requested_site_code": requested_site_code,
        "auto_processed": bool(execute_result and execute_result.get("auto_processed")),
        "message": (
            "단지코드 자동 등록처리를 완료했습니다. 인증번호 받기를 다시 눌러 계속 진행하세요."
            if execute_result and execute_result.get("auto_processed")
            else "단지코드 등록요청이 접수되었습니다. 최고관리자가 요청함에서 처리할 수 있습니다."
        ),
    }
    if execute_result:
        out["request"] = execute_result.get("request")
        out["site_code"] = str(execute_result.get("site_code") or "")
        out["signup_ready"] = execute_result.get("signup_ready") or {}
    else:
        out["request"] = _site_registry_request_public(req)
    if auto_error:
        out["auto_error"] = auto_error
    return out


@router.get("/site_registry/requests")
def api_site_registry_requests(
    request: Request,
    status: str = Query(default="pending"),
    limit: int = Query(default=100),
):
    _user, _token = _require_super_admin(request)
    raw_status = str(status or "pending").strip().lower()
    if raw_status in {"", "all"}:
        status_filter = ""
    elif raw_status in {"pending", "approved", "executed"}:
        status_filter = raw_status
    elif raw_status == "processed":
        status_filter = "executed"
    else:
        raise HTTPException(status_code=400, detail="status must be one of: pending, approved, executed, processed, all")
    clean_limit = max(1, min(int(limit), 500))
    rows = list_privileged_change_requests(
        change_type=SITE_REGISTRY_REQUEST_CHANGE_TYPE,
        status=status_filter,
        limit=clean_limit,
    )
    pending_rows = list_privileged_change_requests(
        change_type=SITE_REGISTRY_REQUEST_CHANGE_TYPE,
        status="pending",
        limit=500,
    )
    return {
        "ok": True,
        "status": raw_status or "all",
        "pending_count": len(pending_rows),
        "items": [_site_registry_request_public(item) for item in rows],
    }


@router.post("/site_registry/requests/{request_id}/execute")
def api_site_registry_request_execute(
    request: Request,
    request_id: int,
    payload: Dict[str, Any] = Body(default={}),
):
    user, _token = _require_super_admin(request)
    return _execute_site_registry_request(
        request=request,
        request_id=int(request_id),
        actor=user,
        payload=payload,
        auto_processed=False,
        audit_event_type="site_registry_request_execute",
    )


@router.delete("/site_registry/requests/{request_id}")
def api_site_registry_request_delete(request: Request, request_id: int):
    user, _token = _require_super_admin(request)
    item = get_privileged_change_request(int(request_id))
    if not item:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없습니다.")
    if str(item.get("change_type") or "").strip().lower() != SITE_REGISTRY_REQUEST_CHANGE_TYPE:
        raise HTTPException(status_code=400, detail="지원하지 않는 요청 유형입니다.")

    deleted = delete_privileged_change_request(request_id=int(request_id))
    if not deleted:
        raise HTTPException(status_code=404, detail="이미 삭제되었거나 요청을 찾을 수 없습니다.")

    _audit_security(
        user=user,
        event_type="site_registry_request_delete",
        severity="WARNING",
        outcome="ok",
        target_site_code=str(item.get("target_site_code") or "").strip().upper(),
        target_site_name=str(item.get("target_site_name") or "").strip(),
        request_id=int(request_id),
        detail={
            "status": str(item.get("status") or "").strip().lower(),
            "requested_by_login": str(item.get("requested_by_login") or "").strip(),
        },
    )
    return {
        "ok": True,
        "request_id": int(request_id),
        "message": "요청을 삭제했습니다.",
    }


@router.post("/site_registry/register")
def api_site_registry_register(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_super_admin(request)
    site_name = _clean_site_name(payload.get("site_name"), required=True)
    requested_code = _clean_site_code(payload.get("site_code"), required=False)
    site_code = _resolve_site_code_for_site(
        site_name,
        requested_code,
        allow_create=True,
        allow_remap=False,
    )
    resolved = resolve_site_identity(
        site_id=0,
        site_name=site_name,
        site_code=site_code,
        create_site_if_missing=False,
    )
    resolved_name = _clean_site_name(resolved.get("site_name"), required=False) or site_name
    resolved_code = _clean_site_code(resolved.get("site_code"), required=False) or site_code
    _audit_security(
        user=user,
        event_type="site_registry_register",
        target_site_code=resolved_code,
        target_site_name=resolved_name,
        detail={"requested_site_code": requested_code},
    )
    return {
        "ok": True,
        "site_name": resolved_name,
        "site_code": resolved_code,
        "message": "단지코드를 등록했습니다.",
    }


@router.put("/site_env")
def api_site_env_upsert(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_site_env_manager(request)
    if _env_enabled("KA_SPEC_ENV_CHANGE_WINDOW_ENABLED", False):
        _assert_change_window("제원설정 변경")
    _assert_mfa_confirmed(request, payload, operation_label="제원설정 변경")
    _assert_spec_env_manage_code_confirmed(request, user, payload, operation_label="제원설정 변경")
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user,
        payload.get("site_name"),
        payload.get("site_code"),
        payload.get("site_id"),
        require_any=True,
        for_write=True,
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
    resolved_site_name = _clean_site_name(row.get("site_name"), required=False) or clean_site_name
    resolved_identity = resolve_site_identity(
        site_id=_clean_site_id(payload.get("site_id"), required=False),
        site_name=resolved_site_name,
        site_code=resolved_site_code,
        create_site_if_missing=False,
    )
    resolved_site_id = _clean_site_id(resolved_identity.get("site_id"), required=False)
    schema, _env_cfg = _site_schema_and_env(resolved_site_name, resolved_site_code)
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
        "site_id": resolved_site_id,
        "site_name": resolved_site_name,
        "site_code": resolved_site_code,
        "config": cfg,
        "schema": schema,
        "updated_at": row.get("updated_at"),
        "spec_access": verify,
        "prechange_backup": prechange_backup,
    }


@router.delete("/site_env")
def api_site_env_delete(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    site_id: int = Query(default=0),
):
    user, _token = _require_site_env_manager(request)
    if _env_enabled("KA_SPEC_ENV_CHANGE_WINDOW_ENABLED", False):
        _assert_change_window("제원설정 삭제")
    _assert_mfa_confirmed(request, operation_label="제원설정 삭제")
    _assert_spec_env_manage_code_confirmed(request, user, operation_label="제원설정 삭제")
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, site_name, site_code, site_id, require_any=True, for_write=False
    )
    _verify_first_site_registrant_for_spec_env(user, clean_site_name, clean_site_code)
    resolved_identity = resolve_site_identity(
        site_id=_clean_site_id(site_id, required=False),
        site_name=clean_site_name,
        site_code=clean_site_code,
        create_site_if_missing=False,
    )
    resolved_site_id = _clean_site_id(resolved_identity.get("site_id"), required=False)
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
        "site_id": resolved_site_id,
        "site_name": clean_site_name,
        "site_code": clean_site_code,
        "prechange_backup": prechange_backup,
    }


@router.get("/apartment_profile")
def api_apartment_profile_get(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    site_id: int = Query(default=0),
):
    user, _token = _require_site_env_manager(request)
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, site_name, site_code, site_id, require_any=True, for_write=False
    )
    resolved_identity = resolve_site_identity(
        site_id=_clean_site_id(site_id, required=False),
        site_name=clean_site_name,
        site_code=clean_site_code,
        create_site_if_missing=False,
    )
    resolved_site_id = _clean_site_id(resolved_identity.get("site_id"), required=False)

    row = get_site_apartment_profile_record(
        site_id=resolved_site_id,
        site_name=clean_site_name,
        site_code=clean_site_code or None,
    )
    if not row:
        defaults = apartment_profile_defaults()
        return {
            "ok": True,
            "exists": False,
            "site_id": int(resolved_site_id or 0),
            "site_name": clean_site_name,
            "site_code": clean_site_code,
            **defaults,
            "created_at": None,
            "updated_at": None,
        }

    return {
        "ok": True,
        "exists": True,
        "site_id": int(row.get("site_id") or resolved_site_id or 0),
        "site_name": clean_site_name,
        "site_code": clean_site_code,
        "households_total": int(row.get("households_total") or 0),
        "building_start": int(row.get("building_start") or 101),
        "building_count": int(row.get("building_count") or 0),
        "default_line_count": int(row.get("default_line_count") or 8),
        "default_max_floor": int(row.get("default_max_floor") or 60),
        "default_basement_floors": int(row.get("default_basement_floors") or 0),
        "building_overrides": row.get("building_overrides") if isinstance(row.get("building_overrides"), dict) else {},
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


@router.put("/apartment_profile")
def api_apartment_profile_upsert(request: Request, payload: Dict[str, Any] = Body(...)):
    user, _token = _require_site_env_manager(request)
    _assert_mfa_confirmed(request, payload, operation_label="아파트 정보 설정 변경")
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user,
        payload.get("site_name"),
        payload.get("site_code"),
        payload.get("site_id"),
        require_any=True,
        for_write=True,
    )
    resolved_identity = resolve_site_identity(
        site_id=_clean_site_id(payload.get("site_id"), required=False),
        site_name=clean_site_name,
        site_code=clean_site_code,
        create_site_if_missing=False,
    )
    resolved_site_id = _clean_site_id(resolved_identity.get("site_id"), required=False)
    if not resolved_site_id:
        raise HTTPException(status_code=404, detail="단지(site_id)를 찾을 수 없습니다.")

    profile_payload = payload.get("profile") if isinstance(payload.get("profile"), dict) else payload
    reason = str(payload.get("reason") or "").strip()
    try:
        row = upsert_site_apartment_profile(
            site_name=clean_site_name,
            site_code=clean_site_code or None,
            site_id=int(resolved_site_id),
            profile=profile_payload if isinstance(profile_payload, dict) else {},
        )
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="apartment_profile_update",
            severity="ERROR",
            outcome="error",
            target_site_code=clean_site_code,
            target_site_name=clean_site_name,
            detail={"reason": reason, "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e)) from e

    _audit_security(
        user=user,
        event_type="apartment_profile_update",
        severity="INFO",
        outcome="ok",
        target_site_code=clean_site_code,
        target_site_name=clean_site_name,
        detail={"reason": reason, "admin_scope": _admin_scope_from_user(user)},
    )

    return {
        "ok": True,
        "exists": True,
        "site_id": int(row.get("site_id") or resolved_site_id or 0),
        "site_name": clean_site_name,
        "site_code": clean_site_code,
        "households_total": int(row.get("households_total") or 0),
        "building_start": int(row.get("building_start") or 101),
        "building_count": int(row.get("building_count") or 0),
        "default_line_count": int(row.get("default_line_count") or 8),
        "default_max_floor": int(row.get("default_max_floor") or 60),
        "default_basement_floors": int(row.get("default_basement_floors") or 0),
        "building_overrides": row.get("building_overrides") if isinstance(row.get("building_overrides"), dict) else {},
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


@router.delete("/apartment_profile")
def api_apartment_profile_delete(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    site_id: int = Query(default=0),
):
    user, _token = _require_site_env_manager(request)
    _assert_mfa_confirmed(request, operation_label="아파트 정보 설정 삭제")
    clean_site_name, clean_site_code = _resolve_spec_env_site_target(
        user, site_name, site_code, site_id, require_any=True, for_write=False
    )
    resolved_identity = resolve_site_identity(
        site_id=_clean_site_id(site_id, required=False),
        site_name=clean_site_name,
        site_code=clean_site_code,
        create_site_if_missing=False,
    )
    resolved_site_id = _clean_site_id(resolved_identity.get("site_id"), required=False)
    ok = delete_site_apartment_profile(
        site_id=int(resolved_site_id or 0),
        site_name=clean_site_name,
        site_code=clean_site_code or None,
    )
    _audit_security(
        user=user,
        event_type="apartment_profile_delete",
        severity="WARN",
        outcome=("ok" if ok else "noop"),
        target_site_code=clean_site_code,
        target_site_name=clean_site_name,
        detail={"admin_scope": _admin_scope_from_user(user)},
    )
    return {
        "ok": ok,
        "site_id": int(resolved_site_id or 0),
        "site_name": clean_site_name,
        "site_code": clean_site_code,
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
                "site_id": r.get("site_id"),
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
    if _env_enabled("KA_SPEC_ENV_CHANGE_WINDOW_ENABLED", False):
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
    is_admin = int(user.get("is_admin") or 0) == 1
    return {
        "ok": True,
        "timezone": backup_timezone_name(),
        "maintenance": get_maintenance_status(),
        "can_manage_backup": _can_manage_backup(user),
        "can_restore_direct": is_admin,
        "permission_level": _permission_level_from_user(user),
        "download_link_ttl_sec": int(BACKUP_DOWNLOAD_LINK_TTL_SEC),
        "site_daily_limits": {
            "max_runs": int(BACKUP_SITE_DAILY_MAX_RUNS),
            "max_bytes": int(BACKUP_SITE_DAILY_MAX_BYTES),
        },
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
    assigned = _user_site_identity(user)
    site_id = int(assigned.get("site_id") or 0)
    site_code = _clean_site_code(assigned.get("site_code"), required=False)
    site_name = _clean_site_name(assigned.get("site_name"), required=False)
    targets = list_backup_targets()
    if not is_admin:
        if not site_code:
            raise HTTPException(status_code=403, detail="소속 단지코드가 없어 백업할 수 없습니다.")
        targets = [x for x in targets if bool(x.get("site_scoped"))]

    return {
        "ok": True,
        "is_admin": is_admin,
        "site_id": int(site_id or 0),
        "site_code": site_code,
        "site_name": site_name,
        "can_restore_direct": is_admin,
        "include_user_tables_supported": bool(is_admin),
        "include_user_tables_default": bool(is_admin),
        "download_link_ttl_sec": int(BACKUP_DOWNLOAD_LINK_TTL_SEC),
        "site_daily_limits": {
            "max_runs": int(BACKUP_SITE_DAILY_MAX_RUNS),
            "max_bytes": int(BACKUP_SITE_DAILY_MAX_BYTES),
        },
        "allowed_scopes": ["full", "site"] if is_admin else ["site"],
        "targets": targets,
    }


@router.post("/backup/run")
def api_backup_run(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        _audit_security(
            user=user,
            event_type="backup_run",
            severity="WARN",
            outcome="denied",
            detail={"reason": "permission_denied"},
        )
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
    requested_include_user_tables = _clean_bool(payload.get("include_user_tables"), default=True)
    include_user_tables = bool(is_admin and requested_include_user_tables)
    # full 백업은 DB 파일 단위이므로 사용자 테이블 제외가 불가능합니다.
    if scope != "site":
        include_user_tables = True

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

    usage_before: Dict[str, Any] = {}
    site_id = 0
    if scope == "site":
        if is_admin:
            site_code = _clean_site_code(payload.get("site_code"), required=True)
            site_name = _clean_site_name(payload.get("site_name"), required=False)
            request_site_id = _clean_site_id(payload.get("site_id"), required=False)
            if request_site_id > 0:
                resolved_site_id = _site_id_from_identity(site_code, site_name)
                if resolved_site_id > 0 and resolved_site_id != request_site_id:
                    raise HTTPException(status_code=409, detail="site_id와 site_code가 일치하지 않습니다.")
                site_id = request_site_id
            else:
                site_id = _site_id_from_identity(site_code, site_name)
        else:
            assigned = _user_site_identity(user)
            site_code = _clean_site_code(assigned.get("site_code"), required=False)
            site_name = _clean_site_name(assigned.get("site_name"), required=False)
            site_id = int(assigned.get("site_id") or 0)
            if not site_code:
                raise HTTPException(status_code=403, detail="소속 단지코드가 없어 백업할 수 없습니다.")
            payload_site_code = _clean_site_code(payload.get("site_code"), required=False)
            payload_site_id = _clean_site_id(payload.get("site_id"), required=False)
            if payload_site_code and payload_site_code != site_code:
                raise HTTPException(status_code=403, detail="소속 단지코드(site_code) 백업만 실행할 수 있습니다.")
            if payload_site_id > 0 and site_id > 0 and payload_site_id != site_id:
                raise HTTPException(status_code=403, detail="소속 단지(site_id) 백업만 실행할 수 있습니다.")
            try:
                usage_before = _enforce_site_backup_daily_limits(site_code)
            except HTTPException as e:
                _audit_security(
                    user=user,
                    event_type="backup_run",
                    severity="WARN",
                    outcome="denied",
                    target_site_code=site_code,
                    target_site_name=site_name,
                    detail={"scope": scope, "targets": selected_keys, "reason": str(e.detail or "daily_limit")},
                )
                raise
    else:
        site_id = 0
        site_code = ""
        site_name = ""

    actor_login = _clean_login_id(user.get("login_id") or "backup-runner")
    try:
        result = run_manual_backup(
            actor=actor_login,
            trigger="manual",
            target_keys=selected_keys,
            scope=scope,
            site_id=site_id,
            site_code=site_code,
            site_name=site_name,
            with_maintenance=(scope == "full"),
            include_user_tables=include_user_tables,
        )
    except RuntimeError as e:
        _audit_security(
            user=user,
            event_type="backup_run",
            severity="ERROR",
            outcome="error",
            target_site_code=site_code,
            target_site_name=site_name,
            detail={"scope": scope, "targets": selected_keys, "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="backup_run",
            severity="WARN",
            outcome="error",
            target_site_code=site_code,
            target_site_name=site_name,
            detail={"scope": scope, "targets": selected_keys, "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        _audit_security(
            user=user,
            event_type="backup_run",
            severity="ERROR",
            outcome="error",
            target_site_code=site_code,
            target_site_name=site_name,
            detail={"scope": scope, "targets": selected_keys, "error": str(e)},
        )
        raise HTTPException(status_code=500, detail=f"backup failed: {e}") from e

    _audit_security(
        user=user,
        event_type="backup_run",
        severity="INFO",
        outcome="ok",
        target_site_code=site_code,
        target_site_name=site_name,
        detail={
            "scope": scope,
            "targets": selected_keys,
            "include_user_tables": bool(include_user_tables),
            "relative_path": str(result.get("relative_path") or ""),
            "file_size_bytes": int(result.get("file_size_bytes") or 0),
        },
    )
    usage_after = _site_backup_usage_today(site_code) if scope == "site" else {}
    return {
        "ok": True,
        "result": result,
        "maintenance": get_maintenance_status(),
        "site_daily_usage_before": usage_before,
        "site_daily_usage_after": usage_after,
    }


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
    assigned = _user_site_identity(user)
    assigned_site_id = int(assigned.get("site_id") or 0)
    clean_scope = str(scope or "").strip().lower()
    if clean_scope and clean_scope not in {"full", "site"}:
        raise HTTPException(status_code=400, detail="scope must be full or site")

    if is_admin:
        clean_site_code = _clean_site_code(site_code, required=False)
    else:
        clean_scope = "site"
        clean_site_code = _clean_site_code(assigned.get("site_code"), required=False)
        if not clean_site_code:
            raise HTTPException(status_code=403, detail="소속 단지코드가 없어 백업이력을 조회할 수 없습니다.")

    items = list_backup_history(
        limit=max(1, min(int(limit), 200)),
        scope=clean_scope,
        site_code=clean_site_code,
    )
    if not is_admin:
        filtered: List[Dict[str, Any]] = []
        for item in items:
            try:
                perm = _resolve_backup_item_permission(user=user, item=item)
            except HTTPException:
                continue
            manifest = perm.get("manifest") if isinstance(perm.get("manifest"), dict) else {}
            merged = dict(item)
            merged["scope"] = str(perm.get("scope") or merged.get("scope") or "").strip().lower()
            merged["site_code"] = str(perm.get("site_code") or merged.get("site_code") or "").strip().upper()
            merged["site_name"] = str(perm.get("site_name") or merged.get("site_name") or "").strip()
            merged["contains_user_data"] = bool(perm.get("contains_user_data"))
            item_site_id = _clean_site_id(merged.get("site_id"), required=False)
            if item_site_id <= 0:
                item_site_id = _clean_site_id(manifest.get("site_id"), required=False)
            if item_site_id <= 0 and merged.get("site_code"):
                item_site_id = _site_id_from_identity(merged.get("site_code"), merged.get("site_name"))
            if assigned_site_id > 0 and item_site_id > 0 and item_site_id != assigned_site_id:
                continue
            if item_site_id > 0:
                merged["site_id"] = item_site_id
            filtered.append(merged)
        items = filtered
    return {"ok": True, "count": len(items), "items": items}


@router.post("/backup/download/request")
def api_backup_download_request(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        _audit_security(
            user=user,
            event_type="backup_download_issue",
            severity="WARN",
            outcome="denied",
            detail={"reason": "permission_denied"},
        )
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    path = str(payload.get("path") or "").strip()
    try:
        if not path:
            raise HTTPException(status_code=400, detail="path is required")
        item = get_backup_item(path)
        if not item:
            raise HTTPException(status_code=404, detail="backup file not found")
        perm = _resolve_backup_item_permission(user=user, item=item, requested_path=path)
        ticket = _issue_backup_download_token(user=user, relative_path=str(perm.get("relative_path") or ""))
    except HTTPException as e:
        _audit_security(
            user=user,
            event_type="backup_download_issue",
            severity="WARN",
            outcome="denied",
            detail={"path": path, "reason": str(e.detail or "download_issue_denied")},
        )
        raise

    _audit_security(
        user=user,
        event_type="backup_download_issue",
        severity="INFO",
        outcome="ok",
        target_site_code=str(perm.get("site_code") or ""),
        target_site_name=str(perm.get("site_name") or ""),
        detail={
            "relative_path": str(perm.get("relative_path") or ""),
            "scope": str(perm.get("scope") or ""),
            "expires_in_sec": int(BACKUP_DOWNLOAD_LINK_TTL_SEC),
        },
    )
    return {"ok": True, "download": ticket}


@router.get("/backup/download")
def api_backup_download(
    request: Request,
    token: str = Query(default=""),
    path: str = Query(default=""),
):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        _audit_security(
            user=user,
            event_type="backup_download",
            severity="WARN",
            outcome="denied",
            detail={"reason": "permission_denied"},
        )
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    clean_token = str(token or "").strip()
    clean_path = str(path or "").strip()
    try:
        if clean_path and not clean_token:
            # Backward compatibility: path 요청은 1회성 토큰 URL로 리다이렉트합니다.
            item = get_backup_item(clean_path)
            if not item:
                raise HTTPException(status_code=404, detail="backup file not found")
            perm = _resolve_backup_item_permission(user=user, item=item, requested_path=clean_path)
            ticket = _issue_backup_download_token(user=user, relative_path=str(perm.get("relative_path") or ""))
            _audit_security(
                user=user,
                event_type="backup_download_issue",
                severity="INFO",
                outcome="ok",
                target_site_code=str(perm.get("site_code") or ""),
                target_site_name=str(perm.get("site_name") or ""),
                detail={
                    "relative_path": str(perm.get("relative_path") or ""),
                    "scope": str(perm.get("scope") or ""),
                    "expires_in_sec": int(BACKUP_DOWNLOAD_LINK_TTL_SEC),
                    "legacy_path": True,
                },
            )
            return RedirectResponse(url=str(ticket.get("url") or "/api/backup/download"), status_code=307)

        token_payload = _consume_backup_download_token(user=user, token=clean_token)
        relative_path = str(token_payload.get("relative_path") or "").strip()
        item = get_backup_item(relative_path)
        if not item:
            raise HTTPException(status_code=404, detail="backup file not found")
        perm = _resolve_backup_item_permission(user=user, item=item, requested_path=relative_path)

        try:
            target = resolve_backup_file(str(perm.get("relative_path") or relative_path))
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        filename = str(item.get("download_name") or target.name)
        _audit_security(
            user=user,
            event_type="backup_download",
            severity="INFO",
            outcome="ok",
            target_site_code=str(perm.get("site_code") or ""),
            target_site_name=str(perm.get("site_name") or ""),
            detail={
                "relative_path": str(perm.get("relative_path") or ""),
                "scope": str(perm.get("scope") or ""),
                "file_size_bytes": int(item.get("file_size_bytes") or 0),
            },
        )
        return FileResponse(
            path=target,
            media_type="application/zip",
            filename=filename,
        )
    except HTTPException as e:
        _audit_security(
            user=user,
            event_type="backup_download",
            severity="WARN",
            outcome="denied",
            detail={"path": clean_path, "token_used": bool(clean_token), "reason": str(e.detail or "download_denied")},
        )
        raise


@router.post("/backup/restore")
def api_backup_restore(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        _audit_security(
            user=user,
            event_type="backup_restore",
            severity="WARN",
            outcome="denied",
            detail={"reason": "permission_denied"},
        )
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")

    path = str(payload.get("path") or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="path is required")

    manifest = _validate_uploaded_restore_backup(path)
    try:
        _enforce_restore_permission(user, manifest)
    except HTTPException as e:
        _audit_security(
            user=user,
            event_type="backup_restore",
            severity="WARN",
            outcome="denied",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": path, "reason": str(e.detail or "restore_forbidden")},
        )
        raise

    target_payload = payload.get("target_keys", payload.get("targets", []))
    if isinstance(target_payload, str):
        target_keys = [target_payload]
    elif isinstance(target_payload, list):
        target_keys = [str(x or "").strip().lower() for x in target_payload if str(x or "").strip()]
    else:
        raise HTTPException(status_code=400, detail="target_keys must be list")

    is_admin = int(user.get("is_admin") or 0) == 1
    with_maintenance = _clean_bool(payload.get("with_maintenance"), default=True)
    requested_include_user_tables = _clean_bool(payload.get("include_user_tables"), default=True)
    include_user_tables = bool(is_admin and requested_include_user_tables)
    scope = str(manifest.get("scope") or "").strip().lower() or "full"
    # full 복구는 DB 파일 단위이므로 사용자 테이블 제외가 불가능합니다.
    if scope != "site":
        include_user_tables = True
    actor_login = _clean_login_id(user.get("login_id") or "backup-restore")
    try:
        result = restore_backup_zip(
            actor=actor_login,
            relative_path=path,
            target_keys=target_keys,
            with_maintenance=with_maintenance,
            include_user_tables=include_user_tables,
        )
    except RuntimeError as e:
        _audit_security(
            user=user,
            event_type="backup_restore",
            severity="ERROR",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="backup_restore",
            severity="WARN",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        _audit_security(
            user=user,
            event_type="backup_restore",
            severity="WARN",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        _audit_security(
            user=user,
            event_type="backup_restore",
            severity="ERROR",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=500, detail=f"restore failed: {e}") from e

    _audit_security(
        user=user,
        event_type="backup_restore",
        severity="WARN",
        outcome="ok",
        target_site_code=str(manifest.get("site_code") or ""),
        target_site_name=str(manifest.get("site_name") or ""),
        detail={
            "path": path,
            "scope": str(manifest.get("scope") or ""),
            "targets": target_keys,
            "include_user_tables": bool(include_user_tables),
            "rollback_relative_path": str(result.get("rollback_relative_path") or ""),
        },
    )
    return {"ok": True, "result": result, "maintenance": get_maintenance_status()}


@router.post("/backup/restore/request")
def api_backup_restore_request(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")
    if int(user.get("is_admin") or 0) == 1:
        raise HTTPException(status_code=400, detail="관리자는 복구 요청이 아니라 직접 복구를 실행하세요.")

    path = str(payload.get("path") or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="path is required")
    reason = str(payload.get("reason") or "").strip()
    if len(reason) < 4:
        reason = "단지대표자 복구 요청"

    item = get_backup_item(path)
    if not item:
        raise HTTPException(status_code=404, detail="backup file not found")
    perm = _resolve_backup_item_permission(user=user, item=item, requested_path=path)
    if str(perm.get("scope") or "").strip().lower() != "site":
        raise HTTPException(status_code=403, detail="단지코드(site) 백업 파일만 복구 요청할 수 있습니다.")

    actor_login = _clean_login_id(user.get("login_id") or "site-admin")
    req = create_privileged_change_request(
        change_type="backup_restore_site",
        payload={
            "path": str(perm.get("relative_path") or ""),
            "scope": "site",
            "site_code": str(perm.get("site_code") or ""),
            "site_name": str(perm.get("site_name") or ""),
            "requested_by": actor_login,
            "reason": reason,
        },
        requested_by_user_id=int(user.get("id") or 0),
        requested_by_login=actor_login,
        target_site_name=str(perm.get("site_name") or ""),
        target_site_code=str(perm.get("site_code") or ""),
        reason=reason,
        expires_hours=max(1, min(int(payload.get("expires_hours") or 24), 72)),
    )
    _audit_security(
        user=user,
        event_type="backup_restore_request",
        severity="WARN",
        outcome="ok",
        target_site_code=str(perm.get("site_code") or ""),
        target_site_name=str(perm.get("site_name") or ""),
        request_id=int(req.get("id") or 0),
        detail={"path": str(perm.get("relative_path") or ""), "reason": reason},
    )
    return {"ok": True, "request": req}


@router.get("/backup/restore/requests")
def api_backup_restore_requests(
    request: Request,
    status: str = Query(default=""),
    limit: int = Query(default=100),
):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")
    clean_status = str(status or "").strip().lower()
    if clean_status in {"", "all"}:
        clean_status = ""
    elif clean_status not in {"pending", "approved", "executed"}:
        raise HTTPException(status_code=400, detail="status must be one of: pending, approved, executed, all")
    items = list_privileged_change_requests(
        change_type="backup_restore_site",
        status=clean_status,
        limit=max(1, min(int(limit), 300)),
    )
    if int(user.get("is_admin") or 0) != 1:
        actor_id = int(user.get("id") or 0)
        actor_login = str(user.get("login_id") or "").strip().lower()
        filtered: List[Dict[str, Any]] = []
        for item in items:
            requested_user_id = int(item.get("requested_by_user_id") or 0)
            requested_login = str(item.get("requested_by_login") or "").strip().lower()
            if actor_id > 0 and requested_user_id > 0 and actor_id == requested_user_id:
                filtered.append(item)
                continue
            if actor_login and requested_login and actor_login == requested_login:
                filtered.append(item)
        items = filtered
    return {"ok": True, "count": len(items), "items": items}


@router.post("/backup/restore/request/approve")
def api_backup_restore_request_approve(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_admin(request)
    request_id = int(payload.get("request_id") or 0)
    if request_id <= 0:
        raise HTTPException(status_code=400, detail="request_id is required")

    req = get_privileged_change_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    if str(req.get("change_type") or "").strip().lower() != "backup_restore_site":
        raise HTTPException(status_code=400, detail="invalid request type")

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
        event_type="backup_restore_request_approve",
        severity="WARN",
        outcome="ok",
        target_site_code=str(item.get("target_site_code") or ""),
        target_site_name=str(item.get("target_site_name") or ""),
        request_id=request_id,
        detail={"status": item.get("status")},
    )
    return {"ok": True, "request": item}


@router.post("/backup/restore/request/execute")
def api_backup_restore_request_execute(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_admin(request)
    request_id = int(payload.get("request_id") or 0)
    if request_id <= 0:
        raise HTTPException(status_code=400, detail="request_id is required")

    req = get_privileged_change_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    if str(req.get("change_type") or "").strip().lower() != "backup_restore_site":
        raise HTTPException(status_code=400, detail="invalid request type")
    actor_login = _clean_login_id(user.get("login_id") or "backup-restore")
    req_status = str(req.get("status") or "").strip().lower()
    if req_status == "pending":
        try:
            req = approve_privileged_change_request(
                request_id=request_id,
                approver_user_id=int(user.get("id") or 0),
                approver_login=actor_login,
            )
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        req_status = str(req.get("status") or "").strip().lower()
    if req_status != "approved":
        raise HTTPException(status_code=409, detail="request is not approved")

    req_payload = req.get("payload") if isinstance(req.get("payload"), dict) else {}
    path = str(req_payload.get("path") or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="restore request payload.path is required")

    manifest = _validate_uploaded_restore_backup(path)
    scope = str(manifest.get("scope") or "").strip().lower() or "full"
    if scope != "site":
        raise HTTPException(status_code=409, detail="backup_restore_site 요청은 site 백업 파일만 실행할 수 있습니다.")
    if bool(manifest.get("contains_user_data")):
        raise HTTPException(status_code=409, detail="사용자정보 포함 백업파일은 승인 실행 대상에서 제외됩니다.")

    requested_code = _clean_site_code(
        req.get("target_site_code") or req_payload.get("site_code"),
        required=False,
    )
    manifest_code = _clean_site_code(manifest.get("site_code"), required=False)
    if requested_code and manifest_code and requested_code != manifest_code:
        raise HTTPException(status_code=409, detail="요청 단지코드와 백업파일 단지코드가 일치하지 않습니다.")

    with_maintenance = _clean_bool(payload.get("with_maintenance"), default=False)
    try:
        result = restore_backup_zip(
            actor=actor_login,
            relative_path=path,
            target_keys=[],
            with_maintenance=with_maintenance,
            include_user_tables=False,
        )
    except RuntimeError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute",
            severity="ERROR",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute",
            severity="WARN",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute",
            severity="WARN",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute",
            severity="ERROR",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=500, detail=f"restore execute failed: {e}") from e

    executed = mark_privileged_change_request_executed(
        request_id=request_id,
        executed_by_user_id=int(user.get("id") or 0),
        executed_by_login=actor_login,
        result={
            "path": path,
            "scope": "site",
            "restore": result,
        },
    )
    _audit_security(
        user=user,
        event_type="backup_restore_request_execute",
        severity="WARN",
        outcome="ok",
        target_site_code=manifest_code,
        target_site_name=str(manifest.get("site_name") or ""),
        request_id=request_id,
        detail={
            "path": path,
            "rollback_relative_path": str((result or {}).get("rollback_relative_path") or ""),
        },
    )
    return {"ok": True, "request": executed, "result": result, "maintenance": get_maintenance_status()}


@router.post("/backup/restore/request/execute_self")
def api_backup_restore_request_execute_self(request: Request, payload: Dict[str, Any] = Body(default={})):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")
    if int(user.get("is_admin") or 0) == 1:
        raise HTTPException(status_code=400, detail="관리자 계정은 관리자 실행 API를 사용하세요.")

    request_id = int(payload.get("request_id") or 0)
    if request_id <= 0:
        raise HTTPException(status_code=400, detail="request_id is required")

    req = get_privileged_change_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    if str(req.get("change_type") or "").strip().lower() != "backup_restore_site":
        raise HTTPException(status_code=400, detail="invalid request type")
    if str(req.get("status") or "").strip().lower() != "approved":
        raise HTTPException(status_code=409, detail="요청이 아직 승인되지 않았습니다.")

    actor_user_id = int(user.get("id") or 0)
    actor_login = _clean_login_id(user.get("login_id") or "site-admin")
    requested_user_id = int(req.get("requested_by_user_id") or 0)
    requested_login = str(req.get("requested_by_login") or "").strip().lower()
    if requested_user_id > 0 and actor_user_id > 0 and requested_user_id != actor_user_id:
        raise HTTPException(status_code=403, detail="본인이 요청한 건만 실행할 수 있습니다.")
    if requested_login and requested_login != actor_login:
        raise HTTPException(status_code=403, detail="본인이 요청한 건만 실행할 수 있습니다.")

    req_payload = req.get("payload") if isinstance(req.get("payload"), dict) else {}
    path = str(req_payload.get("path") or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="restore request payload.path is required")

    manifest = _validate_uploaded_restore_backup(path)
    scope = str(manifest.get("scope") or "").strip().lower() or "full"
    if scope != "site":
        raise HTTPException(status_code=409, detail="backup_restore_site 요청은 site 백업 파일만 실행할 수 있습니다.")
    if bool(manifest.get("contains_user_data")):
        raise HTTPException(status_code=409, detail="사용자정보 포함 백업파일은 단지대표자가 실행할 수 없습니다.")

    requested_code = _clean_site_code(
        req.get("target_site_code") or req_payload.get("site_code"),
        required=False,
    )
    manifest_code = _clean_site_code(manifest.get("site_code"), required=False)
    assigned = _user_site_identity(user)
    assigned_code = _clean_site_code(assigned.get("site_code"), required=False)
    if assigned_code and requested_code and assigned_code != requested_code:
        raise HTTPException(status_code=403, detail="소속 단지코드 요청만 실행할 수 있습니다.")
    if assigned_code and manifest_code and assigned_code != manifest_code:
        raise HTTPException(status_code=403, detail="소속 단지코드 백업만 실행할 수 있습니다.")
    if requested_code and manifest_code and requested_code != manifest_code:
        raise HTTPException(status_code=409, detail="요청 단지코드와 백업파일 단지코드가 일치하지 않습니다.")

    with_maintenance = _clean_bool(payload.get("with_maintenance"), default=False)
    try:
        result = restore_backup_zip(
            actor=actor_login,
            relative_path=path,
            target_keys=[],
            with_maintenance=with_maintenance,
            include_user_tables=False,
        )
    except RuntimeError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute_self",
            severity="ERROR",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute_self",
            severity="WARN",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute_self",
            severity="WARN",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        _audit_security(
            user=user,
            event_type="backup_restore_request_execute_self",
            severity="ERROR",
            outcome="error",
            target_site_code=manifest_code,
            target_site_name=str(manifest.get("site_name") or ""),
            request_id=request_id,
            detail={"path": path, "error": str(e)},
        )
        raise HTTPException(status_code=500, detail=f"restore execute failed: {e}") from e

    executed = mark_privileged_change_request_executed(
        request_id=request_id,
        executed_by_user_id=int(user.get("id") or 0),
        executed_by_login=actor_login,
        result={
            "path": path,
            "scope": "site",
            "restore": result,
            "executed_by": "requester",
        },
    )
    _audit_security(
        user=user,
        event_type="backup_restore_request_execute_self",
        severity="WARN",
        outcome="ok",
        target_site_code=manifest_code,
        target_site_name=str(manifest.get("site_name") or ""),
        request_id=request_id,
        detail={
            "path": path,
            "rollback_relative_path": str((result or {}).get("rollback_relative_path") or ""),
        },
    )
    return {"ok": True, "request": executed, "result": result, "maintenance": get_maintenance_status()}


@router.post("/backup/restore/upload")
async def api_backup_restore_upload(
    request: Request,
    backup_file: UploadFile = File(...),
    with_maintenance: str = Form("true"),
    include_user_tables: str = Form("true"),
):
    user, _token = _require_auth(request)
    if not _can_manage_backup(user):
        _audit_security(
            user=user,
            event_type="backup_restore_upload",
            severity="WARN",
            outcome="denied",
            detail={"reason": "permission_denied"},
        )
        raise HTTPException(status_code=403, detail="백업 권한이 없습니다.")
    if int(user.get("is_admin") or 0) != 1:
        _audit_security(
            user=user,
            event_type="backup_restore_upload",
            severity="WARN",
            outcome="denied",
            detail={"reason": "site_admin_restore_forbidden"},
        )
        raise HTTPException(
            status_code=403,
            detail="단지대표자는 직접 복구할 수 없습니다. 복구 요청을 등록하고 최고관리자 승인을 받으세요.",
        )

    actor_login = _clean_login_id(user.get("login_id") or "backup-restore")
    saved = _store_uploaded_restore_backup(backup_file, actor_login=actor_login)
    manifest = _validate_uploaded_restore_backup(str(saved.get("relative_path") or ""))
    _enforce_restore_permission(user, manifest)
    requested_include_user_tables = _clean_bool(include_user_tables, default=True)
    restore_include_user_tables = bool(requested_include_user_tables)
    scope = str(manifest.get("scope") or "").strip().lower() or "full"
    if scope != "site":
        restore_include_user_tables = True

    try:
        result = restore_backup_zip(
            actor=actor_login,
            relative_path=str(saved.get("relative_path") or ""),
            target_keys=[],
            with_maintenance=_clean_bool(with_maintenance, default=True),
            include_user_tables=restore_include_user_tables,
        )
    except RuntimeError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_upload",
            severity="ERROR",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": str(saved.get("relative_path") or ""), "error": str(e)},
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    except ValueError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_upload",
            severity="WARN",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": str(saved.get("relative_path") or ""), "error": str(e)},
        )
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        _audit_security(
            user=user,
            event_type="backup_restore_upload",
            severity="WARN",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": str(saved.get("relative_path") or ""), "error": str(e)},
        )
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        _audit_security(
            user=user,
            event_type="backup_restore_upload",
            severity="ERROR",
            outcome="error",
            target_site_code=str(manifest.get("site_code") or ""),
            target_site_name=str(manifest.get("site_name") or ""),
            detail={"path": str(saved.get("relative_path") or ""), "error": str(e)},
        )
        raise HTTPException(status_code=500, detail=f"restore failed: {e}") from e

    _audit_security(
        user=user,
        event_type="backup_restore_upload",
        severity="WARN",
        outcome="ok",
        target_site_code=str(manifest.get("site_code") or ""),
        target_site_name=str(manifest.get("site_name") or ""),
        detail={
            "path": str(saved.get("relative_path") or ""),
            "scope": str(manifest.get("scope") or ""),
            "include_user_tables": bool(restore_include_user_tables),
            "rollback_relative_path": str(result.get("rollback_relative_path") or ""),
        },
    )
    return {
        "ok": True,
        "uploaded": saved,
        "result": result,
        "maintenance": get_maintenance_status(),
    }


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

    if BOOTSTRAP_TOKEN:
        supplied = (
            str(request.headers.get("X-KA-BOOTSTRAP-TOKEN") or "").strip()
            or str(payload.get("bootstrap_token") or "").strip()
        )
        if not supplied or not hmac.compare_digest(supplied, BOOTSTRAP_TOKEN):
            raise HTTPException(status_code=403, detail="bootstrap token is required")

    login_id = _clean_login_id(payload.get("login_id"))
    name = _clean_name(payload.get("name") or payload.get("login_id"))
    role = _effective_role_for_permission_level(
        _clean_role(payload.get("role") or ROLE_LABEL_BY_PERMISSION["admin"]),
        "admin",
    )
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
        ip_address=(_client_ip(request) or None),
    )
    body = {
        "ok": True,
        "token": session["token"],
        "expires_at": session["expires_at"],
        "user": _public_user(user),
    }
    resp = JSONResponse(body)
    _set_auth_cookie(resp, session["token"])
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.post("/auth/public_access")
def auth_public_access(request: Request):
    if not _public_access_enabled():
        raise HTTPException(status_code=403, detail="public access is disabled")

    user = _ensure_public_access_user()
    # Invalidate any stale public-access sessions before issuing a new one.
    revoke_all_user_sessions(int(user["id"]))
    mark_staff_user_login(int(user["id"]))
    cleanup_expired_sessions()
    session = create_auth_session(
        int(user["id"]),
        ttl_hours=12,
        user_agent=request.headers.get("user-agent"),
        ip_address=(_client_ip(request) or None),
    )
    body = {
        "ok": True,
        "token": session["token"],
        "expires_at": session["expires_at"],
        "user": _public_user(user),
        "landing_path": _default_landing_path_for_user(user),
    }
    resp = JSONResponse(body)
    _set_auth_cookie(resp, session["token"])
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    return resp


@router.get("/auth/signup/check_login_id")
def auth_signup_check_login_id(login_id: str = Query(...), phone: str = Query(default="")):
    clean_login_id = _clean_signup_login_id(login_id)
    clean_phone = _normalize_phone(phone, required=False, field_name="phone") or ""
    available = _is_signup_login_id_available(clean_login_id, phone=clean_phone)
    return {
        "ok": True,
        "login_id": clean_login_id,
        "available": bool(available),
        "message": "사용 가능한 아이디입니다." if available else "이미 사용 중인 아이디입니다.",
    }


@router.post("/auth/signup/request_ready_verification")
def auth_signup_request_ready_verification(request: Request, payload: Dict[str, Any] = Body(...)):
    phone = _normalize_phone(payload.get("phone"), required=True, field_name="phone")
    existing = get_staff_user_by_phone(phone)
    if existing:
        return {
            "ok": True,
            "already_registered": True,
            "login_id": existing.get("login_id"),
            "message": "이미 등록된 휴대폰번호입니다. 기존 아이디로 로그인하세요.",
        }

    _assert_signup_sms_rate_limit(request, phone)
    req_item = _latest_executed_site_registry_request_for_phone(phone)
    if not req_item:
        raise HTTPException(status_code=404, detail="등록처리 완료 내역을 찾을 수 없습니다.")

    req_payload = req_item.get("payload") if isinstance(req_item.get("payload"), dict) else {}
    req_result = req_item.get("result") if isinstance(req_item.get("result"), dict) else {}
    site_name = _clean_site_name(
        req_result.get("site_name") or req_payload.get("site_name") or req_item.get("target_site_name"),
        required=True,
    )
    site_code = _clean_site_code(
        req_result.get("site_code") or req_payload.get("requested_site_code") or req_item.get("target_site_code"),
        required=False,
    )
    if not site_code:
        site_code = _resolve_site_code_for_site(site_name, "", allow_create=False, allow_remap=False)
    notice = _issue_site_registry_signup_ready_notice(
        request=request,
        req_payload=req_payload,
        site_name=site_name,
        site_code=site_code,
    )
    if not notice.get("notified"):
        raise HTTPException(
            status_code=409,
            detail=str(notice.get("reason") or "등록처리 정보가 불완전합니다. 가입정보를 다시 입력해 주세요."),
        )

    out = {
        "ok": True,
        "already_registered": False,
        "phone": phone,
        "request_id": int(req_item.get("id") or 0),
        "expires_at": str(notice.get("expires_at") or ""),
        "expires_in_sec": PHONE_VERIFY_TTL_MINUTES * 60,
        "delivery": str(notice.get("delivery") or "sms"),
        "setup_url": str(notice.get("setup_url") or ""),
        "message": str(notice.get("message") or "인증번호를 전송했습니다."),
    }
    if notice.get("debug_code"):
        out["debug_code"] = str(notice.get("debug_code"))
    return out


@router.post("/auth/signup/request_phone_verification")
def auth_signup_request_phone_verification(request: Request, payload: Dict[str, Any] = Body(...)):
    name = _clean_name(payload.get("name"))
    phone = _normalize_phone(payload.get("phone"), required=True, field_name="phone")
    _assert_signup_sms_rate_limit(request, phone)
    desired_login_id = _clean_signup_login_id(payload.get("login_id"))
    if not _is_signup_login_id_available(desired_login_id, phone=phone):
        raise HTTPException(status_code=409, detail="이미 사용 중인 아이디입니다. 다른 아이디를 입력하세요.")
    site_code = _clean_site_code(payload.get("site_code"), required=False)
    site_name = _clean_required_text(payload.get("site_name"), 80, "site_name")
    raw_role = _clean_role(payload.get("role"))
    signup_level = _permission_level_from_role_text(raw_role)
    if signup_level == "admin":
        raise HTTPException(status_code=403, detail="최고/운영관리자 계정은 자가가입할 수 없습니다.")
    role = _effective_role_for_permission_level(raw_role, signup_level)
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
        "desired_login_id": desired_login_id,
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
        request_ip=(_client_ip(request) or None),
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
        normalized_existing = normalize_staff_user_site_identity(int(existing.get("id") or 0))
        if normalized_existing:
            existing = normalized_existing
        existing_site_name = _clean_required_text(existing.get("site_name"), 80, "site_name")
        existing_site_code = _clean_site_code(existing.get("site_code"), required=False)
        if not existing_site_code:
            try:
                resolved_code = _resolve_site_code_for_site(
                    existing_site_name,
                    "",
                    allow_create=SITE_CODE_AUTOCREATE_NON_ADMIN,
                    allow_remap=False,
                )
            except HTTPException as e:
                if e.status_code == 404:
                    raise HTTPException(
                        status_code=403,
                        detail="단지코드가 등록되지 않았습니다. 최고/운영관리자에게 등록을 요청하세요.",
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
    raw_role = _clean_role(profile.get("role"))
    signup_level = _permission_level_from_role_text(raw_role)
    if signup_level == "admin":
        raise HTTPException(status_code=403, detail="최고/운영관리자 계정은 자가가입할 수 없습니다.")
    role = _effective_role_for_permission_level(raw_role, signup_level)
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
            allow_create=SITE_CODE_AUTOCREATE_NON_ADMIN,
            allow_remap=False,
        )
    except HTTPException as e:
        if e.status_code == 404:
            raise HTTPException(
                status_code=403,
                detail="단지코드가 등록되지 않았습니다. 최고/운영관리자에게 등록을 요청하세요.",
            ) from e
        raise
    _assert_resident_household_available(
        role=role,
        site_code=resolved_site_code,
        household_key=household_key,
        is_active=True,
    )
    desired_login_id = str(profile.get("desired_login_id") or "").strip()
    if desired_login_id:
        try:
            suggested_login_id = _clean_signup_login_id(desired_login_id)
        except HTTPException:
            suggested_login_id = _generate_login_id_from_phone(phone)
    else:
        suggested_login_id = _generate_login_id_from_phone(phone)
    if not _is_signup_login_id_available(suggested_login_id, phone=phone):
        suggested_login_id = _generate_login_id_from_phone(phone)
    signup_token = _issue_signup_finalize_token(verification_id=int(row["id"]), phone=phone)
    return {
        "ok": True,
        "already_registered": False,
        "signup_token": signup_token,
        "login_id_suggestion": suggested_login_id,
        "password_policy": _signup_password_policy_meta(),
        "message": "휴대폰 인증이 완료되었습니다. 비밀번호를 설정해 가입을 완료하세요.",
    }


@router.post("/auth/signup/complete")
def auth_signup_complete(payload: Dict[str, Any] = Body(...)):
    token_data = _parse_signup_finalize_token(payload.get("signup_token"))
    phone = token_data["phone"]
    verification_id = int(token_data["verification_id"])

    row = get_latest_signup_phone_verification(phone)
    if not row:
        raise HTTPException(status_code=404, detail="verification request not found")
    if int(row.get("id") or 0) != verification_id:
        raise HTTPException(status_code=409, detail="verification token is stale; request a new code")
    if row.get("consumed_at"):
        raise HTTPException(status_code=409, detail="verification already used; request a new code")
    if str(row.get("expires_at") or "") <= datetime.now().replace(microsecond=0).isoformat(sep=" "):
        raise HTTPException(status_code=410, detail="verification expired; request a new code")

    profile = {}
    try:
        profile = json.loads(str(row.get("payload_json") or "{}"))
    except Exception:
        profile = {}

    name = _clean_name(profile.get("name"))
    raw_role = _clean_role(profile.get("role"))
    signup_level = _permission_level_from_role_text(raw_role)
    if signup_level == "admin":
        raise HTTPException(status_code=403, detail="최고/운영관리자 계정은 자가가입할 수 없습니다.")
    role = _effective_role_for_permission_level(raw_role, signup_level)
    unit_label, household_key = _extract_resident_household(profile, role=role)
    site_code = _clean_site_code(profile.get("site_code"), required=False)
    site_name = _clean_required_text(profile.get("site_name"), 80, "site_name")
    address = _clean_required_text(profile.get("address"), 200, "address")
    office_phone = _normalize_phone(profile.get("office_phone"), required=True, field_name="office_phone")
    office_fax = _normalize_phone(profile.get("office_fax"), required=True, field_name="office_fax")

    raw_login_id = payload.get("login_id") or profile.get("desired_login_id") or _generate_login_id_from_phone(phone)
    try:
        login_id = _clean_signup_login_id(raw_login_id)
    except HTTPException:
        if payload.get("login_id"):
            raise
        login_id = _generate_login_id_from_phone(phone)
    password = _assert_signup_password_policy(payload.get("password"), login_id=login_id, phone=phone)
    if "password_confirm" not in payload:
        raise HTTPException(status_code=400, detail="password_confirm is required")
    password_confirm = str(payload.get("password_confirm") or "")
    if password_confirm != password:
        raise HTTPException(status_code=400, detail="password confirmation does not match")

    existing = get_staff_user_by_phone(phone)
    if existing:
        normalized_existing = normalize_staff_user_site_identity(int(existing.get("id") or 0))
        if normalized_existing:
            existing = normalized_existing
        touch_signup_phone_verification_attempt(int(row["id"]), success=True, issued_login_id=str(existing.get("login_id") or ""))
        return {
            "ok": True,
            "already_registered": True,
            "login_id": existing.get("login_id"),
            "temporary_password": None,
            "user": _public_user(existing),
            "message": "이미 등록된 휴대폰번호입니다. 기존 아이디를 안내합니다.",
        }

    if not _is_signup_login_id_available(login_id, phone=phone):
        raise HTTPException(status_code=409, detail="이미 사용 중인 아이디입니다. 다른 아이디를 입력하세요.")

    try:
        resolved_site_code = _resolve_site_code_for_site(
            site_name,
            site_code,
            allow_create=SITE_CODE_AUTOCREATE_NON_ADMIN,
            allow_remap=False,
        )
    except HTTPException as e:
        if e.status_code == 404:
            raise HTTPException(
                status_code=403,
                detail="단지코드가 등록되지 않았습니다. 최고/운영관리자에게 등록을 요청하세요.",
            ) from e
        raise
    _assert_resident_household_available(
        role=role,
        site_code=resolved_site_code,
        household_key=household_key,
        is_active=True,
    )
    try:
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
            password_hash=hash_password(password),
            is_admin=0,
            is_site_admin=1 if signup_level == "site_admin" else 0,
            is_active=1,
        )
    except Exception as e:
        msg = str(e)
        if "staff_users.login_id" in msg or "UNIQUE constraint failed" in msg:
            raise HTTPException(status_code=409, detail="이미 사용 중인 아이디입니다. 다른 아이디를 입력하세요.") from e
        raise
    touch_signup_phone_verification_attempt(int(row["id"]), success=True, issued_login_id=login_id)
    return {
        "ok": True,
        "already_registered": False,
        "login_id": login_id,
        "temporary_password": None,
        "user": _public_user(user),
        "must_change_password": False,
        "message": "가입이 완료되었습니다. 설정한 아이디/비밀번호로 로그인하세요.",
    }


@router.post("/auth/login")
def auth_login(request: Request, payload: Dict[str, Any] = Body(...)):
    login_id = _clean_login_id(payload.get("login_id"))
    password = _clean_password(payload.get("password"), required=True)
    _check_login_fail_rate(request, login_id)

    user = get_staff_user_by_login(login_id)
    if not user or int(user.get("is_active") or 0) != 1:
        _record_login_failure(request, login_id)
        raise HTTPException(status_code=401, detail="invalid credentials")
    user = _bind_user_site_identity(user)
    password_hash = user.get("password_hash")
    if not password_hash:
        _record_login_failure(request, login_id)
        raise HTTPException(status_code=403, detail="password is not set for this account")
    if not verify_password(password, str(password_hash)):
        _record_login_failure(request, login_id)
        raise HTTPException(status_code=401, detail="invalid credentials")

    _clear_login_failures(request, login_id)
    mark_staff_user_login(int(user["id"]))
    cleanup_expired_sessions()
    session = create_auth_session(
        int(user["id"]),
        ttl_hours=12,
        user_agent=request.headers.get("user-agent"),
        ip_address=(_client_ip(request) or None),
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
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
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


@router.get("/modules/contracts")
def api_modules_contracts(request: Request):
    user, _token = _require_auth(request)
    allowed_modules = _allowed_modules_for_user(user)
    allowed_set = {str(x or "").strip() for x in allowed_modules if str(x or "").strip()}
    items_by_key: Dict[str, Dict[str, Any]] = {}
    try:
        for item in list_module_contracts(active_only=True):
            key = str(item.get("module_key") or "").strip()
            if key and key in allowed_set:
                items_by_key[key] = item
    except Exception:
        items_by_key = {}
    fallback_by_key: Dict[str, Dict[str, Any]] = {
        "main": {"module_key": "main", "module_name": "메인 운영", "ui_path": "/pwa/", "api_prefix": "/api"},
        "parking": {"module_key": "parking", "module_name": "주차관리", "ui_path": "/parking/admin2", "api_prefix": "/api/parking"},
        "complaints": {"module_key": "complaints", "module_name": "민원관리", "ui_path": "/pwa/complaints.html", "api_prefix": "/api/v1"},
        "inspection": {"module_key": "inspection", "module_name": "점검관리", "ui_path": "/pwa/inspection.html", "api_prefix": "/api/inspection"},
        "electrical_ai": {"module_key": "electrical_ai", "module_name": "전기AI", "ui_path": "/pwa/electrical_ai.html", "api_prefix": "/api/elec"},
    }
    contracts: List[Dict[str, Any]] = []
    for module_key in allowed_modules:
        if module_key in items_by_key:
            contracts.append(items_by_key[module_key])
            continue
        if module_key in fallback_by_key:
            contracts.append(dict(fallback_by_key[module_key]))
    return {
        "ok": True,
        "allowed_modules": allowed_modules,
        "contracts": contracts,
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
        ip_address=(_client_ip(request) or None),
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
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
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
            {"key": "site_admin", "label": "단지대표자"},
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
    actor = _bind_user_site_identity(actor)
    return {"ok": True, "user": _public_user(actor)}


@router.patch("/users/me")
def api_users_me_patch(request: Request, payload: Dict[str, Any] = Body(...)):
    actor, _token = _require_auth(request)
    user_id = int(actor.get("id") or 0)
    current = get_staff_user(user_id)
    if not current:
        raise HTTPException(status_code=404, detail="user not found")

    if "password" in payload:
        raise HTTPException(status_code=400, detail="비밀번호 변경은 '비밀번호 변경' 기능(/api/auth/change_password)을 사용하세요.")

    restricted_keys = {
        "login_id",
        "role",
        "site_code",
        "site_name",
        "site_id",
        "is_admin",
        "is_site_admin",
        "admin_scope",
        "admin_scope_label",
        "permission_level",
        "is_active",
        "allowed_modules",
        "default_landing_path",
        "account_type",
        "created_at",
        "updated_at",
        "last_login_at",
        "household_key",
    }
    blocked = sorted([str(k) for k in payload.keys() if str(k) in restricted_keys])
    if blocked:
        raise HTTPException(
            status_code=403,
            detail=f"해당 항목은 관리자만 수정할 수 있습니다: {', '.join(blocked)}",
        )

    current_password = _clean_password(payload.get("current_password"), required=True)
    login_id = _clean_login_id(current.get("login_id"))
    db_user = get_staff_user_by_login(login_id)
    if not db_user or not db_user.get("password_hash"):
        raise HTTPException(status_code=404, detail="user not found")
    if not verify_password(str(current_password), str(db_user.get("password_hash"))):
        raise HTTPException(status_code=401, detail="current password is incorrect")

    name = _clean_name(payload.get("name", current.get("name")))
    role = _clean_role(current.get("role"))
    phone = _normalize_phone(
        payload["phone"] if "phone" in payload else current.get("phone"),
        required=False,
        field_name="phone",
    )
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
    if not _is_resident_role(role) and "unit_label" not in payload:
        unit_label = str(current.get("unit_label") or "").strip() or None
        household_key = str(current.get("household_key") or "").strip().upper() or None
    site_code = _clean_site_code(current.get("site_code"), required=False)
    is_active = 1 if bool(current.get("is_active")) else 0
    _assert_resident_household_available(
        role=role,
        site_code=site_code,
        household_key=household_key,
        is_active=bool(is_active),
        exclude_user_id=user_id,
    )

    try:
        user = update_staff_user_profile_fields(
            user_id,
            name=name,
            phone=phone,
            address=address,
            office_phone=office_phone,
            office_fax=office_fax,
            unit_label=unit_label,
            household_key=household_key,
            note=note,
        )
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            raise HTTPException(status_code=409, detail="login_id already exists")
        raise
    if not user:
        raise HTTPException(status_code=404, detail="user not found")

    fresh = get_staff_user(user_id) or user
    return {"ok": True, "user": _public_user(fresh)}


@router.post("/users/me/withdraw")
def api_users_me_withdraw(request: Request, payload: Dict[str, Any] = Body(...)):
    actor, _token = _require_auth(request)
    user_id = int(actor.get("id") or 0)
    current = get_staff_user(user_id)
    if not current:
        raise HTTPException(status_code=404, detail="user not found")

    password = _clean_password(payload.get("password"), required=True)
    confirm = str(payload.get("confirm") or "").strip()
    if confirm != "탈퇴":
        raise HTTPException(status_code=400, detail="확인 문구(confirm)는 '탈퇴' 이어야 합니다.")

    login_id = _clean_login_id(current.get("login_id"))
    db_user = get_staff_user_by_login(login_id)
    if not db_user or not db_user.get("password_hash"):
        raise HTTPException(status_code=404, detail="user not found")
    if not verify_password(str(password), str(db_user.get("password_hash"))):
        raise HTTPException(status_code=401, detail="password is incorrect")

    if bool(current.get("is_admin")) and bool(current.get("is_active")) and count_staff_admins(active_only=True) <= 1:
        raise HTTPException(status_code=400, detail="at least one active admin is required")
    if (
        bool(current.get("is_admin"))
        and _admin_scope_from_user(current) == "super_admin"
        and bool(current.get("is_active"))
        and count_super_admins(active_only=True) <= 1
    ):
        raise HTTPException(status_code=400, detail="at least one active super admin is required")

    withdrawn = withdraw_staff_user(user_id)
    revoke_all_user_sessions(user_id)
    if not withdrawn:
        raise HTTPException(status_code=404, detail="user not found")

    resp = JSONResponse({"ok": True})
    _clear_auth_cookie(resp)
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    return resp


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

    # Legacy rows may still contain malformed site_code; do not fail the whole list response.
    def _safe_site_code(value: Any) -> str:
        try:
            return _clean_site_code(value, required=False)
        except HTTPException:
            return ""

    source_users = [_public_user(x) for x in list_staff_users(active_only=bool(active_only))]
    for u in source_users:
        u["region"] = _region_from_address(u.get("address"))

    site_bucket: Dict[str, Dict[str, Any]] = {}
    region_bucket: Dict[str, int] = {}
    for u in source_users:
        row_code = _safe_site_code(u.get("site_code"))
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
        users = [u for u in users if _safe_site_code(u.get("site_code")) == clean_site_code]
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
    allow_site_code_create = bool(actor_super and SITE_CODE_AUTOCREATE_NON_ADMIN)
    if is_admin and actor_super:
        allow_site_code_create = True
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
                    detail="해당 단지코드가 등록되어 있지 않습니다. 최고/운영관리자에게 등록 요청하세요.",
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
    allow_site_code_create = bool(actor_super and SITE_CODE_AUTOCREATE_NON_ADMIN)
    if is_admin and actor_super:
        allow_site_code_create = True
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
                    detail="해당 단지코드가 등록되어 있지 않습니다. 최고/운영관리자에게 등록 요청하세요.",
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
    if _is_public_access_user(user):
        raise HTTPException(status_code=403, detail="로그인 없이 사용자는 저장할 수 없습니다. 신규가입 후 사용해 주세요.")
    site_ident = _resolve_main_site_identity(
        user, payload.get("site_name"), payload.get("site_code"), required=True
    )
    site_id = int(site_ident.get("site_id") or 0)
    site_name = str(site_ident.get("site_name") or "")
    site_code = str(site_ident.get("site_code") or "")
    entry_date = safe_ymd(payload.get("date") or "")

    raw_tabs = payload.get("tabs") or {}
    if not isinstance(raw_tabs, dict):
        raise HTTPException(status_code=400, detail="tabs must be object")

    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
    tabs = normalize_tabs_payload(raw_tabs, schema_defs=schema)
    entry_work_type = _entry_work_type_from_tabs(tabs, default="일일")
    if "home" in tabs:
        tabs["home"] = dict(tabs["home"] or {})
        tabs["home"]["work_type"] = entry_work_type
    ignored_tabs = sorted(set(str(k) for k in raw_tabs.keys()) - set(tabs.keys()))

    entry_id = upsert_entry(site_id, entry_date, entry_work_type)

    for tab_key, fields in tabs.items():
        save_tab_values(entry_id, tab_key, fields, schema_defs=schema)
        upsert_tab_domain_data(
            site_id,
            entry_date,
            tab_key,
            fields,
            work_type=entry_work_type,
            site_name=site_name,
        )

    return {
        "ok": True,
        "site_id": site_id,
        "site_name": site_name,
        "site_code": site_code,
        "date": entry_date,
        "work_type": entry_work_type,
        "saved_tabs": sorted(tabs.keys()),
        "ignored_tabs": ignored_tabs,
    }


@router.get("/load")
def api_load(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date: str = Query(...),
    work_type: str = Query(default=""),
):
    user, _token = _require_auth(request)
    site_ident = _resolve_main_site_identity(user, site_name, site_code, required=True)
    site_id = int(site_ident.get("site_id") or 0)
    site_name = str(site_ident.get("site_name") or "")
    site_code = str(site_ident.get("site_code") or "")
    entry_date = safe_ymd(date)
    entry_work_type = _clean_work_type(work_type, default="일일")
    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
    tabs = load_entry(
        site_id,
        entry_date,
        entry_work_type,
        fallback_empty_work_type=True,
        allowed_keys_by_tab=_schema_allowed_keys(schema),
    )
    if isinstance(tabs, dict):
        home = tabs.get("home")
        if isinstance(home, dict):
            home["work_type"] = str(home.get("work_type") or entry_work_type).strip() or entry_work_type
    return {
        "ok": True,
        "site_id": site_id,
        "site_name": site_name,
        "site_code": site_code,
        "date": entry_date,
        "work_type": entry_work_type,
        "tabs": tabs,
    }


@router.delete("/delete")
def api_delete(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date: str = Query(...),
    work_type: str = Query(default=""),
):
    user, _token = _require_auth(request)
    site_ident = _resolve_main_site_identity(user, site_name, site_code, required=True)
    site_id = int(site_ident.get("site_id") or 0)
    site_name = str(site_ident.get("site_name") or "")
    site_code = str(site_ident.get("site_code") or "")
    entry_date = safe_ymd(date)
    entry_work_type = _clean_work_type(work_type, default="일일")
    ok = delete_entry(site_id, entry_date, entry_work_type)
    return {"ok": ok, "site_id": site_id, "site_name": site_name, "site_code": site_code, "work_type": entry_work_type}


@router.get("/list_range")
def api_list_range(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    work_type: str = Query(default=""),
):
    user, _token = _require_auth(request)
    site_ident = _resolve_main_site_identity(user, site_name, site_code, required=True)
    site_id = int(site_ident.get("site_id") or 0)
    site_name = str(site_ident.get("site_name") or "")
    site_code = str(site_ident.get("site_code") or "")
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df
    entry_work_type = _clean_work_type(work_type, default="일일")
    entries = list_entries(site_id, df, dt, entry_work_type)
    items: List[Dict[str, Any]] = []
    dates: List[str] = []
    for row in entries:
        entry_date = str(row["entry_date"] or "")
        dates.append(entry_date)
        row_work_type = str(row["work_type"] or "").strip() if "work_type" in row.keys() else ""
        items.append(
            {
                "entry_id": int(row["id"] or 0),
                "entry_date": entry_date,
                "work_type": row_work_type or entry_work_type,
                "created_at": str(row["created_at"] or ""),
                "updated_at": str(row["updated_at"] or ""),
            }
        )
    return {
        "ok": True,
        "site_id": site_id,
        "site_name": site_name,
        "site_code": site_code,
        "work_type": entry_work_type,
        "date_from": df,
        "date_to": dt,
        "items": items,
        "dates": dates,
    }


@router.get("/export")
def api_export(
    request: Request,
    site_name: str = Query(default=""),
    site_code: str = Query(default=""),
    date_from: str = Query(default=""),
    date_to: str = Query(default=""),
    work_type: str = Query(default=""),
):
    user, _token = _require_auth(request)
    site_ident = _resolve_main_site_identity(user, site_name, site_code, required=True)
    site_id = int(site_ident.get("site_id") or 0)
    site_name = str(site_ident.get("site_name") or "")
    site_code = str(site_ident.get("site_code") or "")
    df = safe_ymd(date_from) if date_from else today_ymd()
    dt = safe_ymd(date_to) if date_to else today_ymd()
    if df > dt:
        df, dt = dt, df

    entry_work_type = _clean_work_type(work_type, default="일일")
    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
    allowed = _schema_allowed_keys(schema)
    entries = list_entries(site_id, df, dt, entry_work_type)
    rows: List[Dict[str, Any]] = []
    for e in entries:
        rows.append(
            {
                "entry_date": e["entry_date"],
                "work_type": str(e["work_type"] or entry_work_type),
                "tabs": load_entry_by_id(int(e["id"]), allowed_keys_by_tab=allowed),
            }
        )

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
    work_type: str = Query(default=""),
):
    user, _token = _require_auth(request)
    site_ident = _resolve_main_site_identity(user, site_name, site_code, required=True)
    site_id = int(site_ident.get("site_id") or 0)
    site_name = str(site_ident.get("site_name") or "")
    site_code = str(site_ident.get("site_code") or "")
    entry_date = safe_ymd(date)
    entry_work_type = _clean_work_type(work_type, default="일일")
    schema, _env_cfg = _site_schema_and_env(site_name, site_code)
    tabs = load_entry(
        site_id,
        entry_date,
        entry_work_type,
        fallback_empty_work_type=True,
        allowed_keys_by_tab=_schema_allowed_keys(schema),
    )
    if isinstance(tabs, dict):
        home = tabs.get("home")
        if isinstance(home, dict):
            home["work_type"] = str(home.get("work_type") or entry_work_type).strip() or entry_work_type

    worker_name = str(user.get("name") or user.get("login_id") or "").strip()
    pbytes = build_pdf(
        site_name,
        entry_date,
        tabs,
        worker_name=worker_name,
        schema_defs=schema,
        site_env_config=_env_cfg,
    )
    from urllib.parse import quote

    filename = f"전기일지_{site_name}_{entry_date}.pdf"
    ascii_fallback = "report.pdf"
    cd = f"attachment; filename={ascii_fallback}; filename*=UTF-8''{quote(filename)}"
    return StreamingResponse(
        io.BytesIO(pbytes),
        media_type="application/pdf",
        headers={"Content-Disposition": cd},
    )
