import os, hmac, hashlib, base64
from typing import Any
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import Request, HTTPException

_TRUTHY = {"1", "true", "yes", "on"}
_WEAK_SECRET_MARKERS = {
    "change-me",
    "change-this-secret",
    "ka-part-dev-secret",
    "parking-dev-secret-change-me",
}


def _allow_insecure_defaults() -> bool:
    return (os.getenv("ALLOW_INSECURE_DEFAULTS") or "").strip().lower() in _TRUTHY


def _read_secret_key() -> str:
    raw = (os.getenv("PARKING_SECRET_KEY") or "").strip()
    lowered = raw.lower()
    if raw and lowered not in _WEAK_SECRET_MARKERS and len(raw) >= 16:
        return raw
    if not raw:
        generated = base64.urlsafe_b64encode(os.urandom(48)).decode("ascii")
        os.environ.setdefault("PARKING_SECRET_KEY", generated)
        return generated
    if _allow_insecure_defaults():
        return raw
    if lowered in _WEAK_SECRET_MARKERS:
        raise RuntimeError("PARKING_SECRET_KEY uses an insecure default-like value")
    raise RuntimeError("PARKING_SECRET_KEY must be at least 16 characters")


SECRET_KEY = _read_secret_key()
SESSION_MAX_AGE = int(os.getenv("PARKING_SESSION_MAX_AGE", "43200"))

_ser = URLSafeTimedSerializer(SECRET_KEY, salt="parking-session")

def pbkdf2_hash(password: str, salt: bytes | None = None) -> str:
    if salt is None:
        salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return base64.b64encode(salt + dk).decode("utf-8")

def pbkdf2_verify(password: str, stored: str) -> bool:
    raw = base64.b64decode(stored.encode("utf-8"))
    salt, dk = raw[:16], raw[16:]
    dk2 = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return hmac.compare_digest(dk, dk2)

def make_session(
    username: str,
    role: str,
    site_code: str | None = None,
    extras: dict[str, Any] | None = None,
) -> str:
    payload = {"u": username, "r": role}
    if site_code:
        payload["sc"] = str(site_code).strip().upper()
    if extras:
        for key, value in extras.items():
            txt = str(value or "").strip()
            if txt:
                payload[str(key)] = txt
    return _ser.dumps(payload)

def read_session(request: Request) -> dict | None:
    token = request.cookies.get("parking_session")
    if not token:
        return None
    try:
        return _ser.loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None

def require_login(request: Request) -> dict:
    s = read_session(request)
    if not s:
        raise HTTPException(status_code=401, detail="Login required")
    return s

def require_role(request: Request, roles: set[str]) -> dict:
    s = require_login(request)
    if s.get("r") not in roles:
        raise HTTPException(status_code=403, detail="Forbidden")
    return s
