import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any

from fastapi import HTTPException

_TRUTHY = {"1", "true", "yes", "on"}
_WEAK_SECRET_MARKERS = {
    "change-me",
    "change-this-secret",
    "ka-part-dev-secret",
    "parking-dev-secret-change-me",
}


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    pad = "=" * ((4 - (len(data) % 4)) % 4)
    return base64.urlsafe_b64decode((data + pad).encode("ascii"))


def _allow_insecure_defaults() -> bool:
    return (os.getenv("ALLOW_INSECURE_DEFAULTS") or "").strip().lower() in _TRUTHY


def _secret() -> bytes:
    raw = (os.getenv("PARKING_TOKEN_SECRET") or "").strip()
    lowered = raw.lower()
    if raw and lowered not in _WEAK_SECRET_MARKERS and len(raw) >= 16:
        return raw.encode("utf-8")
    if not raw:
        generated = base64.urlsafe_b64encode(os.urandom(48)).decode("ascii")
        os.environ.setdefault("PARKING_TOKEN_SECRET", generated)
        return generated.encode("utf-8")
    if _allow_insecure_defaults():
        return raw.encode("utf-8")
    if lowered in _WEAK_SECRET_MARKERS:
        raise RuntimeError("PARKING_TOKEN_SECRET uses an insecure default-like value")
    raise RuntimeError("PARKING_TOKEN_SECRET must be at least 16 characters")


def token_ttl_seconds() -> int:
    raw = os.getenv("PARKING_TOKEN_TTL_SECONDS", "43200").strip()
    try:
        ttl = int(raw)
    except Exception:
        ttl = 43200
    return max(300, ttl)


def create_parking_token(claims: dict[str, Any], ttl_seconds: int | None = None) -> str:
    now = int(time.time())
    ttl = ttl_seconds if ttl_seconds is not None else token_ttl_seconds()
    payload = {
        **claims,
        "iat": now,
        "exp": now + int(ttl),
    }
    payload_raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    payload_b64 = _b64url_encode(payload_raw)
    sig = hmac.new(_secret(), payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url_encode(sig)}"


def verify_parking_token(token: str) -> dict[str, Any]:
    if not token or "." not in token:
        raise HTTPException(status_code=401, detail="invalid parking token")
    payload_b64, sig_b64 = token.split(".", 1)
    expected_sig = _b64url_encode(hmac.new(_secret(), payload_b64.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(sig_b64, expected_sig):
        raise HTTPException(status_code=401, detail="invalid parking token signature")

    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=401, detail="invalid parking token payload")

    now = int(time.time())
    exp = int(payload.get("exp") or 0)
    if exp <= now:
        raise HTTPException(status_code=401, detail="parking token expired")
    return payload
