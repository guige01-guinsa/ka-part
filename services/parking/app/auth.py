import os, hmac, hashlib, base64
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import Request, HTTPException

SECRET_KEY = os.getenv("PARKING_SECRET_KEY", "change-this-secret")
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

def make_session(username: str, role: str) -> str:
    return _ser.dumps({"u": username, "r": role})

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
