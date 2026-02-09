import os
import json
from datetime import date
from pathlib import Path
import uuid
import html as _html

from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Literal
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from .db import init_db, seed_demo, seed_users, connect, normalize_site_code
from .auth import make_session, pbkdf2_verify, read_session

API_KEY = os.getenv("PARKING_API_KEY", "change-me")
ROOT_PATH = os.getenv("PARKING_ROOT_PATH", "").strip()
if ROOT_PATH and not ROOT_PATH.startswith("/"):
    ROOT_PATH = f"/{ROOT_PATH}"
ROOT_PATH = ROOT_PATH.rstrip("/")
DEFAULT_SITE_CODE = normalize_site_code(os.getenv("PARKING_DEFAULT_SITE_CODE", "COMMON"))
LOCAL_LOGIN_ENABLED = os.getenv("PARKING_LOCAL_LOGIN_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
CONTEXT_SECRET = os.getenv("PARKING_CONTEXT_SECRET", os.getenv("PARKING_SECRET_KEY", "change-this-secret"))
CONTEXT_MAX_AGE = int(os.getenv("PARKING_CONTEXT_MAX_AGE", "300"))
_ctx_ser = URLSafeTimedSerializer(CONTEXT_SECRET, salt="parking-context")
UPLOAD_DIR = Path(os.getenv("PARKING_UPLOAD_DIR", str(Path(__file__).resolve().parent / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Parking Enforcer API", version="1.0.0", root_path=ROOT_PATH)
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")


def app_url(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    if ROOT_PATH:
        return f"{ROOT_PATH}{path}"
    return path


def _auto_entry_page(next_path: str) -> str:
    quoted_next = json.dumps(next_path)
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Parking Entry</title>
</head>
<body>
  <p>주차관리 접속 처리 중입니다...</p>
  <script>
    (async function () {{
      const nextPath = {quoted_next};
      const loginUrl = "/pwa/login.html?next=" + encodeURIComponent(nextPath);
      let token = "";
      try {{
        token = (localStorage.getItem("ka_part_auth_token_v1") || "").trim();
      }} catch (_e) {{
        token = "";
      }}
      if (!token) {{
        window.location.replace(loginUrl);
        return;
      }}
      try {{
        const res = await fetch("/api/parking/context", {{
          method: "GET",
          headers: {{ "Authorization": "Bearer " + token }}
        }});
        if (!res.ok) {{
          throw new Error(String(res.status));
        }}
        const data = await res.json();
        window.location.replace((data && data.url) ? String(data.url) : "/parking/sso");
      }} catch (_e) {{
        window.location.replace(loginUrl);
      }}
    }})();
  </script>
</body>
</html>"""


def require_key(x_api_key: str | None):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def map_permission_to_role(permission_level: str) -> str:
    raw = (permission_level or "").strip().lower()
    if raw == "admin":
        return "admin"
    if raw == "site_admin":
        return "guard"
    if raw == "user":
        return "viewer"
    raise HTTPException(status_code=400, detail="Invalid permission_level")


def resolve_site_scope(request: Request, x_site_code: str | None = None) -> str:
    sess = read_session(request)
    if sess and sess.get("sc"):
        return normalize_site_code(str(sess["sc"]))
    if x_site_code:
        return normalize_site_code(x_site_code)
    return DEFAULT_SITE_CODE

@app.on_event("startup")
def _startup():
    init_db()
    seed_demo()
    seed_users()

def today_iso() -> str:
    return date.today().isoformat()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def home():
    return RedirectResponse(url=app_url("/admin2"), status_code=302)

@app.get("/login", response_class=HTMLResponse)
def login_page():
    if not LOCAL_LOGIN_ENABLED:
        return RedirectResponse(url=app_url("/admin2"), status_code=302)
    login_action = app_url("/login")
    return f"<h2>Login</h2><form method='POST' action='{login_action}'><input name='username'/><input name='password' type='password'/><button>Login</button></form>"

@app.post("/login")
def login_submit(username: str = Form(...), password: str = Form(...)):
    if not LOCAL_LOGIN_ENABLED:
        return RedirectResponse(url=app_url("/admin2"), status_code=302)
    u = username.strip()
    with connect() as con:
        row = con.execute("SELECT * FROM users WHERE username=?", (u,)).fetchone()
    if not row or not pbkdf2_verify(password, row["pw_hash"]):
        return HTMLResponse("Invalid credentials", status_code=401)
    token = make_session(u, row["role"], site_code=DEFAULT_SITE_CODE)
    resp = RedirectResponse(url=app_url("/admin2"), status_code=302)
    resp.set_cookie("parking_session", token, httponly=True, samesite="lax", path=ROOT_PATH or "/")
    return resp


@app.get("/sso")
def sso_login(ctx: str):
    try:
        payload = _ctx_ser.loads(ctx, max_age=CONTEXT_MAX_AGE)
    except SignatureExpired as exc:
        raise HTTPException(status_code=401, detail="Context token expired") from exc
    except BadSignature as exc:
        raise HTTPException(status_code=401, detail="Invalid context token") from exc

    site_code = normalize_site_code(payload.get("site_code"))
    permission_level = str(payload.get("permission_level") or "").strip().lower()
    role = map_permission_to_role(permission_level)
    token = make_session("ka-part-user", role, site_code=site_code)
    resp = RedirectResponse(url=app_url("/admin2"), status_code=302)
    resp.set_cookie("parking_session", token, httponly=True, samesite="lax", path=ROOT_PATH or "/")
    return resp

@app.post("/logout")
def logout():
    target = app_url("/login") if LOCAL_LOGIN_ENABLED else "/pwa/"
    resp = RedirectResponse(url=target, status_code=302)
    resp.delete_cookie("parking_session", path=ROOT_PATH or "/")
    return resp

class CheckResponse(BaseModel):
    site_code: str
    plate: str
    verdict: Literal["OK","UNREGISTERED","BLOCKED","EXPIRED","TEMP"]
    message: str
    unit: Optional[str] = None
    owner_name: Optional[str] = None
    status: Optional[str] = None
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None

@app.get("/api/plates/check", response_model=CheckResponse)
def check_plate(
    plate: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_site_code: str | None = Header(default=None, alias="X-Site-Code"),
):
    require_key(x_api_key)
    site_code = resolve_site_scope(request, x_site_code)
    p = plate.strip().upper()
    with connect() as con:
        row = con.execute("SELECT * FROM vehicles WHERE site_code = ? AND plate = ?", (site_code, p)).fetchone()
    if not row:
        return CheckResponse(site_code=site_code, plate=p, verdict="UNREGISTERED", message="미등록 차량")
    status = (row["status"] or "active").lower()
    vf, vt = row["valid_from"], row["valid_to"]
    t = today_iso()
    if status == "blocked":
        return CheckResponse(site_code=site_code, plate=p, verdict="BLOCKED", message="차단 차량", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)
    if vt and t > vt:
        return CheckResponse(site_code=site_code, plate=p, verdict="EXPIRED", message="기간 만료", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)
    if status == "temp":
        return CheckResponse(site_code=site_code, plate=p, verdict="TEMP", message="임시 등록", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)
    return CheckResponse(site_code=site_code, plate=p, verdict="OK", message="정상 등록", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)

class ViolationOut(BaseModel):
    id: int
    site_code: str
    plate: str
    verdict: str
    rule_code: Optional[str] = None
    location: Optional[str] = None
    memo: Optional[str] = None
    inspector: Optional[str] = None
    photo_path: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    created_at: str

@app.post("/api/violations/upload", response_model=ViolationOut)
def create_violation_with_photo(
    request: Request,
    plate: str = Form(...),
    verdict: str = Form(...),
    rule_code: str | None = Form(None),
    location: str | None = Form(None),
    memo: str | None = Form(None),
    inspector: str | None = Form(None),
    lat: float | None = Form(None),
    lng: float | None = Form(None),
    photo: UploadFile = File(...),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_site_code: str | None = Header(default=None, alias="X-Site-Code"),
):
    require_key(x_api_key)
    site_code = resolve_site_scope(request, x_site_code)
    p = plate.strip().upper()
    ext = os.path.splitext(photo.filename or "")[1].lower() or ".jpg"
    fname = f"{uuid.uuid4().hex}{ext}"
    fpath = UPLOAD_DIR / fname
    with open(fpath, "wb") as f:
        f.write(photo.file.read())
    rel = app_url(f"/uploads/{fname}")
    with connect() as con:
        cur = con.execute(
            "INSERT INTO violations (site_code, plate, verdict, rule_code, location, memo, inspector, photo_path, lat, lng) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (site_code, p, verdict, rule_code, location, memo, inspector, rel, lat, lng),
        )
        vid = cur.lastrowid
        row = con.execute("SELECT * FROM violations WHERE id = ?", (vid,)).fetchone()
    return ViolationOut(**dict(row))

def esc(v): return _html.escape(str(v)) if v is not None else ""

@app.get("/admin2", response_class=HTMLResponse)
def admin2(request: Request):
    s = read_session(request)
    if not s:
        if LOCAL_LOGIN_ENABLED:
            raise HTTPException(status_code=401, detail="Login required")
        return HTMLResponse(_auto_entry_page(app_url("/admin2")), status_code=200)

    if s.get("r") not in {"admin", "guard", "viewer"}:
        raise HTTPException(status_code=403, detail="Forbidden")

    site_code = normalize_site_code(s.get("sc"))
    with connect() as con:
        vs = con.execute(
            "SELECT * FROM vehicles WHERE site_code=? ORDER BY updated_at DESC LIMIT 200",
            (site_code,),
        ).fetchall()
        logs = con.execute(
            "SELECT * FROM violations WHERE site_code=? ORDER BY created_at DESC LIMIT 100",
            (site_code,),
        ).fetchall()
    v_rows = "".join([f"<tr><td>{esc(r['plate'])}</td><td>{esc(r['status'])}</td><td>{esc(r['unit'])}</td><td>{esc(r['owner_name'])}</td></tr>" for r in vs])
    l_rows = "".join([f"<tr><td>{esc(r['created_at'])}</td><td>{esc(r['plate'])}</td><td>{esc(r['verdict'])}</td><td>{esc(r['photo_path'] or '-')}</td></tr>" for r in logs])
    logout_path = app_url("/logout")
    top_link = (
        f"""<a href="{logout_path}" onclick="fetch('{logout_path}',{{method:'POST'}});return false;">Logout</a>"""
        if LOCAL_LOGIN_ENABLED
        else """<a href="/pwa/">시설관리로 돌아가기</a>"""
    )
    return f"""<h2>Admin ({esc(site_code)})</h2>{top_link}
    <h3>Vehicles</h3><table border=1><tr><th>Plate</th><th>Status</th><th>Unit</th><th>Owner</th></tr>{v_rows}</table>
    <h3>Violations</h3><table border=1><tr><th>At</th><th>Plate</th><th>Verdict</th><th>Photo</th></tr>{l_rows}</table>"""
