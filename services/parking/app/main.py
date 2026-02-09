import os
from datetime import date
from pathlib import Path
import uuid
import html as _html

from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, Literal

from .db import init_db, seed_demo, seed_users, connect
from .auth import make_session, pbkdf2_verify, require_role

API_KEY = os.getenv("PARKING_API_KEY", "change-me")
ROOT_PATH = os.getenv("PARKING_ROOT_PATH", "").strip()
if ROOT_PATH and not ROOT_PATH.startswith("/"):
    ROOT_PATH = f"/{ROOT_PATH}"
ROOT_PATH = ROOT_PATH.rstrip("/")
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

def require_key(x_api_key: str | None):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

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

@app.get("/login", response_class=HTMLResponse)
def login_page():
    login_action = app_url("/login")
    return f"<h2>Login</h2><form method='POST' action='{login_action}'><input name='username'/><input name='password' type='password'/><button>Login</button></form>"

@app.post("/login")
def login_submit(username: str = Form(...), password: str = Form(...)):
    u = username.strip()
    with connect() as con:
        row = con.execute("SELECT * FROM users WHERE username=?", (u,)).fetchone()
    if not row or not pbkdf2_verify(password, row["pw_hash"]):
        return HTMLResponse("Invalid credentials", status_code=401)
    token = make_session(u, row["role"])
    resp = RedirectResponse(url=app_url("/admin2"), status_code=302)
    resp.set_cookie("parking_session", token, httponly=True, samesite="lax", path=ROOT_PATH or "/")
    return resp

@app.post("/logout")
def logout():
    resp = RedirectResponse(url=app_url("/login"), status_code=302)
    resp.delete_cookie("parking_session", path=ROOT_PATH or "/")
    return resp

class CheckResponse(BaseModel):
    plate: str
    verdict: Literal["OK","UNREGISTERED","BLOCKED","EXPIRED","TEMP"]
    message: str
    unit: Optional[str] = None
    owner_name: Optional[str] = None
    status: Optional[str] = None
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None

@app.get("/api/plates/check", response_model=CheckResponse)
def check_plate(plate: str, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
    require_key(x_api_key)
    p = plate.strip().upper()
    with connect() as con:
        row = con.execute("SELECT * FROM vehicles WHERE plate = ?", (p,)).fetchone()
    if not row:
        return CheckResponse(plate=p, verdict="UNREGISTERED", message="미등록 차량")
    status = (row["status"] or "active").lower()
    vf, vt = row["valid_from"], row["valid_to"]
    t = today_iso()
    if status == "blocked":
        return CheckResponse(plate=p, verdict="BLOCKED", message="차단 차량", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)
    if vt and t > vt:
        return CheckResponse(plate=p, verdict="EXPIRED", message="기간 만료", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)
    if status == "temp":
        return CheckResponse(plate=p, verdict="TEMP", message="임시 등록", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)
    return CheckResponse(plate=p, verdict="OK", message="정상 등록", unit=row["unit"], owner_name=row["owner_name"], status=status, valid_from=vf, valid_to=vt)

class ViolationOut(BaseModel):
    id: int
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
):
    require_key(x_api_key)
    p = plate.strip().upper()
    ext = os.path.splitext(photo.filename or "")[1].lower() or ".jpg"
    fname = f"{uuid.uuid4().hex}{ext}"
    fpath = UPLOAD_DIR / fname
    with open(fpath, "wb") as f:
        f.write(photo.file.read())
    rel = app_url(f"/uploads/{fname}")
    with connect() as con:
        cur = con.execute(
            "INSERT INTO violations (plate, verdict, rule_code, location, memo, inspector, photo_path, lat, lng) VALUES (?,?,?,?,?,?,?,?,?)",
            (p, verdict, rule_code, location, memo, inspector, rel, lat, lng),
        )
        vid = cur.lastrowid
        row = con.execute("SELECT * FROM violations WHERE id = ?", (vid,)).fetchone()
    return ViolationOut(**dict(row))

def esc(v): return _html.escape(str(v)) if v is not None else ""

@app.get("/admin2", response_class=HTMLResponse)
def admin2(request: Request):
    require_role(request, {"admin","guard","viewer"})
    with connect() as con:
        vs = con.execute("SELECT * FROM vehicles ORDER BY updated_at DESC LIMIT 200").fetchall()
        logs = con.execute("SELECT * FROM violations ORDER BY created_at DESC LIMIT 100").fetchall()
    v_rows = "".join([f"<tr><td>{esc(r['plate'])}</td><td>{esc(r['status'])}</td><td>{esc(r['unit'])}</td><td>{esc(r['owner_name'])}</td></tr>" for r in vs])
    l_rows = "".join([f"<tr><td>{esc(r['created_at'])}</td><td>{esc(r['plate'])}</td><td>{esc(r['verdict'])}</td><td>{esc(r['photo_path'] or '-')}</td></tr>" for r in logs])
    logout_path = app_url("/logout")
    return f"""<h2>Admin</h2><a href="{logout_path}" onclick="fetch('{logout_path}',{{method:'POST'}});return false;">Logout</a>
    <h3>Vehicles</h3><table border=1><tr><th>Plate</th><th>Status</th><th>Unit</th><th>Owner</th></tr>{v_rows}</table>
    <h3>Violations</h3><table border=1><tr><th>At</th><th>Plate</th><th>Verdict</th><th>Photo</th></tr>{l_rows}</table>"""
