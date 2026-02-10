import os
import json
import logging
import re
import urllib.parse
from datetime import date
from pathlib import Path
import uuid
import html as _html

from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
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
CONTEXT_SECRET = (
    os.getenv("PARKING_CONTEXT_SECRET")
    or os.getenv("PARKING_SECRET_KEY")
    or os.getenv("KA_PHONE_VERIFY_SECRET", "ka-part-dev-secret")
)
CONTEXT_MAX_AGE = int(os.getenv("PARKING_CONTEXT_MAX_AGE", "300"))
PORTAL_URL = (os.getenv("PARKING_PORTAL_URL") or "").strip()
PORTAL_LOGIN_URL = (os.getenv("PARKING_PORTAL_LOGIN_URL") or "").strip()
_ctx_ser = URLSafeTimedSerializer(CONTEXT_SECRET, salt="parking-context")
UPLOAD_DIR = Path(os.getenv("PARKING_UPLOAD_DIR", str(Path(__file__).resolve().parent / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Parking Enforcer API", version="1.0.0", root_path=ROOT_PATH)
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")
logger = logging.getLogger("ka-part.parking")
_PLATE_RE = re.compile(r"[^0-9A-Za-z가-힣]")
_VERDICT_OPTIONS = {"OK", "UNREGISTERED", "BLOCKED", "EXPIRED", "TEMP"}


def app_url(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    if ROOT_PATH:
        return f"{ROOT_PATH}{path}"
    return path


def portal_login_url(next_path: str | None = None) -> str:
    nxt = app_url("/admin2") if not next_path else next_path
    nxt_enc = urllib.parse.quote(nxt, safe="")

    base = PORTAL_LOGIN_URL
    if not base:
        if PORTAL_URL:
            base = PORTAL_URL if "login.html" in PORTAL_URL else f"{PORTAL_URL.rstrip('/')}/login.html"
        else:
            base = "https://www.ka-part.com/pwa/login.html"

    if "{next}" in base:
        return base.replace("{next}", nxt_enc)
    if "next=" in base:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}next={nxt_enc}"


def integration_required_page(status_code: int = 200) -> HTMLResponse:
    target = portal_login_url(app_url("/admin2"))
    target_js = json.dumps(target, ensure_ascii=False)
    link = (
        f"""<p><a href="{_html.escape(target)}">아파트 시설관리 시스템으로 이동</a></p>"""
    )
    body = (
        "<h2>Parking Login</h2><p>통합 로그인 전용입니다.</p>"
        f"{link}<script>window.location.replace({target_js});</script>"
    )
    return HTMLResponse(body, status_code=status_code)


def _auto_entry_page(next_path: str) -> str:
    quoted_next = json.dumps(next_path)
    login_url = portal_login_url(next_path)
    quoted_login = json.dumps(login_url, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Parking Entry</title>
</head>
<body>
  <p id="msg">주차관리 접속 처리 중입니다...</p>
  <script>
    (async function () {{
      const msgEl = document.getElementById("msg");
      const setMsg = (txt) => {{ if (msgEl) msgEl.textContent = txt; }};
      const nextPath = {quoted_next};
      const loginUrl = {quoted_login};
      const LOOP_KEY = "ka_parking_entry_retry_v1";
      const readRetry = () => {{
        try {{
          const raw = (sessionStorage.getItem(LOOP_KEY) || "0").trim();
          const n = Number.parseInt(raw, 10);
          return Number.isFinite(n) && n > 0 ? n : 0;
        }} catch (_e) {{
          return 0;
        }}
      }};
      const bumpRetry = () => {{
        try {{
          sessionStorage.setItem(LOOP_KEY, String(readRetry() + 1));
        }} catch (_e) {{
          // ignore
        }}
      }};
      const clearRetry = () => {{
        try {{
          sessionStorage.removeItem(LOOP_KEY);
        }} catch (_e) {{
          // ignore
        }}
      }};
      let token = "";
      try {{
        token = (localStorage.getItem("ka_part_auth_token_v1") || "").trim();
      }} catch (_e) {{
        token = "";
      }}
      if (!token) {{
        if (readRetry() >= 2) {{
          setMsg("로그인이 필요합니다. 시설관리 로그인 후 다시 시도하세요.");
          return;
        }}
        bumpRetry();
        window.location.replace(loginUrl);
        return;
      }}
      try {{
        const res = await fetch("/api/parking/context", {{
          method: "GET",
          headers: {{ "Authorization": "Bearer " + token }}
        }});
        const ct = res.headers.get("content-type") || "";
        const data = ct.includes("application/json") ? await res.json() : {{}};
        if (res.status === 401) {{
          try {{
            localStorage.removeItem("ka_part_auth_token_v1");
            localStorage.removeItem("ka_part_auth_user_v1");
          }} catch (_e) {{
            // ignore
          }}
          if (readRetry() >= 2) {{
            setMsg("세션이 만료되었습니다. 다시 로그인하세요.");
            return;
          }}
          bumpRetry();
          window.location.replace(loginUrl);
          return;
        }}
        if (!res.ok) {{
          const detail = data && data.detail ? String(data.detail) : ("HTTP " + String(res.status));
          throw new Error(detail);
        }}
        clearRetry();
        window.location.replace((data && data.url) ? String(data.url) : nextPath);
      }} catch (e) {{
        setMsg("주차 연동 오류: " + String((e && e.message) || e));
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


def normalize_plate(value: str) -> str:
    return _PLATE_RE.sub("", str(value or "").upper()).strip()


def check_plate_record(site_code: str, plate: str) -> "CheckResponse":
    p = normalize_plate(plate)
    if len(p) < 4:
        raise HTTPException(status_code=400, detail="plate is too short")

    with connect() as con:
        row = con.execute("SELECT * FROM vehicles WHERE site_code = ? AND plate = ?", (site_code, p)).fetchone()

    if not row:
        return CheckResponse(site_code=site_code, plate=p, verdict="UNREGISTERED", message="미등록 차량")

    status = (row["status"] or "active").lower()
    vf, vt = row["valid_from"], row["valid_to"]
    today = today_iso()

    if status == "blocked":
        return CheckResponse(
            site_code=site_code,
            plate=p,
            verdict="BLOCKED",
            message="차단 차량",
            unit=row["unit"],
            owner_name=row["owner_name"],
            status=status,
            valid_from=vf,
            valid_to=vt,
        )
    if vt and today > vt:
        return CheckResponse(
            site_code=site_code,
            plate=p,
            verdict="EXPIRED",
            message="기간 만료",
            unit=row["unit"],
            owner_name=row["owner_name"],
            status=status,
            valid_from=vf,
            valid_to=vt,
        )
    if status == "temp":
        return CheckResponse(
            site_code=site_code,
            plate=p,
            verdict="TEMP",
            message="임시 등록",
            unit=row["unit"],
            owner_name=row["owner_name"],
            status=status,
            valid_from=vf,
            valid_to=vt,
        )
    return CheckResponse(
        site_code=site_code,
        plate=p,
        verdict="OK",
        message="정상 등록",
        unit=row["unit"],
        owner_name=row["owner_name"],
        status=status,
        valid_from=vf,
        valid_to=vt,
    )

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
        return integration_required_page(status_code=200)
    login_action = app_url("/login")
    return f"<h2>Login</h2><form method='POST' action='{login_action}'><input name='username'/><input name='password' type='password'/><button>Login</button></form>"

@app.post("/login")
def login_submit(username: str = Form(...), password: str = Form(...)):
    if not LOCAL_LOGIN_ENABLED:
        return integration_required_page(status_code=403)
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

    raw_site_code = str(payload.get("site_code") or "").strip()
    if not raw_site_code:
        raise HTTPException(status_code=400, detail="Context token missing site_code")
    site_code = normalize_site_code(raw_site_code)

    permission_level = str(payload.get("permission_level") or "").strip().lower()
    if not permission_level:
        raise HTTPException(status_code=400, detail="Context token missing permission_level")
    role = map_permission_to_role(permission_level)
    session_user = str(payload.get("login_id") or payload.get("user_login") or "ka-part-user").strip() or "ka-part-user"
    extras = {
        "display_name": str(payload.get("user_name") or "").strip(),
        "site_name": str(payload.get("site_name") or "").strip(),
    }
    token = make_session(session_user, role, site_code=site_code, extras=extras)
    resp = RedirectResponse(url=app_url("/admin2"), status_code=302)
    resp.set_cookie("parking_session", token, httponly=True, samesite="lax", path=ROOT_PATH or "/")
    return resp

@app.post("/logout")
def logout():
    target = app_url("/login")
    if (not LOCAL_LOGIN_ENABLED) and PORTAL_URL:
        target = PORTAL_URL
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
    return check_plate_record(site_code, plate)


@app.get("/api/session/plates/check", response_model=CheckResponse)
def check_plate_session(
    plate: str,
    request: Request,
):
    sess = read_session(request)
    if not sess:
        raise HTTPException(status_code=401, detail="Login required")
    site_code = normalize_site_code(str(sess.get("sc") or DEFAULT_SITE_CODE))
    return check_plate_record(site_code, plate)

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


class SessionViolationIn(BaseModel):
    plate: str
    verdict: Literal["OK", "UNREGISTERED", "BLOCKED", "EXPIRED", "TEMP"] = "UNREGISTERED"
    location: Optional[str] = None
    memo: Optional[str] = None


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


@app.post("/api/session/violations", response_model=ViolationOut)
def create_violation_session(request: Request, payload: SessionViolationIn):
    sess = read_session(request)
    if not sess:
        raise HTTPException(status_code=401, detail="Login required")

    site_code = normalize_site_code(str(sess.get("sc") or DEFAULT_SITE_CODE))
    plate = normalize_plate(payload.plate)
    if len(plate) < 4:
        raise HTTPException(status_code=400, detail="plate is too short")

    verdict = str(payload.verdict or "UNREGISTERED").strip().upper()
    if verdict not in _VERDICT_OPTIONS:
        raise HTTPException(status_code=400, detail="invalid verdict")

    inspector = str(sess.get("display_name") or sess.get("u") or "ka-part-user").strip() or "ka-part-user"
    with connect() as con:
        cur = con.execute(
            "INSERT INTO violations (site_code, plate, verdict, location, memo, inspector) VALUES (?,?,?,?,?,?)",
            (site_code, plate, verdict, payload.location, payload.memo, inspector),
        )
        vid = cur.lastrowid
        row = con.execute("SELECT * FROM violations WHERE id = ?", (vid,)).fetchone()
    return ViolationOut(**dict(row))


@app.get("/api/session/violations/recent")
def list_violations_session(request: Request, limit: int = 20):
    sess = read_session(request)
    if not sess:
        raise HTTPException(status_code=401, detail="Login required")

    site_code = normalize_site_code(str(sess.get("sc") or DEFAULT_SITE_CODE))
    use_limit = max(1, min(int(limit), 100))
    with connect() as con:
        rows = con.execute(
            """
            SELECT id, site_code, plate, verdict, rule_code, location, memo, inspector,
                   photo_path, lat, lng, created_at
            FROM violations
            WHERE site_code=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (site_code, use_limit),
        ).fetchall()
    return {"ok": True, "items": [dict(r) for r in rows]}

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

    site_code = normalize_site_code(s.get("sc") or DEFAULT_SITE_CODE)
    site_name = str(s.get("site_name") or "").strip()
    display_name = str(s.get("display_name") or s.get("u") or "").strip() or "user"
    try:
        with connect() as con:
            vs = con.execute(
                "SELECT * FROM vehicles WHERE site_code=? ORDER BY updated_at DESC LIMIT 200",
                (site_code,),
            ).fetchall()
            logs = con.execute(
                "SELECT * FROM violations WHERE site_code=? ORDER BY created_at DESC LIMIT 100",
                (site_code,),
            ).fetchall()
    except Exception as exc:
        logger.exception("parking admin2 query failed for site_code=%s: %s", site_code, exc)
        return integration_required_page(status_code=503)
    v_rows = "".join([f"<tr><td>{esc(r['plate'])}</td><td>{esc(r['status'])}</td><td>{esc(r['unit'])}</td><td>{esc(r['owner_name'])}</td></tr>" for r in vs])
    l_rows = "".join([f"<tr><td>{esc(r['created_at'])}</td><td>{esc(r['plate'])}</td><td>{esc(r['verdict'])}</td><td>{esc(r['photo_path'] or '-')}</td></tr>" for r in logs])
    logout_path = app_url("/logout")
    top_link = (
        f"""<a href="{logout_path}" onclick="fetch('{logout_path}',{{method:'POST'}});return false;">Logout</a>"""
        if LOCAL_LOGIN_ENABLED
        else """<a href="/pwa/">시설관리로 돌아가기</a>"""
    )
    scanner_path = app_url("/scanner")
    top_nav = f"""{top_link} | <a href="{scanner_path}">번호판 스캐너</a>"""
    site_line = f"{site_code}" if not site_name else f"{site_code} / {site_name}"
    return f"""<h2>Admin ({esc(site_line)})</h2><p>사용자: {esc(display_name)}</p>{top_nav}
    <h3>Vehicles</h3><table border=1><tr><th>Plate</th><th>Status</th><th>Unit</th><th>Owner</th></tr>{v_rows}</table>
    <h3>Violations</h3><table border=1><tr><th>At</th><th>Plate</th><th>Verdict</th><th>Photo</th></tr>{l_rows}</table>"""


SCANNER_HTML_TEMPLATE = r"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1" />
  <meta name="theme-color" content="#0a2633" />
  <title>주차 스캐너</title>
  <link rel="manifest" href="./manifest.webmanifest" />
  <style>
    :root { --bg:#081923; --panel:#102533; --line:#244255; --text:#e7f1f6; --muted:#8cabbb; --ok:#16a34a; --bad:#dc2626; --warn:#d97706; --btn:#1d4ed8; }
    * { box-sizing: border-box; }
    body { margin:0; font-family: "Noto Sans KR", system-ui, sans-serif; background: radial-gradient(circle at top, #123347, #081923 58%); color:var(--text); }
    .app { max-width: 920px; margin: 0 auto; padding: 16px; display: grid; gap: 12px; }
    .panel { background: rgba(16,37,51,.92); border: 1px solid var(--line); border-radius: 14px; padding: 14px; }
    .head { display:flex; align-items:center; justify-content:space-between; gap:10px; flex-wrap: wrap; }
    .title { margin: 0; font-size: 22px; }
    .ctx { color: var(--muted); font-size: 13px; }
    .row { display:flex; gap:8px; margin-top: 8px; flex-wrap: wrap; }
    .btn { border:1px solid var(--line); background: var(--btn); color:white; border-radius:10px; padding:10px 12px; font-weight:700; cursor:pointer; text-decoration:none; display:inline-block; }
    .btn.ghost { background: transparent; color: var(--text); }
    .btn.warn { background: var(--warn); border-color: #b45309; }
    input { width: 100%; background:#0b1f2b; color:var(--text); border:1px solid var(--line); border-radius:10px; padding:10px 12px; }
    video { width:100%; border-radius:12px; background:#031017; border:1px solid var(--line); }
    .result { border-radius:12px; padding: 10px 12px; border:1px solid var(--line); background:#0a1f2b; }
    .result.ok { border-color: #166534; background: #052112; }
    .result.bad { border-color: #7f1d1d; background: #2d0b0b; }
    .result.warn { border-color: #92400e; background: #2c1508; }
    .hint { font-size: 12px; color: var(--muted); }
    .list { display:grid; gap:8px; margin-top:8px; }
    .item { border:1px solid var(--line); border-radius:10px; padding:8px 10px; background:#0a1f2b; }
    .item .meta { color:var(--muted); font-size:12px; margin-top:4px; }
  </style>
</head>
<body>
  <main class="app">
    <section class="panel">
      <div class="head">
        <h1 class="title">주차 단속 스캐너</h1>
        <a href="./admin2" class="btn ghost">관리화면</a>
      </div>
      <p class="ctx" id="ctxLine">로딩 중...</p>
      <p class="hint">번호판을 카메라로 촬영하거나 수동 입력 후 조회하세요.</p>
      <video id="video" playsinline autoplay muted></video>
      <canvas id="canvas" hidden></canvas>
      <div class="row">
        <button id="btnCam" class="btn ghost">카메라 켜기</button>
        <button id="btnShot" class="btn">캡처 + OCR</button>
      </div>
      <div class="row">
        <input id="plateInput" placeholder="예: 123가4567" />
      </div>
      <div class="row">
        <button id="btnCheck" class="btn">불법주차 조회</button>
      </div>
      <p class="hint" id="hintLine">OCR 대기 중</p>
      <article id="result" class="result">
        <strong>조회 결과</strong>
        <div id="resultText">조회 전</div>
      </article>
    </section>

    <section class="panel">
      <h2 style="margin:0 0 8px 0;">위반 로그 저장</h2>
      <div class="row">
        <input id="locationInput" placeholder="위치(선택)" />
      </div>
      <div class="row">
        <input id="memoInput" placeholder="메모(선택)" />
      </div>
      <div class="row">
        <button id="btnSaveViolation" class="btn warn">현재 판정으로 위반기록 저장</button>
      </div>
      <div class="list" id="recentList"></div>
    </section>
  </main>

  <script>window.__BOOT__ = __BOOTSTRAP_JSON__;</script>
  <script src="https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js" defer></script>
  <script>
  (function(){
    const $ = (id) => document.getElementById(id);
    const state = { stream: null, last: null };
    const ILLEGAL = new Set(["UNREGISTERED", "BLOCKED", "EXPIRED"]);
    const boot = window.__BOOT__ || {};

    const video = $("video");
    const canvas = $("canvas");
    const plateInput = $("plateInput");
    const hintLine = $("hintLine");
    const result = $("result");
    const resultText = $("resultText");
    const recentList = $("recentList");

    function setCtx() {
      const site = [boot.site_code || "-", boot.site_name || ""].filter(Boolean).join(" / ");
      const user = boot.display_name || boot.user_login || "-";
      $("ctxLine").textContent = `단지: ${site} · 사용자: ${user}`;
    }

    function setHint(v) { hintLine.textContent = v; }

    function norm(raw) {
      return String(raw || "").replace(/[^0-9A-Za-z가-힣]/g, "").toUpperCase().trim();
    }

    function findPlate(raw) {
      const txt = norm(raw);
      const m = txt.match(/\d{2,3}[가-힣A-Z]\d{4}/g);
      if (m && m.length) return m[0];
      return txt;
    }

    function setResult(kind, text) {
      result.className = "result" + (kind ? ` ${kind}` : "");
      resultText.textContent = text;
    }

    async function startCamera() {
      if (state.stream) return;
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
        setHint("이 브라우저는 카메라 API를 지원하지 않습니다.");
        return;
      }
      state.stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: "environment" }, width: { ideal: 1280 }, height: { ideal: 720 } },
        audio: false,
      });
      video.srcObject = state.stream;
      setHint("카메라 활성화");
    }

    async function runOcr() {
      if (!window.Tesseract || !window.Tesseract.recognize) {
        throw new Error("OCR 엔진 로딩 중입니다.");
      }
      const w = video.videoWidth || 1280;
      const h = video.videoHeight || 720;
      canvas.width = w;
      canvas.height = h;
      const ctx = canvas.getContext("2d");
      ctx.drawImage(video, 0, 0, w, h);
      setHint("OCR 처리 중...");
      const ocr = await window.Tesseract.recognize(canvas, "kor+eng");
      const plate = findPlate((ocr && ocr.data && ocr.data.text) || "");
      if (!plate) throw new Error("번호판 인식 실패");
      setHint(`OCR 추출: ${plate}`);
      return plate;
    }

    async function checkPlate(raw, source) {
      const plate = norm(raw);
      if (plate.length < 4) {
        setResult("warn", "번호판 입력값이 너무 짧습니다.");
        return;
      }
      const u = new URL("./api/session/plates/check", location.href);
      u.searchParams.set("plate", plate);
      const res = await fetch(u.toString(), { credentials: "include" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
      state.last = data;
      plateInput.value = data.plate || plate;
      const suffix = source ? ` [${source}]` : "";
      if (ILLEGAL.has(data.verdict)) {
        setResult("bad", `불법주차 의심 차량 (${data.verdict})${suffix} - ${data.message}`);
      } else if (data.verdict === "OK" || data.verdict === "TEMP") {
        setResult("ok", `정상/임시 등록 차량 (${data.verdict})${suffix} - ${data.message}`);
      } else {
        setResult("warn", `판정: ${data.verdict}${suffix}`);
      }
      await loadRecent();
    }

    async function saveViolation() {
      if (!state.last) {
        setResult("warn", "먼저 조회를 실행하세요.");
        return;
      }
      if (!ILLEGAL.has(state.last.verdict)) {
        setResult("warn", "현재 판정은 위반 저장 대상이 아닙니다.");
        return;
      }
      const payload = {
        plate: state.last.plate,
        verdict: state.last.verdict,
        location: ($("locationInput").value || "").trim() || null,
        memo: ($("memoInput").value || "").trim() || null,
      };
      const res = await fetch("./api/session/violations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        credentials: "include",
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
      setResult("bad", `위반기록 저장 완료: ${data.plate} (${data.verdict})`);
      await loadRecent();
    }

    function renderRecent(items) {
      if (!items || !items.length) {
        recentList.innerHTML = '<div class="item">최근 위반기록 없음</div>';
        return;
      }
      recentList.innerHTML = items.map((it) => (
        `<div class="item"><strong>${it.plate || "-"}</strong> · ${it.verdict || "-"}`
        + `<div class="meta">${it.created_at || "-"} · ${it.inspector || "-"}</div></div>`
      )).join("");
    }

    async function loadRecent() {
      const u = new URL("./api/session/violations/recent", location.href);
      u.searchParams.set("limit", "20");
      const res = await fetch(u.toString(), { credentials: "include" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
      renderRecent(data.items || []);
    }

    $("btnCam").addEventListener("click", async () => {
      try { await startCamera(); } catch (e) { setHint(String(e.message || e)); }
    });

    $("btnShot").addEventListener("click", async () => {
      try {
        await startCamera();
        const plate = await runOcr();
        plateInput.value = plate;
        await checkPlate(plate, "OCR");
      } catch (e) {
        setHint(`OCR 오류: ${String(e.message || e)}`);
      }
    });

    $("btnCheck").addEventListener("click", async () => {
      try {
        await checkPlate(plateInput.value, "MANUAL");
      } catch (e) {
        setResult("warn", `조회 실패: ${String(e.message || e)}`);
      }
    });

    $("btnSaveViolation").addEventListener("click", async () => {
      try {
        await saveViolation();
      } catch (e) {
        setResult("warn", `저장 실패: ${String(e.message || e)}`);
      }
    });

    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("./sw.js").catch(() => {});
    }
    setCtx();
    loadRecent().catch(() => {});
  })();
  </script>
</body>
</html>
"""


@app.get("/scanner", response_class=HTMLResponse)
def scanner_page(request: Request):
    sess = read_session(request)
    if not sess:
        if LOCAL_LOGIN_ENABLED:
            raise HTTPException(status_code=401, detail="Login required")
        return HTMLResponse(_auto_entry_page(app_url("/scanner")), status_code=200)

    if sess.get("r") not in {"admin", "guard", "viewer"}:
        raise HTTPException(status_code=403, detail="Forbidden")

    context = {
        "site_code": normalize_site_code(str(sess.get("sc") or DEFAULT_SITE_CODE)),
        "site_name": str(sess.get("site_name") or "").strip(),
        "display_name": str(sess.get("display_name") or "").strip(),
        "user_login": str(sess.get("u") or "").strip(),
    }
    html = SCANNER_HTML_TEMPLATE.replace("__BOOTSTRAP_JSON__", json.dumps(context, ensure_ascii=False))
    return HTMLResponse(html, status_code=200)


@app.get("/manifest.webmanifest")
def scanner_manifest():
    return JSONResponse(
        {
            "name": "주차 단속 스캐너",
            "short_name": "주차스캐너",
            "start_url": app_url("/scanner"),
            "scope": app_url("/"),
            "display": "standalone",
            "background_color": "#081923",
            "theme_color": "#0a2633",
            "description": "카메라로 번호판을 인식하고 불법주차 여부를 확인하는 앱",
        }
    )


@app.get("/sw.js")
def scanner_sw():
    sw = """
self.addEventListener('install', (event) => {
  event.waitUntil(self.skipWaiting());
});
self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});
"""
    return Response(content=sw, media_type="application/javascript")
