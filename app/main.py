import logging
import os
import sys
import json
import urllib.parse
import urllib.error
import urllib.request
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .db import init_db
from .complaints_db import init_complaints_db
from .backup_manager import (
    get_maintenance_status,
    start_backup_scheduler,
    stop_backup_scheduler,
)
from .routes.api import router as api_router
from .routes.complaints import router as complaints_router

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

app = FastAPI(title="ka-part", version="2.9.0")
logger = logging.getLogger("ka-part")

@app.on_event("startup")
def _startup():
    init_db()
    init_complaints_db()
    start_backup_scheduler()


@app.on_event("shutdown")
def _shutdown():
    stop_backup_scheduler()

app.include_router(api_router, prefix="/api")
app.include_router(complaints_router, prefix="/api/v1")


@app.middleware("http")
async def _maintenance_guard(request: Request, call_next):
    path = request.url.path or ""
    if path.startswith("/api"):
        status = get_maintenance_status()
        if bool(status.get("active")):
            allow = (
                path.startswith("/api/backup")
                or path in {"/api/health", "/api/auth/me", "/api/auth/logout"}
            )
            if not allow:
                return JSONResponse(
                    {
                        "ok": False,
                        "detail": str(status.get("message") or "서버 점검 중입니다. 잠시 후 다시 시도해 주세요."),
                        "maintenance": status,
                    },
                    status_code=503,
                )
    return await call_next(request)

# PWA static
app.mount("/pwa", StaticFiles(directory="static/pwa", html=True), name="pwa")

PARKING_PORTAL_URL = (os.getenv("PARKING_PORTAL_URL") or "").strip()
PARKING_PORTAL_LOGIN_URL = (os.getenv("PARKING_PORTAL_LOGIN_URL") or "").strip()


def _load_env_file_if_exists(path: Path) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            os.environ.setdefault(key, value)


def _portal_login_url(next_path: str = "/parking/admin2") -> str:
    nxt_enc = urllib.parse.quote(next_path, safe="")
    portal_url = (os.getenv("PARKING_PORTAL_URL") or PARKING_PORTAL_URL or "").strip()
    base = (os.getenv("PARKING_PORTAL_LOGIN_URL") or PARKING_PORTAL_LOGIN_URL or "").strip()
    if not base:
        if portal_url:
            base = portal_url if "login.html" in portal_url else f"{portal_url.rstrip('/')}/login.html"
        else:
            base = "https://www.ka-part.com/pwa/login.html"
    if "{next}" in base:
        return base.replace("{next}", nxt_enc)
    if "next=" in base:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}next={nxt_enc}"


def _parking_handoff_page(next_path: str = "/parking/admin2") -> str:
    quoted_next = json.dumps(next_path)
    quoted_login = json.dumps(_portal_login_url(next_path), ensure_ascii=False)
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
      const MANUAL_LOGOUT_KEY = "ka_parking_manual_logout_v1";
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
      const TOKEN_KEY = "ka_part_auth_token_v1";
      const USER_KEY = "ka_part_auth_user_v1";
      const SITE_NAME_KEY = "ka_current_site_name_v1";
      const SITE_CODE_KEY = "ka_current_site_code_v1";
      const readStore = (store, key) => {{
        try {{
          return (store.getItem(key) || "").trim();
        }} catch (_e) {{
          return "";
        }}
      }};
      const clearAuthStore = () => {{
        try {{ sessionStorage.removeItem(TOKEN_KEY); }} catch (_e) {{}}
        try {{ sessionStorage.removeItem(USER_KEY); }} catch (_e) {{}}
        try {{ localStorage.removeItem(TOKEN_KEY); }} catch (_e) {{}}
        try {{ localStorage.removeItem(USER_KEY); }} catch (_e) {{}}
      }};
      const consumeManualLogout = () => {{
        try {{
          const raw = (sessionStorage.getItem(MANUAL_LOGOUT_KEY) || "").trim();
          if (raw !== "1") return false;
          sessionStorage.removeItem(MANUAL_LOGOUT_KEY);
          return true;
        }} catch (_e) {{
          return false;
        }}
      }};
      const readSiteValue = (storageKey, queryKey) => {{
        const fromSession = readStore(sessionStorage, storageKey);
        if (fromSession) return fromSession;
        const fromLocal = readStore(localStorage, storageKey);
        if (fromLocal) {{
          try {{ sessionStorage.setItem(storageKey, fromLocal); }} catch (_e) {{}}
          return fromLocal;
        }}
        try {{
          const u = new URL(window.location.href);
          return (u.searchParams.get(queryKey) || "").trim();
        }} catch (_e) {{
          return "";
        }}
      }};
      let token = readStore(sessionStorage, TOKEN_KEY);
      if (consumeManualLogout()) {{
        clearRetry();
        clearAuthStore();
        window.location.replace(loginUrl);
        return;
      }}
      if (!token) {{
        const legacyToken = readStore(localStorage, TOKEN_KEY);
        if (legacyToken) {{
          token = legacyToken;
          try {{ sessionStorage.setItem(TOKEN_KEY, legacyToken); }} catch (_e) {{}}
          const legacyUser = readStore(localStorage, USER_KEY);
          if (legacyUser) {{
            try {{ sessionStorage.setItem(USER_KEY, legacyUser); }} catch (_e) {{}}
          }}
          try {{ localStorage.removeItem(TOKEN_KEY); }} catch (_e) {{}}
          try {{ localStorage.removeItem(USER_KEY); }} catch (_e) {{}}
        }}
      }}
      try {{
        const headers = {{}};
        if (token) headers.Authorization = "Bearer " + token;
        const siteName = readSiteValue(SITE_NAME_KEY, "site_name");
        const siteCode = readSiteValue(SITE_CODE_KEY, "site_code").toUpperCase();
        const siteQs = new URLSearchParams();
        if (siteName) siteQs.set("site_name", siteName);
        if (siteCode) siteQs.set("site_code", siteCode);
        const endpoint = siteQs.toString() ? "/api/parking/context?" + siteQs.toString() : "/api/parking/context";
        const res = await fetch(endpoint, {{
          method: "GET",
          headers
        }});
        const ct = res.headers.get("content-type") || "";
        const data = ct.includes("application/json") ? await res.json() : {{}};
        if (res.status === 401) {{
          clearAuthStore();
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


def _register_parking_gateway_routes() -> None:
    parking_base_url = (os.getenv("PARKING_BASE_URL") or "").strip()
    parking_sso_path = (os.getenv("PARKING_SSO_PATH") or "/sso").strip()
    if not parking_sso_path.startswith("/"):
        parking_sso_path = f"/{parking_sso_path}"

    @app.get("/parking")
    def parking_root():
        return RedirectResponse(url="/parking/", status_code=302)

    @app.get("/parking/", response_class=HTMLResponse)
    @app.get("/parking/login", response_class=HTMLResponse)
    @app.get("/parking/admin2", response_class=HTMLResponse)
    def parking_entry():
        return HTMLResponse(_parking_handoff_page("/parking/admin2"), status_code=200)

    @app.get("/parking/sso")
    def parking_sso_bridge(ctx: str):
        if not parking_base_url:
            return HTMLResponse(
                "<h2>주차 연동 설정 오류</h2><p>PARKING_BASE_URL 환경변수를 설정하세요.</p>",
                status_code=503,
            )
        target = f"{parking_base_url.rstrip('/')}{parking_sso_path}?ctx={urllib.parse.quote(ctx, safe='')}"
        return RedirectResponse(url=target, status_code=302)

    @app.get("/parking/health")
    def parking_gateway_health():
        if not parking_base_url:
            return JSONResponse(
                {
                    "ok": False,
                    "mode": "gateway",
                    "detail": "PARKING_BASE_URL is required",
                },
                status_code=503,
            )
        upstream = f"{parking_base_url.rstrip('/')}/health"
        req = urllib.request.Request(upstream, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                status_code = int(getattr(resp, "status", 502))
                body = resp.read().decode("utf-8", errors="ignore")
            upstream_ok = (200 <= status_code < 300) and ('"ok"' in body.lower())
            return JSONResponse(
                {
                    "ok": upstream_ok,
                    "mode": "gateway",
                    "parking_base_url": parking_base_url,
                    "upstream_status": status_code,
                },
                status_code=(200 if upstream_ok else 502),
            )
        except urllib.error.URLError as e:
            return JSONResponse(
                {
                    "ok": False,
                    "mode": "gateway",
                    "parking_base_url": parking_base_url,
                    "detail": f"upstream unreachable: {e.reason}",
                },
                status_code=502,
            )


def _mount_parking_if_enabled() -> None:
    parking_base_url = (os.getenv("PARKING_BASE_URL") or "").strip()
    embed_raw = os.getenv("ENABLE_PARKING_EMBED")
    if embed_raw is None:
        enabled = True
    else:
        enabled = embed_raw.strip().lower() not in ("0", "false", "no", "off")
    if not enabled:
        _register_parking_gateway_routes()
        logger.info("Parking embed disabled; gateway mode enabled (PARKING_BASE_URL=%s)", parking_base_url or "unset")
        return

    parking_root = ROOT_DIR / "services" / "parking"
    if not parking_root.exists():
        logger.info("Parking service directory not found: %s", parking_root)
        return

    _load_env_file_if_exists(parking_root / ".env.production")

    # Defaults for single-process deployment (e.g., Render + ka-part.com).
    os.environ.setdefault("PARKING_ROOT_PATH", "/parking")
    os.environ.setdefault("PARKING_DB_PATH", str(parking_root / "app" / "data" / "parking.db"))
    os.environ.setdefault("PARKING_UPLOAD_DIR", str(parking_root / "app" / "uploads"))
    os.environ.setdefault("PARKING_LOCAL_LOGIN_ENABLED", "0")
    context_secret = (
        os.getenv("PARKING_CONTEXT_SECRET")
        or os.getenv("PARKING_SECRET_KEY")
        or os.getenv("KA_PHONE_VERIFY_SECRET")
        or ""
    ).strip()
    if context_secret:
        os.environ.setdefault("PARKING_CONTEXT_SECRET", context_secret)
    db_path = os.getenv("PARKING_DB_PATH", "").strip()
    if db_path and not Path(db_path).is_absolute():
        os.environ["PARKING_DB_PATH"] = str((parking_root / db_path).resolve())
    upload_path = os.getenv("PARKING_UPLOAD_DIR", "").strip()
    if upload_path and not Path(upload_path).is_absolute():
        os.environ["PARKING_UPLOAD_DIR"] = str((parking_root / upload_path).resolve())

    try:
        from services.parking.app.db import init_db as parking_init_db, seed_demo as parking_seed_demo, seed_users as parking_seed_users
        parking_init_db()
        parking_seed_demo()
        parking_seed_users()
        from services.parking.app.main import app as parking_app
        app.mount("/parking", parking_app)
        logger.info("Parking service mounted at /parking")
    except Exception as exc:
        logger.exception("Failed to mount parking service: %s", exc)


_mount_parking_if_enabled()

@app.get("/")
def root():
    return RedirectResponse(url="/pwa/")
