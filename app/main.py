import logging
import os
import sys
import json
import urllib.parse
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from .db import init_db
from .routes.api import router as api_router

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

app = FastAPI(title="ka-part", version="2.9.0")
logger = logging.getLogger("ka-part")

@app.on_event("startup")
def _startup():
    init_db()

app.include_router(api_router, prefix="/api")

# PWA static
app.mount("/pwa", StaticFiles(directory="static/pwa", html=True), name="pwa")


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


def _parking_handoff_page(next_path: str = "/parking/admin2") -> str:
    quoted_next = json.dumps(next_path)
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
        const ct = res.headers.get("content-type") || "";
        const data = ct.includes("application/json") ? await res.json() : {{}};
        if (res.status === 401) {{
          window.location.replace(loginUrl);
          return;
        }}
        if (!res.ok) {{
          const detail = data && data.detail ? String(data.detail) : ("HTTP " + String(res.status));
          throw new Error(detail);
        }}
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
    parking_sso_path = (os.getenv("PARKING_SSO_PATH") or "/parking/sso").strip()
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


def _mount_parking_if_enabled() -> None:
    parking_base_url = (os.getenv("PARKING_BASE_URL") or "").strip()
    embed_raw = os.getenv("ENABLE_PARKING_EMBED")
    if embed_raw is None:
        enabled = (parking_base_url == "")
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
    os.environ.setdefault(
        "PARKING_CONTEXT_SECRET",
        os.getenv("PARKING_SECRET_KEY", os.getenv("KA_PHONE_VERIFY_SECRET", "ka-part-dev-secret")),
    )
    db_path = os.getenv("PARKING_DB_PATH", "").strip()
    if db_path and not Path(db_path).is_absolute():
        os.environ["PARKING_DB_PATH"] = str((parking_root / db_path).resolve())
    upload_path = os.getenv("PARKING_UPLOAD_DIR", "").strip()
    if upload_path and not Path(upload_path).is_absolute():
        os.environ["PARKING_UPLOAD_DIR"] = str((parking_root / upload_path).resolve())

    try:
        from services.parking.app.main import app as parking_app
        app.mount("/parking", parking_app)
        logger.info("Parking service mounted at /parking")
    except Exception as exc:
        logger.exception("Failed to mount parking service: %s", exc)


_mount_parking_if_enabled()

@app.get("/")
def root():
    return RedirectResponse(url="/pwa/")
