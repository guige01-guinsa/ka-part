import logging
import os
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
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


def _mount_parking_if_enabled() -> None:
    enabled = os.getenv("ENABLE_PARKING_EMBED", "1").strip().lower() not in ("0", "false", "no", "off")
    if not enabled:
        logger.info("Parking embed disabled by ENABLE_PARKING_EMBED")
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
