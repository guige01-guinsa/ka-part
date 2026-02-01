import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse

from app.routers import (
    me, works, ui, events, reports, search, attachments, procurement, complaints, admin,
)

def env_flag(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "on")

# 프로젝트 루트(ka-part) 기준 경로 고정
BASE_DIR = Path(__file__).resolve().parents[1]          # .../ka-part
STATIC_DIR = BASE_DIR / "static"
FAVICON_PATH = STATIC_DIR / "favicon.ico"              # 있으면 제공

app = FastAPI(title="ka-part", version="0.1.0")

# static (절대경로로 고정해서 작업폴더가 달라도 안전)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --- 소음 제거/운영 편의 ---
@app.get("/", include_in_schema=False)
def root():
    # 기본 진입은 UI works로
    return RedirectResponse(url="/ui/works?login=admin")

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    # 없으면 204로 조용히 끝냄
    if FAVICON_PATH.exists():
        return FileResponse(str(FAVICON_PATH))
    return ("", 204)

# --- core routers (항상 ON) ---
app.include_router(me.router)
app.include_router(works.router)
app.include_router(events.router)
app.include_router(reports.router)
app.include_router(search.router)
app.include_router(attachments.router)
app.include_router(procurement.router)
app.include_router(ui.router)
app.include_router(complaints.router)
app.include_router(admin.router)

# --- optional routers (기본 OFF) ---
# 점검/검침은 준비될 때까지 기본적으로 import조차 하지 않음(사고 예방)
if env_flag("FEATURE_INSPECTIONS", "0"):
    from app.routers import inspections
    app.include_router(inspections.router)

if env_flag("FEATURE_METERS", "0"):
    from app.routers import meters
    app.include_router(meters.router)
