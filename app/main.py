import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .build_info import build_info_html
from .db import bootstrap_from_env, init_db
from .engine_db import init_engine_db
from .facility_db import init_facility_db
from .info_db import init_info_db
from .ops_db import init_ops_db
from .routes.core import router as core_router
from .routes.engine import router as engine_router
from .routes.facility import router as facility_router
from .routes.info import router as info_router
from .routes.ops import router as ops_router
from .routes.voice import router as voice_router
from .voice_db import init_voice_db
from .work_report_batch import init_work_report_batch

logger = logging.getLogger("ka-part")
PWA_NO_CACHE_PATHS = {
    "/pwa",
    "/pwa/",
    "/pwa/index.html",
    "/pwa/login.html",
    "/pwa/public.html",
    "/pwa/manifest.webmanifest",
    "/pwa/sw.js",
    "/diag/build",
    "/api/build_info",
}
PWA_VERSIONED_ASSET_SUFFIXES = (".css", ".js", ".png", ".svg", ".woff", ".woff2", ".ttf")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    init_engine_db()
    init_facility_db()
    init_info_db()
    init_ops_db()
    init_voice_db()
    init_work_report_batch()
    seeded = bootstrap_from_env()
    if seeded.get("admin") or seeded.get("tenant") or seeded.get("users"):
        logger.info(
            "Bootstrap seed applied: admin=%s tenant=%s users=%s",
            bool(seeded.get("admin")),
            (seeded.get("tenant") or {}).get("id"),
            len(seeded.get("users") or []),
        )
    logger.info("Complaint engine startup complete")
    yield


app = FastAPI(title="KA-PART AI 민원처리 엔진", version="4.0.0", lifespan=lifespan)


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _is_https(request: Request) -> bool:
    proto = str(request.headers.get("x-forwarded-proto") or request.url.scheme or "").strip().lower()
    return proto == "https"


def _apply_pwa_cache_headers(request: Request, response) -> None:
    path = str(request.url.path or "").strip() or "/"
    if path in PWA_NO_CACHE_PATHS:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return

    if path.startswith("/pwa/") and path.endswith(PWA_VERSIONED_ASSET_SUFFIXES):
        if str(request.url.query or "").strip():
            response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")
        else:
            response.headers.setdefault("Cache-Control", "public, max-age=300")


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    if _env_truthy("KA_HSTS_ENABLED", True) and _is_https(request):
        resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    _apply_pwa_cache_headers(request, resp)
    return resp


app.include_router(core_router, prefix="/api")
app.include_router(engine_router, prefix="/api")
app.include_router(facility_router, prefix="/api")
app.include_router(info_router, prefix="/api")
app.include_router(ops_router, prefix="/api")
app.include_router(voice_router, prefix="/api")

app.mount("/fonts", StaticFiles(directory="fonts"), name="fonts")
app.mount("/pwa", StaticFiles(directory="static/pwa", html=True), name="pwa")


@app.get("/")
def root():
    return RedirectResponse(url="/pwa/public.html")


@app.get("/diag/build")
def diag_build() -> HTMLResponse:
    return HTMLResponse(build_info_html())


@app.api_route("/health", methods=["GET", "HEAD"])
def root_health():
    return {"ok": True, "service": "ka-part-complaint-engine"}
