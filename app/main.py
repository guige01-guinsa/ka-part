import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from .db import bootstrap_from_env, init_db
from .engine_db import init_engine_db
from .routes.core import router as core_router
from .routes.engine import router as engine_router
from .routes.voice import router as voice_router
from .voice_db import init_voice_db

logger = logging.getLogger("ka-part")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    init_engine_db()
    init_voice_db()
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


@app.middleware("http")
async def _security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    if _env_truthy("KA_HSTS_ENABLED", True) and _is_https(request):
        resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return resp


app.include_router(core_router, prefix="/api")
app.include_router(engine_router, prefix="/api")
app.include_router(voice_router, prefix="/api")

app.mount("/fonts", StaticFiles(directory="fonts"), name="fonts")
app.mount("/pwa", StaticFiles(directory="static/pwa", html=True), name="pwa")


@app.get("/")
def root():
    return RedirectResponse(url="/pwa/public.html")


@app.api_route("/health", methods=["GET", "HEAD"])
def root_health():
    return {"ok": True, "service": "ka-part-complaint-engine"}
