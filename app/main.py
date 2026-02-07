from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from .db import init_db
from .routes.api import router as api_router

app = FastAPI(title="ka-part", version="2.7.0")

@app.on_event("startup")
def _startup():
    init_db()

app.include_router(api_router, prefix="/api")

# PWA static
app.mount("/pwa", StaticFiles(directory="static/pwa", html=True), name="pwa")

@app.get("/")
def root():
    return RedirectResponse(url="/pwa/")
