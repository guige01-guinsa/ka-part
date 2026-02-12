from fastapi import FastAPI

from .routers import admin, public


def create_app() -> FastAPI:
    app = FastAPI(
        title="Apartment Complaints API (Scaffold)",
        version="1.0.0",
    )
    app.include_router(public.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
    return app


app = create_app()
