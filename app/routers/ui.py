import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")

router = APIRouter(prefix="/ui", tags=["ui"])


def env_flag(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default).strip().lower()
    return v in ("1", "true", "yes", "on")


def feature_flags() -> dict:
    return {
        "inspections": env_flag("FEATURE_INSPECTIONS", "0"),
        "meters": env_flag("FEATURE_METERS", "0"),
    }


def login_from_query(request: Request) -> str:
    # UI querystring login -> JS가 X-User-Login 헤더로 호출
    return (request.query_params.get("login") or "admin").strip() or "admin"


@router.get("/works", response_class=HTMLResponse)
async def ui_works(request: Request):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/works.html",
        {
            "request": request,
            "title": "작업",
            "active": "works",
            "login": login,
            "features": feature_flags(),
        },
    )


@router.get("/works/{work_id}", response_class=HTMLResponse)
async def ui_work_detail(request: Request, work_id: int):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/work_detail.html",
        {
            "request": request,
            "title": "작업 상세",
            "active": "works",
            "login": login,
            "work_id": work_id,
            "features": feature_flags(),
        },
    )


@router.get("/monthly_work", response_class=HTMLResponse)
async def ui_monthly(request: Request):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/monthly_work.html",
        {
            "request": request,
            "title": "월간보고",
            "active": "monthly",
            "login": login,
            "features": feature_flags(),
        },
    )


@router.get("/inspections", response_class=HTMLResponse)
async def ui_inspections(request: Request):
    # FEATURE_INSPECTIONS=0이면 메뉴는 숨김, 직접 접근 시 안내 화면
    login = login_from_query(request)
    feats = feature_flags()
    if not feats["inspections"]:
        return templates.TemplateResponse(
            "ui/feature_off.html",
            {
                "request": request,
                "title": "점검 (OFF)",
                "active": "inspections",
                "login": login,
                "features": feats,
                "feature_name": "FEATURE_INSPECTIONS",
            },
        )

    return templates.TemplateResponse(
        "ui/inspections.html",
        {
            "request": request,
            "title": "점검",
            "active": "inspections",
            "login": login,
            "features": feats,
        },
    )


@router.get("/meters", response_class=HTMLResponse)
async def ui_meters(request: Request):
    login = login_from_query(request)
    feats = feature_flags()
    if not feats["meters"]:
        return templates.TemplateResponse(
            "ui/feature_off.html",
            {
                "request": request,
                "title": "검침 (OFF)",
                "active": "meters",
                "login": login,
                "features": feats,
                "feature_name": "FEATURE_METERS",
            },
        )

    return templates.TemplateResponse(
        "ui/meters.html",
        {
            "request": request,
            "title": "검침",
            "active": "meters",
            "login": login,
            "features": feats,
        },
    )


@router.get("/admin/users", response_class=HTMLResponse)
async def ui_admin_users(request: Request):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/admin_users.html",
        {
            "request": request,
            "title": "관리자",
            "active": "admin",
            "login": login,
            "features": feature_flags(),
        },
    )


@router.get("/admin/notifications", response_class=HTMLResponse)
async def ui_admin_notifications(request: Request):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/admin_notifications.html",
        {
            "request": request,
            "title": "알림",
            "active": "admin",
            "login": login,
            "features": feature_flags(),
        },
    )


@router.get("/admin/templates", response_class=HTMLResponse)
async def ui_admin_templates(request: Request):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/admin_templates.html",
        {
            "request": request,
            "title": "템플릿",
            "active": "admin",
            "login": login,
            "features": feature_flags(),
        },
    )


@router.get("/admin/masters", response_class=HTMLResponse)
async def ui_admin_masters(request: Request):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/admin_masters.html",
        {
            "request": request,
            "title": "분류/위치",
            "active": "admin",
            "login": login,
            "features": feature_flags(),
        },
    )


@router.get("/vendor", response_class=HTMLResponse)
async def ui_vendor(request: Request):
    login = login_from_query(request)
    return templates.TemplateResponse(
        "ui/vendor_portal.html",
        {
            "request": request,
            "title": "외주 포털",
            "active": "vendor",
            "login": login,
            "features": feature_flags(),
        },
    )
