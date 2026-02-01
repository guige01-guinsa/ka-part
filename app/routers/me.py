from fastapi import APIRouter, Request
from app.auth import get_current_user

router = APIRouter(prefix="/api", tags=["me"])

@router.get("/me")
async def me(request: Request):
    user = get_current_user(request)

    # 호환: 기존 코드가 roles를 기대하더라도 무너지지 않게 "roles"를 만들어준다.
    roles = user.get("roles")
    if roles is None:
        r = user.get("role") or "user"
        roles = [r]  # roles는 리스트로 통일

    return {
        "ok": True,
        "is_admin": bool(user.get("is_admin")),
        "roles": roles,
        "user": {
            "id": user.get("id"),
            "login": user.get("login"),
            "name": user.get("name"),
            "role": user.get("role", "user"),
            "roles": roles,
            "is_admin": bool(user.get("is_admin")),
        },
    }
