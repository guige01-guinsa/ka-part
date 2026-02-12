from dataclasses import dataclass

from fastapi import Depends, Header, HTTPException, status


@dataclass
class CurrentUser:
    user_id: int
    role: str


def get_current_user(
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
    x_role: str | None = Header(default="resident", alias="X-Role"),
) -> CurrentUser:
    if not x_user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing X-User-Id header",
        )
    try:
        uid = int(x_user_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="X-User-Id must be an integer") from exc

    role = (x_role or "resident").strip().lower()
    if role not in {"resident", "staff", "admin"}:
        raise HTTPException(status_code=400, detail="X-Role must be resident/staff/admin")
    return CurrentUser(user_id=uid, role=role)


def get_admin_user(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    if user.role not in {"admin", "staff"}:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin only")
    return user
