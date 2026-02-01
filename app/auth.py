from typing import Callable, Optional, Tuple

from fastapi import HTTPException, Request

from app.db import db_conn


def _table_has_column(db, table: str, col: str) -> bool:
    rows = db.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r["name"] == col for r in rows)


def _load_roles(db, user_id: int, fallback_role: Optional[str]) -> Tuple[list[str], list[str], list[str]]:
    """
    Returns: (roles, role_codes, role_names)
    - roles: combined set for easy membership checks
    - role_codes: only role codes (e.g., CHIEF, STAFF)
    - role_names: only role names (e.g., 관리소장)
    """
    role_codes: set[str] = set()
    role_names: set[str] = set()

    if fallback_role:
        role_codes.add(fallback_role.strip())

    if _table_has_column(db, "user_roles", "role_id") and _table_has_column(db, "roles", "code"):
        rows = db.execute(
            """
            SELECT r.code AS code, r.name AS name
            FROM user_roles ur
            JOIN roles r ON r.id = ur.role_id
            WHERE ur.user_id = ?
            """,
            (user_id,),
        ).fetchall()
        for r in rows:
            if r["code"]:
                role_codes.add(str(r["code"]).strip())
            if r["name"]:
                role_names.add(str(r["name"]).strip())

    roles = sorted(role_codes | role_names)
    return roles, sorted(role_codes), sorted(role_names)


def get_current_user(request: Request) -> dict:
    """
    헤더 'X-User-Login' 또는 query '?login=' 값을 사용해 로그인으로 사용.
    role 컬럼이 없어도 기본 role='admin'으로 계속 진행 가능하도록 방어.
    """
    login = (request.headers.get("X-User-Login") or request.query_params.get("login") or "").strip()
    if not login:
        raise HTTPException(status_code=401, detail="missing X-User-Login")

    with db_conn() as db:
        t = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users';"
        ).fetchone()
        if not t:
            raise HTTPException(status_code=500, detail="users table not found (run schema.sql)")

        has_role = _table_has_column(db, "users", "role")
        has_vendor = _table_has_column(db, "users", "vendor_id")

        if has_role and has_vendor:
            row = db.execute(
                "SELECT id, login, role, name, vendor_id FROM users WHERE login = ?",
                (login,),
            ).fetchone()
        elif has_role:
            row = db.execute(
                "SELECT id, login, role, name FROM users WHERE login = ?",
                (login,),
            ).fetchone()
        else:
            row = db.execute(
                "SELECT id, login, name FROM users WHERE login = ?",
                (login,),
            ).fetchone()

        if not row:
            raise HTTPException(status_code=401, detail=f"unknown user: {login}")

        fallback_role = row["role"] if ("role" in row.keys()) else "admin"
        roles, role_codes, role_names = _load_roles(db, row["id"], fallback_role)

    user = {
        "id": row["id"],
        "login": row["login"],
        "name": row["name"] if "name" in row.keys() else row["login"],
        "role": fallback_role,
        "roles": roles,
        "role_codes": role_codes,
        "role_names": role_names,
        "vendor_id": row["vendor_id"] if ("vendor_id" in row.keys()) else None,
    }

    admin_roles = {"CHIEF", "MANAGER", "ADMIN", "admin", "관리소장"}
    user["is_admin"] = (login == "admin") or any(r in admin_roles for r in roles)
    return user


def require_role(*allowed_roles: str) -> Callable:
    """
    라우터에서
      user = await require_role("admin","chief")(request)
    또는 Depends로도 쉽게 구성 가능
    """
    allowed = set(r.strip() for r in allowed_roles if r and r.strip())

    async def _dep(request: Request) -> dict:
        user = get_current_user(request)
        if allowed and not (set(user.get("roles") or []) & allowed):
            raise HTTPException(status_code=403, detail="forbidden")
        return user

    return _dep
