from typing import Annotated

import httpx
from fastapi import Depends, HTTPException, Request, status
from jose import JWTError, jwt

from app.core.settings import settings


ALLOWED_BUCHHALTUNG_ROLES = {"admin", "buchhaltung"}


class AuthenticatedUser:
    """Caller identity, resolved via shared JWT + FZR /auth/me lookup."""

    __slots__ = ("id", "email", "role", "full_name")

    def __init__(self, user_id: int, email: str, role: str, full_name: str | None = None):
        self.id = user_id
        self.email = email
        self.role = role
        self.full_name = full_name

    def to_dict(self) -> dict:
        return {"id": self.id, "email": self.email, "role": self.role, "full_name": self.full_name}


def _extract_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization")
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1].strip()
    return request.cookies.get(settings.access_cookie_name)


def _assert_valid_jwt(token: str) -> None:
    """Verify the token's signature + expiration early (fail fast before network call)."""
    try:
        jwt.decode(token, settings.secret_key, algorithms=[settings.jwt_algorithm])
    except JWTError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token") from exc


def _fetch_me(token: str) -> dict:
    url = f"{settings.fzr_api_base_url.rstrip('/')}/auth/me"
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=5.0,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"FZR backend unreachable: {exc}",
        ) from exc

    if response.status_code == 401:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired")
    if response.status_code >= 400:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"FZR /auth/me returned {response.status_code}",
        )

    return response.json()


def get_current_user(request: Request) -> AuthenticatedUser:
    token = _extract_token(request)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    _assert_valid_jwt(token)
    me = _fetch_me(token)

    return AuthenticatedUser(
        user_id=int(me["id"]),
        email=me.get("email", ""),
        role=me.get("role", ""),
        full_name=me.get("full_name"),
    )


def require_buchhaltung_user(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> AuthenticatedUser:
    if user.role not in ALLOWED_BUCHHALTUNG_ROLES:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
    return user
