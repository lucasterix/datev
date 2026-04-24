"""Temporary echo endpoint to verify shared-JWT cross-subdomain auth works.
Remove in Phase 1 once a real authenticated route exists."""

from typing import Annotated

from fastapi import APIRouter, Depends

from app.core.auth import AuthenticatedUser, require_buchhaltung_user

router = APIRouter(tags=["debug"])


@router.get("/me-echo")
def me_echo(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
) -> dict:
    return user.to_dict()
