"""DATEV OAuth2 + OIDC helpers.

Implements the Authorization Code Flow with PKCE (RFC 7636) against
the DATEV Sandbox or Production OIDC endpoints, including discovery
caching and refresh-token rotation.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.core.settings import settings
from app.models.datev_oauth_state import DatevOAuthState
from app.models.datev_token import DatevToken

logger = get_logger("datev.oauth")

STATE_TTL_SECONDS = 600  # 10 minutes — DATEV spec doesn't mandate; we tolerate slow humans
PRIMARY_SCOPE_KEY = "primary"
ACCESS_TOKEN_SAFETY_MARGIN_SECONDS = 60  # refresh 1 min before true expiry


@dataclass(slots=True)
class DiscoveryDocument:
    authorization_endpoint: str
    token_endpoint: str
    userinfo_endpoint: str
    revocation_endpoint: str | None
    issuer: str


class _DiscoveryCache:
    """Discovery doc rarely changes; cache in-process to avoid 1 extra
    HTTP call per auth attempt. Auto-refreshes if older than 1 hour."""

    _lock = threading.Lock()
    _doc: DiscoveryDocument | None = None
    _fetched_at: float = 0.0
    _ttl_seconds: int = 3600

    @classmethod
    def get(cls) -> DiscoveryDocument:
        with cls._lock:
            now = time.time()
            if cls._doc and (now - cls._fetched_at) < cls._ttl_seconds:
                return cls._doc

            if not settings.datev_discovery_url:
                raise RuntimeError("DATEV_DISCOVERY_URL not configured")

            response = httpx.get(settings.datev_discovery_url, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            cls._doc = DiscoveryDocument(
                authorization_endpoint=data["authorization_endpoint"],
                token_endpoint=data["token_endpoint"],
                userinfo_endpoint=data["userinfo_endpoint"],
                revocation_endpoint=data.get("revocation_endpoint"),
                issuer=data["issuer"],
            )
            cls._fetched_at = now
            return cls._doc


def _pkce_pair() -> tuple[str, str]:
    """Return (code_verifier, code_challenge) per RFC 7636 using SHA256."""
    code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).decode("ascii").rstrip("=")
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return code_verifier, code_challenge


def build_authorize_url(
    db: Session,
    initiated_by_user_id: int | None,
    initiated_by_email: str | None,
    return_to: str | None,
) -> str:
    if not settings.datev_client_id or not settings.datev_redirect_uri:
        raise RuntimeError("DATEV client_id / redirect_uri not configured")

    code_verifier, code_challenge = _pkce_pair()
    state = secrets.token_urlsafe(32)

    db.add(
        DatevOAuthState(
            state=state,
            code_verifier=code_verifier,
            initiated_by_user_id=initiated_by_user_id,
            initiated_by_email=initiated_by_email,
            return_to=return_to,
        )
    )
    db.commit()

    doc = _DiscoveryCache.get()
    params = {
        "response_type": "code",
        "client_id": settings.datev_client_id,
        "redirect_uri": settings.datev_redirect_uri,
        "scope": " ".join(settings.datev_scopes_list),
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{doc.authorization_endpoint}?{httpx.QueryParams(params)}"


def _prune_expired_states(db: Session) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=STATE_TTL_SECONDS * 2)
    db.query(DatevOAuthState).filter(DatevOAuthState.created_at < cutoff).delete()


def exchange_code(db: Session, code: str, state: str) -> DatevToken:
    pending = db.execute(
        select(DatevOAuthState).where(DatevOAuthState.state == state)
    ).scalar_one_or_none()
    if pending is None:
        raise ValueError("Unknown or already-consumed state")

    age = datetime.now(timezone.utc) - pending.created_at
    if age.total_seconds() > STATE_TTL_SECONDS:
        db.delete(pending)
        db.commit()
        raise ValueError("State expired")

    doc = _DiscoveryCache.get()
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": settings.datev_redirect_uri,
        "client_id": settings.datev_client_id,
        "client_secret": settings.datev_client_secret,
        "code_verifier": pending.code_verifier,
    }

    response = httpx.post(
        doc.token_endpoint,
        data=payload,
        headers={"Accept": "application/json"},
        timeout=15.0,
    )
    if response.status_code >= 400:
        logger.error(
            "datev_token_exchange_failed",
            status=response.status_code,
            body=response.text[:500],
        )
        response.raise_for_status()

    token_data = response.json()
    token_row = _upsert_token(
        db,
        token_data,
        connected_by_user_id=pending.initiated_by_user_id,
        connected_by_email=pending.initiated_by_email,
        is_refresh=False,
    )

    db.delete(pending)
    _prune_expired_states(db)
    db.commit()

    return token_row


def _upsert_token(
    db: Session,
    token_data: dict,
    connected_by_user_id: int | None,
    connected_by_email: str | None,
    is_refresh: bool,
) -> DatevToken:
    access_token = token_data["access_token"]
    refresh_token = token_data.get("refresh_token")
    expires_in = int(token_data.get("expires_in", 900))
    scope = token_data.get("scope", "")
    token_type = token_data.get("token_type", "Bearer")
    id_token = token_data.get("id_token")

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
    id_token_claims = _decode_id_token_unverified(id_token) if id_token else None

    existing = db.execute(
        select(DatevToken).where(DatevToken.scope_key == PRIMARY_SCOPE_KEY)
    ).scalar_one_or_none()

    if existing is None:
        row = DatevToken(
            scope_key=PRIMARY_SCOPE_KEY,
            access_token=access_token,
            refresh_token=refresh_token,
            token_type=token_type,
            scope=scope,
            expires_at=expires_at,
            id_token_claims=id_token_claims,
            connected_by_user_id=connected_by_user_id,
            connected_by_email=connected_by_email,
            last_refreshed_at=datetime.now(timezone.utc) if is_refresh else None,
        )
        db.add(row)
        db.flush()
        return row

    existing.access_token = access_token
    if refresh_token:
        existing.refresh_token = refresh_token
    existing.token_type = token_type
    if scope:
        existing.scope = scope
    existing.expires_at = expires_at
    if id_token_claims:
        existing.id_token_claims = id_token_claims
    if is_refresh:
        existing.last_refreshed_at = datetime.now(timezone.utc)
    else:
        existing.connected_by_user_id = connected_by_user_id
        existing.connected_by_email = connected_by_email
        existing.connected_at = datetime.now(timezone.utc)
        existing.last_refreshed_at = None
    db.flush()
    return existing


def _decode_id_token_unverified(id_token: str) -> dict | None:
    """Decode the ID token payload WITHOUT signature verification.
    We only need it for display (who connected), not for trust decisions —
    the access token is what grants API access. Signature verification
    can be added later by fetching jwks_uri and using python-jose."""
    try:
        _header, payload_b64, _sig = id_token.split(".")
        padding = "=" * (-len(payload_b64) % 4)
        payload_json = base64.urlsafe_b64decode(payload_b64 + padding)
        import json

        return json.loads(payload_json)
    except Exception:
        logger.warning("datev_id_token_decode_failed")
        return None


def get_current_token(db: Session) -> DatevToken | None:
    return db.execute(
        select(DatevToken).where(DatevToken.scope_key == PRIMARY_SCOPE_KEY)
    ).scalar_one_or_none()


def refresh_if_needed(db: Session, token: DatevToken) -> DatevToken:
    """Refresh the access token if it's within the safety margin of expiry.
    No-op if still fresh. Raises if no refresh token is available or
    the refresh call fails (caller decides whether to prompt re-auth)."""
    now = datetime.now(timezone.utc)
    if token.expires_at - now > timedelta(seconds=ACCESS_TOKEN_SAFETY_MARGIN_SECONDS):
        return token
    if not token.refresh_token:
        raise RuntimeError("No refresh_token stored — reconnect required")

    doc = _DiscoveryCache.get()
    response = httpx.post(
        doc.token_endpoint,
        data={
            "grant_type": "refresh_token",
            "refresh_token": token.refresh_token,
            "client_id": settings.datev_client_id,
            "client_secret": settings.datev_client_secret,
        },
        headers={"Accept": "application/json"},
        timeout=15.0,
    )
    if response.status_code >= 400:
        logger.error(
            "datev_token_refresh_failed",
            status=response.status_code,
            body=response.text[:500],
        )
        response.raise_for_status()

    token_data = response.json()
    return _upsert_token(
        db,
        token_data,
        connected_by_user_id=token.connected_by_user_id,
        connected_by_email=token.connected_by_email,
        is_refresh=True,
    )
