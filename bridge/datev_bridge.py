"""DATEV-Bridge - Application-Layer-Reverse-Proxy für DATEVconnect.

Background: DATEV's localhost API on port 58454 is served by Microsoft
http.sys, which only accepts connections originating from 127.0.0.1.
Tailscale Serve's raw-TCP forwarding gets rejected at the TCP layer
(Connection-Reset), even though the upstream IS localhost - http.sys
sees the foreign source and drops the connection before HTTP auth.

This bridge sits on the LuG-PC, listens on the Tailscale interface,
re-issues each incoming HTTP request as a fresh request *from* 127.0.0.1
to localhost:58454, injects Basic-Auth, and streams the response back.

It runs unattended as a Windows service (configured via the installer).
Logs go to ``C:\\datev-bridge\\bridge.log``.

Failure handling: if DATEVconnect is offline (LuG closed, stick out,
session expired) we surface a clear 503 with a German message instead
of silently breaking the caller.
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp
from aiohttp import web

# --- config (overridable via env, set by the installer) -------------------

LISTEN_HOST = os.getenv("DATEV_BRIDGE_LISTEN", "0.0.0.0")
LISTEN_PORT = int(os.getenv("DATEV_BRIDGE_PORT", "8765"))
DATEV_TARGET = os.getenv("DATEV_BRIDGE_TARGET", "http://localhost:58454")

LOG_FILE = Path(os.getenv("DATEV_BRIDGE_LOG", r"C:\datev-bridge\bridge.log"))
LOG_LEVEL = os.getenv("DATEV_BRIDGE_LOG_LEVEL", "INFO")

# Load bridge.env if present so the Scheduled Task can run python.exe
# directly without a wrapper script. ASCII-only key=value lines.
_env_file = Path(os.getenv("DATEV_BRIDGE_ENV_FILE", r"C:\datev-bridge\bridge.env"))
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8-sig").splitlines():
        _line = _line.strip()
        if not _line or _line.startswith("#") or "=" not in _line:
            continue
        _k, _v = _line.split("=", 1)
        os.environ.setdefault(_k.strip(), _v.strip())

USER = os.environ.get("DATEV_BRIDGE_USER")
PASSWORD = os.environ.get("DATEV_BRIDGE_PASSWORD")
if not USER or not PASSWORD:
    sys.stderr.write(
        "DATEV_BRIDGE_USER / DATEV_BRIDGE_PASSWORD nicht gesetzt - "
        "muss vom Installer in bridge.env eingetragen werden\n"
    )
    sys.exit(2)


# --- logging ---------------------------------------------------------------

LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
log_format = "%(asctime)s [%(levelname)s] %(message)s"
file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
file_handler.setFormatter(logging.Formatter(log_format))
logging.basicConfig(
    level=LOG_LEVEL,
    format=log_format,
    handlers=[file_handler, logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("datev_bridge")


# --- proxy handler ---------------------------------------------------------

# Hop-by-hop headers we must not forward. We also strip Authorization
# and Host so we can inject our own.
_STRIP_INBOUND = {
    "host", "authorization", "connection", "keep-alive",
    "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
}
_STRIP_UPSTREAM = {"transfer-encoding", "connection", "keep-alive"}


async def proxy(request: web.Request) -> web.StreamResponse:
    target_url = f"{DATEV_TARGET}{request.path_qs}"

    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _STRIP_INBOUND
    }
    headers["Host"] = "localhost:58454"

    body = await request.read()
    auth = aiohttp.BasicAuth(USER, PASSWORD)

    log.info("%s %s", request.method, request.path_qs)

    timeout = aiohttp.ClientTimeout(total=60, connect=5)
    try:
        async with aiohttp.ClientSession(timeout=timeout, auto_decompress=False) as session:
            async with session.request(
                request.method, target_url,
                headers=headers, data=body, auth=auth, allow_redirects=False,
            ) as upstream:
                resp_body = await upstream.read()
                response = web.Response(status=upstream.status, body=resp_body)
                for k, v in upstream.headers.items():
                    if k.lower() in _STRIP_UPSTREAM:
                        continue
                    response.headers[k] = v
                log.info("  -> %s (%d bytes)", upstream.status, len(resp_body))
                return response
    except aiohttp.ClientConnectorError as exc:
        log.warning("  -> 503 DATEV unreachable: %s", exc)
        return web.json_response(
            {
                "error": "DATEV LuG nicht erreichbar",
                "hint": "Stick eingesteckt? LuG offen?",
                "detail": str(exc),
            },
            status=503,
        )
    except Exception as exc:  # noqa: BLE001 - keep the bridge running
        log.exception("  -> 500 unexpected error")
        return web.json_response(
            {"error": "Bridge-internal error", "detail": str(exc)},
            status=500,
        )


# --- entry point ----------------------------------------------------------


def main() -> None:
    log.info(
        "datev-bridge starting on %s:%d -> %s (user=%s)",
        LISTEN_HOST, LISTEN_PORT, DATEV_TARGET, USER,
    )
    app = web.Application(client_max_size=20 * 1024 * 1024)
    app.router.add_route("*", "/{path:.*}", proxy)
    web.run_app(
        app, host=LISTEN_HOST, port=LISTEN_PORT,
        print=lambda *_args, **_kw: None,
        access_log=None,  # we log per request ourselves
    )


if __name__ == "__main__":
    main()
