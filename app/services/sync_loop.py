"""Background sync loop. Started from FastAPI's lifespan handler.

Three cadences:

- **Drain (every 15s)**: apply pending_operation rows that are due.
  This is what makes user edits flow to DATEV/Patti without anyone
  having to click "Queue abarbeiten".
- **DATEV pull (every 5 min)**: refresh the employee list from DATEV
  (thin list, ~5s). Picks up new hires, terminations, name changes.
- **Patti pull (every 10 min)**: refresh contact data for linked
  employees + auto-link any newly-pulled DATEV employees.

If the bridge is unreachable, the DATEV pull is skipped (DATEV calls
would just fail, no point in logging more 503s). The drain is always
attempted — Patti operations don't need the bridge, only DATEV ones,
and the bridge-down ones will mark themselves transient and reschedule.

Shutdown is handled via the asyncio cancellation propagated from
FastAPI's lifespan exit.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone

from app.core import datev_local_client
from app.core.logging import get_logger
from app.db.session import SessionLocal
from app.services import operation_apply, sync as sync_service

logger = get_logger("datev.sync_loop")


# Tunables (could move to settings if we ever need per-env tuning)
DRAIN_INTERVAL = 15  # seconds
DATEV_PULL_INTERVAL = 300  # 5 min
PATTI_PULL_INTERVAL = 600  # 10 min


async def _drain_once() -> None:
    """One drain pass — never raises out, all logged."""
    try:
        with SessionLocal() as db:
            result = operation_apply.drain(db, max_ops=20)
            if result["total"]:
                logger.info(
                    "sync_loop_drain",
                    total=result["total"],
                    done=result.get("done", 0),
                    retry=result.get("retry", 0),
                    error=result.get("error", 0),
                )
    except Exception:  # noqa: BLE001 — keep loop alive
        logger.exception("sync_loop_drain_error")


async def _datev_pull_once() -> None:
    """One DATEV pull pass — only runs if bridge is up."""
    try:
        if not datev_local_client.ping():
            return  # silent skip — health endpoint already reports OFFLINE
        with SessionLocal() as db:
            result = sync_service.pull_employees_from_datev(db, fetch_details=False)
            logger.info(
                "sync_loop_datev_pull",
                ok=result.get("ok"),
                listed=result.get("listed"),
                created=result.get("created"),
                updated=result.get("updated"),
            )
    except Exception:  # noqa: BLE001
        logger.exception("sync_loop_datev_pull_error")


async def _patti_pull_once() -> None:
    """One Patti pull pass — also runs auto_link to pick up new Employees."""
    try:
        with SessionLocal() as db:
            link = sync_service.auto_link_employees(db)
            if link.get("linked"):
                logger.info("sync_loop_auto_link", **link)
            refresh = sync_service.pull_patti_for_linked_employees(db)
            logger.info("sync_loop_patti_pull", **refresh)
    except Exception:  # noqa: BLE001
        logger.exception("sync_loop_patti_pull_error")


async def run() -> None:
    """The background coroutine. Cancellation-safe."""
    logger.info(
        "sync_loop_started",
        drain_interval=DRAIN_INTERVAL,
        datev_interval=DATEV_PULL_INTERVAL,
        patti_interval=PATTI_PULL_INTERVAL,
    )

    last_datev = datetime.fromtimestamp(0, tz=timezone.utc)
    last_patti = datetime.fromtimestamp(0, tz=timezone.utc)

    while True:
        now = datetime.now(timezone.utc)

        # Drain runs every cycle
        await _drain_once()

        # DATEV pull runs every DATEV_PULL_INTERVAL
        if (now - last_datev).total_seconds() >= DATEV_PULL_INTERVAL:
            await _datev_pull_once()
            last_datev = now

        # Patti pull runs every PATTI_PULL_INTERVAL
        if (now - last_patti).total_seconds() >= PATTI_PULL_INTERVAL:
            await _patti_pull_once()
            last_patti = now

        try:
            await asyncio.sleep(DRAIN_INTERVAL)
        except asyncio.CancelledError:
            logger.info("sync_loop_cancelled")
            return


@contextlib.asynccontextmanager
async def lifespan_task():
    """FastAPI lifespan: start the sync loop on app boot, cancel on shutdown."""
    task = asyncio.create_task(run(), name="datev-sync-loop")
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
