from __future__ import annotations

import asyncio
import logging
from typing import List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config.loader import AppConfig, ScreenerConfig, WatchlistEntry

log = logging.getLogger(__name__)


async def _run_job(app_config: AppConfig, screener_config: ScreenerConfig, watchlist: List[WatchlistEntry]) -> None:
    """Job function called by APScheduler on each scheduled trigger."""
    mode = app_config.scheduler.mode
    log.info("Scheduler triggered — running '%s' pipeline", mode)
    try:
        if mode == "screener":
            from src.pipeline.screener_pipeline import run_screener_pipeline
            path = await run_screener_pipeline(app_config, screener_config)
        else:
            from src.pipeline.watchlist_pipeline import run_watchlist_pipeline
            path = await run_watchlist_pipeline(app_config, watchlist)
        log.info("Scheduled job complete — output: %s", path)
        print(f"[scheduler] Output written to: {path}")
    except Exception:
        log.exception("Scheduled job failed")


def start_scheduler(
    app_config: AppConfig,
    screener_config: ScreenerConfig,
    watchlist: List[WatchlistEntry],
) -> None:
    """
    Start the APScheduler daemon.

    Uses AsyncIOScheduler (not BackgroundScheduler) so it shares the asyncio
    event loop that ib_async requires. Blocks until KeyboardInterrupt.
    """
    cron_cfg = app_config.scheduler.cron

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    scheduler = AsyncIOScheduler(event_loop=loop)
    trigger = CronTrigger(
        day_of_week=cron_cfg.day_of_week,
        hour=cron_cfg.hour,
        minute=cron_cfg.minute,
        timezone=cron_cfg.timezone,
    )

    scheduler.add_job(
        _run_job,
        trigger=trigger,
        args=[app_config, screener_config, watchlist],
        name="ibk_pull",
    )

    scheduler.start()
    log.info(
        "Scheduler started — will run '%s' pipeline at %02d:%02d on %s (%s)",
        app_config.scheduler.mode,
        cron_cfg.hour,
        cron_cfg.minute,
        cron_cfg.day_of_week,
        cron_cfg.timezone,
    )
    print(
        f"Scheduler running. Next job: {app_config.scheduler.mode} pipeline "
        f"at {cron_cfg.hour:02d}:{cron_cfg.minute:02d} {cron_cfg.timezone} on {cron_cfg.day_of_week}.\n"
        "Press Ctrl+C to stop."
    )

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopping...")
    finally:
        scheduler.shutdown()
        loop.close()
