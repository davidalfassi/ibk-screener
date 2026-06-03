#!/usr/bin/env python3
"""
IBK-Pull-Date — Interactive Brokers pre-market data puller.

Usage:
  python main.py --mode screener            # run screener pipeline now
  python main.py --mode watchlist           # run watchlist pipeline now
  python main.py --scheduler                # start APScheduler daemon
  python main.py --mode screener --dry-run  # validate without writing output
  python main.py --mode watchlist --no-hours-check  # skip pre-market time gate
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

import click
import pytz

DEFAULT_CONFIG_DIR = Path(__file__).parent / "config"


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    # Suppress noisy ib_async internal logs unless debugging
    if level != "DEBUG":
        logging.getLogger("ib_async").setLevel(logging.WARNING)


def _check_premarket_hours(tz_name: str) -> bool:
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    start = now.replace(hour=4, minute=0, second=0, microsecond=0)
    end = now.replace(hour=9, minute=30, second=0, microsecond=0)
    return start <= now <= end


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--mode",
    type=click.Choice(["screener", "watchlist"], case_sensitive=False),
    default=None,
    help="Run the specified pipeline immediately.",
)
@click.option(
    "--scheduler",
    "use_scheduler",
    is_flag=True,
    default=False,
    help="Start the APScheduler daemon (ignores --mode).",
)
@click.option(
    "--config-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=DEFAULT_CONFIG_DIR,
    show_default=True,
    help="Directory containing settings.yaml, screener.yaml, watchlist.yaml.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Connect to IB, fetch data, but do not write output file.",
)
@click.option(
    "--no-hours-check",
    is_flag=True,
    default=False,
    help="Skip the pre-market hours validation (useful for testing outside market hours).",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    show_default=True,
    help="Logging verbosity.",
)
def main(
    mode: str | None,
    use_scheduler: bool,
    config_dir: Path,
    dry_run: bool,
    no_hours_check: bool,
    log_level: str,
) -> None:
    _setup_logging(log_level.upper())
    log = logging.getLogger("main")

    # Load all configs upfront (fail fast on bad YAML)
    from src.config.loader import load_screener, load_settings, load_watchlist

    settings_path = config_dir / "settings.yaml"
    screener_path = config_dir / "screener.yaml"
    watchlist_path = config_dir / "watchlist.yaml"

    if not settings_path.exists():
        click.echo(f"ERROR: settings.yaml not found at {settings_path}", err=True)
        sys.exit(1)

    app_config = load_settings(settings_path)

    # Lazy-load screener / watchlist only when needed
    screener_config = None
    watchlist = None

    if use_scheduler:
        # Scheduler needs both configs regardless of mode
        screener_config = load_screener(screener_path)
        watchlist = load_watchlist(watchlist_path)
        from src.scheduler.runner import start_scheduler
        start_scheduler(app_config, screener_config, watchlist)
        return

    if not mode:
        click.echo("Specify --mode screener, --mode watchlist, or --scheduler.\nUse --help for details.")
        sys.exit(1)

    # Pre-market hours check
    if app_config.pre_market.check_hours and not no_hours_check:
        if not _check_premarket_hours(app_config.pre_market.tz):
            log.warning(
                "Current time is outside pre-market hours (4:00–9:30 AM ET, weekdays). "
                "Data may be unavailable or stale. Use --no-hours-check to suppress this warning."
            )

    if mode == "screener":
        screener_config = load_screener(screener_path)
        from src.pipeline.screener_pipeline import run_screener_pipeline
        path = asyncio.run(run_screener_pipeline(app_config, screener_config, dry_run=dry_run))
    else:
        watchlist = load_watchlist(watchlist_path)
        from src.pipeline.watchlist_pipeline import run_watchlist_pipeline
        path = asyncio.run(run_watchlist_pipeline(app_config, watchlist, dry_run=dry_run))

    if not dry_run:
        click.echo(f"Output: {path}")


if __name__ == "__main__":
    main()
