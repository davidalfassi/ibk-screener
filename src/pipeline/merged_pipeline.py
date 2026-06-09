"""Merged pipeline: runs screener and watchlist, combines results."""
from __future__ import annotations

import logging
from typing import List

from src.config.loader import AppConfig, ScreenerConfig, WatchlistEntry
from src.pipeline.screener_pipeline import run_screener_pipeline
from src.pipeline.watchlist_pipeline import run_watchlist_pipeline
from src.processing.enrichment import StockRecord
from src.output.writer import write_output
from pathlib import Path

log = logging.getLogger(__name__)


async def run_merged_pipeline(
    app_config: AppConfig,
    screener_config: ScreenerConfig,
    watchlist: List[WatchlistEntry],
    dry_run: bool = False,
) -> Path:
    """Run both screener and watchlist pipelines, merge results, and write output."""
    log.info("Starting merged pipeline (screener + watchlist)...")

    # Run both pipelines concurrently
    screener_records = await run_screener_pipeline(
        app_config, screener_config, dry_run=dry_run
    )
    watchlist_records = await run_watchlist_pipeline(
        app_config, watchlist, dry_run=dry_run
    )

    # Combine records (watchlist first, then screener)
    # Deduplicate by symbol to avoid duplicates
    seen_symbols = set()
    combined_records: List[StockRecord] = []

    for rec in watchlist_records:
        if rec.symbol not in seen_symbols:
            combined_records.append(rec)
            seen_symbols.add(rec.symbol)

    for rec in screener_records:
        if rec.symbol not in seen_symbols:
            combined_records.append(rec)
            seen_symbols.add(rec.symbol)

    log.info(
        "Merged results: %d watchlist + %d screener = %d total (after dedup)",
        len(watchlist_records),
        len(screener_records),
        len(combined_records),
    )

    # Write combined output
    if not dry_run:
        output_path = write_output(
            combined_records,
            app_config.output,
            max_stocks=None,
        )
        log.info("Merged output written to: %s", output_path)
        return output_path
    else:
        log.info("Dry-run: skipping output write")
        return Path("output/merged.csv")
