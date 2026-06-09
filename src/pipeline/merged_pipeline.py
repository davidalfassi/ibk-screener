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

    # Import pipeline internals to get records without writing
    from src.ib.client import IBClient
    from src.ib.contract_details import fetch_all_contract_details
    from src.ib.market_data import fetch_market_snapshots
    from src.ib.historical import fetch_all_daily_bars
    from src.processing.enrichment import build_records
    from src.processing.filters import apply_screener_filters, filter_symbols_by_atr
    from src.processing.atr import calculate_atr

    screener_records: List[StockRecord] = []
    watchlist_records: List[StockRecord] = []

    async with IBClient(app_config.ib_gateway) as ib:
        # Run screener pipeline
        log.info("Running screener pipeline...")
        from src.ib.scanner import run_scanner_batches
        screener_symbols = await run_scanner_batches(ib, screener_config)
        log.info("Screener found %d symbols", len(screener_symbols))

        if screener_symbols:
            contract_infos = await fetch_all_contract_details(
                ib, screener_symbols, delay=app_config.pacing.contract_details
            )
            snapshots = await fetch_market_snapshots(
                ib, list(contract_infos.values()), pacing=app_config.pacing
            )
            bars_map = await fetch_all_daily_bars(
                ib, list(contract_infos.values()), pacing=app_config.pacing
            )
            atr_map = {
                sym: calculate_atr(bars, screener_config.atr_period)
                for sym, bars in bars_map.items()
            }
            screener_records = build_records(
                contract_infos, snapshots, bars_map, atr_map, screener_config.atr_period
            )
            screener_records = apply_screener_filters(screener_records, screener_config)

        # Run watchlist pipeline
        log.info("Running watchlist pipeline...")
        watchlist_symbols = [entry.symbol for entry in watchlist]
        if watchlist_symbols:
            contract_infos = await fetch_all_contract_details(
                ib, watchlist_symbols, delay=app_config.pacing.contract_details
            )
            snapshots = await fetch_market_snapshots(
                ib, list(contract_infos.values()), pacing=app_config.pacing
            )
            bars_map = await fetch_all_daily_bars(
                ib, list(contract_infos.values()), pacing=app_config.pacing
            )
            atr_map = {
                sym: calculate_atr(bars, 14)
                for sym, bars in bars_map.items()
            }
            watchlist_records = build_records(
                contract_infos, snapshots, bars_map, atr_map, 14
            )
            log.info("Watchlist fetched %d records", len(watchlist_records))

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
