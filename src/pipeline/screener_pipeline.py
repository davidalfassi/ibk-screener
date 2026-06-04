from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from src.config.loader import AppConfig, ScreenerConfig
from src.ib.client import IBClient
from src.ib.contract_details import fetch_all_contract_details
from src.ib.historical import fetch_all_daily_bars
from src.ib.market_data import fetch_market_snapshots
from src.ib.scanner import run_scanner_batches
from src.output.writer import write_output
from src.processing.enrichment import build_records
from src.processing.filters import apply_screener_filters, filter_symbols_by_atr

log = logging.getLogger(__name__)


async def run_screener_pipeline(
    app_config: AppConfig,
    screener_config: ScreenerConfig,
    dry_run: bool = False,
) -> Path:
    """
    Full screener pipeline:
      scanner → contract details → historical bars → ATR pre-filter
      → market snapshots → enrich → sector/price filter → write YAML
    """
    async with IBClient(app_config.ib_gateway) as ib:

        # Step 1: run the IB scanner batches to get up to 150 deduplicated symbols
        symbols = await run_scanner_batches(ib, screener_config)
        if not symbols:
            log.warning("Scanner returned no symbols — check your scan_code and filters")
            return _empty_output(app_config, dry_run)

        # Longer pause to allow all scanner subscriptions to fully clean up
        # (we ran 3 scanner batches, each creates a subscription that needs cleanup)
        # IB Gateway needs time to fully release scanner resources before contract requests
        await asyncio.sleep(5.0)

        # Step 2: resolve contract details (company name, sector, qualified contract)
        contract_infos = await fetch_all_contract_details(
            ib, symbols, delay=app_config.pacing.market_data_delay_seconds
        )
        if not contract_infos:
            log.error("Could not resolve any contract details")
            return _empty_output(app_config, dry_run)

        # Step 3: fetch historical bars for ATR calculation
        contracts = [info.contract for info in contract_infos.values()]
        bars_map = await fetch_all_daily_bars(ib, contracts, app_config.pacing)

        # Step 4: calculate ATR and pre-filter symbols below the threshold
        atr_map: dict[str, float | None] = {}
        from src.processing.atr import calculate_atr
        for sym, bars in bars_map.items():
            atr_map[sym] = calculate_atr(bars)

        surviving_symbols = filter_symbols_by_atr(
            list(contract_infos.keys()), atr_map, screener_config.atr_min
        )
        surviving_contracts = [
            contract_infos[s].contract for s in surviving_symbols if s in contract_infos
        ]

        if not surviving_contracts:
            log.warning("All symbols filtered out by ATR threshold")
            return _empty_output(app_config, dry_run)

        # Step 5: fetch pre-market snapshots (price, volume, change%, market cap)
        snapshots = await fetch_market_snapshots(ib, surviving_contracts, app_config.pacing)

        # Step 6: assemble StockRecord list
        surviving_infos = {s: contract_infos[s] for s in surviving_symbols if s in contract_infos}
        records = build_records(surviving_infos, snapshots, bars_map)

        # Step 7: apply remaining client-side filters (sector, price_min)
        records = apply_screener_filters(records, screener_config)

        if not records:
            log.warning("No records passed all filters")
            return _empty_output(app_config, dry_run)

    # Step 8: write output (outside the IB context — connection already closed)
    if dry_run:
        log.info("[dry-run] Would write %d records to %s", len(records), app_config.output.directory)
        return Path(app_config.output.directory) / "dry_run.yaml"

    return write_output(records, app_config.output, max_stocks=app_config.max_number_of_stocks)


def _empty_output(app_config: AppConfig, dry_run: bool) -> Path:
    from src.processing.enrichment import StockRecord
    if dry_run:
        return Path(app_config.output.directory) / "dry_run.yaml"
    return write_output([], app_config.output)
