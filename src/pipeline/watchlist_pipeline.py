from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from src.config.loader import AppConfig, WatchlistEntry
from src.ib.client import IBClient
from src.ib.contract_details import fetch_all_contract_details
from src.ib.historical import fetch_all_daily_bars
from src.ib.market_data import fetch_market_snapshots
from src.output.writer import write_output
from src.processing.enrichment import build_records

log = logging.getLogger(__name__)


async def run_watchlist_pipeline(
    app_config: AppConfig,
    watchlist: List[WatchlistEntry],
    dry_run: bool = False,
) -> Path:
    """
    Watchlist pipeline:
      contract details → historical bars → market snapshots → enrich → write YAML

    All watchlist symbols always appear in output (no filtering applied).
    Missing data fields appear as null in the YAML.
    """
    symbols = [entry.symbol for entry in watchlist]
    log.info("Running watchlist pipeline for %d symbols: %s", len(symbols), symbols)

    async with IBClient(app_config.ib_gateway) as ib:

        # Step 1: resolve contract details
        contract_infos = await fetch_all_contract_details(
            ib, symbols, delay=app_config.pacing.historical_delay_seconds
        )
        if not contract_infos:
            log.error("Could not resolve any contract details from watchlist")
            return write_output([], app_config.output)

        unresolved = set(symbols) - set(contract_infos.keys())
        if unresolved:
            log.warning("Could not resolve: %s", sorted(unresolved))

        # Step 2: fetch historical bars for ATR calculation
        contracts = [info.contract for info in contract_infos.values()]
        bars_map = await fetch_all_daily_bars(ib, contracts, app_config.pacing)

        # Step 3: fetch pre-market snapshots for ALL watchlist symbols (no pre-filter)
        snapshots = await fetch_market_snapshots(ib, contracts, app_config.pacing)

        # Step 4: assemble records (no filters — all symbols included)
        records = build_records(contract_infos, snapshots, bars_map)

    if dry_run:
        log.info("[dry-run] Would write %d records to %s", len(records), app_config.output.directory)
        return Path(app_config.output.directory) / "dry_run.yaml"

    return write_output(records, app_config.output)
