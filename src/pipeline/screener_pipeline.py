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
from src.processing.atr import calculate_atr
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

        # Extended pause to allow scanner subscriptions to fully clean up before
        # contract detail requests (IB Gateway needs time to release scanner resources)
        await asyncio.sleep(8.0)

        # Step 2: resolve contract details (company name, sector, qualified contract)
        contract_infos = await fetch_all_contract_details(
            ib, symbols, delay=app_config.pacing.contract_details_delay_seconds
        )
        if not contract_infos:
            log.error("Could not resolve any contract details")
            return _empty_output(app_config, dry_run)

        # Step 2b: drop ETFs if exclude_etfs is set
        if screener_config.exclude_etfs:
            etf_types = {"ETF", "ETN", "ETV"}
            before = len(contract_infos)
            contract_infos = {
                sym: info for sym, info in contract_infos.items()
                if info.stock_type.upper() not in etf_types
            }
            dropped = before - len(contract_infos)
            if dropped:
                log.info("Excluded %d ETF/ETN symbols (exclude_etfs=True)", dropped)
            if not contract_infos:
                log.warning("All symbols were ETFs/ETNs — no stocks remain after ETF filter")
                return _empty_output(app_config, dry_run)

        # Step 3: fetch historical bars for ATR calculation
        contracts = [info.contract for info in contract_infos.values()]
        bars_map = await fetch_all_daily_bars(ib, contracts, app_config.pacing)

        # Step 4: calculate ATR and pre-filter symbols below the threshold
        atr_map: dict[str, float | None] = {
            sym: calculate_atr(bars_map[sym], screener_config.atr_period) if sym in bars_map else None
            for sym in contract_infos.keys()
        }

        surviving_symbols = filter_symbols_by_atr(
            list(contract_infos.keys()), atr_map, screener_config.atr_min,
            bars_map=bars_map,
        )
        surviving_contracts = [
            contract_infos[s].contract for s in surviving_symbols if s in contract_infos
        ]

        if not surviving_contracts:
            log.warning("All symbols filtered out by ATR threshold")
            return _empty_output(app_config, dry_run)

        # Step 5: fetch pre-market snapshots (price, volume, change%, market cap from tick 258)
        log.info("Fetching market snapshots for %d surviving symbols...", len(surviving_contracts))
        snapshots = await fetch_market_snapshots(ib, surviving_contracts, app_config.pacing)
        log.info("Market snapshots retrieved: %d / %d symbols with data", len(snapshots), len(surviving_contracts))
        
        # Validate snapshot data quality
        snapshots_with_price = {
            sym: snap for sym, snap in snapshots.items()
            if snap.pre_market_price is not None or snap.prev_close is not None
        }
        log.info("Snapshots with valid price data: %d / %d", len(snapshots_with_price), len(snapshots))
        
        # Debug: log which symbols failed to get snapshots
        missing_snapshots = set(surviving_symbols) - set(snapshots.keys())
        if missing_snapshots:
            log.warning("Missing snapshots for %d symbols: %s", len(missing_snapshots), sorted(missing_snapshots))
        
        # Warn about snapshots with no price data
        no_price_snapshots = set(snapshots.keys()) - set(snapshots_with_price.keys())
        if no_price_snapshots:
            log.warning("Snapshots with no price data for %d symbols: %s", len(no_price_snapshots), sorted(no_price_snapshots))

        # Step 6: assemble StockRecord list
        # Only include symbols that have snapshot data AND valid price data
        surviving_infos = {
            s: contract_infos[s]
            for s in surviving_symbols
            if s in contract_infos and s in snapshots_with_price
        }
        log.info("Symbols with contract_infos AND valid price snapshots: %d / %d", 
                 len(surviving_infos), len(surviving_symbols))
        
        if not surviving_infos:
            log.error("No symbols have valid price data in snapshots — check IB connection and market data subscriptions")
            return _empty_output(app_config, dry_run)
        
        # Reuse already-computed ATR values; restrict to the surviving set
        atr_map_surviving = {sym: atr_map[sym] for sym in surviving_infos if sym in atr_map}

        records = build_records(surviving_infos, snapshots, bars_map, atr_map=atr_map_surviving)
        log.info("Built %d records from %d surviving symbols with snapshot data", len(records), len(surviving_infos))

        # Step 7: apply remaining client-side filters (sector, price_min)
        records_before_filter = len(records)
        records = apply_screener_filters(records, screener_config)
        log.info("Screener filters removed %d records (%d → %d)", 
                 records_before_filter - len(records), records_before_filter, len(records))

        log.info(
            "Pipeline summary: %d scanner → %d contracts → %d historical → %d ATR pass"
            " → %d snapshots → %d records → %d after filters",
            len(symbols), len(contract_infos), len(bars_map),
            len(surviving_symbols), len(snapshots), records_before_filter, len(records),
        )

        if not records:
            log.warning("No records passed all filters")
            return _empty_output(app_config, dry_run)

    # Step 8: write output (outside the IB context — connection already closed)
    if dry_run:
        log.info("[dry-run] Would write %d records to %s", len(records), app_config.output.directory)
        return Path(app_config.output.directory) / "dry_run.yaml"

    return write_output(records, app_config.output, max_stocks=app_config.max_number_of_stocks)


def _empty_output(app_config: AppConfig, dry_run: bool) -> Path:
    if dry_run:
        return Path(app_config.output.directory) / "dry_run.yaml"
    return write_output([], app_config.output)
