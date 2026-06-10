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

        # Extended pause to allow all scanner subscriptions to fully clean up
        # (we ran 3 scanner batches, each creates a subscription that needs cleanup)
        # IB Gateway needs time to fully release scanner resources before contract requests
        await asyncio.sleep(8.0)

        # Step 2: resolve contract details (company name, sector, qualified contract)
        contract_infos = await fetch_all_contract_details(
            ib, symbols, delay=app_config.pacing.historical_delay_seconds
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
        for sym in contract_infos.keys():
            bars = bars_map.get(sym)
            atr_map[sym] = calculate_atr(bars, screener_config.atr_period) if bars else None

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
        snapshots = await fetch_market_snapshots(ib, surviving_contracts, app_config.pacing)
        log.info("Market snapshots retrieved: %d symbols with data", len(snapshots))
        log.info("Snapshot keys: %s", sorted(snapshots.keys()))
        log.info("Surviving symbols before snapshot filter: %s", sorted(surviving_symbols))
        
        # Debug: log snapshot details for each surviving symbol
        for sym in surviving_symbols:
            snap = snapshots.get(sym)
            if snap:
                log.info(
                    "%s snapshot: price=%.2f, vol=%.0f, mktcap=%s, chg=%.2f%%",
                    sym,
                    snap.prev_close or 0.0,
                    snap.pre_market_volume or 0.0,
                    f"{snap.market_cap_usd / 1e9:.2f}B" if snap.market_cap_usd else "None",
                    snap.pre_market_chg_pct or 0.0,
                )
            else:
                log.warning("%s: NO SNAPSHOT DATA RETRIEVED", sym)

        # Step 6: assemble StockRecord list
        # Only include symbols that have snapshot data to avoid None values in filtered fields
        surviving_infos = {
            s: contract_infos[s]
            for s in surviving_symbols
            if s in contract_infos and s in snapshots
        }
        log.info("Symbols with both contract_infos AND snapshots: %s", sorted(surviving_infos.keys()))
        log.info("Symbols in surviving_symbols but NOT in snapshots: %s", 
                 sorted(set(surviving_symbols) - set(snapshots.keys())))
        
        # Recalculate atr_map for only the surviving symbols with snapshot data
        atr_map_filtered: dict[str, float | None] = {}
        for sym in surviving_infos.keys():
            bars = bars_map.get(sym)
            atr_map_filtered[sym] = calculate_atr(bars, screener_config.atr_period) if bars else None
        
        records = build_records(surviving_infos, snapshots, bars_map, atr_map=atr_map_filtered)
        log.info("Built %d records from %d surviving symbols with snapshot data", len(records), len(surviving_infos))
        
        # Debug: log which records have None values that might cause filter rejection
        for rec in records:
            if rec.price is None or rec.pre_market_volume is None:
                log.warning(
                    "%s: incomplete data — price=%s, vol=%s, atr=%s (may fail filters)",
                    rec.symbol,
                    rec.price,
                    rec.pre_market_volume,
                    rec.atr,
                )

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
    from src.processing.enrichment import StockRecord
    if dry_run:
        return Path(app_config.output.directory) / "dry_run.yaml"
    return write_output([], app_config.output)
