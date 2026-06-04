from __future__ import annotations

import asyncio
import logging
from typing import List

from ib_async import IB, ScannerSubscription

from src.config.loader import ScannerBatch, ScreenerConfig

log = logging.getLogger(__name__)


async def run_scanner_batches(ib: IB, config: ScreenerConfig) -> List[str]:
    """
    Run one scanner call per batch, deduplicate, and return combined symbol list.

    If scan_batches is empty, falls back to a single scan using the global
    market_cap_min/max_usd fields (backward-compatible with older configs).
    """
    batches = config.scan_batches or [
        ScannerBatch(
            market_cap_min_usd=config.market_cap_min_usd,
            market_cap_max_usd=config.market_cap_max_usd,
        )
    ]

    seen: set[str] = set()
    symbols: List[str] = []

    for i, batch in enumerate(batches):
        if i > 0:
            await asyncio.sleep(2.0)   # brief pause between scanner calls
        for sym in await _run_single_scan(ib, config, batch):
            if sym not in seen:
                seen.add(sym)
                symbols.append(sym)

    log.info("Scanner: %d unique symbols from %d batches", len(symbols), len(batches))
    return symbols


async def _run_single_scan(ib: IB, config: ScreenerConfig, batch: ScannerBatch) -> List[str]:
    """
    Execute one IB scanner call with cap range overridden by `batch`.

    IB scanner quirks:
    - marketCapAbove / aboveVolume are set directly on ScannerSubscription (raw USD / shares).
    - scannerSubscriptionFilterOptions TagValues like usdMarketCapAbove require a paid
      data subscription and are disabled on most paper-trading accounts — avoid them.
    - Max 50 rows per scan (IB hard limit).
    - Returns contract identifiers only — no price/volume data.
    """
    sub = ScannerSubscription()
    sub.instrument = config.instrument
    sub.locationCode = config.location_code
    sub.scanCode = config.scan_code
    sub.numberOfRows = min(config.number_of_rows, 50)

    # IMPORTANT: marketCapAbove / marketCapBelow are in MILLIONS of USD (not raw dollars).
    if batch.market_cap_min_usd:
        sub.marketCapAbove = batch.market_cap_min_usd / 1_000_000
    if batch.market_cap_max_usd:
        sub.marketCapBelow = batch.market_cap_max_usd / 1_000_000

    if config.avg_volume_min:
        sub.aboveVolume = int(config.avg_volume_min)

    cap_above = f"{batch.market_cap_min_usd / 1_000_000:.0f}M" if batch.market_cap_min_usd else "none"
    cap_below = f"{batch.market_cap_max_usd / 1_000_000:.0f}M" if batch.market_cap_max_usd else "none"
    log.info(
        "Running IB scanner: scanCode=%s, rows=%d, marketCapAbove=%s, marketCapBelow=%s, aboveVolume=%s",
        config.scan_code, sub.numberOfRows, cap_above, cap_below, config.avg_volume_min,
    )

    try:
        scan_data = await ib.reqScannerDataAsync(sub)
        # Extract symbols immediately while subscription is active
        symbols = [item.contractDetails.contract.symbol for item in scan_data]
    except Exception as e:
        log.error("Scanner request failed: %s", e)
        return []

    log.info("Scanner batch returned %d symbols: %s", len(symbols), symbols)
    return symbols
