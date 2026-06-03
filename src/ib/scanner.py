from __future__ import annotations

import logging
from typing import List

from ib_async import IB, ScannerSubscription

from src.config.loader import ScreenerConfig

log = logging.getLogger(__name__)


async def run_scanner(ib: IB, config: ScreenerConfig) -> List[str]:
    """
    Execute an IB scanner subscription and return a list of ticker symbols.

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

    # Set built-in ScannerSubscription attributes — universally supported,
    # no extra data subscription required.
    # IMPORTANT: marketCapAbove / marketCapBelow are in MILLIONS of USD (not raw dollars).
    if config.market_cap_min_usd:
        sub.marketCapAbove = config.market_cap_min_usd / 1_000_000   # convert $ → millions

    if config.market_cap_max_usd:
        sub.marketCapBelow = config.market_cap_max_usd / 1_000_000   # convert $ → millions

    if config.avg_volume_min:
        sub.aboveVolume = int(config.avg_volume_min)                  # minimum daily volume in shares

    log.info(
        "Running IB scanner: scanCode=%s, rows=%d, marketCapAbove=%.0fM, aboveVolume=%s",
        config.scan_code,
        sub.numberOfRows,
        (config.market_cap_min_usd or 0) / 1_000_000,
        config.avg_volume_min,
    )

    try:
        scan_data = await ib.reqScannerDataAsync(sub)
    except Exception as e:
        log.error("Scanner request failed: %s", e)
        return []

    symbols = [item.contractDetails.contract.symbol for item in scan_data]
    log.info("Scanner returned %d symbols: %s", len(symbols), symbols)
    return symbols
