from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

from ib_async import IB, BarData, Contract

from src.config.loader import PacingConfig

log = logging.getLogger(__name__)

# Request 20 calendar days to guarantee ≥14 trading days (accounts for weekends + holidays)
_DURATION = "20 D"
_BAR_SIZE = "1 day"


async def fetch_daily_bars(
    ib: IB,
    contract: Contract,
) -> Optional[List[BarData]]:
    """Fetch 20 days of daily OHLC bars for a contract. Returns None on failure."""
    try:
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",       # empty = up to now
            durationStr=_DURATION,
            barSizeSetting=_BAR_SIZE,
            whatToShow="TRADES",
            useRTH=True,          # regular trading hours for clean ATR inputs
            formatDate=1,
        )
        if not bars:
            log.warning("No historical bars returned for %s", contract.symbol)
            return None
        return list(bars)
    except Exception as e:
        log.warning("reqHistoricalData failed for %s: %s", contract.symbol, e)
        return None


async def fetch_all_daily_bars(
    ib: IB,
    contracts: List[Contract],
    pacing: PacingConfig,
) -> dict[str, List[BarData]]:
    """Fetch daily bars for all contracts sequentially with pacing delay."""
    results: dict[str, List[BarData]] = {}
    for contract in contracts:
        bars = await fetch_daily_bars(ib, contract)
        if bars:
            results[contract.symbol] = bars
        # IB enforces max 60 historical requests per 10 minutes
        await asyncio.sleep(pacing.historical_delay_seconds)
    log.info("Fetched historical bars for %d / %d symbols", len(results), len(contracts))
    return results
