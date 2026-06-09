from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import List, Optional

from ib_async import IB, BarData, Contract

from src.config.loader import PacingConfig

log = logging.getLogger(__name__)

# Request 20 calendar days to guarantee ≥14 trading days (accounts for weekends + holidays)
_DURATION = "20 D"
_BAR_SIZE = "1 day"


class _HistoricalLimiter:
    """≤59 requests per any 10-minute sliding window, with min spacing between calls."""
    _MAX = 59
    _WINDOW = 600.0

    def __init__(self) -> None:
        self._timestamps: deque[float] = deque()

    async def acquire(self, min_spacing: float) -> None:
        now = time.monotonic()
        # Remove timestamps older than the 10-minute window
        while self._timestamps and now - self._timestamps[0] > self._WINDOW:
            self._timestamps.popleft()
        
        # If we've hit the limit, calculate how long to wait
        if len(self._timestamps) >= self._MAX:
            # Wait until the oldest request falls out of the window
            wait = self._WINDOW - (now - self._timestamps[0]) + 0.1
            log.info("Historical rate limit: pausing %.1f s (queue: %d/%d)", wait, len(self._timestamps), self._MAX)
            await asyncio.sleep(wait)
            now = time.monotonic()
            # Clean up again after waiting
            while self._timestamps and now - self._timestamps[0] > self._WINDOW:
                self._timestamps.popleft()
        
        self._timestamps.append(now)
        await asyncio.sleep(min_spacing)


async def fetch_daily_bars(
    ib: IB,
    contract: Contract,
    max_attempts: int = 2,
) -> Optional[List[BarData]]:
    """Fetch 20 days of daily OHLC bars for a contract. Returns None on failure.

    Retries once (max_attempts=2) with a 3-second backoff to handle IB transient
    failures (pacing errors, temporary data gaps, server-side throttle).
    """
    sym = contract.symbol
    for attempt in range(1, max_attempts + 1):
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
            if bars:
                log.debug("%s: received %d bars (%s → %s)", sym, len(bars), bars[0].date, bars[-1].date)
                return list(bars)
            log.warning("%s: empty bars on attempt %d/%d", sym, attempt, max_attempts)
        except Exception as e:
            log.warning("%s: reqHistoricalData failed on attempt %d/%d: %s", sym, attempt, max_attempts, e)

        if attempt < max_attempts:
            log.info("%s: retrying historical bars in 3s...", sym)
            await asyncio.sleep(3.0)

    log.error("%s: historical bars unavailable after %d attempt(s) — symbol will be dropped", sym, max_attempts)
    return None


async def fetch_all_daily_bars(
    ib: IB,
    contracts: List[Contract],
    pacing: PacingConfig,
) -> dict[str, List[BarData]]:
    """Fetch daily bars for all contracts sequentially with pacing delay."""
    results: dict[str, List[BarData]] = {}
    limiter = _HistoricalLimiter()
    for contract in contracts:
        bars = await fetch_daily_bars(ib, contract)
        if bars:
            results[contract.symbol] = bars
        await limiter.acquire(pacing.historical_delay_seconds)
    log.info("Fetched historical bars for %d / %d symbols", len(results), len(contracts))
    return results
