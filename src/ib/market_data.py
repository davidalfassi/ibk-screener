from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Optional

from ib_async import IB, Contract

from src.config.loader import PacingConfig

log = logging.getLogger(__name__)

# Tick types requested:
#   233 = RTVolume (real-time volume, last price, last size)
#   236 = Shortable shares
#   258 = Fundamental Ratios (market cap, PE, etc.)
_GENERIC_TICKS = "233,236,258"

# Seconds to wait after opening subscriptions before reading ticks.
# Using snapshot=False (streaming) so IB pushes all ticks continuously;
# 10 s is enough for close, bid/ask, volume, and fundamentalRatios to arrive.
_STREAM_WAIT = 10.0


@dataclass
class MarketSnapshot:
    symbol: str
    pre_market_price: Optional[float]   # ticker.last during pre-market
    prev_close: Optional[float]         # regular-session close
    pre_market_volume: Optional[float]  # volume traded in pre-market session
    market_cap_usd: Optional[float]     # from fundamentalRatios tick (tick 258)
    pre_market_chg_pct: Optional[float] # calculated: (last - close) / close * 100


def _safe(val: float) -> Optional[float]:
    """Return None if val is nan/None, otherwise the value."""
    if val is None:
        return None
    try:
        return None if math.isnan(val) else val
    except TypeError:
        return None


def _calc_chg_pct(price: Optional[float], close: Optional[float]) -> Optional[float]:
    if price is None or close is None or close == 0:
        return None
    return (price - close) / close * 100


async def fetch_market_snapshots(
    ib: IB,
    contracts: List[Contract],
    pacing: PacingConfig,
    market_caps: Optional[Dict[str, Optional[float]]] = None,
) -> Dict[str, MarketSnapshot]:
    """
    Fetch pre-market market data for a batch of contracts.

    Uses snapshot=False (streaming) so IB pushes all tick types continuously.
    snapshot=True often misses tick type 9 (prev close) and tick 258
    (fundamentalRatios) in pre-market conditions. We open subscriptions for
    all contracts, wait for ticks to arrive, read, then cancel everything.

    Processes in batches of max_concurrent_mkt_data to respect IB's hard limit.
    
    Args:
        market_caps: Optional dict mapping symbol to market cap in USD.
                     If provided, these values override fundamentalRatios data.
    """
    if not contracts:
        return {}

    if market_caps is None:
        market_caps = {}

    results: Dict[str, MarketSnapshot] = {}

    for batch_start in range(0, len(contracts), pacing.max_concurrent_mkt_data):
        batch = contracts[batch_start: batch_start + pacing.max_concurrent_mkt_data]

        # Phase 1: open streaming subscriptions for the whole batch (non-blocking)
        tickers = {}
        for contract in batch:
            ticker = ib.reqMktData(
                contract,
                genericTickList=_GENERIC_TICKS,
                snapshot=False,          # streaming → pushes close, bid/ask, volume reliably
                regulatorySnapshot=False,
            )
            tickers[contract.symbol] = (ticker, contract)
            await asyncio.sleep(pacing.market_data_delay_seconds)

        # Phase 2: wait for all ticks to arrive
        log.debug("Waiting %ss for market data ticks (batch of %d)...", _STREAM_WAIT, len(batch))
        await asyncio.sleep(_STREAM_WAIT)

        # Phase 3: read and immediately cancel every subscription
        for symbol, (ticker, contract) in tickers.items():
            snapshot = _extract_snapshot(symbol, ticker)
            # Override market cap if provided from fundamental data
            if symbol in market_caps:
                snapshot.market_cap_usd = market_caps[symbol]
            results[symbol] = snapshot
            ib.cancelMktData(contract)

    log.info("Fetched market snapshots for %d / %d symbols", len(results), len(contracts))
    return results


def _extract_snapshot(symbol: str, ticker) -> MarketSnapshot:
    last = _safe(ticker.last)
    close = _safe(ticker.close)

    # Fallback: if last is unavailable, use bid/ask midpoint
    if last is None:
        bid = _safe(ticker.bid)
        ask = _safe(ticker.ask)
        if bid is not None and ask is not None:
            last = (bid + ask) / 2
            log.debug("%s: using bid/ask midpoint as pre-market price (last was nan)", symbol)

    market_cap: Optional[float] = None
    try:
        fr = ticker.fundamentalRatios
        if fr is not None:
            safe_val = _safe(fr.mktCap)          # handles nan → None
            if safe_val is not None and safe_val > 0:
                market_cap = safe_val * 1_000_000  # mktCap is in millions USD
    except (AttributeError, TypeError, ValueError):
        pass
    log.debug("%s: fundamentalRatios=%s market_cap=%s", symbol, ticker.fundamentalRatios, market_cap)

    volume = _safe(ticker.volume)

    return MarketSnapshot(
        symbol=symbol,
        pre_market_price=last,
        prev_close=close,
        pre_market_volume=volume,
        market_cap_usd=market_cap,
        pre_market_chg_pct=_calc_chg_pct(last, close),
    )
