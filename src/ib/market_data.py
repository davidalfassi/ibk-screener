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
) -> Dict[str, MarketSnapshot]:
    """
    Fetch pre-market market data for a batch of contracts.

    Uses snapshot=False (streaming) so IB pushes all tick types continuously.
    snapshot=True often misses tick type 9 (prev close) and tick 258
    (fundamentalRatios) in pre-market conditions. We open subscriptions for
    all contracts, wait for ticks to arrive, read, then cancel everything.

    Processes in batches of max_concurrent_mkt_data to respect IB's hard limit.
    """
    if not contracts:
        return {}

    results: Dict[str, MarketSnapshot] = {}

    for batch_num, batch_start in enumerate(range(0, len(contracts), pacing.max_concurrent_mkt_data), 1):
        batch = contracts[batch_start: batch_start + pacing.max_concurrent_mkt_data]
        log.info("Market data batch %d: fetching %d symbols...", batch_num, len(batch))

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
        log.debug("Waiting %ss for market data ticks (batch %d of %d)...", _STREAM_WAIT, batch_num, 
                  (len(contracts) + pacing.max_concurrent_mkt_data - 1) // pacing.max_concurrent_mkt_data)
        await asyncio.sleep(_STREAM_WAIT)

        # Phase 3: read and immediately cancel every subscription
        batch_results = 0
        for symbol, (ticker, contract) in tickers.items():
            snapshot = _extract_snapshot(symbol, ticker)
            results[symbol] = snapshot
            ib.cancelMktData(contract)
            
            # Log only if we got meaningful data
            if snapshot.pre_market_price is not None or snapshot.prev_close is not None:
                batch_results += 1
                log.debug(
                    "%s: last=%.2f  close=%.2f  vol=%s  mktcap=%s  chg=%s%%",
                    symbol,
                    snapshot.pre_market_price or 0.0,
                    snapshot.prev_close or 0.0,
                    f"{snapshot.pre_market_volume:.0f}" if snapshot.pre_market_volume is not None else "n/a",
                    f"{snapshot.market_cap_usd / 1e9:.2f}B" if snapshot.market_cap_usd else "n/a",
                    f"{snapshot.pre_market_chg_pct:.2f}" if snapshot.pre_market_chg_pct is not None else "n/a",
                )
            else:
                log.warning("%s: no market data received (price and close both None)", symbol)
        
        log.info("Batch %d: got data for %d / %d symbols", batch_num, batch_results, len(tickers))

        # Wait for all cancellations to process before moving to next batch
        # IB needs time to fully release market data subscriptions and clean up resources
        # Increased from 2s to 3s to ensure reliable cleanup
        if batch_num < (len(contracts) + pacing.max_concurrent_mkt_data - 1) // pacing.max_concurrent_mkt_data:
            log.debug("Waiting 3s for market data subscription cancellations to process...")
            await asyncio.sleep(3.0)

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
    
    # Try to extract market cap from raw tick data (tick type 258 = fundamental ratios)
    # IB sends tick 258 with market cap in the value field as a string
    try:
        if hasattr(ticker, 'ticks') and ticker.ticks:
            for tick in ticker.ticks:
                # Tick 258 contains fundamental ratios including market cap
                if hasattr(tick, 'tickType') and tick.tickType == 258:
                    # Market cap is in the 'value' field as a string (e.g., "1.23456e+10")
                    if hasattr(tick, 'value') and tick.value:
                        try:
                            mkt_cap_val = float(tick.value)
                            if mkt_cap_val > 0:
                                market_cap = mkt_cap_val
                                log.info("%s: extracted market_cap=%.2e from tick 258", symbol, market_cap)
                                break
                        except (ValueError, TypeError) as e:
                            log.debug("%s: could not parse tick 258 value '%s': %s", symbol, tick.value, e)
    except Exception as e:
        log.debug("%s: error extracting market cap from ticks: %s", symbol, e)
    
    if market_cap is None:
        log.debug("%s: market cap not found in tick 258", symbol)
    
    volume = _safe(ticker.volume)

    return MarketSnapshot(
        symbol=symbol,
        pre_market_price=last,
        prev_close=close,
        pre_market_volume=volume,
        market_cap_usd=market_cap,
        pre_market_chg_pct=_calc_chg_pct(last, close),
    )
