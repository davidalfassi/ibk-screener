from __future__ import annotations

from typing import List, Optional

from ib_async import BarData


def calculate_atr(bars: List[BarData], period: int = 14) -> Optional[float]:
    """
    Wilder's ATR(period) from a list of daily OHLC bars, returned as a percentage.

    Requires at least period+1 bars (need a prior close to compute the first TR).
    Returns None if insufficient data.

    Wilder smoothing:
        ATR(0) = simple average of first `period` TR values
        ATR(i) = (ATR(i-1) * (period - 1) + TR(i)) / period

    The result is expressed as a percentage of the most recent close price.
    """
    if len(bars) < period + 1:
        return None

    # Compute True Range for each bar starting at index 1
    trs: List[float] = []
    for i in range(1, len(bars)):
        high = bars[i].high
        low = bars[i].low
        prev_close = bars[i - 1].close
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)

    if len(trs) < period:
        return None

    # Seed: simple average of first `period` TR values
    atr = sum(trs[:period]) / period

    # Wilder smoothing over remaining bars
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period

    # Convert to percentage of the most recent close
    most_recent_close = bars[-1].close
    if most_recent_close is None or most_recent_close == 0:
        return None

    atr_pct = (atr / most_recent_close) * 100
    return round(atr_pct, 2)
