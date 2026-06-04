from __future__ import annotations

import logging
from typing import List

from src.config.loader import ScreenerConfig
from src.processing.enrichment import StockRecord

log = logging.getLogger(__name__)


def apply_screener_filters(records: List[StockRecord], config: ScreenerConfig) -> List[StockRecord]:
    """
    Apply client-side filters to records after IB data has been fetched.

    Symbols with missing (None) values for a filtered field are treated as
    failing that filter and are excluded.
    """
    kept: List[StockRecord] = []
    for rec in records:
        reason = _reject_reason(rec, config)
        if reason:
            log.info("Filtered out %s: %s", rec.symbol, reason)
        else:
            kept.append(rec)

    log.info("Screener filters: %d → %d records", len(records), len(kept))
    return kept


def _reject_reason(rec: StockRecord, cfg: ScreenerConfig) -> str:
    if cfg.atr_min is not None:
        if rec.atr is None:
            return f"ATR unavailable (min required: {cfg.atr_min}%)"
        if rec.atr < cfg.atr_min:
            return f"ATR {rec.atr:.2f}% < min {cfg.atr_min}%"

    if cfg.price_min is not None:
        # Use pre-market price as fallback when prev-close tick hasn't arrived
        effective_price = rec.price if rec.price is not None else rec.pre_market_price
        if effective_price is None:
            # Both unavailable — ATR filter already passed so it's unlikely a penny stock
            log.warning("%s: price data unavailable, skipping price_min check", rec.symbol)
        elif effective_price < cfg.price_min:
            return f"price {effective_price:.2f} < min {cfg.price_min}"

    if cfg.exclude_sectors:
        if rec.sector and rec.sector in cfg.exclude_sectors:
            return f"sector '{rec.sector}' is excluded"

    if cfg.pre_market_vol_min is not None:
        vol = rec.pre_market_volume
        if vol is not None and vol > 0 and vol < cfg.pre_market_vol_min:
            return f"pre_market_volume {vol:.0f} < min {cfg.pre_market_vol_min:.0f}"

    return ""


def filter_symbols_by_atr(
    symbols: List[str],
    atr_map: dict[str, float | None],
    atr_min: float | None,
) -> List[str]:
    """
    Quick pre-filter on ATR percentage values before fetching market data snapshots.
    Saves API calls for symbols that won't pass the ATR threshold.
    ATR values are expressed as percentages.
    """
    if atr_min is None:
        return symbols

    passed = []
    for sym in symbols:
        atr = atr_map.get(sym)
        if atr is None or atr < atr_min:
            log.info("Pre-filtered %s: ATR=%s%% (min=%s%%)", sym, atr, atr_min)
        else:
            passed.append(sym)

    log.info("ATR pre-filter: %d → %d symbols", len(symbols), len(passed))
    return passed
