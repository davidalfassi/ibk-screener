from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import yaml

from src.config.loader import OutputConfig
from src.processing.enrichment import StockRecord

log = logging.getLogger(__name__)


# ── Formatters ─────────────────────────────────────────────────────────────────

def _fmt_market_cap(usd: Optional[float]) -> Optional[str]:
    if usd is None:
        return None
    if usd >= 1_000_000_000:
        return f"{usd / 1_000_000_000:.2f}B"
    if usd >= 1_000_000:
        return f"{usd / 1_000_000:.2f}M"
    return f"{usd:.0f}"


def _fmt_chg_pct(pct: Optional[float]) -> Optional[str]:
    if pct is None:
        return None
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.2f}%"


def _fmt_volume(vol: Optional[float]) -> Optional[str]:
    if vol is None:
        return None
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.2f}M"
    if vol >= 1_000:
        return f"{vol / 1_000:.2f}K"
    return f"{vol:.0f}"


def _fmt_price(price: Optional[float]) -> Optional[float]:
    if price is None:
        return None
    return round(price, 2)


def _fmt_atr(atr: Optional[float]) -> Optional[str]:
    if atr is None:
        return None
    return f"{atr:.2f}%"


# ── Serialisation ──────────────────────────────────────────────────────────────

def _record_to_dict(rec: StockRecord) -> dict:
    return {
        "symbol": rec.symbol,
        "company_name": rec.company_name,
        "market_cap": _fmt_market_cap(rec.market_cap_usd),
        "pre_market_chg": _fmt_chg_pct(rec.pre_market_chg_pct),
        "pre_market_volume": _fmt_volume(rec.pre_market_volume),
        "pre_market_price": _fmt_price(rec.pre_market_price),
        "atr": _fmt_atr(rec.atr),
        "price": _fmt_price(rec.price),
    }


def _sort_key(rec: StockRecord) -> float:
    # None values sink to the bottom (use -inf as sort key)
    return rec.pre_market_chg_pct if rec.pre_market_chg_pct is not None else float("-inf")


# ── Writer ─────────────────────────────────────────────────────────────────────

def write_output(
    records: List[StockRecord],
    config: OutputConfig,
    max_stocks: Optional[int] = None,
) -> Path:
    """Sort records by pre-market change %, cap to top N, format fields, and write YAML file."""
    output_dir = Path(config.directory)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{config.filename_prefix}_{timestamp}.yaml"
    filepath = output_dir / filename

    sorted_records = sorted(records, key=_sort_key, reverse=True)
    if max_stocks is not None:
        sorted_records = sorted_records[:max_stocks]
    payload = {"stocks": [_record_to_dict(r) for r in sorted_records]}

    with open(filepath, "w") as f:
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    log.info("Output written to %s (%d records)", filepath, len(sorted_records))
    return filepath
