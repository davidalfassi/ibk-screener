from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from ib_async import BarData, Contract

from src.ib.contract_details import ContractInfo
from src.ib.market_data import MarketSnapshot
from src.processing.atr import calculate_atr

log = logging.getLogger(__name__)


@dataclass
class StockRecord:
    symbol: str
    company_name: str
    sector: str
    contract: Contract

    # Raw numeric values (used for filtering and sorting)
    market_cap_usd: Optional[float]
    pre_market_price: Optional[float]
    pre_market_volume: Optional[float]
    pre_market_chg_pct: Optional[float]
    price: Optional[float]          # previous regular-session close
    atr: Optional[float]


def build_records(
    contract_infos: Dict[str, ContractInfo],
    snapshots: Dict[str, MarketSnapshot],
    bars_map: Dict[str, List[BarData]],
    atr_period: int = 14,
) -> List[StockRecord]:
    """Assemble a StockRecord for every symbol that has contract details."""
    records: List[StockRecord] = []

    for symbol, info in contract_infos.items():
        snap = snapshots.get(symbol)
        bars = bars_map.get(symbol, [])
        atr_val = calculate_atr(bars, atr_period) if bars else None

        records.append(StockRecord(
            symbol=symbol,
            company_name=info.company_name,
            sector=info.sector,
            contract=info.contract,
            market_cap_usd=snap.market_cap_usd if snap else None,
            pre_market_price=snap.pre_market_price if snap else None,
            pre_market_volume=snap.pre_market_volume if snap else None,
            pre_market_chg_pct=snap.pre_market_chg_pct if snap else None,
            price=snap.prev_close if snap else None,
            atr=atr_val,
        ))

    log.info("Built %d stock records", len(records))
    return records
