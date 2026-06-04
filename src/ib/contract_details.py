from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional

from ib_async import IB, Contract, Stock

log = logging.getLogger(__name__)


# Map IB's detailed categories to broader sector names
_CATEGORY_TO_SECTOR = {
    # Health Care sector
    "Pharmaceuticals": "Health Care",
    "Healthcare-Services": "Health Care",
    "Healthcare-Products": "Health Care",
    "Biotechnology": "Health Care",
    "Medical-Drugs": "Health Care",
    "Medical-HMO": "Health Care",
    "Medical-Instruments": "Health Care",
    "Medical-Supplies": "Health Care",
    
    # Energy sector
    "Oil & Gas": "Energy",
    "Oil & Gas Exploration": "Energy",
    "Oil & Gas Refining": "Energy",
    "Oil & Gas Storage": "Energy",
    "Oil & Gas Drilling": "Energy",
    "Coal": "Energy",
    "Utilities": "Utilities",
    
    # Consumer Defensive
    "Food Processing": "Consumer Defensive",
    "Beverages": "Consumer Defensive",
    "Tobacco": "Consumer Defensive",
    "Grocery": "Consumer Defensive",
}


def _map_category_to_sector(category: str) -> str:
    """Map IB's detailed category to a broader sector name."""
    if not category:
        return ""
    # Try exact match first
    if category in _CATEGORY_TO_SECTOR:
        return _CATEGORY_TO_SECTOR[category]
    # Return the category as-is if no mapping exists
    return category


@dataclass
class ContractInfo:
    symbol: str
    company_name: str
    sector: str
    primary_exchange: str
    contract: Contract  # fully qualified contract for subsequent IB calls


async def fetch_contract_details(
    ib: IB,
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
) -> Optional[ContractInfo]:
    """Resolve a symbol to a qualified contract and retrieve company metadata."""
    contract = Stock(symbol, exchange, currency)
    try:
        details_list = await ib.reqContractDetailsAsync(contract)
    except Exception as e:
        log.warning("reqContractDetails failed for %s: %s", symbol, e)
        return None

    if not details_list:
        log.warning("No contract details found for %s — symbol may be invalid or delisted", symbol)
        return None

    if len(details_list) > 1:
        log.debug("%s matched %d contracts, using first result", symbol, len(details_list))

    detail = details_list[0]
    
    # Map IB's detailed category to broader sector name
    category = detail.category or ""
    sector = _map_category_to_sector(category)
    
    log.debug(
        "%s: category=%s → sector=%s, industry=%s, longName=%s",
        symbol,
        category,
        sector,
        getattr(detail, 'industry', 'N/A'),
        detail.longName,
    )
    
    return ContractInfo(
        symbol=symbol,
        company_name=detail.longName or symbol,
        sector=sector,
        primary_exchange=detail.contract.primaryExchange or exchange,
        contract=detail.contract,
    )


async def fetch_all_contract_details(
    ib: IB,
    symbols: List[str],
    delay: float = 0.1,
) -> dict[str, ContractInfo]:
    """Fetch contract details for a list of symbols sequentially with pacing."""
    results: dict[str, ContractInfo] = {}
    for symbol in symbols:
        info = await fetch_contract_details(ib, symbol)
        if info:
            results[symbol] = info
        await asyncio.sleep(delay)
    log.info("Resolved %d / %d contract details", len(results), len(symbols))
    return results
