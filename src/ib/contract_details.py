from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional

from ib_async import IB, Contract, Stock

log = logging.getLogger(__name__)


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
    return ContractInfo(
        symbol=symbol,
        company_name=detail.longName or symbol,
        sector=detail.category or "",
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
