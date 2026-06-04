from __future__ import annotations

import asyncio
import logging
from typing import List, Optional
import xml.etree.ElementTree as ET

from ib_async import IB, Contract

log = logging.getLogger(__name__)


async def fetch_market_cap(ib: IB, contract: Contract) -> Optional[float]:
    """
    Fetch market cap in USD using IB's fundamental data API.
    
    Returns None if data is unavailable.
    Market cap is returned in millions, so we multiply by 1,000,000.
    """
    try:
        # Request fundamental data (ReportType: "ReportsFinSummary" includes market cap)
        fundamental_data = await ib.reqFundamentalDataAsync(
            contract,
            reportType="ReportsFinSummary",
        )
        
        if not fundamental_data:
            log.debug("%s: No fundamental data returned", contract.symbol)
            return None
        
        # Parse XML response to extract market cap
        try:
            root = ET.fromstring(fundamental_data)
            
            # Search for MarketCap in the XML
            # The structure varies, so we search all elements
            for elem in root.iter():
                if 'MarketCap' in elem.tag or 'marketcap' in elem.tag.lower():
                    try:
                        value = float(elem.text)
                        # Market cap from IB is typically in millions
                        market_cap_usd = value * 1_000_000
                        log.debug("%s: market cap = $%.0f", contract.symbol, market_cap_usd)
                        return market_cap_usd
                    except (ValueError, TypeError):
                        continue
            
            log.debug("%s: MarketCap element not found in fundamental data", contract.symbol)
            return None
            
        except ET.ParseError as e:
            log.debug("%s: Failed to parse fundamental data XML: %s", contract.symbol, e)
            return None
        
    except Exception as e:
        log.debug("%s: reqFundamentalData failed: %s", contract.symbol, e)
        return None


async def fetch_all_market_caps(
    ib: IB,
    contracts: List[Contract],
    delay: float = 0.5,
) -> dict[str, Optional[float]]:
    """Fetch market cap for all contracts sequentially with pacing."""
    results: dict[str, Optional[float]] = {}
    for contract in contracts:
        market_cap = await fetch_market_cap(ib, contract)
        results[contract.symbol] = market_cap
        await asyncio.sleep(delay)
    
    # Log how many we successfully fetched
    successful = sum(1 for v in results.values() if v is not None)
    log.info("Fetched market cap for %d / %d symbols", successful, len(contracts))
    return results
