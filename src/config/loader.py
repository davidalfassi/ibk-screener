from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import dacite
import yaml

log = logging.getLogger(__name__)


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class IBGatewayConfig:
    host: str = "127.0.0.1"
    port: int = 4002
    client_id: int = 1
    timeout: int = 30


@dataclass
class PacingConfig:
    historical_delay_seconds: float = 0.6
    contract_details_delay_seconds: float = 0.1
    market_data_delay_seconds: float = 0.1
    max_concurrent_mkt_data: int = 50


@dataclass
class OutputConfig:
    directory: str = "./output"
    filename_prefix: str = "premarket"


@dataclass
class PreMarketConfig:
    check_hours: bool = True
    tz: str = "America/New_York"


@dataclass
class CronConfig:
    day_of_week: str = "mon-fri"
    hour: int = 8
    minute: int = 0
    timezone: str = "America/New_York"


@dataclass
class SchedulerConfig:
    enabled: bool = False
    mode: str = "screener"
    cron: CronConfig = field(default_factory=CronConfig)


@dataclass
class AppConfig:
    ib_gateway: IBGatewayConfig = field(default_factory=IBGatewayConfig)
    pacing: PacingConfig = field(default_factory=PacingConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    pre_market: PreMarketConfig = field(default_factory=PreMarketConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    max_number_of_stocks: int = 70


@dataclass
class ScannerBatch:
    market_cap_min_usd: Optional[float] = None
    market_cap_max_usd: Optional[float] = None


@dataclass
class ScreenerConfig:
    scan_code: str = "TOP_PERC_GAIN"
    instrument: str = "STK"
    location_code: str = "STK.US.MAJOR"
    number_of_rows: int = 50
    market_cap_min_usd: Optional[float] = None
    market_cap_max_usd: Optional[float] = None
    avg_volume_min: Optional[int] = None
    exclude_etfs: bool = True
    atr_period: int = 14
    atr_min: Optional[float] = None
    price_min: Optional[float] = None
    pre_market_vol_min: Optional[float] = None
    exclude_sectors: List[str] = field(default_factory=list)
    scan_batches: List[ScannerBatch] = field(default_factory=list)


@dataclass
class WatchlistEntry:
    symbol: str
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"


# ── Loaders ───────────────────────────────────────────────────────────────────

def _read_yaml(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def load_settings(path: Path) -> AppConfig:
    raw = _read_yaml(path)
    try:
        return dacite.from_dict(AppConfig, raw, dacite.Config(strict=False))
    except dacite.DaciteError as e:
        raise ValueError(f"Invalid settings.yaml: {e}") from e


def load_screener(path: Path) -> ScreenerConfig:
    raw = _read_yaml(path)
    screener_raw = raw.get("screener", {})
    try:
        return dacite.from_dict(ScreenerConfig, screener_raw, dacite.Config(strict=False))
    except dacite.DaciteError as e:
        raise ValueError(f"Invalid screener.yaml: {e}") from e


def load_watchlist(path: Path) -> List[WatchlistEntry]:
    raw = _read_yaml(path)
    entries_raw = raw.get("watchlist", [])
    result = []
    for item in entries_raw:
        try:
            result.append(dacite.from_dict(WatchlistEntry, item, dacite.Config(strict=False)))
        except dacite.DaciteError as e:
            log.warning("Skipping invalid watchlist entry %s: %s", item, e)
    if not result:
        raise ValueError("watchlist.yaml contains no valid entries")
    return result
