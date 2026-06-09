# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run once
python3 main.py --mode screener
python3 main.py --mode watchlist

# Test connectivity without writing output
python3 main.py --mode watchlist --dry-run --no-hours-check

# Start scheduled daemon
python3 main.py --scheduler

# Verbose logging for debugging IB responses
python3 main.py --mode watchlist --log-level DEBUG --no-hours-check
```

IB Gateway or TWS must be running before any command. Port is set in `config/settings.yaml` (default `7497` = TWS paper; `4002` = Gateway paper).

## Architecture

The app pulls pre-market stock data from Interactive Brokers via `ib_async` (asyncio) and writes a YAML file sorted by pre-market change %.

**Two pipeline modes**, both in `src/pipeline/`:
- `screener_pipeline.py` — runs an IB scanner to discover symbols, then enriches and filters them
- `watchlist_pipeline.py` — fetches data for an explicit symbol list from `config/watchlist.yaml`; no filtering applied, all symbols always appear in output

**Screener pipeline data flow** (sequential, all async):
1. `scanner.py` → IB scanner returns up to 50 symbols (server-side filters: market cap, volume)
2. `contract_details.py` → resolves company name + sector for each symbol
3. `historical.py` → fetches 20 days of daily OHLC bars (sequential with pacing delay to respect IB's 60 req/10 min limit)
4. `atr.py` → computes Wilder ATR(14); symbols below `atr_min` are dropped *before* market data is fetched (saves API calls)
5. `market_data.py` → opens streaming `reqMktData` subscriptions (not snapshots — snapshots miss tick types 9 and 258 in pre-market), waits 10 s for ticks, reads, cancels
6. `enrichment.py` → assembles `StockRecord` dataclasses from contract info + snapshots + bars
7. `filters.py` → applies client-side sector and price filters
8. `writer.py` → formats numeric fields as strings, sorts by `pre_market_chg_pct` descending, writes YAML

**Config loading** (`src/config/loader.py`): YAML files are parsed into typed dataclasses via `dacite`. All three YAML files (`settings.yaml`, `screener.yaml`, `watchlist.yaml`) are loaded at startup to fail fast on malformed config.

**Scheduler** (`src/scheduler/runner.py`): Uses `AsyncIOScheduler` (not `BackgroundScheduler`) because `ib_async` requires the same asyncio event loop. The scheduler owns the event loop via `loop.run_forever()`.

## IB API Quirks

- `marketCapAbove` / `marketCapBelow` on `ScannerSubscription` are in **millions of USD**, not raw dollars — the scanner module divides config values by `1_000_000` before setting them.
- Market data uses `snapshot=False` (streaming) because `snapshot=True` frequently misses tick type 9 (prev close) and tick 258 (fundamentalRatios/market cap) during pre-market hours.
- Historical data must be fetched sequentially with ≥ 0.6 s delay; IB enforces a hard cap of 60 requests per 10 minutes.
- `clientId` collisions after an unclean disconnect are retried once with `client_id + 1` automatically.
- `scannerSubscriptionFilterOptions` TagValues (e.g. `usdMarketCapAbove`) require a paid data subscription and don't work on paper accounts — avoid them; use built-in `ScannerSubscription` attributes instead.

## Key Data Types

| Type | File | Role |
|---|---|---|
| `AppConfig` | `src/config/loader.py` | Top-level settings (gateway, pacing, output, scheduler) |
| `ScreenerConfig` | `src/config/loader.py` | Screener filters (both server-side and client-side) |
| `WatchlistEntry` | `src/config/loader.py` | Single symbol in a watchlist |
| `MarketSnapshot` | `src/ib/market_data.py` | Raw tick data from IB (price, close, volume, market cap) |
| `StockRecord` | `src/processing/enrichment.py` | Enriched record combining all IB data; input to filters and writer |
