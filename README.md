# IBK-Pull-Date

Pulls real-time pre-market stock and ETF data from Interactive Brokers Gateway and writes a YAML file sorted by pre-market change %.

Two input modes:
- **Screener** — filter the market by cap, ATR, volume, sector
- **Watchlist** — fetch data for a specific list of symbols

---

## Requirements

- Python 3.9+
- Interactive Brokers account (paper or live)
- IB Gateway or TWS running locally with API access enabled

---

## Installation

### 1. Clone / open the project folder

```bash
cd /path/to/IBK-Pull-Date
```

### 2. (Recommended) Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate      # macOS / Linux
# .venv\Scripts\activate       # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

Dependencies installed:

| Package | Purpose |
|---|---|
| `ib_async` | IB Gateway / TWS API client (asyncio) |
| `APScheduler` | Cron-style scheduler |
| `PyYAML` | Config and output file parsing |
| `click` | CLI interface |
| `dacite` | YAML dict → typed dataclasses |
| `pytz` | Timezone support for pre-market hours check |

### 4. Enable IB Gateway API access

In IB Gateway (or TWS):
1. Go to **Configure → Settings → API → Settings**
2. Enable **"Enable ActiveX and Socket Clients"**
3. Set the **Socket port** to match `config/settings.yaml` (default: `4002` for Gateway paper)
4. Add `127.0.0.1` to the **Trusted IP addresses** list (or leave blank to allow localhost)

---

## Configuration

All configuration is in the `config/` directory. Edit these files before running.

### `config/settings.yaml` — Connection & scheduler

```yaml
ib_gateway:
  host: "127.0.0.1"
  port: 4002        # 4002=Gateway paper | 4001=Gateway live
                    # 7497=TWS paper     | 7496=TWS live
  client_id: 1
  timeout: 30

scheduler:
  enabled: false
  mode: "screener"          # screener | watchlist
  cron:
    day_of_week: "mon-fri"
    hour: 8                 # 08:00 AM New York time
    minute: 0
    timezone: "America/New_York"

max_number_of_stocks: 70    # top N stocks by pre_market_chg_pct emitted to output
```

**Key setting:** Set `port` to match your IB Gateway/TWS configuration.

### `config/screener.yaml` — Screener filters (Option 1)

```yaml
screener:
  scan_code: "TOP_PERC_GAIN"     # IB scanner ranking (pre-market gainers)
  number_of_rows: 50             # rows per batch (IB hard cap: 50)

  # Server-side filters (IB applies these before returning results)
  market_cap_min_usd: 2000000000  # $2 billion minimum
  avg_volume_min: 1000000         # 1M shares average daily volume

  # Client-side filters (applied after data is fetched)
  atr_min: 4.0                   # ATR(14) minimum in dollars
  price_min: 2.0                 # minimum stock price
  pre_market_vol_min: 1200       # minimum pre-market shares traded
  exclude_sectors:
    - "Health Care"
    # - "Energy"
    # - "Utilities"

  # Batch scanner — splits the market-cap universe into non-overlapping ranges.
  # Each batch runs one IB scanner call (up to 50 rows). Results are combined,
  # deduplicated, enriched, and sorted by pre_market_chg_pct (highest first).
  scan_batches:
    - market_cap_min_usd: 2000000000    # $2B – $10B
      market_cap_max_usd: 10000000000
    - market_cap_min_usd: 10000000000   # $10B – $50B
      market_cap_max_usd: 50000000000
    - market_cap_min_usd: 50000000000   # $50B+
      market_cap_max_usd: null
```

The three batches give up to **150 unique candidates**. After enrichment and filtering the top `max_total_rows` gainers are written to output.

**Available `scan_code` values** (IB scanner ranking modes):

| Code | Description |
|---|---|
| `TOP_PERC_GAIN` | Top % gainers (pre-market) |
| `TOP_PERC_LOSE` | Top % losers (pre-market) |
| `HOT_BY_VOLUME` | Highest pre-market volume |
| `TOP_TRADE_RATE` | Highest trade rate |

### `config/watchlist.yaml` — Explicit symbol list (Option 2)

```yaml
watchlist:
  - symbol: "AAPL"
    sec_type: "STK"
  - symbol: "SPY"
    sec_type: "STK"   # ETFs also use STK on IB's SMART routing
```

Add or remove symbols as needed. All listed symbols always appear in the output (no filtering applied), with `null` for any unavailable fields.

---

## How to Run

> **Important:** IB Gateway (or TWS) must be running and logged in before executing any command.

### Run once — screener mode

```bash
python3 main.py --mode screener
```

Uses filters from `config/screener.yaml`. Runs the IB scanner, fetches pre-market data, and writes a YAML file to `./output/`.

### Run once — watchlist mode

```bash
python3 main.py --mode watchlist
```

Fetches pre-market data for all symbols in `config/watchlist.yaml`.

### Start the scheduler (repeating runs)

```bash
python3 main.py --scheduler
```

Runs in the foreground as a daemon. Triggers the configured pipeline (`screener` or `watchlist`) at the cron schedule set in `config/settings.yaml`. Press `Ctrl+C` to stop.

### Test without writing output

```bash
python3 main.py --mode watchlist --dry-run
```

Connects to IB, fetches all data, logs results, but does **not** write a YAML file. Useful for validating connectivity.

### Run outside pre-market hours (testing)

```bash
python3 main.py --mode watchlist --no-hours-check
```

By default the app warns if run outside 4:00–9:30 AM ET on weekdays (pre-market data may be unavailable or stale). This flag suppresses that warning.

### All CLI options

| Option | Default | Description |
|---|---|---|
| `--mode screener` | — | Run screener pipeline immediately |
| `--mode watchlist` | — | Run watchlist pipeline immediately |
| `--scheduler` | — | Start APScheduler cron daemon |
| `--config-dir PATH` | `./config` | Override config directory |
| `--dry-run` | off | Fetch data but skip file write |
| `--no-hours-check` | off | Skip pre-market hours validation |
| `--log-level` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `-h, --help` | — | Show help |

---

## Output

Files are written to `./output/` with the naming pattern:

```
output/premarket_20260525_080012.yaml
```

### Output format

```yaml
stocks:
  - symbol: "NVDA"
    company_name: "NVIDIA Corporation"
    market_cap: "2.85T"
    pre_market_chg: "+3.45%"
    pre_market_volume: "1.23M"
    pre_market_price: 142.50
    atr: 8.32
    price: 137.80

  - symbol: "AAPL"
    company_name: "Apple Inc"
    market_cap: "3.10T"
    pre_market_chg: "+1.20%"
    pre_market_volume: "890.45K"
    pre_market_price: 212.30
    atr: 4.15
    price: 209.77
```

Results are **sorted by `pre_market_chg` (descending)** — highest gainers first. Fields with unavailable data appear as `null`.

---

## Pre-Market Hours

IB pre-market data is available **4:00 AM – 9:30 AM ET, Monday–Friday**. Running outside these hours will still work but may return stale or null prices. Use `--no-hours-check` to suppress the warning.

---

## Project Structure

```
IBK-Pull-Date/
├── main.py                    CLI entry point
├── requirements.txt
├── config/
│   ├── settings.yaml          IB connection, pacing, scheduler
│   ├── screener.yaml          Screener filters (Option 1)
│   └── watchlist.yaml         Symbol list (Option 2)
├── output/                    Generated YAML files (auto-created)
└── src/
    ├── config/loader.py       Loads YAML config into typed dataclasses
    ├── ib/
    │   ├── client.py          IB connection lifecycle manager
    │   ├── scanner.py         IB Scanner (market cap / volume server filters)
    │   ├── contract_details.py Company name + sector lookup
    │   ├── historical.py      Daily OHLC bars for ATR calculation
    │   └── market_data.py     Pre-market price / volume / change snapshot
    ├── processing/
    │   ├── atr.py             Wilder ATR(14) calculation
    │   ├── enrichment.py      Assembles StockRecord from all IB data
    │   └── filters.py         ATR pre-filter + sector / price post-filter
    ├── output/writer.py       Formats numbers and writes YAML output
    ├── pipeline/
    │   ├── screener_pipeline.py  Full screener flow
    │   └── watchlist_pipeline.py Full watchlist flow
    └── scheduler/runner.py    APScheduler asyncio daemon
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `ConnectionRefusedError` | IB Gateway not running, or wrong port in `settings.yaml` |
| `TimeoutError` on connect | IB Gateway running but API not enabled — check Configure → API → Settings |
| Scanner returns 0 symbols | Filters too strict, or `scan_code` not valid for current market hours |
| All fields `null` in output | Running outside pre-market hours — use `--no-hours-check` to confirm |
| `dacite.DaciteError` | YAML config has a type mismatch — check indentation and value types |
| `clientId already in use` | Previous run crashed — wait 10s or change `client_id` in `settings.yaml` |
