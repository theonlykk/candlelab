# CandleLab

CandleLab is a small Flask web app that helps a beginner build a simple, rule-based trading “strategy” in a guided 9‑step wizard. It backtests candlestick-pattern signals on recent market data (pulled via TradingView), shows a price chart with trade markers, and renders a simple equity curve based on fixed win/loss outcomes.

> Note: CandleLab is an educational/prototyping tool. The “device UUID” is a UX identifier, not authentication.

## What it does

- **Wizard (9 steps)**: pick instrument → timeframe → strategy direction → anchor candlestick pattern → optional complement pattern → optional indicator filter → session filter → SL/TP sliders → review + “go live”
- **Backtest summary**: win rate, signal count, trade log, basic stats, and a simple equity curve (starts at $1000)
- **Saved strategies**: “My Strategies” overlay lists strategies saved for this browser/device and shows live PnL derived from stored trades
- **AI text (optional)**: uses Anthropic to generate short tips/commentary/tweaks if configured

## Tech stack

- **Backend**: Python 3.11, Flask
- **Server**: Gunicorn (1 worker, 4 threads in Docker)
- **Data/compute**: pandas, numpy
- **Market data**: `tvdatafeed-enhanced` (TradingView wrapper; no explicit API key in this repo)
- **Caching**: Redis (optional)
- **Scheduler**: APScheduler (background refresh job; see “Known issues”)
- **AI (optional)**: `anthropic` SDK
- **Frontend**: single-file `templates/index.html` (vanilla JS + inline CSS)
- **Charts**: TradingView Lightweight Charts v3.8.0 (via CDN)

## Running locally

### 1) Create and activate a virtualenv

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Environment variables

All variables are optional unless noted.

- **`PORT`**: port for Flask dev server when running `python app.py` (default: `7860`)
- **`DB_PATH`**: path to SQLite OHLC database (default: `fx_ohlc.db`)
- **`REDIS_URL`**: enable Redis caching (example: `redis://localhost:6379/0`)
- **Anthropic**: the SDK uses your standard Anthropic environment configuration (commonly `ANTHROPIC_API_KEY`). If not set or if the call fails, the app falls back to non-AI/default text in several places.

### 3) Initialize the OHLC database (recommended)

```bash
python -c "from data import init_db; init_db()"
```

### 4) Run the app

Simple (Flask dev server):

```bash
python app.py
```

Then open `http://localhost:7860`.

Production-like (Gunicorn):

```bash
gunicorn -b 0.0.0.0:7860 --workers 1 --threads 4 --timeout 120 app:app
```

### Docker

The Docker image installs requirements, initializes the DB, then starts Gunicorn.

```bash
docker build -t candlelab .
docker run --rm -p 7860:7860 -e REDIS_URL=redis://host.docker.internal:6379/0 candlelab
```

## API endpoints

All endpoints are served by `app.py`.

- **`GET /`**: serves the single-page wizard UI (`templates/index.html`)
- **`GET /api/patterns`**: returns backtest stats for all patterns (ranked), for the chosen instrument/timeframe/direction
  - Query params: `instrument`, `interval`, `direction`
- **`POST /api/complement`**: suggests complement pattern + connector combinations that improve win rate vs the anchor alone
  - JSON body: `anchor`, `direction`, `instrument`, `interval`
- **`POST /api/indicator-check`**: compares anchor win rate with vs without a selected indicator filter
  - JSON body includes: `anchor`, `instrument`, `interval`, `indicator`, plus optional combo fields
- **`POST /api/finalise`**: preview or finalize a strategy; when not previewing, also saves a strategy and returns chart payloads
  - JSON body: `preview` (bool), plus wizard config fields (instrument/interval/anchor/etc.)
- **`POST /api/analyse`**: AI-generated 3-sentence commentary for the final strategy result (falls back on failure)
- **`POST /api/tweaks`**: AI-generated tweak suggestions (falls back on failure)
- **`GET /api/live`**: list saved strategies for a given device UUID (used by “My Strategies” overlay)
  - Query params: `device_uuid` (also accepted via `X-Device-UUID`)
- **`DELETE /api/strategy/<id>`**: soft-delete a saved strategy (device UUID scoped)
  - Query params: `device_uuid`

## File structure

Top-level files (core):

- **`app.py`**: Flask app, API routes, ProcessPool-based analysis, and the **custom per-pattern backtest engine** used by the API
- **`data.py`**: TradingView OHLC fetch + SQLite persistence + on-demand backfill logic
- **`patterns.py`**: candlestick pattern detectors + registry (`PATTERNS`, `detect_all`)
- **`indicators.py`**: MA/RSI confirmation filters (numpy implementations)
- **`strategy_store.py`**: SQLite persistence for user strategies and logged trades
- **`cache.py`**: Redis cache wrapper (optional; degrades gracefully when Redis unavailable)
- **`scheduler.py`**: APScheduler background job that precomputes a cached payload (currently not clearly consumed)
- **`backtest.py`**: an alternate backtest engine that evaluates all patterns in one pass (see note below)

Frontend:

- **`templates/index.html`**: single-file UI (HTML + inline CSS + JS wizard state machine, API calls, chart rendering, “My Strategies” overlay)

Other:

- **`Dockerfile`**: container build + Gunicorn entrypoint
- **`requirements.txt`**: Python dependencies
- **`scripts/Project_to_text.py`**: optional utility script for dumping the repo into a text file (ignored by git)

## Known issues / deferred work

### UX and frontend

- **Decouple price chart and equity chart**: remove all sync code; each chart uses `fitContent` independently. Price chart shows full 30 days, equity chart shows full range always. Bidirectional scroll sync is currently broken and causes one chart to freeze.
- **Welcome screen**: larger headline with pulse animation on Start button, more striking financial background illustration.
- **Step transitions**: slide-up animation when advancing steps, slide-down when going back, 250 ms ease-out.
- **Step 4 AI tip**: typewriter effect so characters appear one by one over 1 second, making it feel like the AI is responding live.
- **Go Live button**: change to amber/gold `#F5A623` to differentiate it as the final commitment action.
- **Sticky header**: show step number like `4/9` on all steps, not just some.
- **Instrument sparklines**: show a tiny price sparkline on each instrument card in step 1 so users can see recent price action before choosing.
- **My Strategies page**: full tracking overlay showing live PnL per strategy since going live, signal count, coloured status dot, and delete button.

### Product features

- **Smart market suggestions**: replace hardcoded Gold recommendations in `/api/analyse` with data-driven suggestions based on which instruments have the most signals for the current pattern type over the last 30 days.
- **tryOnMarket fix**: ensure the Try on Gold button passes the complete strategy config unchanged, only swapping the instrument.
- **Progressive identity**: after going live, show a prompt asking for email to enable cross-device strategy access. Implement magic-link auth. Upgrade device UUID to a proper user account in PostgreSQL when email is provided.
- **n_bars increase**: bump `tvdatafeed` fetch from 12 000 to 50 000 bars to get 6+ months of history for more statistically meaningful backtests.
- **Out-of-sample testing**: option to train on first 60 days and test on next 30 days to check if edge persists.
- **Honesty disclaimer**: AI commentary should always flag when signal count is below 30 as statistically insufficient, and warn that 60 %+ win rates on short backtests often reflect overfitting.
- **Full CSV export**: download should include every candle in the backtest period with columns: `timestamp`, `open`, `high`, `low`, `close`, `volume`, `5-period SMA`, `20-period SMA`, `ATR(14)`, `RSI(14)`, pattern signal value for the anchor pattern, complement pattern signal, indicator filter pass/fail, whether a trade was active on that candle, trade entry price, trade TP level, trade SL level, MTM PnL on that candle, cumulative equity. This allows independent recreation and verification of backtest results in Excel or Python.
- **Session analysis**: after going live, show which sessions the strategy fired most in, to guide session filter choice on a second iteration.
- **OANDA integration**: port strategy to OANDA REST API for live auto-execution. $1 activation fee. Adapter pattern so other brokers (IBKR, MT5, Alpaca) can be added later.
- **Live signal detector**: APScheduler background worker that watches incoming candles for the user's live strategies and fires OANDA orders when patterns trigger.

### Technical debt (from code audit)

- **Two backtest engines**: `app.py` contains a custom single-pattern engine used by all API routes; `backtest.py` contains a separate all-patterns engine used by the scheduler. Decide which is canonical and migrate all callers. Changes to trading rules currently need to be made in two places.
- **Scheduler payload unused**: `scheduler.py` precomputes and caches a payload describing an `/api/all` response, but no `/api/all` route exists. Either add the route or rewrite the scheduler to precompute `/api/patterns` for common instrument/interval/direction combinations, which are actually consumed.
- **Backfill on request path**: `data.get_ohlc()` triggers a TradingView fetch before serving reads, which can cause latency spikes. Move backfill to a background job only; serve reads from SQLite cache always.
- **ProcessPool overhead**: `/api/patterns` and `/api/complement` pass large pandas DataFrames into subprocesses via pickling. Consider converting to NumPy arrays before pickling or using shared memory.
- **Exception swallowing**: multiple bare `except Exception` blocks hide systemic failures. Add structured logging with error context so production issues are diagnosable.
- **Redis fail-once** (fixed in `refactor/cleanup`): caching now retries on every call if Redis was temporarily unavailable at startup.

### Future vision

- **Social comparison**: show how many other users are running similar strategies and their aggregate win rates.
- **Strategy marketplace**: users can publish strategies for others to clone and test.
- **Multi-timeframe confirmation**: require the signal to appear on both 5 m and 15 m before firing.
- **Walk-forward testing**: automated rolling-window backtest to check if edge persists over time.
- **Broker integration beyond OANDA**: Interactive Brokers via `ib_insync`, MetaTrader 5 via the `mt5` Python library, Alpaca for US equities.

## Collaboration notes

- **Device UUID**: strategies are scoped to `localStorage["candlelab_uuid"]` and passed to the backend. It’s meant for UX continuity, not security.
- **Charts**: Lightweight Charts is loaded via CDN; the chart code lives in `templates/index.html` and expects candle times in Unix seconds plus a list of recent trades for markers/equity.

