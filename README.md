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

High-signal items for collaborators:

- **Two backtest engines (important)**:
  - `app.py` contains a **custom single-pattern engine** (`_simulate_trades`, `_backtest_pattern`, etc.) and the API endpoints currently use it.
  - `backtest.py` contains a **separate “all-patterns in one pass” engine** (`run_backtest`) used by the scheduler payload builder.
  - This duplication is intentional history, not ideal: changes to trading rules can drift between engines. If you plan to evolve the backtest logic, decide which engine is canonical and migrate callers.

- **Scheduler payload may be unused**:
  - `scheduler.py` describes caching an “`/api/all` payload”, but no `/api/all` route exists. The scheduled precompute may be legacy or incomplete.

- **On-demand backfill on request path**:
  - `data.get_ohlc()` triggers backfill before serving reads. Under load, this can cause latency spikes (network fetches + SQLite writes).

- **ProcessPool in request handlers**:
  - `/api/patterns` and `/api/complement` use `ProcessPoolExecutor` and pass large objects (pandas DataFrames) to workers. This is convenient but can be expensive (pickling overhead) and can become a bottleneck.

- **Repo hygiene**:
  - `venv/` is excluded via `.gitignore` and should never be committed. If you see it tracked, run `git rm -r --cached venv` to remove it.

## Collaboration notes

- **Device UUID**: strategies are scoped to `localStorage["candlelab_uuid"]` and passed to the backend. It’s meant for UX continuity, not security.
- **Charts**: Lightweight Charts is loaded via CDN; the chart code lives in `templates/index.html` and expects candle times in Unix seconds plus a list of recent trades for markers/equity.

