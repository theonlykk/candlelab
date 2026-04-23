# Project Context
This repository is part of a quantitative finance ecosystem (CandleLab / Oanda-Trading). 
We are building institutional-grade algorithms, execution engines, and dashboards. 
Performance, concurrency, and reliability are strictly prioritized over "quick and dirty" implementations.
# Infrastructure & Topology
- Environment: Dockerized containers hosted on Railway.
- Server: Python Flask running via Gunicorn.
- Concurrency: Gunicorn uses `gthread` workers. Multiple workers and multiple services boot simultaneously.
- Database: A single, shared Postgres instance accessed by all services. 
- Broker API: OANDA v3 REST API.
# Critical Architectural Rules (NEVER BREAK THESE)
## 1. Database Migrations & DDL (The "Lock" Rule)
- NEVER write module-level DDL (`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`).
- NEVER place database migrations or schema alterations inside application startup code, `__init__.py`, or module global scope. 
- Application code must strictly assume the schema is already in the correct state.
- All schema changes must be written as standalone SQL statements intended to be run manually via the root `db_migrate.py` script.
- Any manual SQL generation must include: `SET lock_timeout = '5s';` to prevent AccessExclusiveLock queue pile-ups.
## 2. Postgres Connection Safety
- All SQLAlchemy Engine instantiations MUST include `pool_pre_ping=True` to prevent poisoned connections from hanging Gunicorn workers (e.g., recovering from 524 Cloudflare timeouts).
- Queries running inside executor loops must gracefully handle `OperationalError` and never crash the main thread.
## 3. Concurrency & State
- Code runs in a multi-worker, multi-instance environment. Do not use local memory (e.g., Python dicts) for state that must be shared across workers unless it is explicitly an isolated cache.
- The system evaluates trades via high-frequency executor loops. API calls inside these loops (like `PUT /trades/{id}/close`) MUST be wrapped in `try/except` blocks and should ideally be dispatched concurrently to prevent blocking the tick cycle.
## 4. OANDA API Handling
- Never use local timestamps (`pd.Timestamp.utcnow()`) for trade execution times. Always extract the authoritative `time` field from OANDA's `ORDER_FILL` transaction.
- When filtering OANDA transactions, remember that `type` filters behave inconsistently. To fetch SL/TP triggers or Closes, filter the full transaction stream by `tradeID`, not `clientOrderID`.
## 5. Fallbacks and Risk Management
- Any environment variables dictating trade risk (e.g., `MAX_CONCURRENT_TRADES`) MUST default to a safe, conservative number (e.g., `3`) or fail closed (`0`). Never fallback to high or infinite limits.
