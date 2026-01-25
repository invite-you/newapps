# DB Outage Pipeline Retry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Stop the current collection cycle when the DB stays unavailable after 3 retries (5/10/20s), return a DB-unavailable exit code, and cool down 60s before the next cycle.

**Architecture:** Detect DB outages in connection helpers and raise `DatabaseUnavailableError`, propagate that error through collectors and entrypoints, convert it to a special exit code in child scripts, and have the full pipeline treat that code as a cycle abort with cooldown in daemon mode.

**Tech Stack:** Python 3, psycopg, pytest, subprocess, logging.

### Task 1: DB outage detection in sitemap DB + retry semantics

**Files:**
- Modify: `database/sitemap_apps_db.py`
- Modify: `database/app_details_db.py`
- Test: `tests/test_db_outage_handling.py`

**Step 1: Write the failing test**

```python
def test_sitemap_get_connection_raises_db_unavailable(monkeypatch):
    def raise_operational(*_args, **_kwargs):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg, "connect", raise_operational)
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    sitemap_apps_db._DB_CONNECTION = None

    with pytest.raises(DatabaseUnavailableError):
        sitemap_apps_db.get_connection()
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_db_outage_handling.py::test_sitemap_get_connection_raises_db_unavailable -v`
Expected: FAIL (DatabaseUnavailableError not raised).

**Step 3: Write minimal implementation**

```python
# database/sitemap_apps_db.py
from database.db_errors import DatabaseUnavailableError

DB_OUTAGE_MAX_RETRIES = int(os.getenv("SITEMAP_DB_OUTAGE_MAX_RETRIES", "3"))
DB_OUTAGE_BACKOFFS_RAW = os.getenv("SITEMAP_DB_OUTAGE_BACKOFFS_SEC", "5,10,20")

def _connect_once() -> psycopg.Connection:
    dsn = _build_dsn()
    return psycopg.connect(dsn, row_factory=dict_row)

def _get_db_outage_backoffs() -> list[float]:
    backoffs = []
    for item in DB_OUTAGE_BACKOFFS_RAW.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            backoffs.append(float(item))
        except ValueError:
            continue
    if not backoffs:
        backoffs = [5.0, 10.0, 20.0]
    if DB_OUTAGE_MAX_RETRIES <= 0:
        return backoffs[:1]
    if len(backoffs) < DB_OUTAGE_MAX_RETRIES:
        backoffs.extend([backoffs[-1]] * (DB_OUTAGE_MAX_RETRIES - len(backoffs)))
    return backoffs[:DB_OUTAGE_MAX_RETRIES]

def _is_db_unavailable_error(exc: BaseException) -> bool:
    return isinstance(exc, (DatabaseUnavailableError, psycopg.OperationalError, psycopg.errors.AdminShutdown)) \
        or "connection refused" in str(exc).lower()

def _reset_connection() -> None:
    global _DB_CONNECTION
    if _DB_CONNECTION and not _DB_CONNECTION.closed:
        _DB_CONNECTION.close()
    _DB_CONNECTION = None

def _raise_db_unavailable(context: str, exc: BaseException) -> None:
    for attempt, backoff in enumerate(_get_db_outage_backoffs(), 1):
        DB_LOGGER.error(
            "[DB UNAVAILABLE] context=%s attempt=%s/%s backoff=%.1fs error=%s",
            context, attempt, DB_OUTAGE_MAX_RETRIES, backoff, type(exc).__name__,
        )
        _reset_connection()
        try:
            conn = _connect_once()
        except psycopg.OperationalError as retry_exc:
            exc = retry_exc
            if attempt < DB_OUTAGE_MAX_RETRIES:
                time.sleep(backoff)
            continue
        else:
            conn.close()
            return
    raise DatabaseUnavailableError(f"Database unavailable during {context}") from exc

def get_connection() -> psycopg.Connection:
    try:
        return _connect_with_retry()
    except psycopg.OperationalError as exc:
        _raise_db_unavailable("get_connection", exc)
```

Also adjust `database/app_details_db.py` `_raise_db_unavailable()` to `return` when a retry connect succeeds instead of always raising.

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_db_outage_handling.py::test_sitemap_get_connection_raises_db_unavailable -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add database/sitemap_apps_db.py database/app_details_db.py tests/test_db_outage_handling.py
git commit -m "feat: add DB outage detection for sitemap DB"
```

### Task 2: Propagate DB-unavailable exit code from entrypoints

**Files:**
- Modify: `collect_app_details.py`
- Modify: `collect_sitemaps.py`
- Modify: `collect_full_pipeline.py`
- Test: `tests/test_db_outage_handling.py`

**Step 1: Write failing tests**

```python
def test_collect_sitemaps_returns_db_exit_code(monkeypatch):
    def raise_db_unavailable(*_args, **_kwargs):
        raise DatabaseUnavailableError("db down")

    monkeypatch.setattr(collect_sitemaps, "init_database", raise_db_unavailable)
    monkeypatch.setattr(sys, "argv", ["collect_sitemaps.py", "--stats"])

    result = collect_sitemaps.main()
    assert result == DB_UNAVAILABLE_EXIT_CODE
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_db_outage_handling.py::test_collect_sitemaps_returns_db_exit_code -v`
Expected: FAIL (exit code is 1 or exception).

**Step 3: Write minimal implementation**

```python
# collect_app_details.py / collect_sitemaps.py
from database.db_errors import DatabaseUnavailableError, DB_UNAVAILABLE_EXIT_CODE

def main() -> int:
    try:
        init_database()
        ...
        return 0
    except DatabaseUnavailableError:
        return DB_UNAVAILABLE_EXIT_CODE

if __name__ == "__main__":
    raise SystemExit(main())
```

Also update `collect_full_pipeline.run_script()` to raise `DatabaseUnavailableError`
when child exit code equals `DB_UNAVAILABLE_EXIT_CODE`.

**Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_db_outage_handling.py::test_collect_sitemaps_returns_db_exit_code -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add collect_app_details.py collect_sitemaps.py collect_full_pipeline.py tests/test_db_outage_handling.py
git commit -m "feat: propagate DB-unavailable exit code in entrypoints"
```

### Task 3: Daemon cooldown on DB outage in full pipeline

**Files:**
- Modify: `collect_full_pipeline.py`
- Test: `tests/test_db_outage_handling.py`

**Step 1: Write failing test**

```python
def test_daemon_cooldown_on_db_unavailable(monkeypatch):
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        raise SystemExit

    monkeypatch.setattr(collect_full_pipeline, "_shutdown_requested", False)
    monkeypatch.setattr(time, "sleep", fake_sleep)

    def raise_db_unavailable(*_args, **_kwargs):
        raise DatabaseUnavailableError("db down")

    monkeypatch.setattr(collect_full_pipeline, "run_pipeline", raise_db_unavailable)
    monkeypatch.setattr(sys, "argv", ["collect_full_pipeline.py", "--daemon", "--interval", "10"])

    with pytest.raises(SystemExit):
        collect_full_pipeline.main()

    assert sum(sleeps) >= 60
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_db_outage_handling.py::test_daemon_cooldown_on_db_unavailable -v`
Expected: FAIL (no cooldown sleep).

**Step 3: Write minimal implementation**

```python
# collect_full_pipeline.py
from database.db_errors import DatabaseUnavailableError, DB_UNAVAILABLE_EXIT_CODE

def run_script(...):
    ...
    if result.returncode == DB_UNAVAILABLE_EXIT_CODE:
        raise DatabaseUnavailableError(f"{script_name} DB unavailable")

def main() -> int:
    ...
    if not daemon_mode:
        try:
            success = run_pipeline(limit, run_tests, logger)
        except DatabaseUnavailableError:
            return DB_UNAVAILABLE_EXIT_CODE
        return 0 if success else 1
    ...
    while not _shutdown_requested:
        try:
            run_pipeline(limit, run_tests, logger)
        except DatabaseUnavailableError:
            cooldown = max(60, interval)
            logger.error("[DAEMON] DB unavailable; cooldown %ss", cooldown)
            for _ in range(cooldown):
                if _shutdown_requested:
                    break
                time.sleep(1)
            continue
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_db_outage_handling.py::test_daemon_cooldown_on_db_unavailable -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add collect_full_pipeline.py tests/test_db_outage_handling.py
git commit -m "feat: add DB outage cooldown in full pipeline daemon"
```

### Task 4: Re-raise DB outage in collectors

**Files:**
- Modify: `scrapers/app_store_details_collector.py`
- Modify: `scrapers/play_store_details_collector.py`
- Modify: `scrapers/app_store_reviews_collector.py`
- Modify: `scrapers/play_store_reviews_collector.py`
- Test: `tests/test_db_outage_handling.py`

**Step 1: Write failing test**

```python
def test_details_collector_propagates_db_unavailable(monkeypatch):
    collector = app_store_details_collector.AppStoreDetailsCollector(
        verbose=False,
        session_id="test-session",
    )

    def raise_db_unavailable(*_args, **_kwargs):
        raise DatabaseUnavailableError("db down")

    monkeypatch.setattr(collector, "collect_app", raise_db_unavailable)

    with pytest.raises(DatabaseUnavailableError):
        collector.collect_batch(["123"])
```

**Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_db_outage_handling.py::test_details_collector_propagates_db_unavailable -v`
Expected: FAIL (error swallowed).

**Step 3: Write minimal implementation**

```python
# scrapers/*_collector.py
from database.db_errors import DatabaseUnavailableError

try:
    self.collect_app(app_id)
except DatabaseUnavailableError:
    raise
except Exception as e:
    ...
```

**Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_db_outage_handling.py::test_details_collector_propagates_db_unavailable -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add scrapers/app_store_details_collector.py \
  scrapers/play_store_details_collector.py \
  scrapers/app_store_reviews_collector.py \
  scrapers/play_store_reviews_collector.py \
  tests/test_db_outage_handling.py
git commit -m "feat: propagate DB outage from collectors"
```
