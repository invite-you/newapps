import sys
import time

import psycopg
import pytest

import collect_app_details
import collect_sitemaps
import collect_full_pipeline
from scrapers import app_store_details_collector
from database import app_details_db
from database import sitemap_apps_db
from database.db_errors import DatabaseUnavailableError, DB_UNAVAILABLE_EXIT_CODE


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


def test_get_connection_raises_db_unavailable(monkeypatch):
    def raise_operational(*args, **kwargs):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg, "connect", raise_operational)
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    app_details_db._DB_CONNECTION = None

    with pytest.raises(DatabaseUnavailableError):
        app_details_db.get_connection()


def test_sitemap_get_connection_raises_db_unavailable(monkeypatch):
    def raise_operational(*args, **kwargs):
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg, "connect", raise_operational)
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    sitemap_apps_db._DB_CONNECTION = None

    with pytest.raises(DatabaseUnavailableError):
        sitemap_apps_db.get_connection()


def test_collect_app_details_returns_db_exit_code(monkeypatch):
    def raise_db_unavailable(*args, **kwargs):
        raise DatabaseUnavailableError("db down")

    monkeypatch.setattr(collect_app_details, "init_database", raise_db_unavailable)
    monkeypatch.setattr(sys, "argv", ["collect_app_details.py", "--stats"])

    result = collect_app_details.main()

    assert result == DB_UNAVAILABLE_EXIT_CODE


def test_collect_sitemaps_returns_db_exit_code(monkeypatch):
    def raise_db_unavailable(*args, **kwargs):
        raise DatabaseUnavailableError("db down")

    monkeypatch.setattr(collect_sitemaps, "init_database", raise_db_unavailable)
    monkeypatch.setattr(sys, "argv", ["collect_sitemaps.py", "--stats"])

    result = collect_sitemaps.main()

    assert result == DB_UNAVAILABLE_EXIT_CODE


def test_run_script_raises_db_unavailable(monkeypatch):
    class DummyResult:
        returncode = DB_UNAVAILABLE_EXIT_CODE

    def fake_run(*args, **kwargs):
        return DummyResult()

    monkeypatch.setattr(collect_full_pipeline.subprocess, "run", fake_run)

    with pytest.raises(DatabaseUnavailableError):
        collect_full_pipeline.run_script("TEST", "noop.py", [], DummyLogger())


def test_daemon_cooldown_on_db_unavailable(monkeypatch):
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        if sum(sleeps) >= 60:
            raise SystemExit

    def raise_db_unavailable(*args, **kwargs):
        raise DatabaseUnavailableError("db down")

    monkeypatch.setattr(collect_full_pipeline, "_shutdown_requested", False)
    monkeypatch.setattr(time, "sleep", fake_sleep)
    monkeypatch.setattr(collect_full_pipeline, "run_pipeline", raise_db_unavailable)
    monkeypatch.setattr(sys, "argv", ["collect_full_pipeline.py", "--daemon", "--interval", "10"])

    with pytest.raises(SystemExit):
        collect_full_pipeline.main()

    assert sum(sleeps) >= 60


def test_details_collector_propagates_db_unavailable(monkeypatch):
    collector = app_store_details_collector.AppStoreDetailsCollector(
        verbose=False,
        session_id="test-session",
    )

    def raise_db_unavailable(*args, **kwargs):
        raise DatabaseUnavailableError("db down")

    monkeypatch.setattr(collector, "collect_app", raise_db_unavailable)

    with pytest.raises(DatabaseUnavailableError):
        collector.collect_batch(["123"])
