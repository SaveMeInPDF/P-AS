"""Подключение к DuckDB и версионируемые миграции схемы (слои raw / ref / stg / mart / meta)."""
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


MIGRATIONS = [
    (1, "initial layers", [
        "CREATE SCHEMA IF NOT EXISTS raw",    # сырые данные «как пришли» + служебные поля
        "CREATE SCHEMA IF NOT EXISTS ref",    # справочники
        "CREATE SCHEMA IF NOT EXISTS stg",    # очищенные и проверенные данные
        "CREATE SCHEMA IF NOT EXISTS mart",   # витрины для дашбордов и моделей
        """CREATE TABLE IF NOT EXISTS meta.load_log (
              log_id VARCHAR PRIMARY KEY, run_id VARCHAR, source VARCHAR, entity VARCHAR,
              started_at TIMESTAMP, finished_at TIMESTAMP, status VARCHAR,
              date_from DATE, date_to DATE,
              rows_fetched BIGINT, rows_inserted BIGINT, attempts INTEGER, error_message VARCHAR)""",
        """CREATE TABLE IF NOT EXISTS raw.prices (
              ticker VARCHAR, "date" DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
              adj_close DOUBLE, volume BIGINT, _load_id VARCHAR, _loaded_at TIMESTAMP,
              PRIMARY KEY (ticker, "date"))""",
        """CREATE TABLE IF NOT EXISTS raw.news (
              ticker VARCHAR, link VARCHAR, news_id VARCHAR, pub_date TIMESTAMP, title VARCHAR,
              content VARCHAR, publisher VARCHAR, _load_id VARCHAR, _loaded_at TIMESTAMP,
              PRIMARY KEY (ticker, link))""",
        """CREATE TABLE IF NOT EXISTS raw.macro (
              series_id VARCHAR, "date" DATE, value DOUBLE, _load_id VARCHAR, _loaded_at TIMESTAMP,
              PRIMARY KEY (series_id, "date"))""",
        """CREATE TABLE IF NOT EXISTS ref.company (
              ticker VARCHAR PRIMARY KEY, name VARCHAR, country VARCHAR, region VARCHAR,
              exchange VARCHAR, sector VARCHAR, currency VARCHAR, instrument_type VARCHAR)""",
    ]),
    # Новые изменения схемы добавляются сюда как (2, "описание", [sql, ...]) — существующие данные не теряются.
]


def connect(path) -> duckdb.DuckDBPyConnection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def migrate(con) -> int:
    con.execute("CREATE SCHEMA IF NOT EXISTS meta")
    con.execute("""CREATE TABLE IF NOT EXISTS meta.schema_version (
        version INTEGER PRIMARY KEY, description VARCHAR, applied_at TIMESTAMP)""")
    current = con.execute("SELECT coalesce(max(version), 0) FROM meta.schema_version").fetchone()[0]
    for version, description, statements in MIGRATIONS:
        if version <= current:
            continue
        con.execute("BEGIN TRANSACTION")
        try:
            for stmt in statements:
                con.execute(stmt)
            con.execute("INSERT INTO meta.schema_version VALUES (?, ?, ?)",
                        [version, description, utcnow()])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        current = version
    return current
