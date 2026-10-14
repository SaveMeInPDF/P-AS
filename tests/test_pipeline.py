"""Тесты: чистые функции работают без сети и без DuckDB; интеграционные — при наличии duckdb."""
import types

import pandas as pd
import pytest

from pipeline.retry import RetryExhausted, call_with_retry
from pipeline.sources import fred_macro, yfinance_news, yfinance_prices

RETRY = {"attempts": 3, "base_delay_sec": 1, "rate_limit_delay_sec": 30}


def test_retry_succeeds_after_failures():
    calls, delays = {"n": 0}, []

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError("timeout")
        return "ok"

    result, attempts = call_with_retry(flaky, RETRY, sleep=delays.append)
    assert (result, attempts, delays) == ("ok", 3, [1, 2])


def test_retry_rate_limit_waits_longer_and_exhausts():
    delays = []
    with pytest.raises(RetryExhausted) as e:
        call_with_retry(lambda: (_ for _ in ()).throw(RuntimeError("Too Many Requests")),
                        RETRY, sleep=delays.append)
    assert delays == [30, 30] and e.value.attempts == 3


def test_prices_normalize_multiindex_and_tz():
    idx = pd.DatetimeIndex(["2026-01-02", "2026-01-05"], tz="America/New_York", name="Date")
    cols = pd.MultiIndex.from_product([["Open", "High", "Low", "Close", "Adj Close", "Volume"], ["AAPL"]])
    raw = pd.DataFrame([[1, 2, 0.5, 1.5, 1.4, 100], [1.5, 2, 1, 1.8, 1.7, 120]], index=idx, columns=cols)
    df = yfinance_prices.normalize(raw, "AAPL")
    assert list(df.columns) == yfinance_prices.COLUMNS
    assert str(df["date"].iloc[0]) == "2026-01-02" and df["volume"].iloc[1] == 120


def test_prices_normalize_empty():
    assert yfinance_prices.normalize(pd.DataFrame(), "X").empty


def test_news_parse_skips_items_without_link():
    items = [{"id": "1", "content": {"title": "A", "pubDate": "2026-09-25T13:55:20Z",
                                     "canonicalUrl": {"url": "http://x/1"}, "provider": {"displayName": "P"}}},
             {"id": "2", "content": {"title": "no link"}}]
    df = yfinance_news.parse(items, "AAPL")
    assert len(df) == 1 and df["publisher"].iloc[0] == "P"


def test_fred_parse_drops_missing_values():
    df = fred_macro.parse("observation_date,VIXCLS\n2026-01-02,15.2\n2026-01-03,.\n", "VIXCLS")
    assert len(df) == 1 and df["value"].iloc[0] == 15.2


# ---------- интеграционные (нужен duckdb) ----------
duckdb = pytest.importorskip("duckdb")


def _fake_prices(entity, start, end, cfg):
    days = pd.bdate_range("2026-01-01", periods=60)
    df = pd.DataFrame({"ticker": entity, "date": [d.date() for d in days],
                       "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0,
                       "adj_close": [10.0 + i * 0.1 for i in range(60)], "volume": 1000})
    df["volume"] = df["volume"].astype("Int64")
    return df


def test_incremental_and_idempotent(monkeypatch, tmp_path):
    from pipeline import load
    from pipeline.config import load_config
    from pipeline.db import connect, migrate
    from pipeline.transform import load_reference, run_transform

    cfg = load_config()
    cfg["sources"]["yfinance_news"]["enabled"] = False
    cfg["sources"]["fred_macro"]["enabled"] = False
    cfg["load"]["pause_between_requests_sec"] = 0
    monkeypatch.setitem(load.SPECS, "yfinance_prices",
                        load.SPECS["yfinance_prices"].__class__(**{**load.SPECS["yfinance_prices"].__dict__,
                                                                   "fetch": _fake_prices}))
    con = connect(tmp_path / "t.duckdb")
    migrate(con)
    tickers = load_reference(con, cfg)[:2]

    load.run_load(con, cfg, tickers, sleep=lambda s: None)
    n1 = con.execute("SELECT count(*) FROM raw.prices").fetchone()[0]
    load.run_load(con, cfg, tickers, sleep=lambda s: None)          # повторный запуск
    n2 = con.execute("SELECT count(*) FROM raw.prices").fetchone()[0]
    assert n1 == n2 == 120

    run_transform(con, cfg)
    assert con.execute("SELECT count(*) FROM mart.mart_drawdown_monitor").fetchone()[0] == 30  # справочник целиком
    assert con.execute("SELECT count(*) FROM mart.fact_price_daily").fetchone()[0] == 120
    assert con.execute("SELECT count(*) FROM meta.load_log WHERE status = 'failed'").fetchone()[0] == 0
