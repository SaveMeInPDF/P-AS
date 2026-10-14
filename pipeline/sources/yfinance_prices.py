"""Источник 1: цены OHLCV с Yahoo Finance (API через библиотеку yfinance)."""
import pandas as pd

COLUMNS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]


def normalize(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Приводит ответ yfinance к плоской таблице raw.prices (без служебных полей)."""
    if raw is None or raw.empty:
        return pd.DataFrame(columns=COLUMNS)
    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):          # новые версии yfinance отдают MultiIndex
        df.columns = df.columns.get_level_values(0)
    df = df.reset_index()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    if "adj_close" not in df.columns:                  # у части индексов Adj Close отсутствует
        df["adj_close"] = df["close"]
    if "volume" not in df.columns:
        df["volume"] = pd.NA
    df["ticker"] = ticker
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.date
    for c in ("open", "high", "low", "close", "adj_close"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").round().astype("Int64")
    return df[COLUMNS]


def fetch(ticker: str, start: str, end, cfg: dict) -> pd.DataFrame:
    import yfinance as yf                              # импорт здесь, чтобы тесты не требовали сети

    raw = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False,
                      timeout=cfg["load"]["timeout_sec"])
    return normalize(raw, ticker)
