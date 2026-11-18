"""Build staging tables and analytical marts from CSV-backed raw data."""
import pandas as pd

from .config import resolve
from .storage import CsvStore

REQUIRED_REF_COLS = ["ticker", "name", "country", "region", "exchange", "sector",
                     "currency", "instrument_type"]
PRICE_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume",
                 "_load_id"]
PRICE_RAW_COLUMNS = PRICE_COLUMNS + ["_loaded_at"]


def load_reference(store: CsvStore, cfg: dict) -> list:
    """Upsert the CSV company reference and return its tickers."""
    df = pd.read_csv(resolve(cfg, cfg["reference"]["companies"]), dtype=str).fillna("")
    missing = [c for c in REQUIRED_REF_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"reference file lacks columns: {missing}")
    df["ticker"] = df["ticker"].str.strip().str.upper()
    if df["ticker"].duplicated().any():
        raise ValueError("duplicate tickers in reference file")
    df = df[REQUIRED_REF_COLS]
    store.upsert("ref.company", df, ("ticker",), update=True)
    return df["ticker"].tolist()


def _numeric(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df[column], errors="coerce")


def build_staging(store: CsvStore) -> None:
    prices = store.read_table("raw.prices")
    companies = store.read_table("ref.company")
    companies["ticker"] = companies["ticker"].astype(str).str.strip().str.upper()
    prices["ticker"] = prices["ticker"].astype(str).str.strip().str.upper()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    for column in ("open", "high", "low", "close", "adj_close", "volume"):
        prices[column] = _numeric(prices, column)

    known = prices["ticker"].isin(companies["ticker"])
    valid = (
        prices["close"].gt(0) & prices["adj_close"].gt(0) & prices["low"].gt(0)
        & prices["high"].ge(prices["low"]) & prices["volume"].fillna(0).ge(0)
    ).fillna(False)
    store.write_table("stg.prices", prices.loc[known & valid, PRICE_COLUMNS])

    rejected = prices.loc[~known | ~valid, PRICE_RAW_COLUMNS].copy()
    rejected["reason"] = "invalid_volume"
    rejected.loc[prices["high"].lt(prices["low"]), "reason"] = "high_below_low"
    rejected.loc[prices["close"].le(0) | prices["adj_close"].le(0) | prices["low"].le(0),
                 "reason"] = "non_positive_price"
    rejected.loc[prices["close"].isna() | prices["adj_close"].isna(), "reason"] = "null_price"
    rejected.loc[~known, "reason"] = "unknown_ticker"
    store.write_table("stg.prices_rejected", rejected)

    news = store.read_table("raw.news")
    news["ticker"] = news["ticker"].astype(str).str.strip().str.upper()
    news["pub_date"] = pd.to_datetime(news["pub_date"], errors="coerce")
    news["title"] = news["title"].fillna("").astype(str).str.strip()
    news["content"] = news["content"].fillna("").astype(str).str.strip()
    valid_news = news["ticker"].isin(companies["ticker"]) & news["title"].ne("") & news["pub_date"].notna()
    store.write_table("stg.news", news.loc[valid_news,
                                           ["ticker", "link", "news_id", "pub_date", "title",
                                            "content", "publisher"]])

    macro = store.read_table("raw.macro")
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
    macro["value"] = pd.to_numeric(macro["value"], errors="coerce")
    valid_macro = macro["date"].notna() & macro["value"].notna()
    store.write_table("stg.macro", macro.loc[valid_macro, ["series_id", "date", "value"]])


def _asof_values(prices: pd.DataFrame, macro: pd.DataFrame, series_id: str, column: str) -> pd.Series:
    values = macro.loc[macro["series_id"].eq(series_id), ["date", "value"]].dropna()
    if values.empty:
        return pd.Series(float("nan"), index=prices.index)
    left = prices[["date"]].copy()
    left["_order"] = range(len(left))
    right = values.sort_values("date").rename(columns={"value": column})
    merged = pd.merge_asof(left.sort_values("date"), right, on="date", direction="backward")
    return merged.set_index("_order")[column].reindex(range(len(prices))).set_axis(prices.index)


def build_marts(store: CsvStore, cfg: dict) -> None:
    drop = float(cfg["monitor"]["drop_threshold"])
    drawdown_limit = float(cfg["monitor"]["drawdown_threshold"])
    companies = store.read_table("ref.company")
    store.write_table("mart.dim_company", companies)

    prices = store.read_table("stg.prices")
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)
    macro = store.read_table("stg.macro")
    macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
    macro["value"] = pd.to_numeric(macro["value"], errors="coerce")

    if prices.empty:
        today = pd.Timestamp.today().normalize()
        calendar_dates = pd.date_range(today, today, freq="D")
    else:
        calendar_dates = pd.date_range(prices["date"].min(), prices["date"].max(), freq="D")
    calendar = pd.DataFrame({"date": calendar_dates})
    calendar["year"] = calendar["date"].dt.year
    calendar["month"] = calendar["date"].dt.month
    calendar["day_of_week"] = (calendar["date"].dt.dayofweek + 1) % 7
    calendar["is_weekend"] = calendar["date"].dt.dayofweek.isin([5, 6])
    store.write_table("mart.dim_calendar", calendar)

    fact = prices[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]].copy()
    grouped = fact.groupby("ticker", sort=False)
    for periods in (1, 5, 20):
        fact[f"ret_{periods}d"] = grouped["adj_close"].transform(
            lambda series: series / series.shift(periods) - 1
        )
    fact["range_pct"] = (fact["high"] - fact["low"]) / fact["close"]
    grouped = fact.groupby("ticker", sort=False)
    fact["ma_20"] = grouped["adj_close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    fact["ma_50"] = grouped["adj_close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
    fact["vol_20"] = fact.groupby("ticker", sort=False)["ret_1d"].transform(
        lambda s: s.rolling(20, min_periods=20).std()
    )
    fact["drawdown_252"] = fact.groupby("ticker", sort=False)["adj_close"].transform(
        lambda s: s / s.rolling(252, min_periods=1).max() - 1
    )
    fact["volume_z20"] = fact.groupby("ticker", sort=False)["volume"].transform(
        lambda s: (s - s.rolling(20, min_periods=20).mean()) /
        s.rolling(20, min_periods=20).std().replace(0, float("nan"))
    )
    fact["vix"] = _asof_values(fact, macro, "VIXCLS", "vix")
    fact["us10y"] = _asof_values(fact, macro, "DGS10", "us10y")
    store.write_table("mart.fact_price_daily", fact)

    news = store.read_table("stg.news")
    detail = news.merge(companies[["ticker", "country", "region", "sector"]], on="ticker", how="inner")
    store.write_table("mart.fact_news", detail)

    monitor = fact.merge(companies, on="ticker", how="inner")
    if not monitor.empty:
        monitor = monitor.sort_values(["ticker", "date"]).groupby("ticker", sort=False).tail(1)
    monitor = monitor.rename(columns={"date": "as_of_date"})
    monitor["is_falling"] = monitor["ret_5d"].le(drop).fillna(False)
    monitor["is_deep_drawdown"] = monitor["drawdown_252"].le(drawdown_limit).fillna(False)
    monitor["alert_level"] = "green"
    monitor.loc[monitor["is_falling"] | monitor["is_deep_drawdown"], "alert_level"] = "yellow"
    monitor.loc[monitor["is_falling"] & monitor["is_deep_drawdown"], "alert_level"] = "red"
    monitor["days_since_update"] = (pd.Timestamp.today().normalize() - monitor["as_of_date"]).dt.days
    monitor_columns = [
        "ticker", "name", "country", "region", "exchange", "sector", "currency",
        "instrument_type", "as_of_date", "adj_close", "ret_1d", "ret_5d", "ret_20d",
        "drawdown_252", "vol_20", "volume_z20", "vix", "is_falling",
        "is_deep_drawdown", "alert_level", "days_since_update",
    ]
    store.write_table("mart.mart_drawdown_monitor", monitor[monitor_columns])


def run_transform(store: CsvStore, cfg: dict) -> None:
    load_reference(store, cfg)
    build_staging(store)
    build_marts(store, cfg)
