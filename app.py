"""Interactive browser dashboards backed by the CSV marts."""
from pathlib import Path

import pandas as pd
import streamlit as st

from pipeline.config import load_config, resolve
from pipeline.storage import CsvStore


st.set_page_config(page_title="Stock drawdown dashboard", layout="wide")


def _read_marts():
    cfg = load_config()
    store = CsvStore(resolve(cfg, cfg["storage"]["directory"]))
    try:
        monitor = store.read_table("mart.mart_drawdown_monitor", dtype=str)
        prices = store.read_table("mart.fact_price_daily", dtype=str)
        news = store.read_table("mart.fact_news", dtype=str)
    except (FileNotFoundError, ValueError) as exc:
        st.error(f"Cannot load dashboard CSV data: {exc}")
        st.info("Run `python -m pipeline run --stage transform` to create the mart CSV files.")
        st.stop()

    for frame, columns in (
        (monitor, ["adj_close", "ret_1d", "ret_5d", "ret_20d", "drawdown_252", "vol_20", "vix"]),
        (prices, ["adj_close", "ma_20", "ma_50", "ret_1d"]),
    ):
        for column in columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    monitor["as_of_date"] = pd.to_datetime(monitor["as_of_date"], errors="coerce")
    monitor["alert_level"] = monitor["alert_level"].astype(str).str.lower()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    news["pub_date"] = pd.to_datetime(news["pub_date"], errors="coerce")
    return store.directory, monitor, prices, news


@st.fragment(run_every="60s")
def dashboard():
    data_dir, monitor, prices, news = _read_marts()
    st.caption(f"CSV source: `{Path(data_dir)}` · automatically refreshed every 60 seconds")
    if monitor.empty:
        st.warning("The drawdown monitor is empty. Run the pipeline to load and transform data.")
        return

    country_options = ["All"] + sorted(monitor["country"].dropna().astype(str).unique())
    sector_options = ["All"] + sorted(monitor["sector"].dropna().astype(str).unique())
    country_col, sector_col = st.columns(2)
    country = country_col.selectbox("Country", country_options)
    sector = sector_col.selectbox("Sector", sector_options)

    filtered = monitor.copy()
    if country != "All":
        filtered = filtered.loc[filtered["country"].eq(country)]
    if sector != "All":
        filtered = filtered.loc[filtered["sector"].eq(sector)]

    alerts = filtered["alert_level"].isin(["yellow", "red"])
    red_alerts = filtered["alert_level"].eq("red")
    latest = filtered["as_of_date"].max()
    metrics = st.columns(4)
    metrics[0].metric("Companies", len(filtered))
    metrics[1].metric("Alerts", int(alerts.sum()))
    metrics[2].metric("Red alerts", int(red_alerts.sum()))
    metrics[3].metric("Latest data", latest.strftime("%Y-%m-%d") if pd.notna(latest) else "N/A")

    overview_tab, prices_tab, news_tab = st.tabs(["Drawdown monitor", "Price history", "News"])
    with overview_tab:
        st.subheader("Latest company status")
        visible = filtered.sort_values(
            "alert_level", key=lambda values: values.map({"red": 0, "yellow": 1, "green": 2}).fillna(3)
        )
        st.dataframe(
            visible[
                ["ticker", "name", "country", "sector", "as_of_date", "adj_close",
                 "ret_1d", "ret_5d", "ret_20d", "drawdown_252", "alert_level"]
            ],
            hide_index=True,
            width="stretch",
            column_config={
                "as_of_date": st.column_config.DateColumn("As of"),
                "adj_close": st.column_config.NumberColumn("Adj. close", format="%.2f"),
                "ret_1d": st.column_config.NumberColumn("1 day", format="percent"),
                "ret_5d": st.column_config.NumberColumn("5 days", format="percent"),
                "ret_20d": st.column_config.NumberColumn("20 days", format="percent"),
                "drawdown_252": st.column_config.NumberColumn("252-day drawdown", format="percent"),
            },
        )

    available_tickers = sorted(filtered["ticker"].dropna().astype(str).unique())
    with prices_tab:
        if not available_tickers:
            st.info("No companies match the selected filters.")
        else:
            ticker = st.selectbox("Ticker", available_tickers, key="price_ticker")
            history = prices.loc[prices["ticker"].eq(ticker)].sort_values("date")
            if history.empty:
                st.info(f"No price history is available for {ticker}.")
            else:
                days = st.selectbox(
                    "History",
                    [90, 180, 365, 1825, 0],
                    format_func=lambda value: "All" if value == 0 else f"Last {value} days",
                    key="history_days",
                )
                if days:
                    history = history.loc[history["date"] >= history["date"].max() - pd.Timedelta(days=days)]
                chart = history.set_index("date")[["adj_close", "ma_20", "ma_50"]].rename(
                    columns={"adj_close": "Adjusted close", "ma_20": "20-day average", "ma_50": "50-day average"}
                )
                st.line_chart(chart)

    with news_tab:
        news_view = news
        if country != "All":
            news_view = news_view.loc[news_view["country"].eq(country)]
        if sector != "All":
            news_view = news_view.loc[news_view["sector"].eq(sector)]
        if available_tickers:
            news_ticker = st.selectbox("Ticker", available_tickers, key="news_ticker")
            news_view = news_view.loc[news_view["ticker"].eq(news_ticker)]
        if news_view.empty:
            st.info("No news matches the selected filters.")
        else:
            news_view = news_view.sort_values("pub_date", ascending=False)
            st.dataframe(
                news_view[["pub_date", "ticker", "title", "publisher", "link"]].head(100),
                hide_index=True,
                width="stretch",
                column_config={
                    "pub_date": st.column_config.DateColumn("Published"),
                    "link": st.column_config.LinkColumn("Article", display_text="Open"),
                },
            )


st.title("Stock drawdown dashboard")
st.write("Explore the latest drawdown alerts, price history, and market news.")
dashboard()
