"""Источник 2: новости по тикеру (JSON, текстовые данные). Провайдер отдаёт лишь ~10 последних
новостей, поэтому история накапливается за счёт регулярных запусков."""
import pandas as pd

COLUMNS = ["ticker", "link", "news_id", "pub_date", "title", "content", "publisher"]


def parse(items: list, ticker: str) -> pd.DataFrame:
    rows = []
    for item in items or []:
        c = item.get("content") or {}
        link = ((c.get("canonicalUrl") or {}).get("url")
                or (c.get("clickThroughUrl") or {}).get("url"))
        if not link:                                    # без ссылки нет ключа дедупликации
            continue
        rows.append({
            "ticker": ticker, "link": link, "news_id": item.get("id"),
            "pub_date": c.get("pubDate"), "title": c.get("title"),
            "content": c.get("summary"), "publisher": (c.get("provider") or {}).get("displayName"),
        })
    df = pd.DataFrame(rows, columns=COLUMNS)
    if not df.empty:
        df["pub_date"] = (pd.to_datetime(df["pub_date"], errors="coerce", utc=True)
                          .dt.tz_localize(None))
    return df


def fetch(ticker: str, start, end, cfg: dict) -> pd.DataFrame:
    import yfinance as yf

    return parse(yf.Ticker(ticker).news, ticker)
