"""Источник 3: макроиндикаторы риска с FRED (CSV по HTTP, без ключа API)."""
import io

import pandas as pd
import requests

COLUMNS = ["series_id", "date", "value"]
URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"


def parse(text: str, series_id: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(text))
    if df.shape[1] < 2:
        raise ValueError(f"unexpected FRED response for {series_id}")
    df = df.iloc[:, :2]
    df.columns = ["date", "value"]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")   # пропуски в FRED записаны как "."
    df = df.dropna(subset=["value"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["series_id"] = series_id
    return df[COLUMNS]


def fetch(series_id: str, start: str, end, cfg: dict) -> pd.DataFrame:
    resp = requests.get(URL, params={"id": series_id, "cosd": start},
                        timeout=cfg["load"]["timeout_sec"])
    resp.raise_for_status()
    return parse(resp.text, series_id)
