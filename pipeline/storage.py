"""CSV-backed tables for the raw, reference, staging, mart, and metadata layers."""
import os
import tempfile
from pathlib import Path

import pandas as pd


TABLE_COLUMNS = {
    "raw.prices": ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume",
                   "_load_id", "_loaded_at"],
    "raw.news": ["ticker", "link", "news_id", "pub_date", "title", "content", "publisher",
                 "_load_id", "_loaded_at"],
    "raw.macro": ["series_id", "date", "value", "_load_id", "_loaded_at"],
    "ref.company": ["ticker", "name", "country", "region", "exchange", "sector", "currency",
                    "instrument_type"],
    "meta.load_log": ["log_id", "run_id", "source", "entity", "started_at", "finished_at",
                      "status", "date_from", "date_to", "rows_fetched", "rows_inserted",
                      "attempts", "error_message"],
    "stg.prices": ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume",
                   "_load_id"],
    "stg.prices_rejected": ["ticker", "date", "open", "high", "low", "close", "adj_close",
                            "volume", "_load_id", "_loaded_at", "reason"],
    "stg.news": ["ticker", "link", "news_id", "pub_date", "title", "content", "publisher"],
    "stg.macro": ["series_id", "date", "value"],
    "mart.dim_company": ["ticker", "name", "country", "region", "exchange", "sector", "currency",
                         "instrument_type"],
    "mart.dim_calendar": ["date", "year", "month", "day_of_week", "is_weekend"],
    "mart.fact_price_daily": ["ticker", "date", "open", "high", "low", "close", "adj_close",
                              "volume", "ret_1d", "ret_5d", "ret_20d", "range_pct", "ma_20",
                              "ma_50", "vol_20", "drawdown_252", "volume_z20", "vix", "us10y"],
    "mart.fact_news": ["ticker", "link", "news_id", "pub_date", "title", "content", "publisher",
                       "country", "region", "sector"],
    "mart.mart_drawdown_monitor": ["ticker", "name", "country", "region", "exchange", "sector",
                                   "currency", "instrument_type", "as_of_date", "adj_close",
                                   "ret_1d", "ret_5d", "ret_20d", "drawdown_252", "vol_20",
                                   "volume_z20", "vix", "is_falling", "is_deep_drawdown",
                                   "alert_level", "days_since_update"],
}


class CsvStore:
    def __init__(self, directory):
        self.directory = Path(directory)

    def path(self, table: str) -> Path:
        if table not in TABLE_COLUMNS:
            raise ValueError(f"unknown CSV table: {table}")
        schema, name = table.split(".", 1)
        return self.directory / schema / f"{name}.csv"

    def initialize(self) -> None:
        for table, columns in TABLE_COLUMNS.items():
            path = self.path(table)
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                self.write_table(table, pd.DataFrame(columns=columns))

    def read_table(self, table: str, dtype=None) -> pd.DataFrame:
        path = self.path(table)
        if not path.exists():
            raise FileNotFoundError(f"CSV table is not initialized: {path}")
        df = pd.read_csv(path, keep_default_na=False, dtype=dtype)
        expected = TABLE_COLUMNS[table]
        missing = [column for column in expected if column not in df.columns]
        if missing:
            raise ValueError(f"{path} lacks required columns: {missing}")
        return df

    def write_table(self, table: str, df: pd.DataFrame) -> None:
        path = self.path(table)
        path.parent.mkdir(parents=True, exist_ok=True)
        columns = TABLE_COLUMNS[table]
        output = df.reindex(columns=columns)
        fd, temp_name = tempfile.mkstemp(prefix=f".{path.stem}.", suffix=".tmp", dir=path.parent)
        os.close(fd)
        try:
            output.to_csv(temp_name, index=False)
            os.replace(temp_name, path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def upsert(self, table: str, incoming: pd.DataFrame, keys: tuple,
               update: bool = True) -> int:
        if incoming.empty:
            return 0
        columns = TABLE_COLUMNS[table]
        data = incoming.reindex(columns=columns).drop_duplicates(subset=list(keys), keep="last")
        existing = self.read_table(table)
        before = len(existing)
        key_columns = [f"_key_{index}" for index in range(len(keys))]
        if update:
            combined = pd.concat([existing, data], ignore_index=True)
            for key, helper in zip(keys, key_columns):
                combined[helper] = combined[key].astype(str)
            combined = combined.drop_duplicates(subset=key_columns, keep="last").drop(columns=key_columns)
        else:
            existing_keys = set(map(tuple, existing[list(keys)].astype(str).to_numpy()))
            new_keys = data[list(keys)].astype(str).apply(tuple, axis=1)
            data = data.loc[~new_keys.isin(existing_keys)]
            combined = pd.concat([existing, data], ignore_index=True)
        self.write_table(table, combined)
        return len(combined) - before
