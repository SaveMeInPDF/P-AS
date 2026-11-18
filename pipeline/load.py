"""Incremental, idempotent source loading with retries and a CSV load log."""
import time
import uuid
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from .retry import RetryExhausted, call_with_retry
from .sources import fred_macro, yfinance_news, yfinance_prices
from .storage import CsvStore


@dataclass
class Spec:
    table: str
    entity_col: str
    keys: tuple
    fetch: callable
    incremental: bool
    on_conflict_update: bool


SPECS = {
    "yfinance_prices": Spec("raw.prices", "ticker", ("ticker", "date"),
                            yfinance_prices.fetch, True, True),
    "yfinance_news": Spec("raw.news", "ticker", ("ticker", "link"),
                          yfinance_news.fetch, False, False),
    "fred_macro": Spec("raw.macro", "series_id", ("series_id", "date"),
                       fred_macro.fetch, True, True),
}


def entities_for(source: str, cfg: dict, tickers: list) -> list:
    return list(cfg["sources"]["fred_macro"]["series"]) if source == "fred_macro" else tickers


def _count(store: CsvStore, spec: Spec, entity: str) -> int:
    df = store.read_table(spec.table)
    return int(df[spec.entity_col].astype(str).eq(str(entity)).sum())


def _watermark(store: CsvStore, spec: Spec, entity: str):
    df = store.read_table(spec.table)
    dates = pd.to_datetime(df.loc[df[spec.entity_col].astype(str).eq(str(entity)), "date"],
                           errors="coerce")
    mark = dates.max()
    return None if pd.isna(mark) else mark.date()


def _log(store: CsvStore, row: dict) -> None:
    table = store.read_table("meta.load_log")
    row = {key: "" if value is None else str(value) for key, value in row.items()}
    exists = table["log_id"].astype(str).eq(str(row["log_id"]))
    table = table.loc[~exists].astype(str)
    table = pd.concat([table, pd.DataFrame([row])], ignore_index=True)
    store.write_table("meta.load_log", table)


def load_entity(store: CsvStore, source: str, entity: str, cfg: dict, run_id: str,
                full_refresh: bool = False, sleep=time.sleep) -> str:
    spec, lcfg = SPECS[source], cfg["load"]
    log_id, started = uuid.uuid4().hex, pd.Timestamp.now(tz="UTC").tz_localize(None)
    row = {
        "log_id": log_id, "run_id": run_id, "source": source, "entity": entity,
        "started_at": started, "finished_at": "", "status": "running", "date_from": "",
        "date_to": "", "rows_fetched": 0, "rows_inserted": 0, "attempts": 0,
        "error_message": "",
    }
    _log(store, row)

    status, error, attempts = "failed", None, 0
    fetched = inserted = 0
    start = end = None
    try:
        mark = _watermark(store, spec, entity) if spec.incremental else None
        if spec.incremental:
            if mark is None or full_refresh:
                start = str(lcfg["start_date"])
            else:
                start = str(mark - timedelta(days=int(lcfg["overlap_days"])))
            end = lcfg.get("end_date")
        before = _count(store, spec, entity)
        df, attempts = call_with_retry(lambda: spec.fetch(entity, start, end, cfg),
                                       lcfg["retry"], sleep=sleep)
        fetched = len(df)
        if fetched == 0 and spec.incremental and mark is None:
            raise ValueError("empty response on first load")
        if fetched:
            df = df.drop_duplicates(subset=list(spec.keys), keep="last").copy()
            df["_load_id"] = run_id
            df["_loaded_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
            store.upsert(spec.table, df, spec.keys, update=spec.on_conflict_update)
        inserted = _count(store, spec, entity) - before
        status = "success" if fetched else "no_new_data"
    except RetryExhausted as exc:
        attempts, error = exc.attempts, repr(exc.last_exc)[:500]
    except Exception as exc:
        error = repr(exc)[:500]

    row.update({
        "finished_at": pd.Timestamp.now(tz="UTC").tz_localize(None),
        "status": status, "date_from": start or "", "date_to": end or "",
        "rows_fetched": fetched, "rows_inserted": inserted, "attempts": attempts,
        "error_message": error or "",
    })
    _log(store, row)
    return status


def run_load(store: CsvStore, cfg: dict, tickers: list, sources=None,
             full_refresh=False, sleep=time.sleep) -> dict:
    """Run all selected entities; a single entity failure does not stop the rest."""
    log = store.read_table("meta.load_log")
    running = log["status"].eq("running")
    if running.any():
        log.loc[running, "status"] = "aborted"
        log.loc[running, "finished_at"] = str(pd.Timestamp.now(tz="UTC").tz_localize(None))
        store.write_table("meta.load_log", log)
    run_id = uuid.uuid4().hex[:12]
    summary = {"run_id": run_id, "success": 0, "no_new_data": 0, "failed": 0}
    for source in (sources or [s for s in SPECS if cfg["sources"][s]["enabled"]]):
        for entity in entities_for(source, cfg, tickers):
            status = load_entity(store, source, entity, cfg, run_id, full_refresh, sleep)
            summary[status] += 1
            sleep(cfg["load"]["pause_between_requests_sec"])
    return summary
