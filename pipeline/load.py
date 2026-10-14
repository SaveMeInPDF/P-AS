"""Загрузка в слой raw: инкрементально, идемпотентно, с повторами и журналом (meta.load_log)."""
import time
import uuid
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd

from .db import utcnow
from .retry import RetryExhausted, call_with_retry
from .sources import fred_macro, yfinance_news, yfinance_prices


@dataclass
class Spec:
    table: str
    entity_col: str          # столбец сущности (тикер / серия)
    keys: tuple              # естественный ключ — основа идемпотентности
    fetch: callable
    incremental: bool        # True — докачка с водяного знака; False — снимок «последних N»
    on_conflict_update: bool # True — перезаписывать совпавшие строки; False — игнорировать


SPECS = {
    "yfinance_prices": Spec("raw.prices", "ticker", ("ticker", "date"),
                            yfinance_prices.fetch, True, True),
    "yfinance_news": Spec("raw.news", "ticker", ("ticker", "link"),
                          yfinance_news.fetch, False, False),
    "fred_macro": Spec("raw.macro", "series_id", ("series_id", "date"),
                       fred_macro.fetch, True, True),
}


def _q(col: str) -> str:
    return f'"{col}"'


def entities_for(source: str, cfg: dict, tickers: list) -> list:
    return list(cfg["sources"]["fred_macro"]["series"]) if source == "fred_macro" else tickers


def _upsert(con, spec: Spec, df: pd.DataFrame, load_id: str) -> None:
    df = df.drop_duplicates(subset=list(spec.keys), keep="last").copy()
    df["_load_id"] = load_id
    df["_loaded_at"] = utcnow()
    cols = list(df.columns)
    col_sql = ", ".join(_q(c) for c in cols)
    key_sql = ", ".join(_q(c) for c in spec.keys)
    if spec.on_conflict_update:
        sets = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in cols if c not in spec.keys)
        conflict = f"ON CONFLICT ({key_sql}) DO UPDATE SET {sets}"
    else:
        conflict = f"ON CONFLICT ({key_sql}) DO NOTHING"
    con.register("incoming", df)
    try:
        con.execute(f"INSERT INTO {spec.table} ({col_sql}) SELECT {col_sql} FROM incoming {conflict}")
    finally:
        con.unregister("incoming")


def _count(con, spec: Spec, entity: str) -> int:
    return con.execute(f"SELECT count(*) FROM {spec.table} WHERE {spec.entity_col} = ?",
                       [entity]).fetchone()[0]


def _watermark(con, spec: Spec, entity: str):
    return con.execute(f'SELECT max("date") FROM {spec.table} WHERE {spec.entity_col} = ?',
                       [entity]).fetchone()[0]


def load_entity(con, source: str, entity: str, cfg: dict, run_id: str,
                full_refresh: bool = False, sleep=time.sleep) -> str:
    spec, lcfg = SPECS[source], cfg["load"]
    log_id, started = uuid.uuid4().hex, utcnow()
    con.execute("""INSERT INTO meta.load_log (log_id, run_id, source, entity, started_at, status)
                   VALUES (?, ?, ?, ?, ?, 'running')""", [log_id, run_id, source, entity, started])

    status, error, attempts = "failed", None, 0
    fetched = inserted = 0
    start = end = None
    try:
        mark = _watermark(con, spec, entity) if spec.incremental else None
        if spec.incremental:
            if mark is None or full_refresh:
                start = str(lcfg["start_date"])
            else:
                start = str(mark - timedelta(days=int(lcfg["overlap_days"])))
            end = lcfg.get("end_date")
        before = _count(con, spec, entity)
        df, attempts = call_with_retry(lambda: spec.fetch(entity, start, end, cfg),
                                       lcfg["retry"], sleep=sleep)
        fetched = len(df)
        if fetched == 0 and spec.incremental and mark is None:
            # пустой ответ при первой загрузке = неверный тикер/недоступный источник, а не «нет новых данных»
            raise ValueError("empty response on first load")
        if fetched:
            _upsert(con, spec, df, load_id=run_id)
        inserted = _count(con, spec, entity) - before
        status = "success" if fetched else "no_new_data"
    except RetryExhausted as exc:
        attempts, error = exc.attempts, repr(exc.last_exc)[:500]
    except Exception as exc:
        error = repr(exc)[:500]

    con.execute("""UPDATE meta.load_log SET finished_at = ?, status = ?, date_from = ?, date_to = ?,
                   rows_fetched = ?, rows_inserted = ?, attempts = ?, error_message = ?
                   WHERE log_id = ?""",
                [utcnow(), status, start, end if isinstance(end, (date, str)) else None,
                 fetched, inserted, attempts, error, log_id])
    return status


def run_load(con, cfg: dict, tickers: list, sources=None, full_refresh=False, sleep=time.sleep) -> dict:
    """Один запуск загрузки. Сбой одной сущности не останавливает остальные."""
    con.execute("""UPDATE meta.load_log SET status = 'aborted', finished_at = ?
                   WHERE status = 'running'""", [utcnow()])      # хвосты упавших запусков
    run_id = uuid.uuid4().hex[:12]
    summary = {"run_id": run_id, "success": 0, "no_new_data": 0, "failed": 0}
    for source in (sources or [s for s in SPECS if cfg["sources"][s]["enabled"]]):
        for entity in entities_for(source, cfg, tickers):
            status = load_entity(con, source, entity, cfg, run_id, full_refresh, sleep)
            summary[status] += 1
            sleep(cfg["load"]["pause_between_requests_sec"])
    return summary
