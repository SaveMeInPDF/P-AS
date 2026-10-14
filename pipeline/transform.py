"""raw -> stg -> mart. Все преобразования детерминированы и пересобираются целиком (идемпотентно)."""
import pandas as pd

from .config import resolve

REQUIRED_REF_COLS = ["ticker", "name", "country", "region", "exchange", "sector",
                     "currency", "instrument_type"]

# Условие корректной строки цен (NULL трактуется как «некорректно»)
GOOD_PRICE = ("p.close > 0 AND p.adj_close > 0 AND p.low > 0 AND p.high >= p.low "
              "AND coalesce(p.volume, 0) >= 0")


def load_reference(con, cfg: dict) -> list:
    """Справочник инструментов: CSV -> ref.company (upsert). Возвращает список тикеров."""
    df = pd.read_csv(resolve(cfg, cfg["reference"]["companies"]), dtype=str).fillna("")
    missing = [c for c in REQUIRED_REF_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"reference file lacks columns: {missing}")
    df["ticker"] = df["ticker"].str.strip().str.upper()
    if df["ticker"].duplicated().any():
        raise ValueError("duplicate tickers in reference file")
    df = df[REQUIRED_REF_COLS]
    cols = ", ".join(REQUIRED_REF_COLS)
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in REQUIRED_REF_COLS[1:])
    con.register("ref_in", df)
    try:
        con.execute(f"INSERT INTO ref.company ({cols}) SELECT {cols} FROM ref_in "
                    f"ON CONFLICT (ticker) DO UPDATE SET {sets}")
    finally:
        con.unregister("ref_in")
    return df["ticker"].tolist()


def build_staging(con) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE stg.prices AS
        SELECT upper(trim(p.ticker)) AS ticker, p."date", p.open, p.high, p.low, p.close,
               p.adj_close, p.volume, p._load_id
        FROM raw.prices p
        JOIN ref.company c ON c.ticker = upper(trim(p.ticker))   -- разрешение сущности + ссылочная целостность
        WHERE coalesce(({GOOD_PRICE}), false)""")
    con.execute(f"""
        CREATE OR REPLACE TABLE stg.prices_rejected AS
        SELECT p.*,
               CASE WHEN c.ticker IS NULL THEN 'unknown_ticker'
                    WHEN p.close IS NULL OR p.adj_close IS NULL THEN 'null_price'
                    WHEN p.close <= 0 OR p.adj_close <= 0 OR p.low <= 0 THEN 'non_positive_price'
                    WHEN p.high < p.low THEN 'high_below_low'
                    ELSE 'invalid_volume' END AS reason
        FROM raw.prices p
        LEFT JOIN ref.company c ON c.ticker = upper(trim(p.ticker))
        WHERE c.ticker IS NULL OR NOT coalesce(({GOOD_PRICE}), false)""")
    con.execute("""
        CREATE OR REPLACE TABLE stg.news AS
        SELECT upper(trim(n.ticker)) AS ticker, n.link, n.news_id, n.pub_date,
               trim(n.title) AS title, trim(n.content) AS content, n.publisher
        FROM raw.news n
        JOIN ref.company c ON c.ticker = upper(trim(n.ticker))
        WHERE n.title IS NOT NULL AND trim(n.title) <> '' AND n.pub_date IS NOT NULL""")
    con.execute("""
        CREATE OR REPLACE TABLE stg.macro AS
        SELECT series_id, "date", value FROM raw.macro WHERE value IS NOT NULL""")


def build_marts(con, cfg: dict) -> None:
    drop = float(cfg["monitor"]["drop_threshold"])
    dd = float(cfg["monitor"]["drawdown_threshold"])

    con.execute("CREATE OR REPLACE TABLE mart.dim_company AS SELECT * FROM ref.company")

    con.execute("""
        CREATE OR REPLACE TABLE mart.dim_calendar AS
        SELECT CAST(d AS DATE) AS "date", year(d) AS year, month(d) AS month,
               dayofweek(d) AS day_of_week, dayofweek(d) IN (0, 6) AS is_weekend
        FROM generate_series(
               (SELECT coalesce(min("date"), current_date) FROM stg.prices),
               (SELECT coalesce(max("date"), current_date) FROM stg.prices),
               INTERVAL 1 DAY) AS t(d)""")

    con.execute("""
        CREATE OR REPLACE TABLE mart.fact_price_daily AS
        WITH base AS (
            SELECT ticker, "date", open, high, low, close, adj_close, volume,
                   adj_close / lag(adj_close)    OVER w - 1 AS ret_1d,
                   adj_close / lag(adj_close, 5)  OVER w - 1 AS ret_5d,
                   adj_close / lag(adj_close, 20) OVER w - 1 AS ret_20d,
                   (high - low) / close AS range_pct
            FROM stg.prices
            WINDOW w AS (PARTITION BY ticker ORDER BY "date")
        ),
        feat AS (
            SELECT *,
                   CASE WHEN count(adj_close) OVER w20 = 20 THEN avg(adj_close) OVER w20 END AS ma_20,
                   CASE WHEN count(adj_close) OVER w50 = 50 THEN avg(adj_close) OVER w50 END AS ma_50,
                   CASE WHEN count(ret_1d) OVER w20 = 20 THEN stddev_samp(ret_1d) OVER w20 END AS vol_20,
                   adj_close / max(adj_close) OVER w252 - 1 AS drawdown_252,
                   CASE WHEN count(volume) OVER w20 = 20
                        THEN (volume - avg(volume) OVER w20) / nullif(stddev_samp(volume) OVER w20, 0)
                   END AS volume_z20
            FROM base
            WINDOW w20  AS (PARTITION BY ticker ORDER BY "date" ROWS BETWEEN 19  PRECEDING AND CURRENT ROW),
                   w50  AS (PARTITION BY ticker ORDER BY "date" ROWS BETWEEN 49  PRECEDING AND CURRENT ROW),
                   w252 AS (PARTITION BY ticker ORDER BY "date" ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
        ),
        vix AS (SELECT "date", value FROM stg.macro WHERE series_id = 'VIXCLS'),
        y10 AS (SELECT "date", value FROM stg.macro WHERE series_id = 'DGS10')
        SELECT f.*, vix.value AS vix, y10.value AS us10y
        FROM feat f
        ASOF LEFT JOIN vix ON f."date" >= vix."date"     -- последнее известное значение на дату
        ASOF LEFT JOIN y10 ON f."date" >= y10."date" """)

    con.execute("""
        CREATE OR REPLACE TABLE mart.fact_news AS
        SELECT n.*, c.country, c.region, c.sector
        FROM stg.news n JOIN mart.dim_company c USING (ticker)""")

    con.execute(f"""
        CREATE OR REPLACE TABLE mart.mart_drawdown_monitor AS
        SELECT f.ticker, c.name, c.country, c.region, c.exchange, c.sector, c.currency, c.instrument_type,
               f."date" AS as_of_date, f.adj_close, f.ret_1d, f.ret_5d, f.ret_20d,
               f.drawdown_252, f.vol_20, f.volume_z20, f.vix,
               coalesce(f.ret_5d <= {drop}, false) AS is_falling,
               coalesce(f.drawdown_252 <= {dd}, false) AS is_deep_drawdown,
               CASE WHEN coalesce(f.ret_5d <= {drop}, false) AND coalesce(f.drawdown_252 <= {dd}, false) THEN 'red'
                    WHEN coalesce(f.ret_5d <= {drop}, false) OR coalesce(f.drawdown_252 <= {dd}, false) THEN 'yellow'
                    ELSE 'green' END AS alert_level,
               date_diff('day', f."date", current_date) AS days_since_update
        FROM mart.fact_price_daily f
        JOIN mart.dim_company c USING (ticker)
        QUALIFY row_number() OVER (PARTITION BY f.ticker ORDER BY f."date" DESC) = 1""")


def run_transform(con, cfg: dict) -> None:
    load_reference(con, cfg)
    build_staging(con)
    build_marts(con, cfg)
