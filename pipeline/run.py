"""Точка входа: python -m pipeline run [--stage load|transform|all] ; python -m pipeline status"""
import argparse
import sys

from .config import load_config, resolve
from .db import connect, migrate
from .load import SPECS, run_load
from .transform import load_reference, run_transform


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline")
    ap.add_argument("--config", default=None)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="загрузка и/или преобразование")
    r.add_argument("--stage", choices=["load", "transform", "all"], default="all")
    r.add_argument("--sources", nargs="*", choices=list(SPECS), help="ограничить набор источников")
    r.add_argument("--full-refresh", action="store_true", help="игнорировать водяной знак (upsert безопасен)")
    sub.add_parser("status", help="последние записи журнала загрузок")
    args = ap.parse_args(argv)

    cfg = load_config(args.config)
    con = connect(resolve(cfg, cfg["database"]["path"]))
    migrate(con)

    if args.cmd == "status":
        print(con.execute("""SELECT run_id, source, entity, status, rows_fetched, rows_inserted, attempts,
                                    started_at, left(coalesce(error_message, ''), 60) AS error
                             FROM meta.load_log ORDER BY started_at DESC LIMIT 30""").fetchdf().to_string())
        return 0

    tickers = load_reference(con, cfg)
    exit_code = 0
    if args.stage in ("load", "all"):
        s = run_load(con, cfg, tickers, args.sources, args.full_refresh)
        print(f"load: {s}")
        if s["failed"] and not (s["success"] or s["no_new_data"]):
            exit_code = 1                                   # не загружено ничего — сигнал оркестратору
    if args.stage in ("transform", "all"):
        run_transform(con, cfg)
        print("transform: staging and marts rebuilt")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
