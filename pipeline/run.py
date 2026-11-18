"""Точка входа: python -m pipeline run [--stage load|transform|all] ; python -m pipeline status"""
import argparse
import sys

from .config import load_config, resolve
from .load import SPECS, run_load
from .storage import CsvStore
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
    store = CsvStore(resolve(cfg, cfg["storage"]["directory"]))
    store.initialize()

    if args.cmd == "status":
        log = store.read_table("meta.load_log")
        columns = ["run_id", "source", "entity", "status", "rows_fetched", "rows_inserted",
                   "attempts", "started_at", "error_message"]
        log = log.sort_values("started_at", ascending=False).head(30)[columns]
        log = log.rename(columns={"error_message": "error"})
        log["error"] = log["error"].astype(str).str[:60]
        print(log.to_string(index=False))
        return 0

    tickers = load_reference(store, cfg)
    exit_code = 0
    if args.stage in ("load", "all"):
        s = run_load(store, cfg, tickers, args.sources, args.full_refresh)
        print(f"load: {s}")
        if s["failed"] and not (s["success"] or s["no_new_data"]):
            exit_code = 1                                   # не загружено ничего — сигнал оркестратору
    if args.stage in ("transform", "all"):
        run_transform(store, cfg)
        print("transform: staging and marts rebuilt")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
