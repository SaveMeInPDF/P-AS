"""Загрузка конфигурации: config.yaml + переопределение через переменные окружения."""
import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config(path=None) -> dict:
    path = Path(path or os.getenv("PAS_CONFIG", ROOT / "config" / "config.yaml"))
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["_root"] = str(ROOT)
    if os.getenv("PAS_DB_PATH"):
        cfg["database"]["path"] = os.environ["PAS_DB_PATH"]
    return cfg


def resolve(cfg: dict, rel: str) -> Path:
    p = Path(rel)
    return p if p.is_absolute() else Path(cfg["_root"]) / p
