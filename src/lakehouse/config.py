"""Configuration loading. Paths in the YAML are resolved against the project root."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SQL_MARTS_DIR = ROOT / "sql" / "marts"


def load_config(path: str | os.PathLike | None = None) -> dict:
    cfg_path = Path(path or os.environ.get("LAKEHOUSE_CONFIG", ROOT / "config" / "pipeline.yaml"))
    with open(cfg_path, encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    cfg["paths"] = {name: str(ROOT / value) for name, value in cfg["paths"].items()}
    return cfg
