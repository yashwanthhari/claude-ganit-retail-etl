"""Tiny JSON-backed state store.

Plays the role of the metadata/control tables you would keep in a real platform:
which landing files were already ingested (bronze) and the last batch processed
per entity (silver). This is what makes the pipeline incremental and re-runnable.
"""
from __future__ import annotations

import json
import os
from pathlib import Path


class StateStore:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)
        self._data: dict = json.loads(self.path.read_text()) if self.path.exists() else {}

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def set(self, key: str, value) -> None:
        self._data[key] = value
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        tmp.replace(self.path)
