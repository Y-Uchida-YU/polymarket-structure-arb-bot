from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


class CsvEventLogger:
    def __init__(self, export_dir: Path) -> None:
        self.export_dir = export_dir
        self.export_dir.mkdir(parents=True, exist_ok=True)

    def log_signal(self, row: dict[str, Any], now_utc: datetime) -> Path:
        return self._append("signals", row, now_utc)

    def log_order(self, row: dict[str, Any], now_utc: datetime) -> Path:
        return self._append("orders", row, now_utc)

    def log_fill(self, row: dict[str, Any], now_utc: datetime) -> Path:
        return self._append("fills", row, now_utc)

    def log_pnl(self, row: dict[str, Any], now_utc: datetime) -> Path:
        return self._append("pnl", row, now_utc)

    def log_error(self, row: dict[str, Any], now_utc: datetime) -> Path:
        return self._append("errors", row, now_utc)

    def _append(self, name: str, row: dict[str, Any], now_utc: datetime) -> Path:
        file_path = self.export_dir / f"{name}_{now_utc:%Y%m%d}.csv"
        frame = pd.DataFrame([row])
        has_header = not file_path.exists()
        frame.to_csv(file_path, mode="a", index=False, header=has_header)
        return file_path
