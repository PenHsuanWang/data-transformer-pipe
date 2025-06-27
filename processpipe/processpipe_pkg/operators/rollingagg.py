from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class RollingAggOperator(Operator):
    def __init__(self, source: str, on: str, window: int, agg: str,
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_rolling")
        self.source = source
        self.on = on
        self.window = int(window)
        self.agg = agg
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        values = [row.get(self.on) for row in df._rows]
        results = []
        for i in range(len(values)):
            start = max(0, i - self.window + 1)
            window_vals = values[start : i + 1]
            if self.agg == "sum":
                val = sum(window_vals)
            elif self.agg in {"mean", "avg"}:
                val = sum(window_vals) / len(window_vals) if window_vals else None
            elif self.agg == "max":
                val = max(window_vals)
            elif self.agg == "min":
                val = min(window_vals)
            else:
                val = None
            results.append(val)
        for row, val in zip(df._rows, results):
            row[f"{self.on}_{self.agg}{self.window}"] = val
        return df
