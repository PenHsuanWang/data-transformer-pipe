from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class RollingAggOperator(Operator):
    def __init__(self, source: str, window, agg: Dict[str, str], *,
                 on: str | None = None, min_periods: int = 1,
                 output: str | None = None) -> None:
        super().__init__(output or f"{source}_rolling")
        self.source = source
        self.window = window
        self.agg = agg
        self.on = on
        self.min_periods = min_periods
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        if self.on:
            roll = df.set_index(self.on).rolling(self.window, min_periods=self.min_periods)
            result = roll.agg(self.agg).reset_index()
        else:
            roll = df.rolling(self.window, min_periods=self.min_periods)
            result = roll.agg(self.agg)
        return result
