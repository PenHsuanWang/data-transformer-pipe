from __future__ import annotations

from typing import Dict, Any
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class UpdateOperator(Operator):
    def __init__(self, source: str, condition: str, set_map: Dict[str, Any], *,
                 output: str | None = None) -> None:
        super().__init__(output or f"{source}_updated")
        self.source = source
        self.condition = condition
        self.set_map = set_map
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        mask = df.query(self.condition).index
        for col, val in self.set_map.items():
            df.loc[mask, col] = val
        return df
