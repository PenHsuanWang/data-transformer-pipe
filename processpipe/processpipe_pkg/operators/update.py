from __future__ import annotations

from typing import Dict, Mapping, Any
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class UpdateOperator(Operator):
    def __init__(self, source: str, condition: str, set_map: Mapping[str, Any],
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_update")
        self.source = source
        self.condition = condition
        self.set_map = dict(set_map)
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        for row in df._rows:
            try:
                cond = bool(eval(self.condition, {}, row))
            except Exception:
                cond = False
            if cond:
                for col, val in self.set_map.items():
                    row[col] = val
        return df
