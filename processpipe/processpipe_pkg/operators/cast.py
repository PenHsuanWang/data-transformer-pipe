from __future__ import annotations

from typing import Dict, Mapping, Callable
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class CastOperator(Operator):
    def __init__(self, source: str, casts: Mapping[str, Callable],
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_cast")
        self.source = source
        self.casts = dict(casts)
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        for row in df._rows:
            for col, func in self.casts.items():
                if col in row and row[col] is not None:
                    try:
                        row[col] = func(row[col])
                    except Exception:
                        pass
        return df
