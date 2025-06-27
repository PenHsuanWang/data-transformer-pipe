from __future__ import annotations

from typing import Dict, List
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class FillNAOperator(Operator):
    def __init__(self, source: str, value, columns: List[str] | None = None,
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_fillna")
        self.source = source
        self.value = value
        self.columns = columns
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        cols = self.columns or df.columns
        for row in df._rows:
            for c in cols:
                if row.get(c) is None:
                    row[c] = self.value
        return df
