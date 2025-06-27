from __future__ import annotations

from typing import Dict, Mapping
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class RenameOperator(Operator):
    def __init__(self, source: str, columns: Mapping[str, str],
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_rename")
        self.source = source
        self.columns = dict(columns)
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        for row in df._rows:
            for old, new in self.columns.items():
                if old in row:
                    row[new] = row.pop(old)
        return df
