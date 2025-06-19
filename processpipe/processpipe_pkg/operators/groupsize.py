from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class GroupSizeOperator(Operator):
    def __init__(self, source: str, groupby: str,
                 *, output: str | None = None):
        super().__init__(output or f"{source}_counts")
        self.source, self.groupby = source, groupby
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        counts = df[self.groupby].value_counts()
        df["group_size"] = df[self.groupby].map(counts)
        return df
