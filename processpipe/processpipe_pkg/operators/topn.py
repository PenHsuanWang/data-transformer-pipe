from __future__ import annotations

from typing import List, Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class TopNOperator(Operator):
    def __init__(self, source: str, n: int, metric: str, *, largest: bool = True,
                 output: str | None = None) -> None:
        super().__init__(output or f"{source}_top{n}")
        self.source = source
        self.n = n
        self.metric = metric
        self.largest = largest
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        if self.largest:
            return df.nlargest(self.n, self.metric)
        else:
            return df.nsmallest(self.n, self.metric)
