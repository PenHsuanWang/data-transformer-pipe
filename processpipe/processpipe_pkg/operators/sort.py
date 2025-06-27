from __future__ import annotations

from typing import List, Dict, Any
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class SortOperator(Operator):
    def __init__(self, source: str, by, ascending: bool | List[bool] = True,
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_sorted")
        self.source = source
        self.by = by
        self.ascending = ascending
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return env[self.source].sort_values(by=self.by, ascending=self.ascending)
