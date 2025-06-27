from __future__ import annotations

from typing import List, Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class RowNumberOperator(Operator):
    def __init__(self, source: str, groupby=None, order_by=None,
                 column_name: str = "row_number", *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_rownum")
        self.source = source
        self.groupby = groupby
        self.order_by = order_by
        self.column_name = column_name
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        if self.groupby is not None:
            df[self.column_name] = df.groupby(self.groupby).cumcount() + 1
        else:
            df[self.column_name] = range(1, len(df) + 1)
        return df
