from __future__ import annotations

from typing import Dict, List
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class RowNumberOperator(Operator):
    def __init__(self, source: str, partition_by: List[str] | None = None,
                 order_by: List[str] | None = None,
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_rownum")
        self.source = source
        self.partition_by = partition_by or []
        self.order_by = order_by or []
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        if self.order_by:
            df._rows.sort(key=lambda r: tuple(r.get(c) for c in self.order_by))
        if self.partition_by:
            groups: Dict[tuple, List[int]] = {}
            for idx, row in enumerate(df._rows):
                key = tuple(row.get(c) for c in self.partition_by)
                groups.setdefault(key, []).append(idx)
            for idxs in groups.values():
                for pos, i in enumerate(idxs):
                    df._rows[i]["row_number"] = pos + 1
        else:
            for pos, row in enumerate(df._rows):
                row["row_number"] = pos + 1
        return df
