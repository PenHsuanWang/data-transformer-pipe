from __future__ import annotations

from typing import Dict, List
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class PartitionAggOperator(Operator):
    def __init__(self, source: str, groupby: List[str], agg_map: Dict[str, str],
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_partagg")
        self.source = source
        self.groupby = groupby
        self.agg_map = agg_map
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        groups: Dict[tuple, List[int]] = {}
        for idx, row in enumerate(df._rows):
            key = tuple(row.get(c) for c in self.groupby)
            groups.setdefault(key, []).append(idx)
        for col, func in self.agg_map.items():
            for key, idxs in groups.items():
                values = [df._rows[i].get(col) for i in idxs]
                if func == "sum":
                    val = sum(values)
                elif func in {"mean", "avg"}:
                    val = sum(values) / len(values) if values else None
                elif func == "max":
                    val = max(values)
                elif func == "min":
                    val = min(values)
                elif func == "count":
                    val = len(values)
                else:
                    val = None
                for i in idxs:
                    df._rows[i][f"{col}_{func}"] = val
        return df
