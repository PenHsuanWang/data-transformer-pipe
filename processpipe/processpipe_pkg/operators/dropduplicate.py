from __future__ import annotations

from typing import Dict, List
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class DropDuplicateOperator(Operator):
    def __init__(self, source: str, subset: List[str] | None = None,
                 *, keep: str = "first", output: str | None = None) -> None:
        super().__init__(output or f"{source}_dedup")
        self.source = source
        self.subset = subset
        self.keep = keep
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        subset = self.subset or df.columns
        seen = {}
        rows = []
        for row in df._rows:
            key = tuple(row.get(c) for c in subset)
            if key not in seen:
                seen[key] = row
                rows.append(row.copy())
            elif self.keep == "last":
                seen[key] = row
        if self.keep == "last":
            rows = [seen[key].copy() for key in seen]
        return pd.DataFrame(rows)
