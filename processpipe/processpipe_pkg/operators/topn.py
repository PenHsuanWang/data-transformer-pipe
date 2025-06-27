from __future__ import annotations

from typing import Dict, List
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class TopNOperator(Operator):
    def __init__(self, source: str, n: int, metric: str,
                 *, largest: bool = True, per_group: bool = False,
                 group_keys: List[str] | None = None,
                 output: str | None = None) -> None:
        super().__init__(output or f"{source}_top{n}")
        self.source = source
        self.n = int(n)
        self.metric = metric
        self.largest = largest
        self.per_group = per_group
        self.group_keys = group_keys or []
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        if self.per_group and self.group_keys:
            groups: Dict[tuple, list] = {}
            for row in df._rows:
                key = tuple(row.get(k) for k in self.group_keys)
                groups.setdefault(key, []).append(row)
            rows: List[dict] = []
            for g_rows in groups.values():
                sorted_rows = sorted(
                    g_rows,
                    key=lambda r: r.get(self.metric),
                    reverse=self.largest,
                )
                rows.extend(sorted_rows[: self.n])
        else:
            rows = sorted(
                df._rows,
                key=lambda r: r.get(self.metric),
                reverse=self.largest,
            )[: self.n]
        return pd.DataFrame([r.copy() for r in rows])
