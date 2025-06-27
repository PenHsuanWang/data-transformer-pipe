from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class PartitionAggOperator(Operator):
    def __init__(self, source: str, groupby, agg_map: Dict[str, str],
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_partition_agg")
        self.source = source
        self.groupby = groupby
        self.agg_map = agg_map
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        grouped = df.groupby(self.groupby)
        result = grouped.transform(self.agg_map)
        return result
