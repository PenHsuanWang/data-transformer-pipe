from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class DeleteOperator(Operator):
    def __init__(self, source: str, condition: str, *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_delete")
        self.source = source
        self.condition = condition
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        rows = []
        for row in df._rows:
            try:
                keep = not eval(self.condition, {}, row)
            except Exception:
                keep = True
            if keep:
                rows.append(row.copy())
        return pd.DataFrame(rows)
