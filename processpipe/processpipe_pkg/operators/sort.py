from __future__ import annotations

from typing import Dict, List
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class SortOperator(Operator):
    def __init__(self, source: str, by: str | List[str], *, ascending: bool = True,
                 output: str | None = None) -> None:
        super().__init__(output or f"{source}_sorted")
        self.source = source
        self.by = [by] if isinstance(by, str) else list(by)
        self.ascending = ascending
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        rows = sorted(
            df._rows,
            key=lambda r: tuple(r.get(c) for c in self.by),
            reverse=not self.ascending,
        )
        return pd.DataFrame(rows)
