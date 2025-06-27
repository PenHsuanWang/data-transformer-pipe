from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class RenameOperator(Operator):
    def __init__(self, source: str, columns: Dict[str, str] | None = None,
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_renamed")
        self.source = source
        self.columns = columns or {}
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        return df.rename(columns=self.columns)
