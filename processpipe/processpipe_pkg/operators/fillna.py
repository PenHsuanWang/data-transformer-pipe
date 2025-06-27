from __future__ import annotations

from typing import Any, Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class FillNAOperator(Operator):
    def __init__(self, source: str, value: Any = None, method: str | None = None,
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_filled")
        self.source = source
        self.value = value
        self.method = method
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        return df.fillna(value=self.value, method=self.method)
