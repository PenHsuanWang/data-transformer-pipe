from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class CastOperator(Operator):
    def __init__(self, source: str, dtype_map: Dict[str, str], *,
                 errors: str = "raise", output: str | None = None) -> None:
        super().__init__(output or f"{source}_cast")
        self.source = source
        self.dtype_map = dtype_map
        self.errors = errors
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        return df.astype(self.dtype_map, errors=self.errors)
