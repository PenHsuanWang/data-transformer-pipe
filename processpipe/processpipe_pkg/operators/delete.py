from __future__ import annotations

from typing import Any, Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class DeleteOperator(Operator):
    def __init__(self, source: str, condition: str, *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_deleted")
        self.source = source
        self.condition = condition
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source]
        to_drop = df.query(self.condition).index
        return df.drop(to_drop)
