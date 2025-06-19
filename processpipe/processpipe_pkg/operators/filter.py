from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class FilterOperator(Operator):
    def __init__(self, source: str, predicate: str,
                 *, output: str | None = None):
        super().__init__(output or f"{source}_filtered")
        self.source, self.predicate = source, predicate
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return backend.query(env[self.source], self.predicate)
