from __future__ import annotations

from typing import List, Dict, Any
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class DropDuplicateOperator(Operator):
    def __init__(self, source: str, subset=None, keep: str | bool = "first",
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_dedup")
        self.source = source
        self.subset = subset
        self.keep = keep
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return env[self.source].drop_duplicates(subset=self.subset, keep=self.keep)
