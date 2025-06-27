from __future__ import annotations

from typing import Dict, List, Any
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class CaseOperator(Operator):
    def __init__(self, source: str, conditions: List[str], choices: List[Any],
                 default: Any = None, output_col: str = "case",
                 *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_case")
        self.source = source
        self.conditions = conditions
        self.choices = choices
        self.default = default
        self.output_col = output_col
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        for row in df._rows:
            val = self.default
            for cond, choice in zip(self.conditions, self.choices):
                try:
                    if eval(cond, {}, row):
                        val = choice
                        break
                except Exception:
                    pass
            row[self.output_col] = val
        return df
