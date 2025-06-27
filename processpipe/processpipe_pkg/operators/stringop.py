from __future__ import annotations

from typing import Dict
import re
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class StringOperator(Operator):
    def __init__(self, source: str, column: str, op: str,
                 pattern: str, replacement: str | None = None,
                 *, output: str | None = None, new_column: str | None = None) -> None:
        super().__init__(output or f"{source}_str_{op}")
        self.source = source
        self.column = column
        self.op = op
        self.pattern = pattern
        self.replacement = replacement
        self.new_column = new_column
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        target = self.new_column or self.column
        for row in df._rows:
            val = str(row.get(self.column, ""))
            if self.op == "contains":
                row[target] = bool(re.search(self.pattern, val))
            elif self.op == "replace":
                row[target] = re.sub(self.pattern, self.replacement or "", val)
        return df
