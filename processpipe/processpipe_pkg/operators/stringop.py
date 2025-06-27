from __future__ import annotations

from typing import Dict, Any
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class StringOperator(Operator):
    def __init__(self, source: str, op: str, pattern: str,
                 replacement: str | None = None, *, regex: bool = True,
                 output: str | None = None) -> None:
        super().__init__(output or f"{source}_str")
        self.source = source
        self.op = op
        self.pattern = pattern
        self.replacement = replacement
        self.regex = regex
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        if self.op == "replace":
            df = df.replace(self.pattern, self.replacement, regex=self.regex)
        return df
