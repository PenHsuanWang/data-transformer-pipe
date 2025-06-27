from __future__ import annotations

from typing import List, Any, Dict
import numpy as np
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class CaseOperator(Operator):
    def __init__(self, source: str, conditions: List[str], choices: List[Any],
                 default: Any = None, output_column: str = "case", *,
                 output: str | None = None) -> None:
        super().__init__(output or f"{source}_case")
        self.source = source
        self.conditions = conditions
        self.choices = choices
        self.default = default
        self.output_column = output_column
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()
        condlist = [df.eval(cond) for cond in self.conditions]
        df[self.output_column] = np.select(condlist, self.choices, default=self.default)
        return df
