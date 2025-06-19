from __future__ import annotations

from typing import List, Union, Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class JoinOperator(Operator):
    def __init__(self, left: str, right: str, on: Union[str, List[str]],
                 how: str = "left", output: str | None = None):
        super().__init__(output or f"{left}_{how}_join_{right}")
        self.left, self.right, self.on, self.how = left, right, on, how
        self.inputs = [left, right]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return backend.merge(env[self.left], env[self.right],
                             on=self.on, how=self.how)
