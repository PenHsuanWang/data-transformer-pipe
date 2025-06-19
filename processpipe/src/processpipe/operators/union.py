from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class UnionOperator(Operator):
    def __init__(self, left: str, right: str, *, output: str | None = None):
        super().__init__(output or f"{left}_union_{right}")
        self.left, self.right = left, right
        self.inputs = [left, right]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return backend.concat([env[self.left], env[self.right]])
