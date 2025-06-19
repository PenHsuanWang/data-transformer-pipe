from typing import List, Dict, Union
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class AggregationOperator(Operator):
    def __init__(self, source: str,
                 groupby: Union[str, List[str]],
                 agg_map: Dict[str, str],
                 *, output: str | None = None):
        super().__init__(output or f"{source}_agg")
        self.source, self.groupby, self.agg_map = source, groupby, agg_map
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return backend.groupby_agg(env[self.source],
                                   self.groupby, self.agg_map)
