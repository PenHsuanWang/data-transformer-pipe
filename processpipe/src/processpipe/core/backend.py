from __future__ import annotations
from typing import Protocol, List, Dict
import pandas as pd


class FrameBackend(Protocol):
    def merge(self, left, right, *, on, how): ...
    def concat(self, frames: List[pd.DataFrame], *, ignore_index): ...
    def groupby_agg(self, df, groupby, agg_map: Dict[str, str]): ...
    def query(self, df, expr: str): ...


class InMemoryBackend(FrameBackend):
    """Pass-through to pandas; all frames remain in RAM."""

    def merge(self, left, right, *, on, how="left"):
        return left.merge(right, on=on, how=how)

    def concat(self, frames, *, ignore_index=True):
        return pd.concat(frames, ignore_index=ignore_index)

    def groupby_agg(self, df, groupby, agg_map):
        return df.groupby(groupby).agg(agg_map).reset_index()

    def query(self, df, expr):
        return df.query(expr)
