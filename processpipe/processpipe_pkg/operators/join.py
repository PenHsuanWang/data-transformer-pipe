from __future__ import annotations

from typing import List, Union, Dict, Tuple
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class JoinOperator(Operator):
    def __init__(
        self,
        left: str,
        right: str,
        on: Union[str, List[str]],
        how: str = "left",
        conditions: List[Tuple[str, str, str]] | None = None,
        output: str | None = None,
    ):
        super().__init__(output or f"{left}_{how}_join_{right}")
        self.left, self.right, self.on, self.how = left, right, on, how
        self.conditions = conditions or []
        self.inputs = [left, right]

    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        left_df = env[self.left]
        right_df = env[self.right]

        if isinstance(self.on, str):
            on_cols = [self.on]
        else:
            on_cols = list(self.on)

        dup_cols = (
            set(left_df.columns) & set(right_df.columns) - set(on_cols)
        )

        def _rename(df, suffix):
            rows = []
            for row in df._rows:
                new_row = {}
                for k, v in row.items():
                    if k in on_cols:
                        new_row[k] = v
                    elif k in dup_cols:
                        new_row[f"{k}{suffix}"] = v
                    else:
                        new_row[k] = v
                rows.append(new_row)
            if hasattr(pd.DataFrame, "from_rows"):
                return pd.DataFrame.from_rows(rows)
            cols: Dict[str, List] = {}
            for r in rows:
                for k, v in r.items():
                    cols.setdefault(k, []).append(v)
            return pd.DataFrame(cols)

        left_df = _rename(left_df, "_left")
        right_df = _rename(right_df, "_right")

        df = backend.merge(left_df, right_df, on=on_cols, how="left")
        if self.how == "inner":
            right_columns = [c for c in right_df.columns if c not in on_cols]
            if right_columns:
                cond = " and ".join(f"{c} is not None" for c in right_columns)
                df = backend.query(df, cond)
        if self.conditions:
            expr_parts = [
                f"{l}_left {op} {r}_right" for l, op, r in self.conditions
            ]
            df = backend.query(df, " and ".join(expr_parts))
        return df
