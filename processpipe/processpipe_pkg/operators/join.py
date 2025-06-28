from __future__ import annotations

from typing import Dict, List, Tuple, Union

import pandas as pd

from .base import Operator
from ..core.backend import FrameBackend


class JoinError(Exception):
    """Base class for join errors."""

    code: str

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class _ParamConflict(JoinError):
    pass


class _MissingColumn(JoinError):
    pass


JoinError.param_conflict = _ParamConflict  # type: ignore[attr-defined]
JoinError.missing_column = _MissingColumn  # type: ignore[attr-defined]


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

    def _execute_core(
        self, backend: FrameBackend, env: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        left_df = env[self.left]
        right_df = env[self.right]

        if isinstance(self.on, str):
            on_cols = [self.on]
        else:
            on_cols = list(self.on)

        for c in on_cols:
            if c not in left_df.columns or c not in right_df.columns:
                raise JoinError.missing_column(
                    f"join key '{c}' missing", code="missing_column"
                )

        dup_cols = set(left_df.columns) & set(right_df.columns) - set(on_cols)

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

        valid_ops = {
            "eq": "==",
            "neq": "!=",
            "gt": ">",
            "lt": "<",
            "gte": ">=",
            "lte": "<=",
        }
        if self.conditions:
            if len(on_cols) != len(self.conditions):
                raise JoinError.param_conflict(
                    "'on' and 'conditions' length mismatch",
                    code="length_mismatch",
                )
            for left_col, op, right_col in self.conditions:
                if op not in valid_ops:
                    raise JoinError.param_conflict(
                        f"bad operator '{op}'", code="bad_operator"
                    )
                if left_col not in left_df.columns or right_col not in right_df.columns:
                    raise JoinError.missing_column(
                        f"condition column '{left_col}' or '{right_col}' missing",
                        code="missing_column",
                    )

        left_df = _rename(left_df, "_left")
        right_df = _rename(right_df, "_right")

        df = backend.merge(left_df, right_df, on=on_cols, how="left")
        if self.how == "inner":
            right_columns = [c for c in right_df.columns if c not in on_cols]
            if right_columns:
                cond = " and ".join(f"{c} is not None" for c in right_columns)
                df = backend.query(df, cond)
        if self.conditions:
            expr_parts = []
            for left_col, op, right_col in self.conditions:
                expr_parts.append(f"{left_col}_left {valid_ops[op]} {right_col}_right")
            df = backend.query(df, " and ".join(expr_parts))
        return df
