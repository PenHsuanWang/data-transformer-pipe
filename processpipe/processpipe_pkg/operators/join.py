from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd

from ..core.backend import FrameBackend
from .base import Operator


class JoinOperator(Operator):
    def __init__(
        self,
        left: str,
        right: str,
        on: List[Tuple[str, str]] | str | List[str] | None,
        *,
        how: str = "inner",
        conditions: List[str] | None = None,
        output: str | None = None,
    ) -> None:
        super().__init__(output or f"{left}_{how}_join_{right}")
        self.left = left
        self.right = right
        self.how = how
        if on is None:
            self.on = []
        elif isinstance(on, str):
            self.on = [(on, on)]
        elif on and isinstance(on[0], (list, tuple)):
            self.on = [tuple(p) for p in on]  # type: ignore[list-item]
        else:
            self.on = [(c, c) for c in on]  # type: ignore[arg-type]
        self.conditions = conditions or ["eq"] * len(self.on)
        self.inputs = [left, right]

    def _execute_core(
        self, backend: FrameBackend, env: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        left_df = env[self.left]
        right_df = env[self.right]

        if len(self.on) != len(self.conditions):
            raise ValueError("length_mismatch")

        eq_pairs = [p for p, c in zip(self.on, self.conditions) if c == "eq"]
        neq_pairs = [(p, c) for p, c in zip(self.on, self.conditions) if c != "eq"]

        on_cols = [lcol for lcol, _ in eq_pairs]
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

        left_df = _rename(left_df, "_left")
        right_df = _rename(right_df, "_right")

        merged = backend.merge(left_df, right_df, on=on_cols, how="left")
        if self.how == "inner":
            right_columns = [c for c in right_df.columns if c not in on_cols]
            if right_columns:
                cond = " and ".join(f"{c} is not None" for c in right_columns)
                merged = backend.query(merged, cond)

        if neq_pairs:
            expr_parts = []
            for (lcol, rcol), op in neq_pairs:
                left_name = f"{lcol}_left"
                right_name = f"{rcol}_right"
                if op == "neq":
                    expr_parts.append(f"{left_name} != {right_name}")
                elif op == "gt":
                    expr_parts.append(f"{left_name} > {right_name}")
                elif op == "gte":
                    expr_parts.append(f"{left_name} >= {right_name}")
                elif op == "lt":
                    expr_parts.append(f"{left_name} < {right_name}")
                elif op == "lte":
                    expr_parts.append(f"{left_name} <= {right_name}")
            merged = backend.query(merged, " and ".join(expr_parts))
        return merged
