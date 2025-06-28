"""Core pipeline classes for data-transformer-pipe."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple, Union

import pandas as pd

log = logging.getLogger("processpipe")


class Operator:
    """Abstract operation with uniform execution wrapper."""

    def __init__(self, output: str) -> None:
        self.output = output
        self.inputs: List[str] = []

    def _execute_core(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        raise NotImplementedError

    def execute(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        for name in self.inputs:
            if name not in env:
                raise KeyError(f"{self.__class__.__name__}: missing '{name}'")
        result = self._execute_core(env)
        log.info(
            "%s -> '%s' shape=%s", self.__class__.__name__, self.output, result.shape
        )
        return result


class JoinOperator(Operator):
    def __init__(
        self,
        left: str,
        right: str,
        on: List[Tuple[str, str]] | str | List[str] | None,
        *,
        how: str = "inner",
        output: str | None = None,
        conditions: List[str] | None = None,
    ) -> None:
        super().__init__(output or f"{left}_{how}_join_{right}")
        self.left = left
        self.right = right
        if on is None:
            self.on = []
        elif isinstance(on, str):
            self.on = [(on, on)]
        elif on and isinstance(on[0], (list, tuple)):
            self.on = [tuple(p) for p in on]  # type: ignore[list-item]
        else:
            self.on = [(c, c) for c in on]  # type: ignore[arg-type]
        self.how = how
        self.conditions = conditions or ["eq"] * len(self.on)
        self.inputs = [left, right]

    def _execute_core(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df_l = env[self.left]
        df_r = env[self.right]

        eq_pairs = [p for p, c in zip(self.on, self.conditions) if c == "eq"]
        neq_pairs = [(p, c) for p, c in zip(self.on, self.conditions) if c != "eq"]

        on_cols = [lcol for lcol, _ in eq_pairs]
        dup_cols = set(df_l.columns) & set(df_r.columns) - set(on_cols)

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

        df_l = _rename(df_l, "_left")
        df_r = _rename(df_r, "_right")

        df = df_l.merge(df_r, on=on_cols, how="left")
        if self.how == "inner":
            right_columns = [c for c in df_r.columns if c not in on_cols]
            if right_columns:
                cond = " and ".join(f"{c} is not None" for c in right_columns)
                df = df.query(cond)

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
            df = df.query(" and ".join(expr_parts))

        return df


class UnionOperator(Operator):
    def __init__(self, left: str, right: str, *, output: str | None = None) -> None:
        super().__init__(output or f"{left}_union_{right}")
        self.left = left
        self.right = right
        self.inputs = [left, right]

    def _execute_core(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return pd.concat([env[self.left], env[self.right]], ignore_index=True)


class FilterOperator(Operator):
    def __init__(
        self, source: str, predicate: str, *, output: str | None = None
    ) -> None:
        super().__init__(output or f"{source}_filtered")
        self.source = source
        self.predicate = predicate
        self.inputs = [source]

    def _execute_core(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return env[self.source].query(self.predicate)


class AggregationOperator(Operator):
    def __init__(
        self,
        source: str,
        groupby: Union[str, List[str]],
        agg_map: Dict[str, str],
        *,
        output: str | None = None,
    ) -> None:
        super().__init__(output or f"{source}_agg")
        self.source = source
        self.groupby = groupby
        self.agg_map = agg_map
        self.inputs = [source]

    def _execute_core(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        grouped = env[self.source].groupby(self.groupby)
        result = grouped.agg(self.agg_map).reset_index()  # type: ignore[attr-defined]
        return result


class GroupSizeOperator(Operator):
    def __init__(self, source: str, groupby: str, *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_counts")
        self.source = source
        self.groupby = groupby
        self.inputs = [source]

    def _execute_core(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        df = env[self.source].copy()  # type: ignore[attr-defined]
        group_sizes = df.groupby(self.groupby).transform("size")
        if isinstance(group_sizes, pd.DataFrame):
            df["group_size"] = group_sizes.iloc[:, 0]
        else:
            df["group_size"] = group_sizes
        return df


class ProcessPipe:
    """Manage DataFrame environment and operator execution."""

    def __init__(self) -> None:
        self.env: Dict[str, pd.DataFrame] = {}
        self.operators: List[Operator] = []

    def add_dataframe(self, name: str, df: pd.DataFrame) -> "ProcessPipe":
        self.env[name] = df
        return self

    def join(
        self,
        left: str,
        right: str,
        *,
        on: List[Tuple[str, str]] | str | List[str] | None,
        how: str = "inner",
        conditions: List[str] | None = None,
        output: str | None = None,
    ) -> "ProcessPipe":
        self.operators.append(
            JoinOperator(
                left=left,
                right=right,
                on=on,
                how=how,
                output=output,
                conditions=conditions,
            )
        )
        return self

    def union(
        self,
        left: str,
        right: str,
        output: str | None = None,
    ) -> "ProcessPipe":
        op = UnionOperator(left=left, right=right, output=output)
        self.operators.append(op)
        return self

    def filter(
        self,
        source: str,
        predicate: str,
        output: str | None = None,
    ) -> "ProcessPipe":
        op = FilterOperator(source, predicate, output=output)
        self.operators.append(op)
        return self

    def aggregate(
        self,
        source: str,
        groupby: Union[str, List[str]],
        agg_map: Dict[str, str],
        output: str | None = None,
    ) -> "ProcessPipe":
        op = AggregationOperator(
            source=source, groupby=groupby, agg_map=agg_map, output=output
        )
        self.operators.append(op)
        return self

    def group_size(
        self,
        source: str,
        groupby: str,
        output: str | None = None,
    ) -> "ProcessPipe":
        op = GroupSizeOperator(source, groupby, output=output)
        self.operators.append(op)
        return self

    def run(self) -> pd.DataFrame:
        if not self.operators:
            raise ValueError("ProcessPipe has no operators to execute")
        result = None
        for op in self.operators:
            result = op.execute(self.env)
            self.env[op.output] = result
        assert result is not None
        return result

    @classmethod
    def build_pipe(cls, pipeline_plan: Dict[str, Any]) -> "ProcessPipe":
        pipe = cls()

        for name, df in pipeline_plan.get("dataframes", {}).items():
            if not isinstance(df, pd.DataFrame):
                msg = f"dataframes['{name}'] is not a pandas DataFrame"
                raise TypeError(msg)
            pipe.add_dataframe(name, df)

        for op in pipeline_plan.get("operations", []):
            op_type = op.get("type")
            if op_type == "join":
                pipe.join(
                    left=op["left"],
                    right=op["right"],
                    on=op.get("on"),
                    how=op.get("how", "inner"),
                    conditions=op.get("conditions"),
                    output=op.get("output"),
                )
            elif op_type == "union":
                pipe.union(
                    left=op["left"],
                    right=op["right"],
                    output=op.get("output"),
                )
            elif op_type == "filter":
                pipe.filter(
                    source=op["source"],
                    predicate=op["predicate"],
                    output=op.get("output"),
                )
            elif op_type == "aggregate":
                pipe.aggregate(
                    source=op["source"],
                    groupby=op["groupby"],
                    agg_map=op["agg_map"],
                    output=op.get("output"),
                )
            elif op_type == "group_size":
                pipe.group_size(
                    source=op["source"],
                    groupby=op["groupby"],
                    output=op.get("output"),
                )
            else:
                raise ValueError(f"Unsupported operation type: {op_type}")

        return pipe


__all__ = [
    "ProcessPipe",
    "Operator",
    "JoinOperator",
    "UnionOperator",
    "FilterOperator",
    "AggregationOperator",
    "GroupSizeOperator",
]
