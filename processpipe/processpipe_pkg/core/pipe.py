from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

import logging
import json
import pandas as pd
import networkx as nx

from ..operators import (
    JoinOperator,
    UnionOperator,
    AggregationOperator,
    GroupSizeOperator,
    FilterOperator,
    RollingAggOperator,
    SortOperator,
    TopNOperator,
    FillNAOperator,
    RenameOperator,
    CastOperator,
    StringOperator,
    DropDuplicateOperator,
    PartitionAggOperator,
    RowNumberOperator,
    DeleteOperator,
    UpdateOperator,
    CaseOperator,
    Operator,
)
from .backend import FrameBackend, InMemoryBackend


log = logging.getLogger("processpipe")
if not log.handlers:
    logging.basicConfig(format="%(levelname)s %(name)s: %(message)s",
                        level=logging.INFO)


class ProcessPipe:
    """Fluent, in-memory pipeline executor with a DAG."""

    def __init__(
        self,
        backend: FrameBackend | None = None,
        spill_enabled: bool = False,
        max_workers: int = 1,
    ) -> None:
        self.backend = backend or InMemoryBackend()
        self.spill_enabled = spill_enabled
        self.max_workers = max_workers
        self.env: Dict[str, pd.DataFrame] = {}
        self.ops: list[Operator] = []
        self.dag = nx.DiGraph()
        self._last_output: str | None = None

    # ── data sources ──────────────────────────────────────────────
    def add_dataframe(self, name: str, df: pd.DataFrame) -> "ProcessPipe":
        if name in self.env:
            raise ValueError(f"DataFrame name '{name}' already exists.")
        self.env[name] = df
        self.dag.add_node(name)
        return self

    # ── fluent operator helpers ───────────────────────────────────
    def join(
        self,
        left: str,
        right: str,
        *,
        on,
        how="inner",
        conditions=None,
        output=None,
    ) -> "ProcessPipe":
        return self._append(JoinOperator(left, right, on, how, conditions, output))

    def union(self, left: str, right: str, *, output=None) -> "ProcessPipe":
        return self._append(UnionOperator(left, right, output=output))

    def aggregate(self, source: str, *, groupby, agg_map,
                  output=None) -> "ProcessPipe":
        return self._append(AggregationOperator(source, groupby, agg_map,
                                                output=output))

    def group_size(self, source: str, *, groupby, output=None) -> "ProcessPipe":
        return self._append(GroupSizeOperator(source, groupby, output=output))

    def filter(self, source: str, *, predicate, output=None) -> "ProcessPipe":
        return self._append(FilterOperator(source, predicate, output=output))

    def rolling_agg(self, source: str, *, on, window, agg, output=None) -> "ProcessPipe":
        return self._append(RollingAggOperator(source, on, window, agg, output=output))

    def sort(self, source: str, *, by, ascending=True, output=None) -> "ProcessPipe":
        return self._append(SortOperator(source, by, ascending=ascending, output=output))

    def top_n(self, source: str, *, n, metric, largest=True, per_group=False, group_keys=None, output=None) -> "ProcessPipe":
        return self._append(TopNOperator(source, n, metric, largest=largest, per_group=per_group, group_keys=group_keys, output=output))

    def fill_na(self, source: str, *, value, columns=None, output=None) -> "ProcessPipe":
        return self._append(FillNAOperator(source, value, columns, output=output))

    def rename(self, source: str, *, columns, output=None) -> "ProcessPipe":
        return self._append(RenameOperator(source, columns, output=output))

    def cast(self, source: str, *, casts, output=None) -> "ProcessPipe":
        return self._append(CastOperator(source, casts, output=output))

    def string_op(self, source: str, *, column, op, pattern, replacement=None, new_column=None, output=None) -> "ProcessPipe":
        return self._append(StringOperator(source, column, op, pattern, replacement, output=output, new_column=new_column))

    def drop_duplicates(self, source: str, *, subset=None, keep="first", output=None) -> "ProcessPipe":
        return self._append(DropDuplicateOperator(source, subset, keep=keep, output=output))

    def partition_agg(self, source: str, *, groupby, agg_map, output=None) -> "ProcessPipe":
        return self._append(PartitionAggOperator(source, groupby, agg_map, output=output))

    def row_number(self, source: str, *, partition_by=None, order_by=None, output=None) -> "ProcessPipe":
        return self._append(RowNumberOperator(source, partition_by, order_by, output=output))

    def delete(self, source: str, *, condition, output=None) -> "ProcessPipe":
        return self._append(DeleteOperator(source, condition, output=output))

    def update(self, source: str, *, condition, set_map, output=None) -> "ProcessPipe":
        return self._append(UpdateOperator(source, condition, set_map, output=output))

    def case(self, source: str, *, conditions, choices, default=None, output_col="case", output=None) -> "ProcessPipe":
        return self._append(CaseOperator(source, conditions, choices, default, output_col, output=output))

    # ── plan helpers ─────────────────────────────────────────────
    @classmethod
    def build_pipe(cls, plan: Dict[str, any]) -> "ProcessPipe":
        """Construct a ``ProcessPipe`` from a plan dictionary."""
        pipe = cls()

        for name, df in plan.get("dataframes", {}).items():
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"dataframes['{name}'] is not a pandas DataFrame")
            pipe.add_dataframe(name, df)

        for op in plan.get("operations", []):
            op_type = op.get("type")
            if op_type == "join":
                pipe.join(
                    op["left"],
                    op["right"],
                    on=op["on"],
                    how=op.get("how", "inner"),
                    conditions=op.get("conditions"),
                    output=op.get("output"),
                )
            elif op_type == "union":
                pipe.union(op["left"], op["right"], output=op.get("output"))
            elif op_type == "aggregate":
                pipe.aggregate(
                    op["source"],
                    groupby=op["groupby"],
                    agg_map=op["agg_map"],
                    output=op.get("output"),
                )
            elif op_type == "group_size":
                pipe.group_size(
                    op["source"],
                    groupby=op["groupby"],
                    output=op.get("output"),
                )
            elif op_type == "filter":
                pipe.filter(
                    op["source"],
                    predicate=op["predicate"],
                    output=op.get("output"),
                )
            elif op_type == "rolling_agg":
                pipe.rolling_agg(
                    op["source"],
                    on=op["on"],
                    window=op["window"],
                    agg=op["agg"],
                    output=op.get("output"),
                )
            elif op_type == "sort":
                pipe.sort(
                    op["source"],
                    by=op["by"],
                    ascending=op.get("ascending", True),
                    output=op.get("output"),
                )
            elif op_type == "top_n":
                pipe.top_n(
                    op["source"],
                    n=op["n"],
                    metric=op["metric"],
                    largest=op.get("largest", True),
                    per_group=op.get("per_group", False),
                    group_keys=op.get("group_keys"),
                    output=op.get("output"),
                )
            elif op_type == "fill_na":
                pipe.fill_na(
                    op["source"],
                    value=op["value"],
                    columns=op.get("columns"),
                    output=op.get("output"),
                )
            elif op_type == "rename":
                pipe.rename(
                    op["source"],
                    columns=op["columns"],
                    output=op.get("output"),
                )
            elif op_type == "cast":
                pipe.cast(
                    op["source"],
                    casts=op["casts"],
                    output=op.get("output"),
                )
            elif op_type == "string_op":
                pipe.string_op(
                    op["source"],
                    column=op["column"],
                    op=op["op"],
                    pattern=op["pattern"],
                    replacement=op.get("replacement"),
                    new_column=op.get("new_column"),
                    output=op.get("output"),
                )
            elif op_type == "drop_duplicates":
                pipe.drop_duplicates(
                    op["source"],
                    subset=op.get("subset"),
                    keep=op.get("keep", "first"),
                    output=op.get("output"),
                )
            elif op_type == "partition_agg":
                pipe.partition_agg(
                    op["source"],
                    groupby=op["groupby"],
                    agg_map=op["agg_map"],
                    output=op.get("output"),
                )
            elif op_type == "row_number":
                pipe.row_number(
                    op["source"],
                    partition_by=op.get("partition_by"),
                    order_by=op.get("order_by"),
                    output=op.get("output"),
                )
            elif op_type == "delete":
                pipe.delete(
                    op["source"],
                    condition=op["condition"],
                    output=op.get("output"),
                )
            elif op_type == "update":
                pipe.update(
                    op["source"],
                    condition=op["condition"],
                    set_map=op["set"],
                    output=op.get("output"),
                )
            elif op_type == "case":
                pipe.case(
                    op["source"],
                    conditions=op["conditions"],
                    choices=op["choices"],
                    default=op.get("default"),
                    output_col=op.get("output_col", "case"),
                    output=op.get("output"),
                )
            else:
                raise ValueError(f"Unsupported operation type: {op_type}")

        return pipe

    # internal
    def _append(self, op: Operator) -> "ProcessPipe":
        self.ops.append(op)
        self.dag.add_node(op.output, operator=op)
        for inp in op.inputs:
            self.dag.add_edge(inp, op.output)
        self._last_output = op.output
        return self

    # ── execution ────────────────────────────────────────────────
    def run(self) -> pd.DataFrame:
        if not self.ops:
            raise ValueError("No operators defined.")
        # assign a level (depth) to each node so that operators with the same
        # dependency depth can run concurrently
        levels: Dict[str, int] = {}
        for node in nx.topological_sort(self.dag):
            preds = list(self.dag.predecessors(node))
            lvl = 0
            if preds:
                lvl = max(levels[p] for p in preds) + 1
            levels[node] = lvl

        level_ops: Dict[int, list[Operator]] = defaultdict(list)
        for node, lvl in levels.items():
            op = self.dag.nodes[node].get("operator")
            if op is not None:
                level_ops[lvl].append(op)

        for lvl in sorted(level_ops):
            ops = level_ops[lvl]
            if self.max_workers > 1 and len(ops) > 1:
                with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                    futures = {ex.submit(op.execute, self.backend, self.env): op for op in ops}
                    for fut, op in futures.items():
                        res = fut.result()
                        self.env[op.output] = res
            else:
                for op in ops:
                    res = op.execute(self.backend, self.env)
                    self.env[op.output] = res

        if self.max_workers > 1 or not isinstance(self.backend, InMemoryBackend):
            lineage = [
                {"operator": op.__class__.__name__, "output": op.output, "inputs": op.inputs}
                for op in self.ops
            ]
            with open("pipeline_run.json", "w") as f:
                json.dump({"max_workers": self.max_workers, "lineage": lineage}, f)

        return self.env[self._last_output]

    def describe(self) -> None:
        """Print the execution order of operators."""
        for op in self.ops:
            print(f"{op.__class__.__name__} -> '{op.output}'")
