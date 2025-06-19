from __future__ import annotations
from typing import Dict
import logging
import pandas as pd
import networkx as nx
from ..operators import (
    JoinOperator, UnionOperator, AggregationOperator,
    GroupSizeOperator, FilterOperator, Operator,
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
    def join(self, left: str, right: str, *, on, how="left",
             output=None) -> "ProcessPipe":
        return self._append(JoinOperator(left, right, on, how, output))

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
        for node in nx.topological_sort(self.dag):
            op = self.dag.nodes[node].get("operator")
            if op is None:
                continue
            res = op.execute(self.backend, self.env)
            self.env[op.output] = res
        return self.env[self._last_output]
