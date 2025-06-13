"""Core pipeline classes for data-transformer-pipe."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Union

import pandas as pd


@dataclass
class Operator:
    """Base class for all operations."""

    output: str

    def execute(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        raise NotImplementedError


@dataclass
class JoinOperator(Operator):
    left: str
    right: str
    on: Union[str, List[str]]
    how: str = "left"

    def execute(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        if self.left not in env or self.right not in env:
            raise KeyError(f"[JoinOperator] missing table: {self.left} or {self.right}")
        df_l = env[self.left]
        df_r = env[self.right]
        result = df_l.merge(df_r, how=self.how, on=self.on)
        print(
            f"[JoinOperator] {self.left} {self.how} join {self.right} "
            f"on {self.on} -> '{self.output}' shape={result.shape}"
        )
        return result


@dataclass
class UnionOperator(Operator):
    left: str
    right: str

    def execute(self, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        if self.left not in env or self.right not in env:
            raise KeyError(
                f"[UnionOperator] missing table: {self.left} or {self.right}"
            )
        df_l = env[self.left]
        df_r = env[self.right]
        result = pd.concat([df_l, df_r], ignore_index=True)
        print(
            f"[UnionOperator] concat {self.left} + {self.right} -> "
            f"'{self.output}' shape={result.shape}"
        )
        return result


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
        on: Union[str, List[str]],
        how: str = "left",
        output: str | None = None,
    ) -> "ProcessPipe":
        if output is None:
            output = f"{left}_{how}_join_{right}"
        self.operators.append(
            JoinOperator(output=output, left=left, right=right, on=on, how=how)
        )
        return self

    def union(self, left: str, right: str, output: str | None = None) -> "ProcessPipe":
        if output is None:
            output = f"{left}_union_{right}"
        self.operators.append(UnionOperator(output=output, left=left, right=right))
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
                raise TypeError(f"dataframes['{name}'] is not a pandas DataFrame")
            pipe.add_dataframe(name, df)

        for op in pipeline_plan.get("operations", []):
            op_type = op.get("type")
            if op_type == "join":
                pipe.join(
                    left=op["left"],
                    right=op["right"],
                    on=op["on"],
                    how=op.get("how", "left"),
                    output=op.get("output"),
                )
            elif op_type == "union":
                pipe.union(left=op["left"], right=op["right"], output=op.get("output"))
            else:
                raise ValueError(f"Unsupported operation type: {op_type}")

        return pipe


__all__ = ["ProcessPipe", "Operator", "JoinOperator", "UnionOperator"]
