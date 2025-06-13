"""Minimal pandas stub for testing without external dependency."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Dict, Iterable, List, Sequence

NA = None


def _rows_from_dict(data: Dict[str, Sequence[Any]]) -> List[Dict[str, Any]]:
    length = len(next(iter(data.values()), []))
    rows = []
    for i in range(length):
        row = {k: list(v)[i] for k, v in data.items()}
        rows.append(row)
    return rows


def _dict_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    columns = set()
    for r in rows:
        columns.update(r.keys())
    return {c: [r.get(c) for r in rows] for c in columns}


@dataclass
class DataFrame:
    data: Dict[str, List[Any]]

    def __init__(self, data: Dict[str, Sequence[Any]]):
        self._rows = _rows_from_dict(data)
        self.columns = list(data.keys())

    @classmethod
    def from_rows(cls, rows: List[Dict[str, Any]]) -> "DataFrame":
        data = _dict_from_rows(rows)
        return cls(data)

    @property
    def shape(self) -> tuple[int, int]:
        if not self._rows:
            return (0, 0)
        return (len(self._rows), len(self._rows[0]))

    def merge(
        self,
        right: "DataFrame",
        how: str = "left",
        on: str | List[str] | None = None,
    ) -> "DataFrame":
        if how != "left":
            raise NotImplementedError("Only left join supported in stub")
        if on is None:
            raise ValueError("'on' argument required")
        on_cols = [on] if isinstance(on, str) else list(on)
        right_index = {}
        for r in right._rows:
            key = tuple(r.get(c) for c in on_cols)
            right_index[key] = r
        rows = []
        for left_row in self._rows:
            key = tuple(left_row.get(c) for c in on_cols)
            r = right_index.get(key)
            new_row = left_row.copy()
            if r:
                for c, v in r.items():
                    if c not in on_cols:
                        new_row[c] = v
            else:
                for c in right.columns:
                    if c not in on_cols:
                        new_row[c] = None
            rows.append(new_row)
        return DataFrame.from_rows(rows)

    def reset_index(self, drop: bool = False) -> "DataFrame":
        return self

    def __eq__(self, other: Any) -> bool:  # type: ignore[override]
        return isinstance(other, DataFrame) and self._rows == other._rows


def concat(dfs: Iterable[DataFrame], ignore_index: bool = False) -> DataFrame:
    rows: List[Dict[str, Any]] = []
    for df in dfs:
        rows.extend(df._rows)
    return DataFrame.from_rows(rows)


def assert_frame_equal(left: DataFrame, right: DataFrame) -> None:
    if left != right:
        message = f"DataFrames not equal:\n{left._rows!r}\n{right._rows!r}"
        raise AssertionError(message)


testing = ModuleType("pandas.testing")
testing.assert_frame_equal = assert_frame_equal

sys.modules[__name__ + ".testing"] = testing
