"""Minimal pandas stub for testing without external dependency."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Dict, Iterable, List, Sequence, cast

NA = None


def _rows_from_dict(data: Dict[str, Sequence[Any]]) -> List[Dict[str, Any]]:
    length = len(next(iter(data.values()), []))
    rows = []
    for i in range(length):
        row = {k: list(v)[i] for k, v in data.items()}
        rows.append(row)
    return rows


def _dict_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    """Convert a row-oriented representation to a column-oriented dict.

    The original implementation used a ``set`` for collecting column names
    which resulted in an arbitrary column order. Tests rely on a stable
    ordering that preserves the order of first appearance, so we accumulate
    the column names in a list instead.
    """

    columns: List[str] = []
    for r in rows:
        for col in r.keys():
            if col not in columns:
                columns.append(col)

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
        return cls(cast(Dict[str, Sequence[Any]], data))

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
            match = right_index.get(key)
            new_row = left_row.copy()
            if match:
                for c, v in match.items():
                    if c not in on_cols:
                        new_row[c] = v
            else:
                for c in right.columns:
                    if c not in on_cols:
                        new_row[c] = None
            rows.append(new_row)
        return DataFrame.from_rows(rows)

    def query(self, expr: str) -> "DataFrame":
        rows: List[Dict[str, Any]] = []
        for r in self._rows:
            try:
                keep = bool(eval(expr, {}, r))
            except Exception:
                keep = False
            if keep:
                rows.append(r.copy())
        return DataFrame.from_rows(rows)

    # ------------------------------------------------------------------
    # Minimal groupby/agg implementation
    def groupby(self, by: str | List[str]):
        return _GroupBy(self, by)

    def reset_index(self, drop: bool = False) -> "DataFrame":
        return self

    def __eq__(self, other: Any) -> bool:  # type: ignore[override]
        return isinstance(other, DataFrame) and self._rows == other._rows

    def to_dict(self) -> Dict[str, List[Any]]:
        return _dict_from_rows(self._rows)

    def __repr__(self) -> str:
        return f"DataFrame({_dict_from_rows(self._rows)})"


def concat(dfs: Iterable[DataFrame], ignore_index: bool = False) -> DataFrame:
    rows: List[Dict[str, Any]] = []
    for df in dfs:
        rows.extend(df._rows)
    return DataFrame.from_rows(rows)


class _GroupBy:
    """Very small subset of pandas GroupBy used in tests."""

    def __init__(self, df: DataFrame, by: str | List[str]):
        self.df = df
        self.by = [by] if isinstance(by, str) else list(by)

    def agg(self, agg_map: Dict[str, str]) -> DataFrame:
        groups: Dict[tuple, List[Dict[str, Any]]] = {}
        for r in self.df._rows:
            key = tuple(r.get(c) for c in self.by)
            groups.setdefault(key, []).append(r)

        out_rows: List[Dict[str, Any]] = []
        for key, rows in groups.items():
            out = {c: v for c, v in zip(self.by, key)}
            for col, func in agg_map.items():
                vals = [r.get(col) for r in rows]
                if func == "sum":
                    val = sum(vals)
                elif func in ("mean", "avg", "average"):
                    val = sum(vals) / len(vals) if vals else NA
                elif func == "min":
                    val = min(vals)
                elif func == "max":
                    val = max(vals)
                elif func == "count":
                    val = len(rows)
                else:
                    raise ValueError(f"Unsupported aggregation '{func}'")
                out[col] = val
            out_rows.append(out)
        return DataFrame.from_rows(out_rows)


def assert_frame_equal(left: DataFrame, right: DataFrame) -> None:
    if left != right:
        message = f"DataFrames not equal:\n{left._rows!r}\n{right._rows!r}"
        raise AssertionError(message)


testing = ModuleType("pandas.testing")
testing.assert_frame_equal = assert_frame_equal  # type: ignore[attr-defined]

sys.modules[__name__ + ".testing"] = testing
