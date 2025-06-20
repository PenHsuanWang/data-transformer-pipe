NA = None

class DataFrame:
    def __init__(self, data):
        if isinstance(data, DataFrame):
            self._rows = [row.copy() for row in data._rows]
        elif isinstance(data, list):
            self._rows = [row.copy() for row in data]
        elif isinstance(data, dict):
            keys = list(data.keys())
            length = len(next(iter(data.values()))) if data else 0
            for v in data.values():
                if len(v) != length:
                    raise ValueError("Column lengths not equal")
            self._rows = [{k: data[k][i] for k in keys} for i in range(length)]
        else:
            raise TypeError("Unsupported data type for DataFrame")

    @property
    def shape(self):
        return (len(self._rows), len(self.columns))

    @property
    def columns(self):
        cols = []
        for row in self._rows:
            for k in row:
                if k not in cols:
                    cols.append(k)
        return cols

    def __getitem__(self, key):
        if isinstance(key, str):
            return [row.get(key) for row in self._rows]
        else:
            raise TypeError("Only column access by name is supported")

    def __setitem__(self, key, values):
        if len(values) != len(self._rows):
            raise ValueError("Length mismatch")
        for row, val in zip(self._rows, values):
            row[key] = val

    def copy(self):
        return DataFrame([row.copy() for row in self._rows])

    def merge(self, right, *, on, how="left"):
        if isinstance(on, str):
            on_cols = [on]
        else:
            on_cols = list(on)
        index = {}
        for r in right._rows:
            key = tuple(r.get(c) for c in on_cols)
            index.setdefault(key, []).append(r)
        result = []
        left_keys_seen = set()
        for l in self._rows:
            key = tuple(l.get(c) for c in on_cols)
            left_keys_seen.add(key)
            rights = index.get(key)
            if rights:
                for r in rights:
                    row = l.copy()
                    for c, v in r.items():
                        if c not in on_cols:
                            row[c] = v
                    result.append(row)
            elif how in ("left", "outer"):
                row = l.copy()
                for c in right.columns:
                    if c not in on_cols:
                        row.setdefault(c, NA)
                result.append(row)
        if how in ("right", "outer"):
            for r in right._rows:
                key = tuple(r.get(c) for c in on_cols)
                if key not in left_keys_seen:
                    row = {}
                    for c in self.columns:
                        if c not in on_cols:
                            row[c] = NA
                    for c, v in r.items():
                        row[c] = v
                    result.append(row)
        return DataFrame(result)

    def query(self, expr):
        rows = []
        for r in self._rows:
            try:
                keep = bool(eval(expr, {}, r))
            except Exception:
                keep = False
            if keep:
                rows.append(r.copy())
        return DataFrame(rows)

    # ------------------------------------------------------------------
    # Minimal groupby/agg implementation used by AggregationOperator
    def groupby(self, by):
        return _GroupBy(self, by)

    def reset_index(self, drop=False):
        return self.copy()

    def to_dict(self):
        data = {c: [] for c in self.columns}
        for r in self._rows:
            for c in data:
                data[c].append(r.get(c))
        return data

    def __repr__(self):
        return f"DataFrame({self.to_dict()})"

def concat(frames, *, ignore_index=True):
    rows = []
    for f in frames:
        rows.extend([row.copy() for row in f._rows])
    return DataFrame(rows)


class _GroupBy:
    """Very small subset of pandas GroupBy for tests."""

    def __init__(self, df: DataFrame, by):
        self._df = df
        if isinstance(by, str):
            self._by = [by]
        else:
            self._by = list(by)

    def agg(self, agg_map):
        groups = {}
        for row in self._df._rows:
            key = tuple(row.get(c) for c in self._by)
            groups.setdefault(key, []).append(row)

        out_rows = []
        for key, rows in groups.items():
            out = {c: v for c, v in zip(self._by, key)}
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
        return DataFrame(out_rows)

# Submodule for testing
from types import SimpleNamespace

def _assert_frame_equal(left, right):
    assert left.columns == right.columns
    assert left.to_dict() == right.to_dict()

# provide pandas.testing namespace
testing = SimpleNamespace(assert_frame_equal=_assert_frame_equal)
__all__ = ["DataFrame", "concat", "NA", "testing"]
