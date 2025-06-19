"""Optional pandasql wrapper used by ProcessPipe."""
from __future__ import annotations
import pandas as pd

try:
    from pandasql import sqldf
except Exception:  # noqa: BLE001
    sqldf = None


def sql_query(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Run an SQL query using pandasql if available."""
    if sqldf is None:
        raise ImportError("pandasql is required for SQL queries")
    return sqldf(query, {"df": df})
