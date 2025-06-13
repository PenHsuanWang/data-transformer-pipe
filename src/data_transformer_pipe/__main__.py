"""Command-line interface for data-transformer-pipe."""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict

import pandas as pd

from .pipe import ProcessPipe


def _convert_dataframes(plan: Dict[str, Any]) -> Dict[str, Any]:
    """Convert dict definitions in *plan* to pandas DataFrames."""
    dfs = plan.get("dataframes", {})
    converted = {}
    for name, data in dfs.items():
        if isinstance(data, pd.DataFrame):
            converted[name] = data
        elif isinstance(data, dict):
            converted[name] = pd.DataFrame(data)
        else:
            raise TypeError(
                f"dataframes['{name}'] must be a pandas DataFrame or a column mapping"
            )
    plan["dataframes"] = converted
    return plan


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run a data-transformer pipeline")
    parser.add_argument("plan", help="Path to JSON file describing the pipeline")
    args = parser.parse_args(argv)

    with open(args.plan, "r", encoding="utf-8") as fh:
        plan = json.load(fh)

    plan = _convert_dataframes(plan)

    pipe = ProcessPipe.build_pipe(plan)
    result = pipe.run()
    print(result)


if __name__ == "__main__":  # pragma: no cover
    main()
