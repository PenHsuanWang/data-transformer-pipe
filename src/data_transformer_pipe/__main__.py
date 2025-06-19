import argparse
import csv
import json
from typing import Any, Dict, Sequence, cast

import pandas as pd

from .pipe import ProcessPipe


def _load_csv(path: str) -> pd.DataFrame:
    """Load a CSV file into the minimal pandas DataFrame."""
    data: Dict[str, list[Any]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key, value in row.items():
                data.setdefault(key, []).append(value)
    return pd.DataFrame(cast(Dict[str, Sequence[Any]], data))


def _write_csv(df: pd.DataFrame, path: str) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        for row in df._rows:  # type: ignore[attr-defined]
            writer.writerow([row.get(col) for col in df.columns])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Execute a data-transformer-pipe pipeline"
    )
    parser.add_argument("plan", help="JSON file describing the pipeline")
    parser.add_argument(
        "--output",
        "-o",
        help="Output CSV file path",
        default=None,
    )
    args = parser.parse_args(argv)

    with open(args.plan) as f:
        plan = json.load(f)

    dataframes = plan.get("dataframes", {})
    loaded = {}
    for name, path in dataframes.items():
        if not isinstance(path, str):
            raise ValueError("DataFrame paths must be strings")
        loaded[name] = _load_csv(path)
    plan["dataframes"] = loaded

    pipe = ProcessPipe.build_pipe(plan)
    result = pipe.run()

    if args.output:
        _write_csv(result, args.output)
    else:
        print(result._rows)  # type: ignore[attr-defined]


if __name__ == "__main__":
    main()
