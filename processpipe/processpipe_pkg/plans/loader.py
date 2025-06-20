from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import pandas as pd
from ..core.pipe import ProcessPipe

try:
    import yaml  # type: ignore
except Exception:  # noqa: BLE001
    yaml = None

def load_plan(path: str | Path) -> ProcessPipe:
    """Load a YAML or JSON plan and build a :class:`ProcessPipe`."""
    file_path = Path(path)
    with open(file_path) as f:
        if file_path.suffix in {".yml", ".yaml"}:
            if yaml is None:
                raise ImportError("PyYAML is required for YAML plans")

            class InlineLoader(yaml.SafeLoader):
                pass

            def _construct_inline(loader: yaml.Loader, node: Any) -> Any:
                return loader.construct_mapping(node)

            InlineLoader.add_constructor("tag:yaml.org,2002:inline", _construct_inline)

            plan = yaml.load(f, Loader=InlineLoader)
        else:
            plan = json.load(f)

    pipe = ProcessPipe()
    for name, df_obj in plan.get("dataframes", {}).items():
        if isinstance(df_obj, pd.DataFrame):
            df = df_obj
        else:
            df = pd.DataFrame(df_obj)
        pipe.add_dataframe(name, df)

    for op in plan.get("operations", []):
        op_type = op.get("type")
        if op_type == "join":
            pipe.join(op["left"], op["right"], on=op["on"], how=op.get("how", "left"), output=op.get("output"))
        elif op_type == "union":
            pipe.union(op["left"], op["right"], output=op.get("output"))
        elif op_type == "aggregate":
            pipe.aggregate(op["source"], groupby=op["groupby"], agg_map=op["agg_map"], output=op.get("output"))
        elif op_type == "group_size":
            pipe.group_size(op["source"], groupby=op["groupby"], output=op.get("output"))
        elif op_type == "filter":
            pipe.filter(op["source"], predicate=op["predicate"], output=op.get("output"))
        else:
            raise ValueError(f"Unsupported operation type: {op_type}")

    return pipe
