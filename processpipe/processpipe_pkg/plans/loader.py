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
        elif op_type == "rolling_agg":
            pipe.rolling_agg(op["source"], on=op["on"], window=op["window"], agg=op["agg"], output=op.get("output"))
        elif op_type == "sort":
            pipe.sort(op["source"], by=op["by"], ascending=op.get("ascending", True), output=op.get("output"))
        elif op_type == "top_n":
            pipe.top_n(op["source"], n=op["n"], metric=op["metric"], largest=op.get("largest", True), per_group=op.get("per_group", False), group_keys=op.get("group_keys"), output=op.get("output"))
        elif op_type == "fill_na":
            pipe.fill_na(op["source"], value=op["value"], columns=op.get("columns"), output=op.get("output"))
        elif op_type == "rename":
            pipe.rename(op["source"], columns=op["columns"], output=op.get("output"))
        elif op_type == "cast":
            pipe.cast(op["source"], casts=op["casts"], output=op.get("output"))
        elif op_type == "string_op":
            pipe.string_op(op["source"], column=op["column"], op=op["op"], pattern=op["pattern"], replacement=op.get("replacement"), new_column=op.get("new_column"), output=op.get("output"))
        elif op_type == "drop_duplicates":
            pipe.drop_duplicates(op["source"], subset=op.get("subset"), keep=op.get("keep", "first"), output=op.get("output"))
        elif op_type == "partition_agg":
            pipe.partition_agg(op["source"], groupby=op["groupby"], agg_map=op["agg_map"], output=op.get("output"))
        elif op_type == "row_number":
            pipe.row_number(op["source"], partition_by=op.get("partition_by"), order_by=op.get("order_by"), output=op.get("output"))
        elif op_type == "delete":
            pipe.delete(op["source"], condition=op["condition"], output=op.get("output"))
        elif op_type == "update":
            pipe.update(op["source"], condition=op["condition"], set_map=op["set"], output=op.get("output"))
        elif op_type == "case":
            pipe.case(op["source"], conditions=op["conditions"], choices=op["choices"], default=op.get("default"), output_col=op.get("output_col", "case"), output=op.get("output"))
        else:
            raise ValueError(f"Unsupported operation type: {op_type}")

    return pipe
