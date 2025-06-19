import pandas as pd
from processpipe import load_plan


def test_load_plan_json():
    plan = {
        "dataframes": {
            "df1": {"id": [1, 2], "v": [10, 20]},
            "df2": {"id": [2], "v2": [30]},
        },
        "operations": [
            {"type": "join", "left": "df1", "right": "df2", "on": "id", "how": "left", "output": "j"},
            {"type": "filter", "source": "j", "predicate": "v2 > 25", "output": "f"},
        ],
    }
    import json, tempfile
    with tempfile.NamedTemporaryFile("w+", suffix=".json", delete=False) as f:
        json.dump(plan, f)
        f.flush()
        pipe = load_plan(f.name)
    result = pipe.run()
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["id", "v", "v2"]
