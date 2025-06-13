import json
import subprocess
import sys
from pathlib import Path


def test_cli_runs(tmp_path: Path) -> None:
    plan = {
        "dataframes": {
            "df1": {"id": [1], "name": ["A"]},
            "df2": {"id": [1], "score": [99]},
        },
        "operations": [{"type": "join", "left": "df1", "right": "df2", "on": "id"}],
    }
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plan))

    result = subprocess.run(
        [sys.executable, "-m", "data_transformer_pipe", str(plan_file)],
        capture_output=True,
        text=True,
        check=True,
    )

    assert "score" in result.stdout
