import json
import subprocess
from pathlib import Path

from click.testing import CliRunner
from processpipe.cli import main


def _create_plan(tmp_path: Path) -> Path:
    plan = {
        "dataframes": {
            "df1": {"id": [1], "v": ["A"]},
            "df2": {"id": [1], "v2": ["B"]},
        },
        "operations": [
            {
                "type": "join",
                "left": "df1",
                "right": "df2",
                "on": [["id", "id"]],
                "output": "j",
            }
        ],
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    return path


def test_cli_run(tmp_path):
    plan_path = _create_plan(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["run", str(plan_path)])
    assert result.exit_code == 0
    assert "DataFrame" in result.output


def test_cli_dag(tmp_path):
    plan_path = _create_plan(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["dag", str(plan_path)])
    assert result.exit_code == 0
    assert "JoinOperator" in result.output


def test_python_module_entrypoint(tmp_path):
    plan_path = _create_plan(tmp_path)
    res = subprocess.run(
        [
            "python",
            "-m",
            "processpipe",
            "run",
            str(plan_path),
        ],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0
    assert "DataFrame" in res.stdout
