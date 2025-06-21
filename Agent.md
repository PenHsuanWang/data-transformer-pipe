
# AGENTS.md  – ProcessPipe guidance for AI coding agents
#
# Agents merge this file with any user-level AGENTS.md.
# Hierarchy:  ~/.codex/AGENTS.md  →  <repo>/AGENTS.md  →  <sub-dir>/AGENTS.md.

## 1  Project layout & what to ignore
| Path | Purpose |
|------|---------|
| **processpipe/processpipe_pkg/core/**        | DAG executor (`pipe.py`), backend abstraction (`backend.py`) |
| **processpipe/processpipe_pkg/operators/**   | Built-in Operator subclasses (join, union, aggregation, etc.) |
| **processpipe/processpipe_pkg/cli/**         | Click CLI (`run_cmd.py`, `dag_cmd.py`) |
| **processpipe/processpipe_pkg/decorators.py**| `@op` / `@asset` helpers |
| **src/data_transformer_pipe/**               | *Legacy* mini example – don’t modify unless tests require. |
| **examples/** / **demo/**                    | End-to-end demos & YAML plan |
| **tests/**            | Unit tests for legacy `data_transformer_pipe` |
| **tests_processpipe/**| Unit tests for advanced engine |
| **click/**, **networkx/**, **pandas/**       | Stub packages for CI – **IGNORE** when importing |

## 2  Quick-start commands
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]            # installs pandas, click, networkx, ruff, etc.
pytest -q tests tests_processpipe
python examples/run_pipeline.py
pp run demo/basic_plan.yml       # CLI demo
```

## 3  Build, lint & test

```bash
make fmt     # black + isort
make lint    # ruff
make test    # pytest
tox -q       # full matrix (if tox installed)
```

## 4  Coding conventions

* **Black** (line length = 88)
* **isort** profile *black*
* **Ruff** for linting
* Run `make fmt lint test` before committing.

## 5  Writing new operators

1. **Decorator route**

   ```python
   from processpipe.processpipe_pkg.decorators import op

   @op()                       # converts fn→Operator
   def drop_nulls(df): return df.dropna()
   pipe._append(drop_nulls("raw", output="clean"))
   ```
2. **Subclass route** – place file in `processpipe/processpipe_pkg/operators/`

   ```python
   class MyOp(Operator):
       def __init__(…): super().__init__("my_out"); self.inputs=[…]
       def _execute_core(self, backend, env): ...
   ```
3. Register helper in `ProcessPipe` and add an example under `examples/`.

## 6  CLI reference

| Command           | Description                               |
| ----------------- | ----------------------------------------- |
| `pp run PLAN.yml` | Build pipeline from YAML/JSON and execute |
| `pp dag PLAN.yml` | Print topological order of the DAG        |
| `pp --version`    | Package version                           |

## 7  Environment variables (Synapse off-load)

| Var                                                                                           | Used by                 | Default               |
| --------------------------------------------------------------------------------------------- | ----------------------- | --------------------- |
| `SYNAPSE_SUBSCRIPTION_ID`                                                                     | SynapseNotebookOperator | *(unset → stub mode)* |
| `SYNAPSE_WORKSPACE_NAME`                                                                      | 〃                       |                       |
| `SYNAPSE_STORAGE_URL`                                                                         | 〃                       |                       |
| If unset, `SynapseNotebookOperator` logs a warning and returns the input DataFrame unchanged. |                         |                       |

## 8  Metrics (optional)

```python
from prometheus_client import start_http_server
start_http_server(9000)
ProcessPipe(max_workers=2).run()   # exposes pp_node_duration_seconds …
```

## 9  Parallelism & spill

* `ProcessPipe(max_workers=N)` → runs independent branches on a `ThreadPoolExecutor`.
* `spill_enabled=True` is ignored by `InMemoryBackend`; will activate once Arrow backend lands.

## 10  Safety rules

* Do **not** import the stub `click/`, `networkx/`, or `pandas/` folders.
* Avoid `git push --force` unless tests instruct explicitly.
* Tests must finish < 30 s on a 2-CPU runner.

## 11  Road-map hints (for autonomous planning)

* Implement `PolarsBackend` (see TODOs in `core/backend.py`).
* Add ArrowSpillBackend + `spill_enabled` toggle.
* CLI flag `pp run --prometheus-port`.
* Replace stubbed Synapse operator with real REST implementation.
