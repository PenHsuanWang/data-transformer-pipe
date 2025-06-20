# ProcessPipe Tutorial

This tutorial walks you through the core features of **ProcessPipe** using only
Python 3.9+ and pandas. Everything executes in-memory so you can experiment on a
single laptop.

## 1. Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install data-transformer-pipe
pp --version    # verify the CLI is available
```

## 2. Your first fluent pipeline

```python
import pandas as pd
from processpipe import ProcessPipe

people = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
scores = pd.DataFrame({"id": [2, 3, 4], "score": [80, 90, 85]})

pipe = (
    ProcessPipe()
    .add_dataframe("people", people)
    .add_dataframe("scores", scores)
    .join("people", "scores", on="id", how="left", output="joined")
    .filter("joined", predicate="score.notna()", output="present")
)

print(pipe.run())
```

Each operator logs one line showing the output name and shape.

## 3. Running a YAML plan

Create a file `examples/basic_plan.yml`:

```yaml
dataframes:
  df1: !!inline {id: [1,2,3], name: [A,B,C]}
  df2: !!inline {id: [2,3,4], score: [80,90,85]}

operations:
  - id: joined
    type: join
    left: df1
    right: df2
    on: id
    how: left
  - id: present
    type: filter
    source: joined
    predicate: "score.notna()"
```

Run the plan with the CLI:

```bash
pp run examples/basic_plan.yml
```

`pp dag examples/basic_plan.yml` prints the topological order without running.

## 4. Authoring custom operators

```python
from processpipe.decorators import op
import pandas as pd

@op()
def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna()

pipe = (
    ProcessPipe()
    .add_dataframe("raw", scores)
    ._append(drop_nulls("raw", output="clean"))
    .group_size("clean", groupby="score", output="counts")
)

print(pipe.run())
```

## 5. Off-loading heavy steps to Azure Synapse

`processpipe` ships a stub `SynapseNotebookOperator`. Without credentials it
echoes the input DataFrame, but you can replace the body with a real REST call
once your environment is configured.

```python
from processpipe.operators import SynapseNotebookOperator

pipe = (
    ProcessPipe()
    .add_dataframe("raw", scores)
    ._append(
        SynapseNotebookOperator(
            "raw", notebook_path="notebooks/transform.ipynb", output="from_synapse"
        )
    )
)
pipe.run()
```

## 6. Parallel execution

Set `max_workers` if your pipeline branches:

```python
ProcessPipe(max_workers=4)
```

## 7. Custom back-ends

All operators use a `FrameBackend`. The default `InMemoryBackend` simply calls
pandas. You can subclass it to integrate other libraries such as Polars.

