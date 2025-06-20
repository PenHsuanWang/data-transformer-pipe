**data-transformer-pipe: A Declarative, Pandas-Based ETL Library**

**Overview**
data-transformer-pipe is a lightweight Python library that empowers data engineers and analysts to build, execute, and visualize complex in-memory transformation pipelines using familiar pandas DataFrames. Rather than scattering imperative code across scripts and notebooks, users describe their entire workflow—joins, unions, filters, aggregations, and more—as a concise, JSON-like plan or via a fluent Python API. Under the hood, data-transformer-pipe translates this plan into a directed acyclic graph (DAG) of atomic “operators,” executes them in order, and captures rich, human-readable logs at each step.

**Key Features**

1. **Declarative Pipeline Definition**
   Define multi-step ETL flows in a single JSON structure or chained method calls. Each operation specifies only its inputs, outputs, and parameters—data-transformer-pipe handles naming, dependency resolution, and execution order automatically.

2. **Operator Abstraction**
   All transformations are encapsulated as subclasses of `BaseOperator` (e.g. `JoinOperator`, `UnionOperator`, `FilterOperator`, `GroupSizeOperator`, `AggregateOperator`). This isolation ensures that each operator can be unit-tested, extended, or replaced independently without touching core logic.

3. **Automatic Chaining & Naming**
   When users omit explicit output names or refer to the special placeholder `_prev`, the library automatically assigns deterministic, collision-free names and wires operators together. This eliminates boilerplate and reduces cognitive load.

4. **In-Memory Execution Engine**
   By leveraging pandas under the hood, pipelines run entirely in memory for data volumes that fit comfortably in RAM. This approach maximizes iteration speed during development and prototyping, without sacrificing the ability to scale up to larger tools like Dask or Polars in the future.

5. **Graphviz Integration & Visualization**
   With one method call—`pipe.describe()`—you can render your pipeline DAG as a PNG, SVG, or PDF diagram. This visual overview makes it trivial to verify complex workflows, share designs with stakeholders, or include pipeline architecture diagrams in documentation.

6. **Rich Logging & Error Surfacing**
   Every operator logs its action in the form

   ```
   [OperatorName] <inputs> → '<output>' shape=(rows,cols)
   ```

   If a required DataFrame or column is missing, the library raises clear, context-rich exceptions, making troubleshooting straightforward.

7. **Extensibility & Plugins**
   Adding new operations is as simple as subclassing `BaseOperator` and registering your class. No modifications to the core engine are needed—perfect for domain-specific transformations, data-quality checks, or custom aggregations.

8. **Fluent API & Builder Methods**
   For users who prefer Python over JSON, the library exposes chainable methods like `.join()`, `.union()`, `.filter()`, `.group_size()`, and `.aggregate()`. This fluent interface produces the same underlying plan, combining readability with brevity.

**Use Cases**

* **Ad-hoc Analytics**: Quickly prototype multi-step data transformations without switching contexts between notebooks, scripts, and external orchestrators.
* **Reusable ETL Jobs**: Package pipelines as version-controlled Python modules, share across teams, and integrate into CI/CD for automated testing.
* **Educational & Documentation**: Teach data transformation concepts using clear diagrams and declarative syntax.
* **Embedded Workflows**: Embed the library in larger applications—web services, reporting tools, or interactive dashboards—to power dynamic data flows without heavy dependencies.

**Getting Started**
Install via pip:

```bash
pip install data-transformer-pipe
```

See [docs/tutorial.md](docs/tutorial.md) for a step-by-step walkthrough.

Then, in your script or notebook:

```python
from processpipe import ProcessPipe

plan = {
  "dataframes": {"df1": df1, "df2": df2},
  "operations": [
    {"type": "join", "left": "df1", "right": "df2", "on": "id"},
    {"type": "filter", "input": "_prev", "condition": "value > 0"}
  ]
}

pipe = ProcessPipe.build_pipe(plan)
pipe.describe("pipeline_diagram")  # Generates pipeline_diagram.png
result = pipe.run()
```

**Quick Start (CLI)**
After installing the package, you can run a pipeline directly from the command line.

1. Create input CSV files `df1.csv` and `df2.csv`:

   ```csv
   # df1.csv
   id,name
   1,A
   2,B
   ```

   ```csv
   # df2.csv
   id,score
   1,10
   2,20
   ```

2. Write a `plan.json` describing the pipeline:

   ```json
   {
     "dataframes": {"df1": "df1.csv", "df2": "df2.csv"},
     "operations": [{"type": "join", "left": "df1", "right": "df2", "on": "id"}]
   }
   ```

3. Execute the plan and save the result:

   ```bash
   pp run plan.json -o result.csv
   ```

   You can inspect the execution order without running it:

   ```bash
   pp dag plan.json
   ```

   The resulting `result.csv` will contain the joined rows.

**Additional Examples**

You can build more complex plans or run them directly from Python.

1. **Union Example**

   ```python
   import pandas as pd
   from processpipe import ProcessPipe

   df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
   df2 = pd.DataFrame({"id": [3], "value": [30]})

   pipe = ProcessPipe().add_dataframe("a", df1).add_dataframe("b", df2)
   pipe.union("a", "b", output="all")
   result = pipe.run()
   print(result._rows)
   ```

2. **Quick Analysis**

   After a pipeline runs you have a normal pandas DataFrame. You can
   perform any analysis you like, for example calculating averages:

   ```python
   values = [int(r["value"]) for r in result._rows]
 avg_value = sum(values) / len(values)
  print("Average:", avg_value)
  ```

## Design & Architecture

The repository is organized as two cooperating packages:

* **`src/data_transformer_pipe`** – exposes a small operator framework and the
  command line interface.  It provides a simple `ProcessPipe` class that can be
  constructed from a plan dictionary and executed directly.
* **`processpipe/processpipe_pkg`** – contains the more feature complete
  pipeline engine used by the examples.  Its submodules are:
  * `core` – the advanced `ProcessPipe` implementation, DAG management and
    backend abstraction.
  * `operators` – individual transformation classes like
    `JoinOperator`, `UnionOperator`, `AggregationOperator`,
    `GroupSizeOperator` and `FilterOperator`.
  * `plans` – helper utilities for loading YAML/JSON pipeline descriptions.

Every pipeline step is represented by an `Operator` subclass that implements
`_execute_core`.  When a pipeline runs, the `ProcessPipe` builds a
`networkx.DiGraph` to track dependencies and executes operators in topological
order.  A pluggable `FrameBackend` (currently `InMemoryBackend` using pandas)
handles DataFrame operations so that alternative implementations can be added
later.  The examples in the `examples/` directory demonstrate how to compose
these pieces.

**Community & Contributions**
data-transformer-pipe is open-source under the MIT license. Contributions are welcome—whether to add new operators, improve documentation, or enhance core features. Visit the GitHub repository `data-transformer-pipe` to file issues, submit pull requests, or join discussions.

## Development

This project uses [tox](https://tox.wiki) to manage testing and linting. To run
all checks locally, install `tox` and run:

```bash
pip install tox
tox
```
