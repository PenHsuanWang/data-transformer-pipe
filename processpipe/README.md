# ProcessPipe

Minimal, chain-style ETL framework for pandas.  
```python
from processpipe import ProcessPipe
...
result = (
    ProcessPipe()
      .add_dataframe("df1", df1)
      .join("df1", "df2", on="id")
      .run()
)
