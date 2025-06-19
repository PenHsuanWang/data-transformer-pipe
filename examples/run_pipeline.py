"""
End-to-end demo of ProcessPipe.
Run:  python examples/run_pipeline.py
"""
from pathlib import Path
import pandas as pd
from processpipe import ProcessPipe

HERE = Path(__file__).parent
customers = pd.DataFrame({
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Carol", "Dan"],
    "region": ["N", "S", "N", "W"]
})
orders = pd.DataFrame({
    "id": [1, 2, 2, 3, 4],
    "amount": [10, 20, 30, 40, 50]
})

pipe = (
    ProcessPipe()
      .add_dataframe("cust", customers)
      .add_dataframe("orders", orders)
      .join("cust", "orders", on="id", how="inner", output="cust_orders")
      .group_size("cust_orders", groupby="region", output="with_counts")
      .filter("with_counts", predicate="group_size >= 2",
              output="active_regions")
      .aggregate("active_regions", groupby="region",
                 agg_map={"amount": "sum"}, output="region_sales")
)

print(pipe.run())
