import pandas as pd
from pandas.testing import assert_frame_equal

from processpipe import ProcessPipe


def test_full_operator_chain():
    customers = pd.DataFrame({"cust_id": [1, 2, 3], "region": ["east", "west", "east"]})
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4, 5],
            "cust_id": [1, 1, 2, 2, 3],
            "amount": [120, 30, 200, 50, 80],
        }
    )
    extra = pd.DataFrame({"region": ["north"], "amount": [0], "group_size": [0]})

    pipe = (
        ProcessPipe()
        .add_dataframe("customers", customers)
        .add_dataframe("orders", orders)
        .add_dataframe("extra", extra)
        .join("orders", "customers", on="cust_id", output="orders_cust")
        .filter("orders_cust", predicate="amount > 100", output="big_orders")
        .group_size("big_orders", groupby="cust_id", output="big_orders_count")
        .aggregate(
            "big_orders_count",
            groupby="region",
            agg_map={"amount": "sum", "group_size": "sum"},
            output="region_totals",
        )
        .union("region_totals", "extra", output="all_regions")
    )

    result = pipe.run().reset_index(drop=True)
    expected = pd.DataFrame(
        {
            "region": ["east", "west", "north"],
            "amount": [120, 200, 0],
            "group_size": [1, 1, 0],
        }
    )
    assert_frame_equal(result, expected)
