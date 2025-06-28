import pandas as pd
from pandas.testing import assert_frame_equal
from processpipe import ProcessPipe


def test_complex_operator_chain():
    customers = pd.DataFrame(
        {"cust_id": [1, 2], "region": ["east", "west"], "status": ["vip", "reg"]}
    )
    orders = pd.DataFrame(
        {"order_id": [1, 2, 3], "cust_id": [1, 1, 2], "amount": [100, 40, 200]}
    )
    returns = pd.DataFrame({"order_id": [2], "return_flag": [True]})
    extra = pd.DataFrame(
        {"region": ["north"], "total_amount": [0], "total_orders": [0]}
    )

    pipe = (
        ProcessPipe()
        .add_dataframe("customers", customers)
        .add_dataframe("orders", orders)
        .add_dataframe("returns", returns)
        .add_dataframe("extra", extra)
        .join(
            "orders",
            "returns",
            on=[("order_id", "order_id")],
            how="left",
            output="orders_ret",
        )
        .fill_na(
            "orders_ret",
            value=False,
            columns=["return_flag"],
            output="orders_ret_filled",
        )
        .join(
            "orders_ret_filled",
            "customers",
            on=[("cust_id", "cust_id")],
            output="orders_full",
        )
        .filter(
            "orders_full",
            predicate="amount > 50 and return_flag == False",
            output="big_orders",
        )
        .group_size("big_orders", groupby="cust_id", output="with_count")
        .aggregate(
            "with_count",
            groupby="region",
            agg_map={"amount": "sum", "group_size": "sum"},
            output="region_totals",
        )
        .rename(
            "region_totals",
            columns={"amount": "total_amount", "group_size": "total_orders"},
            output="renamed_totals",
        )
        .union("renamed_totals", "extra", output="with_extra")
        .sort("with_extra", by="total_amount", ascending=False, output="sorted")
        .row_number("sorted", output="final")
    )

    result = pipe.run().reset_index(drop=True)
    expected = pd.DataFrame(
        {
            "region": ["west", "east", "north"],
            "total_amount": [200, 100, 0],
            "total_orders": [1, 1, 0],
            "row_number": [1, 2, 3],
        }
    )
    assert_frame_equal(result, expected)
