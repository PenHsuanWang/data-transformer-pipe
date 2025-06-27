"""Example script for the integration test scenarios."""

import pandas as pd
from processpipe import ProcessPipe


def scenario_a() -> pd.DataFrame:
    """Big-Basket Customers."""
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "cust_id": [100, 100, 101, 102],
            "sku_id": [10, 11, 12, 13],
            "qty": [2, 1, 5, 3],
            "event_date": pd.to_datetime(
                ["2025-05-01", "2025-05-01", "2025-05-03", "2025-05-04"]
            ),
        }
    )
    late_orders = pd.DataFrame(
        {
            "order_id": [1, 5],
            "cust_id": [100, 103],
            "sku_id": [14, 15],
            "qty": [3, 2],
            "event_date": pd.to_datetime(["2025-05-01", "2025-05-06"]),
        }
    )
    items_dim = pd.DataFrame(
        {
            "sku_id": [10, 11, 12, 13, 14, 15],
            "unit_price": [20, 30, 10, 40, 15, 18],
        }
    )
    returns = pd.DataFrame(
        {
            "order_id": [1, 5],
            "event_date": pd.to_datetime(["2025-05-10", "2025-05-12"]),
            "return_reason": ["DEFECT", "SIZE"],
        }
    )

    pipe = (
        ProcessPipe()
        .add_dataframe("orders", orders)
        .add_dataframe("late_orders", late_orders)
        .add_dataframe("items_dim", items_dim)
        .add_dataframe("returns", returns)
        .join("orders", "items_dim", on="sku_id", how="left", output="orders_j")
        .join("late_orders", "items_dim", on="sku_id", how="left", output="late_j")
        .union("orders_j", "late_j", output="all_orders")
        .group_size("all_orders", groupby="order_id", output="with_counts")
        .filter("with_counts", predicate="group_size >= 5", output="big_orders")
        .aggregate(
            "big_orders",
            groupby="cust_id",
            agg_map={"unit_price": "mean"},
            output="cust_avg",
        )
        .join("cust_avg", "returns", on="cust_id", how="inner", output="with_returns")
        .rolling_agg(
            "with_returns",
            on="unit_price_mean",
            window=30,
            agg="mean",
            output="rolling_avg",
        )
    )
    return pipe.run()


def scenario_b() -> pd.DataFrame:
    """Sensor Drift & Smoothing."""
    day1 = pd.DataFrame(
        {
            "device_id": ["A", "A", "B", "B", "B"],
            "ts": pd.to_datetime(
                [
                    "2025-05-01 00",
                    "2025-05-01 01",
                    "2025-05-01 00",
                    "2025-05-01 01",
                    "2025-05-01 01",
                ]
            ),
            "temp_raw": ["20.1", "nan", "18.5", "18.7", "18.7"],
        }
    )
    day2 = pd.DataFrame(
        {
            "device_id": ["A", "A", "B"],
            "ts": pd.to_datetime(["2025-05-01 02", "2025-05-01 03", "2025-05-01 02"]),
            "temp_raw": ["20.4", "20.8", "19.0"],
        }
    )

    pipe = (
        ProcessPipe()
        .add_dataframe("day1", day1)
        .add_dataframe("day2", day2)
        .union("day1", "day2", output="combined")
        .cast("combined", casts={"temp_raw": float}, output="casted")
        .fill_na("casted", value=None, columns=["temp_raw"], output="filled")
        .drop_duplicates("filled", subset=["device_id", "ts"], output="deduped")
        .sort("deduped", by=["device_id", "ts"], output="sorted")
        .rolling_agg("sorted", on="temp_raw", window=12, agg="mean", output="smoothed")
    )
    return pipe.run()


def scenario_c() -> pd.DataFrame:
    """Churn-Risk Target List."""
    customers = pd.DataFrame(
        {
            "cust_id": [100, 101, 102, 103],
            "region": ["E", "W", "E", "E"],
            "active_flag": [1, 1, 0, 1],
        }
    )
    calls = pd.DataFrame(
        {
            "cust_id": [100, 100, 101, 102, 103, 103],
            "minutes": [30, 15, 50, 2, 5, 4],
            "month": [
                "2025-04",
                "2025-05",
                "2025-05",
                "2025-05",
                "2025-05",
                "2025-05",
            ],
        }
    )
    reactivations = pd.DataFrame({"cust_id": [102], "camp_id": ["SPRING25"]})

    pipe = (
        ProcessPipe()
        .add_dataframe("customers", customers)
        .add_dataframe("calls", calls)
        .add_dataframe("reactivations", reactivations)
        .join("calls", "customers", on="cust_id", how="left", output="with_region")
        .aggregate(
            "with_region",
            groupby=["cust_id", "region"],
            agg_map={"minutes": "sum"},
            output="call_totals",
        )
        .case(
            "call_totals",
            conditions=["minutes < 10"],
            choices=[1],
            output_col="churn_flag",
            output="flagged",
        )
        .delete(
            "flagged", condition="cust_id in reactivations.cust_id", output="filtered"
        )
        .top_n(
            "filtered",
            n=2,
            metric="minutes",
            largest=False,
            per_group=True,
            group_keys="region",
            output="top_risk",
        )
    )
    return pipe.run()


if __name__ == "__main__":
    print("Scenario A Result:\n", scenario_a(), "\n")
    print("Scenario B Result:\n", scenario_b(), "\n")
    print("Scenario C Result:\n", scenario_c(), "\n")
