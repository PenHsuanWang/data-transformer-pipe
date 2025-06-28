import pandas as pd
from pandas.testing import assert_frame_equal

from processpipe import ProcessPipe


def test_right_join_with_conditions():
    left = pd.DataFrame({"id": [1, 2], "val": ["A", "B"]})
    right = pd.DataFrame({"id": [1, 3], "val": ["X", "C"]})

    pipe = (
        ProcessPipe()
        .add_dataframe("l", left)
        .add_dataframe("r", right)
        .join("l", "r", on="id", how="right", conditions=[("val", "!=", "val")], output="out")
    )

    result = pipe.run().reset_index(drop=True)
    expected = pd.DataFrame({"id": [1, 3], "val_left": ["A", None], "val_right": ["X", "C"]})
    assert_frame_equal(result, expected)


def test_outer_join_with_conditions():
    left = pd.DataFrame({"id": [1, 2], "val": ["A", "B"]})
    right = pd.DataFrame({"id": [2, 3], "val": ["B", "C"]})

    pipe = (
        ProcessPipe()
        .add_dataframe("l", left)
        .add_dataframe("r", right)
        .join("l", "r", on="id", how="outer", conditions=[("val", "!=", "val")], output="out")
    )

    result = pipe.run().reset_index(drop=True)
    expected = pd.DataFrame({"id": [1, 3], "val_left": ["A", None], "val_right": [None, "C"]})
    assert_frame_equal(result, expected)


def test_cross_join_with_conditions():
    left = pd.DataFrame({"id": [1, 2], "val": ["A", "B"]})
    right = pd.DataFrame({"id": [10, 20], "val": ["A", "C"]})

    pipe = (
        ProcessPipe()
        .add_dataframe("l", left)
        .add_dataframe("r", right)
        .join("l", "r", on="id", how="cross", conditions=[("val", "!=", "val")], output="out")
    )

    result = pipe.run().reset_index(drop=True)
    expected = pd.DataFrame(
        {
            "id_left": [1, 2, 2],
            "val_left": ["A", "B", "B"],
            "id_right": [20, 10, 20],
            "val_right": ["C", "A", "C"],
        }
    )
    assert_frame_equal(result, expected)
