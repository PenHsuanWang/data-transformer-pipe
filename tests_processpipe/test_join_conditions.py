import pandas as pd
from pandas.testing import assert_frame_equal

import pytest

from processpipe import ProcessPipe
from processpipe.processpipe_pkg.operators.join import JoinError


def test_join_with_conditions():
    left = pd.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]})
    right = pd.DataFrame(
        {"id": [1, 2, 4], "val": ["X", "B", "D"], "score": [10, 20, 30]}
    )

    pipe = (
        ProcessPipe()
        .add_dataframe("left", left)
        .add_dataframe("right", right)
        .join(
            "left",
            "right",
            on="id",
            how="inner",
            conditions=[("val", "neq", "val")],
            output="joined",
        )
    )

    result = pipe.run().reset_index(drop=True)

    expected = pd.DataFrame(
        {
            "id": [1],
            "val_left": ["A"],
            "val_right": ["X"],
            "score": [10],
        }
    )

    assert_frame_equal(result, expected)


def test_join_condition_length_mismatch():
    df = pd.DataFrame({"id": [1]})
    pipe = (
        ProcessPipe()
        .add_dataframe("l", df)
        .add_dataframe("r", df)
        .join("l", "r", on=["id"], conditions=[("id", "eq", "id"), ("id", "eq", "id")])
    )
    with pytest.raises(JoinError.param_conflict) as exc:
        pipe.run()
    assert exc.value.code == "length_mismatch"


def test_join_condition_bad_operator():
    df = pd.DataFrame({"id": [1]})
    pipe = (
        ProcessPipe()
        .add_dataframe("l", df)
        .add_dataframe("r", df)
        .join("l", "r", on="id", conditions=[("id", "~~", "id")])
    )
    with pytest.raises(JoinError.param_conflict) as exc:
        pipe.run()
    assert exc.value.code == "bad_operator"
