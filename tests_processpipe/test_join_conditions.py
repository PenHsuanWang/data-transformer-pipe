import pandas as pd
from pandas.testing import assert_frame_equal

from processpipe import ProcessPipe


def test_join_with_conditions():
    left = pd.DataFrame({"id": [1, 2, 3], "val": ["A", "B", "C"]})
    right = pd.DataFrame({"id": [1, 2, 4], "val": ["X", "B", "D"], "score": [10, 20, 30]})

    pipe = (
        ProcessPipe()
        .add_dataframe("left", left)
        .add_dataframe("right", right)
        .join(
            "left",
            "right",
            on="id",
            how="inner",
            conditions=[("val", "!=", "val")],
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

