import pandas as pd
from pandas.testing import assert_frame_equal

from data_transformer_pipe.pipe import ProcessPipe


def test_join_and_union():
    df1 = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    df2 = pd.DataFrame({"id": [2, 3, 4], "score": [80, 90, 85]})
    df3 = pd.DataFrame({"id": [5], "name": ["D"], "score": [75]})

    plan = {
        "dataframes": {"df1": df1, "df2": df2, "df3": df3},
        "operations": [
            {
                "type": "join",
                "left": "df1",
                "right": "df2",
                "on": "id",
                "output": "joined",
            },
            {
                "type": "union",
                "left": "joined",
                "right": "df3",
                "output": "final",
            },
        ],
    }

    pipe = ProcessPipe.build_pipe(plan)
    result = pipe.run()

    expected = pd.DataFrame(
        {
            "id": [1, 2, 3, 5],
            "name": ["A", "B", "C", "D"],
            "score": [pd.NA, 80, 90, 75],
        }
    )

    assert_frame_equal(result.reset_index(drop=True), expected)
