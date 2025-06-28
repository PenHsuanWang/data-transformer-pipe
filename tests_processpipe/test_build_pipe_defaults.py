import pandas as pd
from pandas.testing import assert_frame_equal

from processpipe import ProcessPipe


def test_build_pipe_join_default_inner():
    plan = {
        "dataframes": {
            "df1": pd.DataFrame({"id": [1, 2], "val1": ["A", "B"]}),
            "df2": pd.DataFrame({"id": [2], "val2": ["X"]}),
        },
        "operations": [
            {
                "type": "join",
                "left": "df1",
                "right": "df2",
                "on": "id",
                "output": "joined",
            }
        ],
    }
    pipe = ProcessPipe.build_pipe(plan)
    result = pipe.run().reset_index(drop=True)
    expected = pd.DataFrame({"id": [2], "val1": ["B"], "val2": ["X"]})
    assert_frame_equal(result, expected)
