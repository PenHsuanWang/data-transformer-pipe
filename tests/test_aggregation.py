import pandas as pd
from data_transformer_pipe.pipe import ProcessPipe
from pandas.testing import assert_frame_equal


def test_groupby_agg():
    df = pd.DataFrame({"cat": [1, 1, 2], "val": [10, 20, 30]})
    pipe = ProcessPipe().add_dataframe("df", df)
    pipe.aggregate("df", groupby="cat", agg_map={"val": "sum"}, output="out")
    result = pipe.run().reset_index(drop=True)
    expected = pd.DataFrame({"cat": [1, 2], "val": [30, 30]})
    assert_frame_equal(result, expected)
