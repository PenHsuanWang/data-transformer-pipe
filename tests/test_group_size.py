import pandas as pd
from data_transformer_pipe.pipe import ProcessPipe
from pandas.testing import assert_series_equal


def test_group_size_single_column():
    df = pd.DataFrame({"region": ["N", "S", "N", "N"], "amount": [1, 2, 3, 4]})
    pipe = ProcessPipe().add_dataframe("df", df).group_size("df", groupby="region", output="out")
    result = pipe.run()
    expected = pd.Series([3, 1, 3, 3], name="group_size")
    assert_series_equal(result["group_size"], expected)
