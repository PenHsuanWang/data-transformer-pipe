import pandas as pd
from processpipe import ProcessPipe


def test_sort_and_fillna():
    df = pd.DataFrame({"id": [2, 1], "val": [1, None]})
    pipe = ProcessPipe().add_dataframe("df", df)
    pipe.sort("df", by="id", output="s").fillna("s", value={"val": 0}, output="f")
    result = pipe.run().reset_index(drop=True)
    assert list(result["id"]) == [1, 2]
    assert list(result["val"]) == [0, 1]


def test_row_number():
    df = pd.DataFrame({"grp": ["a", "a", "b"], "val": [3, 1, 2]})
    pipe = ProcessPipe().add_dataframe("df", df)
    pipe.row_number("df", groupby="grp", output="r")
    result = pipe.run()
    assert list(result["row_number"]) == [1, 2, 1]
