import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from data_transformer_pipe.pipe import ProcessPipe


def test_run_without_operators():
    pipe = ProcessPipe()
    with pytest.raises(ValueError):
        pipe.run()


def test_build_pipe_invalid_dataframe():
    plan = {"dataframes": {"bad": {"id": [1]}}, "operations": []}
    with pytest.raises(TypeError):
        ProcessPipe.build_pipe(plan)


def test_join_missing_dataframe():
    pipe = ProcessPipe()
    df1 = pd.DataFrame({"id": [1]})
    pipe.add_dataframe("df1", df1)
    pipe.join("df1", "missing", on="id")
    with pytest.raises(KeyError):
        pipe.run()


def test_auto_output_names():
    pipe = ProcessPipe()
    df1 = pd.DataFrame({"id": [1], "v": ["A"]})
    df2 = pd.DataFrame({"id": [1], "v2": ["B"]})
    pipe.add_dataframe("df1", df1).add_dataframe("df2", df2)
    pipe.join("df1", "df2", on="id")
    result = pipe.run()
    expected = pd.DataFrame({"id": [1], "v": ["A"], "v2": ["B"]})
    assert_frame_equal(result, expected)
    assert "df1_left_join_df2" in pipe.env
