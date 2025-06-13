from data_transformer_pipe.transformer import DataTransformer


def test_transformer_returns_input():
    transformer = DataTransformer()
    data = {"key": "value"}
    assert transformer.transform(data) == data
