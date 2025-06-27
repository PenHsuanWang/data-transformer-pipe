"""Demonstrate additional ProcessPipe operators."""

import pandas as pd
from processpipe import ProcessPipe


def main() -> None:
    customers = pd.DataFrame({"id": [1, 2, 3], "name": ["Ann", None, "Cory"]})
    scores = pd.DataFrame({"id": [1, 2, 3], "score": [80, 90, 70]})

    pipe = (
        ProcessPipe()
        .add_dataframe("cust", customers)
        .add_dataframe("scores", scores)
        .join("cust", "scores", on="id", output="j")
        .fill_na("j", value="Unknown", columns=["name"], output="filled")
        .sort("filled", by="score", ascending=False, output="sorted")
    )

    result = pipe.run()
    print(result)


if __name__ == "__main__":
    main()
