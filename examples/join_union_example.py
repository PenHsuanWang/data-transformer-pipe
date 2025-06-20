import pandas as pd
from processpipe import ProcessPipe


def main() -> None:
    """Demonstrate join and union operations with simple analysis."""
    # Initial DataFrames
    customers = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
    })

    scores = pd.DataFrame({
        "id": [1, 2, 4],
        "score": [85, 92, 88],
    })

    extra = pd.DataFrame({
        "id": [5],
        "name": ["Daisy"],
        "score": [90],
    })

    # Build and run the pipeline
    pipe = (
        ProcessPipe()
        .add_dataframe("customers", customers)
        .add_dataframe("scores", scores)
        .add_dataframe("extra", extra)
        .join("customers", "scores", on="id", how="left", output="customer_scores")
        .union("customer_scores", "extra", output="all_scores")
    )

    result = pipe.run()
    print("Pipeline result:\n", result, "\n")

    # Simple analysis: average score, ignoring missing values
    avg_score = result["score"].astype(float).mean()
    print("Average score:", avg_score)


if __name__ == "__main__":
    main()
