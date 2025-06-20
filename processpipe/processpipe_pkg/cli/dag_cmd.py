from __future__ import annotations

import click

from ..plans.loader import load_plan


@click.command()
@click.argument("plan")
def dag_cmd(plan: str) -> None:
    """Print the DAG order of the pipeline."""
    pipe = load_plan(plan)
    pipe.describe()
