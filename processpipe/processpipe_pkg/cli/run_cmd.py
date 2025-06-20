from __future__ import annotations

import click

from ..plans.loader import load_plan


@click.command()
@click.argument("plan")
def run_cmd(plan: str) -> None:
    """Execute a pipeline plan file."""
    pipe = load_plan(plan)
    result = pipe.run()
    click.echo(result)
