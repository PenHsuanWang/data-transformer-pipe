from __future__ import annotations

import click

from ..plans.loader import load_plan
from ..core.pipe import ProcessPipe
from .run_cmd import run_cmd
from .dag_cmd import dag_cmd


@click.group()
@click.version_option(package_name="processpipe")
def main() -> None:
    """ProcessPipe command-line interface."""


main.add_command(run_cmd, name="run")
main.add_command(dag_cmd, name="dag")
