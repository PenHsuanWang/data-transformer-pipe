"""Testing utilities for the minimal Click stub."""
from __future__ import annotations

from contextlib import redirect_stdout
from io import StringIO

from . import Result


class CliRunner:
    """Simplified CLI runner used in tests."""

    def invoke(self, cli, args):
        buf = StringIO()
        code = 0
        try:
            with redirect_stdout(buf):
                cli(args)
        except SystemExit as exc:  # pragma: no cover - exit handling
            code = exc.code if isinstance(exc.code, int) else 1
        return Result(code, buf.getvalue())

