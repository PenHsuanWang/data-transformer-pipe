"""Minimal subset of the Click library used for tests."""
from __future__ import annotations

import sys
from functools import wraps


def command():
    """Decorator to mark a function as a command."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args):
            if args and args[0] in {"--help", "-h"}:
                echo(func.__doc__ or "")
                return
            return func(*args)

        return wrapper
    return decorator


def argument(name):  # noqa: ARG001 - name kept for compatibility
    """Decorator for command arguments (no-op)."""
    def decorator(func):
        return func
    return decorator


def group():
    """Decorator to create a command group."""
    def decorator(func):
        commands = {}

        @wraps(func)
        def wrapper(args=None):
            if args is None:
                args = sys.argv[1:]
            if not args or args[0] in {"-h", "--help"}:
                echo("Usage: [COMMAND]")
                echo("Commands:")
                for name in commands:
                    echo(f"  {name}")
                return
            cmd_name = args[0]
            cmd = commands.get(cmd_name)
            if cmd is None:
                raise SystemExit(1)
            if len(args) > 1 and args[1] in {"-h", "--help"}:
                echo(cmd.__doc__ or "")
                return
            return cmd(*args[1:])

        def add_command(cmd, name=None):
            commands[name or cmd.__name__] = cmd

        wrapper.add_command = add_command
        return wrapper

    return decorator


def version_option(package_name=None):  # noqa: ARG001 - unused
    """Decorator for --version option (no-op)."""
    def decorator(func):
        return func
    return decorator


def echo(message):
    """Print a message to stdout."""
    print(message)


class Result:
    def __init__(self, exit_code: int, output: str) -> None:
        self.exit_code = exit_code
        self.output = output


from .testing import CliRunner  # re-export for convenience

