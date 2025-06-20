from __future__ import annotations

import inspect
from typing import Any, Dict, Callable
import pandas as pd

from .operators import Operator
from .core.backend import FrameBackend


def op() -> Callable[[Callable[..., pd.DataFrame]], type[Operator]]:
    """Decorator turning a function into an :class:`Operator` subclass."""

    def wrapper(func: Callable[..., pd.DataFrame]) -> type[Operator]:
        inputs = list(inspect.signature(func).parameters)

        class FunctionOperator(Operator):
            def __init__(self, *, output: str | None = None, **kwargs: Any) -> None:
                super().__init__(output or func.__name__)
                self.kwargs = kwargs
                self.inputs.extend(inputs)

            def _execute_core(
                self, backend: FrameBackend, env: Dict[str, pd.DataFrame]
            ) -> pd.DataFrame:
                args = [env[name] for name in inputs]
                return func(*args, **self.kwargs)

        FunctionOperator.__name__ = func.__name__
        return FunctionOperator

    return wrapper
