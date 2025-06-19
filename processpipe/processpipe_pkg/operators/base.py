from __future__ import annotations
import abc
import pandas as pd
from typing import Dict, List
from ..core.backend import FrameBackend
import logging

log = logging.getLogger("processpipe")


class Operator(abc.ABC):
    """Abstract ETL step: subclasses define `_execute_core` only."""

    def __init__(self, output: str):
        self.output = output
        self.inputs: List[str] = []

    @abc.abstractmethod
    def _execute_core(self, backend: FrameBackend,
                      env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        ...

    def execute(self, backend: FrameBackend,
                env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        for k in self.inputs:
            if k not in env:
                raise KeyError(f"{self.__class__.__name__}: missing '{k}'")
        res = self._execute_core(backend, env)
        log.info("%s -> '%s' shape=%s",
                 self.__class__.__name__, self.output, res.shape)
        return res
