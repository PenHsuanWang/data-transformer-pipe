from __future__ import annotations

from typing import Dict
import pandas as pd
from .base import Operator
from ..core.backend import FrameBackend


class SynapseNotebookOperator(Operator):
    """Stub operator that would offload work to Azure Synapse."""

    def __init__(self, source: str, *, output: str | None = None) -> None:
        super().__init__(output or f"{source}_synapse")
        self.source = source
        self.inputs = [source]

    def _execute_core(self, backend: FrameBackend, env: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        # Placeholder for real REST API interaction. For now just echo back.
        return env[self.source]
