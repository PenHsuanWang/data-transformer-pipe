from . import processpipe_pkg as _pkg

from .processpipe_pkg import cli

__all__ = list(_pkg.__all__) + ["cli"]
globals().update({k: getattr(_pkg, k) for k in _pkg.__all__})
