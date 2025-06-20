from . import processpipe_pkg as _pkg

__all__ = list(_pkg.__all__) + ["cli"]
globals().update({k: getattr(_pkg, k) for k in _pkg.__all__})

def __getattr__(name):
    if name == "cli":
        from .processpipe_pkg import cli
        return cli
    raise AttributeError(f"module {__name__} has no attribute {name}")

