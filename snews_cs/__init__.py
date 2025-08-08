from .core.logging import initialize_logging
import pandas as pd

initialize_logging("debug")

pd.options.mode.chained_assignment = None

try:
    from ._version import version as __version__
except ImportError:
    pass


def __getattr__(name):
    if name == "__version__":
        return __version__
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
