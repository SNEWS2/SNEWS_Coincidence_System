from .core.logging import initialize_logging

initialize_logging("debug")
import pandas as pd
pd.options.mode.chained_assignment = None

try:
    from ._version import version as __version__
except ImportError:
    pass
