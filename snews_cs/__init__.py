from .core.logging import initialize_logging
import pandas as pd
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("snews-cs")
except PackageNotFoundError:
    # Package is not installed (e.g., running from source without installing)
    __version__ = "unknown"

initialize_logging("debug")

pd.options.mode.chained_assignment = None