from .core.logging import initialize_logging

initialize_logging("debug")

try:
    from ._version import version as __version__
except ImportError:
    pass
