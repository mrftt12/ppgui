try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:  # for Python < 3.8
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("powerflow_analysis")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

__all__ = ['__version__']
