"""Arrow-native ingestion primitives."""

from .connectors import ParquetConnector, RestConnector
from .core import Connector, Cursor, MergeStrategy, Target
from .targets import DuckDBTarget, ParquetTarget

__all__ = [
    "Connector",
    "Cursor",
    "DuckDBTarget",
    "MergeStrategy",
    "ParquetConnector",
    "ParquetTarget",
    "RestConnector",
    "Target",
]
