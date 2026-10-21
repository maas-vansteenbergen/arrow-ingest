"""Arrow-native ingestion primitives."""

from .connectors import ParquetConnector, RestConnector
from .core import Connector, MergeStrategy, Target
from .targets import DuckDBTarget, ParquetTarget

__all__ = [
    "Connector",
    "DuckDBTarget",
    "MergeStrategy",
    "ParquetConnector",
    "ParquetTarget",
    "RestConnector",
    "Target",
]
