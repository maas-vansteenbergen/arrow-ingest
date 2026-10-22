"""Arrow-native ingestion primitives."""

from .connectors import ParquetConnector, RestConnector
from .core import Connector, Cursor, MergeConfiguration, MergeStrategy, SCD2Configuration, Target
from .targets import DuckDBTarget, ParquetTarget

__all__ = [
    "Connector",
    "Cursor",
    "MergeConfiguration",
    "DuckDBTarget",
    "MergeStrategy",
    "ParquetConnector",
    "ParquetTarget",
    "RestConnector",
    "SCD2Configuration",
    "Target",
]
