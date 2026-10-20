"""Arrow-native ingestion primitives."""

from .connectors import ParquetConnector, RestConnector
from .core import Connector, MergeStrategy, Target
from .targets import ParquetTarget

__all__ = [
    "Connector",
    "MergeStrategy",
    "ParquetConnector",
    "ParquetTarget",
    "RestConnector",
    "Target",
]
