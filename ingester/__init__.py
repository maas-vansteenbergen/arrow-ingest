"""Arrow-native ingestion primitives."""

from .connectors import RestConnector
from .core import Connector, MergeStrategy, Target
from .targets import ParquetTarget

__all__ = [
    "Connector",
    "MergeStrategy",
    "ParquetTarget",
    "RestConnector",
    "Target",
]
