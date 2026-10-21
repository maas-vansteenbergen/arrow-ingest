"""Target implementations."""

from .duckdb import DuckDBTarget
from .parquet import ParquetTarget

__all__ = ["DuckDBTarget", "ParquetTarget"]
