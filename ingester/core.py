"""Core Arrow ingestion contracts."""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol

import pyarrow as pa


class MergeStrategy(StrEnum):
    APPEND = "append"
    REPLACE = "replace"


class Connector(Protocol):
    """An external system that emits an Arrow stream."""

    def read(self) -> pa.RecordBatchReader:
        """Return the connector's records as an Arrow stream."""


class Target(Protocol):
    """A destination that consumes an Arrow stream."""

    def load(self, stream: pa.RecordBatchReader, strategy: MergeStrategy) -> int:
        """Load an Arrow stream and return the number of written rows."""


def parse_merge_strategy(value: str) -> MergeStrategy:
    try:
        return MergeStrategy(value)
    except ValueError as error:
        supported = ", ".join(strategy.value for strategy in MergeStrategy)
        raise ValueError(
            f"Unsupported merge strategy {value!r}. Supported strategies: {supported}."
        ) from error
