"""Core Arrow ingestion contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import pyarrow as pa


class MergeStrategy(StrEnum):
    APPEND = "append"
    REPLACE = "replace"


@dataclass(frozen=True)
class Cursor:
    """A destination-derived position for incremental connector reads."""

    field: str
    value: int


class Connector(Protocol):
    """An external system that emits an Arrow stream."""

    def destination_cursor_field(self) -> str | None:
        """Return the destination field used to derive this connector's cursor."""

    def read(self, cursor: Cursor | None = None) -> pa.RecordBatchReader:
        """Return the connector's records as an Arrow stream."""


class Target(Protocol):
    """A destination that consumes an Arrow stream."""

    def read_cursor(self, field: str) -> Cursor | None:
        """Return the maximum persisted integer value for a destination field."""

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
