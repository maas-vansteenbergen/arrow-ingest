"""Core Arrow ingestion contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

import pyarrow as pa


class MergeStrategy(StrEnum):
    APPEND = "append"
    REPLACE = "replace"
    UPSERT = "upsert"


@dataclass(frozen=True)
class Cursor:
    """A destination-derived position for incremental connector reads."""

    field: str
    value: int


@dataclass(frozen=True)
class MergeConfiguration:
    """The strategy and key fields that define a target load."""

    strategy: MergeStrategy
    keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not all(isinstance(key, str) and key for key in self.keys):
            raise ValueError("merge keys must be non-empty strings.")
        if len(set(self.keys)) != len(self.keys):
            raise ValueError("merge keys must not contain duplicates.")
        if self.strategy is MergeStrategy.UPSERT and not self.keys:
            raise ValueError("Upsert merge configurations must define one or more keys.")
        if self.strategy is not MergeStrategy.UPSERT and self.keys:
            raise ValueError(f"Merge strategy {self.strategy.value!r} does not accept keys.")


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

    def load(self, stream: pa.RecordBatchReader, merge: MergeConfiguration) -> int:
        """Load an Arrow stream and return the number of written rows."""


def parse_merge_strategy(value: str) -> MergeStrategy:
    try:
        return MergeStrategy(value)
    except ValueError as error:
        supported = ", ".join(strategy.value for strategy in MergeStrategy)
        raise ValueError(
            f"Unsupported merge strategy {value!r}. Supported strategies: {supported}."
        ) from error


def parse_merge_configuration(value: Any) -> MergeConfiguration:
    if isinstance(value, str):
        strategy = parse_merge_strategy(value)
        if strategy is MergeStrategy.UPSERT:
            raise ValueError("merge upsert must define one or more keys.")
        return MergeConfiguration(strategy)
    if not isinstance(value, Mapping):
        raise ValueError("merge must be a strategy string or a mapping.")

    strategy_value = value.get("strategy")
    if not isinstance(strategy_value, str):
        raise ValueError("merge.strategy must be a string.")
    strategy = parse_merge_strategy(strategy_value)
    return MergeConfiguration(strategy, _merge_keys(value.get("keys")))


def _merge_keys(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise ValueError("merge.keys must be a list of non-empty strings.")
    return tuple(value)
