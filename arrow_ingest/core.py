"""Core Arrow ingestion contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

import pyarrow as pa


class MergeStrategy(StrEnum):
    """Select how a target incorporates an incoming Arrow stream."""

    APPEND = "append"
    REPLACE = "replace"
    SCD2 = "scd2"
    UPSERT = "upsert"


@dataclass(frozen=True)
class Cursor:
    """Represent a destination-derived position for incremental connector reads.

    Attributes:
        field: The source field used to order records.
        value: The last loaded value for the field.
    """

    field: str
    value: int


@dataclass(frozen=True)
class MergeConfiguration:
    """Define the strategy and fields that control a target load.

    Attributes:
        strategy: The operation used to merge incoming records.
        keys: Fields that uniquely identify records for upsert and SCD2 loads.
        scd2: Versioning settings required for SCD2 loads.

    Raises:
        ValueError: If the settings are incompatible with the selected strategy
            or contain invalid or duplicate field names.
    """

    strategy: MergeStrategy
    keys: tuple[str, ...] = ()
    scd2: SCD2Configuration | None = None

    def __post_init__(self) -> None:
        if not all(isinstance(key, str) and key for key in self.keys):
            raise ValueError("merge keys must be non-empty strings.")
        if len(set(self.keys)) != len(self.keys):
            raise ValueError("merge keys must not contain duplicates.")
        if self.strategy in (MergeStrategy.UPSERT, MergeStrategy.SCD2) and not self.keys:
            raise ValueError(
                f"{_strategy_label(self.strategy)} merge configurations must define one or more keys."
            )
        if self.strategy not in (MergeStrategy.UPSERT, MergeStrategy.SCD2) and self.keys:
            raise ValueError(f"Merge strategy {self.strategy.value!r} does not accept keys.")
        if self.strategy is MergeStrategy.SCD2 and self.scd2 is None:
            raise ValueError("SCD2 merge configurations must define SCD2 settings.")
        if self.strategy is not MergeStrategy.SCD2 and self.scd2 is not None:
            raise ValueError(
                f"Merge strategy {self.strategy.value!r} does not accept SCD2 settings."
            )
        if self.scd2 is not None and set(self.keys).intersection(self.scd2.tracked_fields):
            raise ValueError("SCD2 tracked fields must not include key fields.")


@dataclass(frozen=True)
class SCD2Configuration:
    """Define the source fields and output columns for SCD2 history.

    Attributes:
        tracked_fields: Source fields whose changes create a new version.
        effective_at: Source field that orders versions.
        valid_from: Output field that stores the version start.
        valid_to: Output field that stores the version end.
        is_current: Output field that identifies the current version.

    Raises:
        ValueError: If fields are missing, empty, duplicated, or conflict.
    """

    tracked_fields: tuple[str, ...]
    effective_at: str
    valid_from: str
    valid_to: str
    is_current: str

    def __post_init__(self) -> None:
        if not self.tracked_fields or not all(
            isinstance(field, str) and field for field in self.tracked_fields
        ):
            raise ValueError("SCD2 tracked fields must be non-empty strings.")
        if len(set(self.tracked_fields)) != len(self.tracked_fields):
            raise ValueError("SCD2 tracked fields must not contain duplicates.")
        fields = (self.effective_at, self.valid_from, self.valid_to, self.is_current)
        if not all(isinstance(field, str) and field for field in fields):
            raise ValueError("SCD2 field names must be non-empty strings.")
        if len(set(fields)) != len(fields):
            raise ValueError("SCD2 effective and metadata field names must be distinct.")


class Connector(Protocol):
    """An external system that emits an Arrow stream."""

    def destination_cursor_field(self) -> str | None:
        """Return the destination field used to derive this connector's cursor.

        Returns:
            The configured cursor field, or ``None`` when no cursor is configured.
        """

    def read(self, cursor: Cursor | None = None) -> pa.RecordBatchReader:
        """Return the connector's records as an Arrow stream.

        Args:
            cursor: The previously loaded position to use for incremental reads.

        Returns:
            A reader that yields the connector's Arrow record batches.
        """


class Target(Protocol):
    """A destination that consumes an Arrow stream."""

    def read_cursor(self, field: str) -> Cursor | None:
        """Return the maximum persisted integer value for a destination field.

        Args:
            field: The destination field to inspect.

        Returns:
            The persisted high-watermark cursor, or ``None`` when unavailable.
        """

    def load(self, stream: pa.RecordBatchReader, merge: MergeConfiguration) -> int:
        """Load an Arrow stream and return the number of written rows.

        Args:
            stream: The incoming Arrow record batches.
            merge: The strategy and settings for incorporating the batches.

        Returns:
            The number of rows written from the incoming stream.
        """


def parse_merge_strategy(value: str) -> MergeStrategy:
    """Parse a configured merge strategy.

    Args:
        value: The configured strategy name.

    Returns:
        The corresponding merge strategy.

    Raises:
        ValueError: If the strategy name is unsupported.
    """
    try:
        return MergeStrategy(value)
    except ValueError as error:
        supported = ", ".join(strategy.value for strategy in MergeStrategy)
        raise ValueError(
            f"Unsupported merge strategy {value!r}. Supported strategies: {supported}."
        ) from error


def parse_merge_configuration(value: Any) -> MergeConfiguration:
    """Parse a merge configuration from YAML-compatible data.

    Args:
        value: A strategy name or mapping containing strategy-specific settings.

    Returns:
        A validated merge configuration.

    Raises:
        ValueError: If the configuration is malformed or incompatible with its
            selected strategy.
    """
    if isinstance(value, str):
        strategy = parse_merge_strategy(value)
        if strategy is MergeStrategy.UPSERT:
            raise ValueError("merge upsert must define one or more keys.")
        if strategy is MergeStrategy.SCD2:
            raise ValueError(f"merge {strategy.value} must be a mapping.")
        return MergeConfiguration(strategy)
    if not isinstance(value, Mapping):
        raise ValueError("merge must be a strategy string or a mapping.")

    strategy_value = value.get("strategy")
    if not isinstance(strategy_value, str):
        raise ValueError("merge.strategy must be a string.")
    strategy = parse_merge_strategy(strategy_value)
    _reject_unsupported_merge_fields(value, strategy)
    keys = _merge_keys(value.get("keys"))
    if strategy is MergeStrategy.SCD2:
        return MergeConfiguration(strategy, keys, _parse_scd2_configuration(value))
    return MergeConfiguration(strategy, keys)


def _merge_keys(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise ValueError("merge.keys must be a list of non-empty strings.")
    return tuple(value)


def _parse_scd2_configuration(value: Mapping[str, Any]) -> SCD2Configuration:
    return SCD2Configuration(
        tracked_fields=_string_fields(value.get("tracked_fields"), "merge.tracked_fields"),
        effective_at=_required_string(value.get("effective_at"), "merge.effective_at"),
        valid_from=_required_string(value.get("valid_from"), "merge.valid_from"),
        valid_to=_required_string(value.get("valid_to"), "merge.valid_to"),
        is_current=_required_string(value.get("is_current"), "merge.is_current"),
    )


def _reject_unsupported_merge_fields(
    value: Mapping[str, Any],
    strategy: MergeStrategy,
) -> None:
    allowed_fields = {"strategy", "keys"}
    if strategy is MergeStrategy.SCD2:
        allowed_fields.update(
            {"tracked_fields", "effective_at", "valid_from", "valid_to", "is_current"}
        )
    unsupported_fields = set(value).difference(allowed_fields)
    if unsupported_fields:
        fields = ", ".join(sorted(unsupported_fields))
        raise ValueError(f"merge strategy {strategy.value!r} does not support fields: {fields}.")


def _string_fields(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise ValueError(f"{name} must be a list of non-empty strings.")
    return tuple(value)


def _required_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string.")
    return value


def _strategy_label(strategy: MergeStrategy) -> str:
    return "SCD2" if strategy is MergeStrategy.SCD2 else strategy.value.capitalize()
