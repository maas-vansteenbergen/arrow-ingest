from __future__ import annotations

import pytest

from arrow_ingest.core import (
    MergeConfiguration,
    MergeStrategy,
    SCD2Configuration,
    parse_merge_configuration,
    parse_merge_strategy,
)


@pytest.mark.parametrize("strategy", [MergeStrategy.REPLACE, MergeStrategy.APPEND])
def test_parses_supported_strategy(strategy: MergeStrategy) -> None:
    assert parse_merge_strategy(strategy.value) is strategy
    assert parse_merge_configuration(strategy.value) == MergeConfiguration(strategy)


def test_parses_upsert_configuration() -> None:
    merge = parse_merge_configuration({"strategy": "upsert", "keys": ["id", "version"]})

    assert merge.strategy is MergeStrategy.UPSERT
    assert merge.keys == ("id", "version")


def test_rejects_direct_upsert_configuration_without_keys() -> None:
    with pytest.raises(
        ValueError, match="Upsert merge configurations must define one or more keys."
    ):
        MergeConfiguration(MergeStrategy.UPSERT)


def test_parses_scd2_configuration() -> None:
    merge = parse_merge_configuration(
        {
            "strategy": "scd2",
            "keys": ["id"],
            "tracked_fields": ["title"],
            "effective_at": "version",
            "valid_from": "valid_from",
            "valid_to": "valid_to",
            "is_current": "is_current",
        }
    )

    assert merge == MergeConfiguration(
        MergeStrategy.SCD2,
        ("id",),
        SCD2Configuration(
            tracked_fields=("title",),
            effective_at="version",
            valid_from="valid_from",
            valid_to="valid_to",
            is_current="is_current",
        ),
    )


@pytest.mark.parametrize(
    ("configuration", "error"),
    [
        (
            None,
            "merge must be a strategy string or a mapping.",
        ),
        (
            {},
            "merge.strategy must be a string.",
        ),
        (
            "upsert",
            "merge upsert must define one or more keys.",
        ),
        (
            "scd2",
            "merge scd2 must be a mapping.",
        ),
        (
            {"strategy": "append", "keys": ["id"]},
            "Merge strategy 'append' does not accept keys.",
        ),
        (
            {"strategy": "upsert"},
            "Upsert merge configurations must define one or more keys.",
        ),
        (
            {"strategy": "upsert", "keys": "id"},
            "merge.keys must be a list of non-empty strings.",
        ),
        (
            {"strategy": "upsert", "keys": ["id", "id"]},
            "merge keys must not contain duplicates.",
        ),
        (
            {
                "strategy": "upsert",
                "keys": ["id"],
                "effective_at": "version",
            },
            "merge strategy 'upsert' does not support fields: effective_at.",
        ),
        (
            {
                "strategy": "scd2",
                "keys": ["id"],
                "tracked_fields": [],
                "effective_at": "version",
                "valid_from": "valid_from",
                "valid_to": "valid_to",
                "is_current": "is_current",
            },
            "SCD2 tracked fields must be non-empty strings.",
        ),
        (
            {
                "strategy": "scd2",
                "keys": ["id"],
                "tracked_fields": ["id"],
                "effective_at": "version",
                "valid_from": "valid_from",
                "valid_to": "valid_to",
                "is_current": "is_current",
            },
            "SCD2 tracked fields must not include key fields.",
        ),
    ],
)
def test_rejects_invalid_merge_configuration(
    configuration: object,
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        parse_merge_configuration(configuration)


def test_rejects_unsupported_strategy() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unsupported merge strategy 'merge'. Supported strategies: append, replace, scd2, upsert.",
    ):
        parse_merge_strategy("merge")
