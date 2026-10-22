from __future__ import annotations

import pytest

from ingester.core import (
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


def test_rejects_upsert_without_keys() -> None:
    with pytest.raises(ValueError, match="merge upsert must define one or more keys."):
        parse_merge_configuration("upsert")


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


def test_rejects_scd2_settings_for_upsert() -> None:
    with pytest.raises(
        ValueError,
        match="merge strategy 'upsert' does not support fields: effective_at.",
    ):
        parse_merge_configuration(
            {
                "strategy": "upsert",
                "keys": ["id"],
                "effective_at": "version",
            }
        )


def test_rejects_unsupported_strategy() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unsupported merge strategy 'merge'. Supported strategies: append, replace, scd2, upsert.",
    ):
        parse_merge_strategy("merge")
