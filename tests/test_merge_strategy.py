from __future__ import annotations

import pytest

from ingester.core import (
    MergeConfiguration,
    MergeStrategy,
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


def test_rejects_unsupported_strategy() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unsupported merge strategy 'merge'. Supported strategies: append, replace, upsert.",
    ):
        parse_merge_strategy("merge")
