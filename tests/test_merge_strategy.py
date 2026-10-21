from __future__ import annotations

import pytest

from ingester.core import MergeStrategy, parse_merge_strategy


@pytest.mark.parametrize("strategy", [MergeStrategy.REPLACE, MergeStrategy.APPEND])
def test_parses_supported_strategy(strategy: MergeStrategy) -> None:
    assert parse_merge_strategy(strategy.value) is strategy


def test_rejects_unsupported_strategy() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unsupported merge strategy 'upsert'. Supported strategies: append, replace.",
    ):
        parse_merge_strategy("upsert")
