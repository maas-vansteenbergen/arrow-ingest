from __future__ import annotations

import pytest

from ingester.core import parse_merge_strategy


def test_rejects_unsupported_strategy() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unsupported merge strategy 'append'. Supported strategies: replace.",
    ):
        parse_merge_strategy("append")
