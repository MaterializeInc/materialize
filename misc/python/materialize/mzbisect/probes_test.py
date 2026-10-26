# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzbisect.probes import (
    _is_stale_timestamp_error,
    _truncate,
    is_corruption_error,
)


def test_corruption_markers_are_recognized_case_insensitively() -> None:
    assert is_corruption_error(
        "Evaluation error: Non-Positive Multiplicity in DistinctBy"
    )
    assert is_corruption_error("negative accumulation in ReduceAccumulable")


def test_ordinary_query_errors_are_not_corruption() -> None:
    assert not is_corruption_error("division by zero")
    assert not is_corruption_error("statement timeout")


def test_stale_timestamp_errors_are_distinguished_from_corruption() -> None:
    msg = "Timestamp (123) is not valid for all inputs"
    assert _is_stale_timestamp_error(msg)
    assert not is_corruption_error(msg)


def test_truncate_leaves_short_values_alone() -> None:
    assert _truncate("abc", limit=10) == "abc"


def test_truncate_caps_long_values_at_the_limit() -> None:
    assert _truncate("a" * 20, limit=10) == "aaaaaaa..."
