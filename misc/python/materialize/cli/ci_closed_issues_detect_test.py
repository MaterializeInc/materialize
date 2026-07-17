# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for `ci-closed-issues-detect`.

The only network-dependent decision is whether a referenced Linear issue counts
as closed. These tests mock the Linear GraphQL response so the state-to-closed
mapping can be exercised without a token, in particular that "Stale" issues (a
canceled-type state used by Linear's inactivity policy) are not reported as
closed.
"""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from materialize.cli import ci_closed_issues_detect


def _mock_response(state_type: str, state_name: str) -> mock.Mock:
    response = mock.Mock()
    response.status_code = 200
    response.json.return_value = {
        "data": {
            "issues": {"nodes": [{"state": {"type": state_type, "name": state_name}}]}
        }
    }
    return response


@pytest.mark.parametrize(
    "state_type,state_name,expected_closed",
    [
        # Stale issues are canceled-type but not genuinely resolved.
        ("canceled", "Stale", False),
        ("canceled", "stale", False),
        # Genuinely closed states.
        ("completed", "Done", True),
        ("canceled", "Canceled", True),
        ("canceled", "Duplicate", True),
        # Open states.
        ("backlog", "Backlog", False),
        ("started", "In Progress", False),
    ],
)
def test_is_issue_closed_on_linear(
    monkeypatch: pytest.MonkeyPatch,
    state_type: str,
    state_name: str,
    expected_closed: bool,
) -> None:
    monkeypatch.setenv("LINEAR_READ_ONLY_TOKEN", "dummy-token")
    with mock.patch(
        "materialize.cli.ci_closed_issues_detect.requests.post",
        return_value=_mock_response(state_type, state_name),
    ):
        assert (
            ci_closed_issues_detect.is_issue_closed_on_linear("SQL-444")
            == expected_closed
        )


def test_is_issue_closed_on_linear_unknown_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LINEAR_READ_ONLY_TOKEN", "dummy-token")
    response = mock.Mock()
    response.status_code = 200
    response.json.return_value = {"data": {"issues": {"nodes": []}}}
    with mock.patch(
        "materialize.cli.ci_closed_issues_detect.requests.post",
        return_value=response,
    ):
        assert not ci_closed_issues_detect.is_issue_closed_on_linear("SQL-9999")


def test_is_issue_closed_on_linear_no_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LINEAR_READ_ONLY_TOKEN", raising=False)
    monkeypatch.setenv("CI", "1")

    def _fail(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("must not hit the network without a token")

    with mock.patch(
        "materialize.cli.ci_closed_issues_detect.requests.post", side_effect=_fail
    ):
        assert not ci_closed_issues_detect.is_issue_closed_on_linear("SQL-444")
