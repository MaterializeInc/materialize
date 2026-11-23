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

from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from materialize.cli import ci_closed_issues_detect


@pytest.mark.parametrize(
    "text,expected",
    [
        ('@disabled("due to DB-236")', [("linear", "DB-236")]),
        ("# TODO: SS-548", [("linear", "SS-548")]),
        ("// TODO(SQL-495): remove this", [("linear", "SQL-495")]),
        ('return YesIgnore("DB-191")', [("linear", "DB-191")]),
        (
            "# TODO: Reenable once SS-346 and PER-49 are fixed",
            [("linear", "SS-346"), ("linear", "PER-49")],
        ),
        ("# TODO: `SS-548`.", [("linear", "SS-548")]),
        (
            "# TODO: [SS-548](https://linear.app/materializeinc/issue/SS-548)",
            [("linear", "SS-548")],
        ),
        ("# TODO: linear#SS-548", [("linear", "SS-548")]),
        ("# TODO: linear#NEWTEAM-123", [("linear", "NEWTEAM-123")]),
        (
            "# TODO: https://linear.app/materializeinc/issue/NEWTEAM-123/title",
            [("linear", "NEWTEAM-123")],
        ),
        (
            "# TODO: database-issues#6249 and DB-236",
            [("MaterializeInc/database-issues", 6249), ("linear", "DB-236")],
        ),
        ("# Regression test for SS-548", []),
        ("# TODO: Liquibase SS-548", []),
    ],
)
def test_detect_referenced_issues(
    tmp_path: Path,
    text: str,
    expected: list[tuple[str, int | str]],
) -> None:
    path = tmp_path / "references.py"
    path.write_text(text)

    refs = ci_closed_issues_detect.detect_referenced_issues(str(path))

    assert [(ref.repository, ref.issue_id) for ref in refs] == expected


@pytest.mark.parametrize(
    "team_key",
    "ANA CDX CLO CLU CNS CPU CS DB DEP DEX EDU GTM MZC OPE PER PRD QAR SAS SEC SQL SS STG WEB".split(),
)
def test_detect_bare_linear_issue_teams(tmp_path: Path, team_key: str) -> None:
    path = tmp_path / "references.py"
    identifier = f"{team_key}-123"
    path.write_text(f"# TODO: {identifier}")

    refs = ci_closed_issues_detect.detect_referenced_issues(str(path))

    assert [(ref.repository, ref.issue_id) for ref in refs] == [("linear", identifier)]


@pytest.mark.parametrize(
    "reference",
    [
        "UTF-8",
        "NEWTEAM-123",
        "ss-548",
        "prefixSS-548",
        "prefix-SS-548",
        "_SS-548",
        "[SS-548]",
        "SS-548suffix",
        "SS-548-suffix",
        "SS-548_",
    ],
)
def test_ignore_non_issue_tokens(tmp_path: Path, reference: str) -> None:
    path = tmp_path / "references.py"
    path.write_text(f"# TODO: {reference}")

    assert ci_closed_issues_detect.detect_referenced_issues(str(path)) == []


def _mock_response(state_type: str, state_name: str) -> mock.Mock:
    response = mock.Mock()
    response.status_code = 200
    response.json.return_value = {
        "data": {"issue": {"state": {"type": state_type, "name": state_name}}}
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
        ("duplicate", "Duplicate", True),
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


def test_is_issue_closed_on_linear_moved_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LINEAR_READ_ONLY_TOKEN", "dummy-token")
    with mock.patch(
        "materialize.cli.ci_closed_issues_detect.requests.post",
        return_value=_mock_response("completed", "Done"),
    ) as post:
        assert ci_closed_issues_detect.is_issue_closed_on_linear("CLU-171")

    assert post.call_args.kwargs["json"]["variables"] == {"identifier": "CLU-171"}


@pytest.mark.parametrize("data", [{"issue": None}, None, {}])
def test_is_issue_closed_on_linear_unknown_issue(
    monkeypatch: pytest.MonkeyPatch,
    data: dict[str, Any] | None,
) -> None:
    monkeypatch.setenv("LINEAR_READ_ONLY_TOKEN", "dummy-token")
    response = mock.Mock()
    response.status_code = 200
    response.json.return_value = {"data": data}
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
