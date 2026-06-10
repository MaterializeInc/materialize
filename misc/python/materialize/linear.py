# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Linear utilities."""

import os
import re
from typing import Any

import requests

from materialize.github import (
    CI_APPLY_TO,
    CI_IGNORE_FAILURE,
    CI_LOCATION,
    CI_RE,
    GitHubIssueWithInvalidRegexp,
    KnownGitHubIssue,
)

LINEAR_CLOSED_STATE_TYPES = {"completed", "canceled"}


def _search_issues_graphql(token: str) -> list[dict[str, Any]]:
    query = """
    query($term: String!, $cursor: String) {
      searchIssues(
        term: $term
        first: 100
        after: $cursor
        includeArchived: false
      ) {
        nodes {
          identifier
          title
          description
          url
          state {
            type
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    all_issues: list[dict[str, Any]] = []
    cursor = None

    while True:
        variables: dict[str, Any] = {"term": "ci-regexp"}
        if cursor:
            variables["cursor"] = cursor

        response = requests.post(
            "https://api.linear.app/graphql",
            headers={
                "Authorization": token,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables},
        )

        if response.status_code != 200:
            raise ValueError(
                f"Bad return code from Linear GraphQL: {response.status_code}, "
                f"response={response.text[:500]}, "
                f"has_token=True"
            )

        result = response.json()
        if "errors" in result:
            raise ValueError(f"Linear GraphQL errors: {result['errors']}")

        search_data = result["data"]["searchIssues"]
        for node in search_data["nodes"]:
            if node is None:
                continue
            description = node.get("description") or ""
            if "ci-regexp:" in description:
                all_issues.append(node)

        if not search_data["pageInfo"]["hasNextPage"]:
            break
        cursor = search_data["pageInfo"]["endCursor"]

    return all_issues


def get_known_issues_from_linear(
    token: str | None = os.getenv("LINEAR_READ_ONLY_TOKEN"),
) -> tuple[list[KnownGitHubIssue], list[GitHubIssueWithInvalidRegexp]]:
    if not token:
        return ([], [])

    issues = _search_issues_graphql(token)

    known_issues = []
    issues_with_invalid_regex = []

    for issue in issues:
        body = issue.get("description") or ""

        state_type = issue.get("state", {}).get("type", "")
        state = "CLOSED" if state_type in LINEAR_CLOSED_STATE_TYPES else "OPEN"

        info = {
            "number": issue["identifier"],
            "title": issue["title"],
            "body": body,
            "url": issue["url"],
            "state": state,
            "source": "linear",
        }

        matches = CI_RE.findall(body)
        matches_apply_to = CI_APPLY_TO.findall(body)
        matches_location = CI_LOCATION.findall(body)
        matches_ignore_failure = CI_IGNORE_FAILURE.findall(body)

        if len(matches) > 1:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="LINEAR_INVALID_REGEXP",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["identifier"],
                    regex_pattern=f"Multiple regexes, but only one supported: {[match.strip() for match in matches]}",
                )
            )
            continue

        if len(matches_ignore_failure) > 1:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="LINEAR_INVALID_IGNORE_FAILURE",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["identifier"],
                    regex_pattern=f"Multiple ci-ignore-failures, but only one supported: {[match.strip() for match in matches_ignore_failure]}",
                )
            )
            continue

        if len(matches) == 0:
            continue

        if len(matches_location) >= 2:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="LINEAR_INVALID_LOCATION",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["identifier"],
                    regex_pattern=f"Multiple ci-locations, but only one supported: {[match.strip() for match in matches_location]}",
                )
            )
            continue

        location: str | None = (
            matches_location[0] if len(matches_location) == 1 else None
        )

        ignore_failure = len(matches_ignore_failure) == 1 and matches_ignore_failure[
            0
        ].strip() in ("true", "yes", "1")

        try:
            regex_pattern = re.compile(matches[0].strip().encode())
        except:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="LINEAR_INVALID_REGEXP",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["identifier"],
                    regex_pattern=matches[0].strip(),
                )
            )
            continue

        if matches_apply_to:
            for match_apply_to in matches_apply_to:
                known_issues.append(
                    KnownGitHubIssue(
                        regex_pattern,
                        match_apply_to.strip().lower(),
                        info,
                        ignore_failure,
                        location,
                    )
                )
        else:
            known_issues.append(
                KnownGitHubIssue(regex_pattern, None, info, ignore_failure, location)
            )

    return (known_issues, issues_with_invalid_regex)
