# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""GitHub utilities."""

import os
import re
from dataclasses import dataclass
from typing import Any

import requests

from materialize.observed_error import ObservedBaseError, WithIssue

CI_RE = re.compile("ci-regexp: (.*)")
CI_APPLY_TO = re.compile("ci-apply-to: (.*)")
CI_LOCATION = re.compile("ci-location: (.*)")
CI_IGNORE_FAILURE = re.compile("ci-ignore-failure: (.*)")


@dataclass
class KnownGitHubIssue:
    regex: re.Pattern[Any]
    apply_to: str | None
    info: dict[str, Any]
    ignore_failure: bool
    location: str | None


@dataclass(kw_only=True, unsafe_hash=True)
class GitHubIssueWithInvalidRegexp(ObservedBaseError, WithIssue):
    regex_pattern: str

    def to_text(self) -> str:
        return f"Invalid regex in ci-regexp: {self.regex_pattern}"

    def to_markdown(self) -> str:
        issue_ref = (
            self.issue_number
            if isinstance(self.issue_number, str)
            else f"#{self.issue_number}"
        )
        return f'<a href="{self.issue_url}">{self.issue_title} ({issue_ref})</a>: Invalid regex in ci-regexp: {self.regex_pattern}, ignoring'


def _search_issues_graphql(token: str, repo: str) -> list[dict[str, Any]]:
    query = """
    query($searchQuery: String!, $cursor: String) {
      search(query: $searchQuery, type: ISSUE, first: 100, after: $cursor) {
        issueCount
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          ... on Issue {
            number
            title
            body
            url
            state
            repository {
              nameWithOwner
            }
          }
        }
      }
    }
    """

    search_query = f'repo:{repo} type:issue in:body "ci-regexp:"'
    all_issues: list[dict[str, Any]] = []
    cursor = None

    while True:
        variables: dict[str, Any] = {"searchQuery": search_query}
        if cursor:
            variables["cursor"] = cursor

        response = requests.post(
            "https://api.github.com/graphql",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables},
            timeout=60,
        )

        if response.status_code != 200:
            rate_limit = response.headers.get("X-RateLimit-Remaining", "unknown")
            rate_limit_reset = response.headers.get("X-RateLimit-Reset", "unknown")
            raise ValueError(
                f"Bad return code from GitHub GraphQL: {response.status_code}, "
                f"rate_limit_remaining={rate_limit}, "
                f"rate_limit_reset={rate_limit_reset}, "
                f"response={response.text[:500]}, "
                f"has_token=True"
            )

        result = response.json()
        if "errors" in result:
            raise ValueError(f"GitHub GraphQL errors: {result['errors']}")

        search_data = result["data"]["search"]
        for node in search_data["nodes"]:
            if node is None:
                continue
            all_issues.append(node)

        if not search_data["pageInfo"]["hasNextPage"]:
            break
        cursor = search_data["pageInfo"]["endCursor"]

    return all_issues


def get_known_issues_from_github(
    token: str | None = os.getenv("GITHUB_TOKEN"),
    repo: str = "MaterializeInc/database-issues",
) -> tuple[list[KnownGitHubIssue], list[GitHubIssueWithInvalidRegexp]]:
    if not token:
        raise ValueError(
            "No GitHub token provided. Set GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN or GITHUB_TOKEN."
        )
    issues = _search_issues_graphql(token, repo)

    known_issues = []
    issues_with_invalid_regex = []

    for issue in issues:
        matches = CI_RE.findall(issue["body"])
        matches_apply_to = CI_APPLY_TO.findall(issue["body"])
        matches_location = CI_LOCATION.findall(issue["body"])
        matches_ignore_failure = CI_IGNORE_FAILURE.findall(issue["body"])

        if len(matches) > 1:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="GITHUB_INVALID_REGEXP",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["number"],
                    regex_pattern=f"Multiple regexes, but only one supported: {[match.strip() for match in matches]}",
                )
            )
            continue

        if len(matches_ignore_failure) > 1:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="GITHUB_INVALID_IGNORE_FAILURE",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["number"],
                    regex_pattern=f"Multiple ci-ignore-failures, but only one supported: {[match.strip() for match in matches_ignore_failure]}",
                )
            )
            continue

        if len(matches) == 0:
            continue

        if len(matches_location) >= 2:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="GITHUB_INVALID_IGNORE_FAILURE",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["number"],
                    regex_pattern=f"Multiple ci-locations, but only one supported: {[match.strip() for match in matches_location]}",
                )
            )
            continue

        location: str | None = (
            matches_location[0].strip() if len(matches_location) == 1 else None
        )

        ignore_failure = len(matches_ignore_failure) == 1 and matches_ignore_failure[
            0
        ].strip() in ("true", "yes", "1")

        try:
            regex_pattern = re.compile(matches[0].strip().encode())
        except:
            issues_with_invalid_regex.append(
                GitHubIssueWithInvalidRegexp(
                    internal_error_type="GITHUB_INVALID_REGEXP",
                    issue_url=issue["url"],
                    issue_title=issue["title"],
                    issue_number=issue["number"],
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
                        issue,
                        ignore_failure,
                        location,
                    )
                )
        else:
            known_issues.append(
                KnownGitHubIssue(regex_pattern, None, issue, ignore_failure, location)
            )

    return (known_issues, issues_with_invalid_regex)


def _auth_headers(token: str | None) -> dict[str, str]:
    if token is None:
        token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("No GitHub token provided. Set GITHUB_TOKEN.")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


def list_release_tags(
    repo: str = "MaterializeInc/materialize",
    token: str | None = None,
) -> set[str]:
    """List the tags of all existing GitHub releases."""
    headers = _auth_headers(token)
    tags = set()
    page = 1
    while True:
        response = requests.get(
            f"https://api.github.com/repos/{repo}/releases",
            headers=headers,
            params={"per_page": 100, "page": page},
            timeout=60,
        )
        if response.status_code != 200:
            raise ValueError(
                f"Failed to list GitHub releases for {repo}: "
                f"{response.status_code}, response={response.text[:500]}"
            )
        releases = response.json()
        if not releases:
            return tags
        tags.update(release["tag_name"] for release in releases)
        page += 1


def create_release(
    tag: str,
    body: str,
    repo: str = "MaterializeInc/materialize",
    token: str | None = None,
    make_latest: bool = True,
) -> None:
    """Create a GitHub release for an existing tag, unless one already exists."""
    headers = _auth_headers(token)

    response = requests.get(
        f"https://api.github.com/repos/{repo}/releases/tags/{tag}",
        headers=headers,
        timeout=60,
    )
    if response.status_code == 200:
        print(f"GitHub release for {tag} already exists: {response.json()['html_url']}")
        return
    if response.status_code != 404:
        raise ValueError(
            f"Failed to check for existing GitHub release for {tag}: "
            f"{response.status_code}, response={response.text[:500]}"
        )

    # Creating a release for a nonexistent tag would silently create the tag
    # at the head of the default branch, so require that the tag exists.
    response = requests.get(
        f"https://api.github.com/repos/{repo}/git/ref/tags/{tag}",
        headers=headers,
        timeout=60,
    )
    if response.status_code != 200:
        raise ValueError(
            f"Tag {tag} does not exist in {repo}: "
            f"{response.status_code}, response={response.text[:500]}"
        )

    response = requests.post(
        f"https://api.github.com/repos/{repo}/releases",
        headers=headers,
        json={
            "tag_name": tag,
            "name": tag,
            "body": body,
            "make_latest": "true" if make_latest else "false",
        },
        timeout=60,
    )
    if response.status_code != 201:
        raise ValueError(
            f"Failed to create GitHub release for {tag}: "
            f"{response.status_code}, response={response.text[:500]}"
        )
    print(f"Created GitHub release: {response.json()['html_url']}")


def for_github_re(text: bytes) -> bytes:
    """
    Matching newlines in regular expressions is kind of annoying, don't expect
    ci-regexp to do that correctly, but instead replace all newlines with a
    space. For examples this makes matching this panic easier:

      thread 'test_auth_deduplication' panicked at src/environmentd/tests/auth.rs:1878:5:
      assertion `left == right` failed

    Previously the regex should have been:
      thread 'test_auth_deduplication' panicked at src/environmentd/tests/auth.rs.*\n.*left == right

    With this function it can be:
      thread 'test_auth_deduplication' panicked at src/environmentd/tests/auth.rs.*left == right
    """
    return text.replace(b"\n", b" ")
