# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_closed_issues_detect.py - Detect references to already closed issues.

import argparse
import os
import re
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from typing import IO

import requests

from materialize import buildkite, spawn

ISSUE_RE = re.compile(
    r"""
    ( TimelyDataflow/timely-dataflow\#(?P<timelydataflow>[0-9]+)
    | ( materialize\# | materialize/issues/ ) (?P<materialize>[0-9]+)
    | ( cloud\# | cloud/issues/ ) (?P<cloud>[0-9]+)
    | ( incidents-and-escalations\# | incidents-and-escalations/issues/ ) (?P<incidentsandescalations>[0-9]+)
    | ( database-issues\# | database-issues/issues/ ) (?P<databaseissues>[0-9]+)
    # only match from the beginning of the line or after a space character to avoid matching Buildkite URLs
    | (^|\s) \# (?P<ambiguous>[0-9]+)
    )
    """,
    re.VERBOSE,
)

GROUP_REPO = {
    "timelydataflow": "TimelyDataflow/timely-dataflow",
    "materialize": "MaterializeInc/materialize",
    "cloud": "MaterializeInc/cloud",
    "incidentsandescalations": "MaterializeInc/incidents-and-escalations",
    "databaseissues": "MaterializeInc/database-issues",
    "ambiguous": None,
}

REFERENCE_RE = re.compile(
    r"""
    ( reenable
    | re-enable
    | reconsider
    | TODO
    # Used in Buildkite pipeline config files
    | skip:
    # Used in platform-checks
    | @disabled
    # Used in pytest
    | @pytest.mark.skip
    # Used in output-consistency framework
    | YesIgnore
    | tracked\ with
    # Used in proto files
    | //\ buf\ breaking:\ ignore
    # Used in documentation
    | in\ the\ future
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

IGNORE_RE = re.compile(
    r"""
    ( discussion\ of\ this\ in
    | discussed\ in
    | [sS]ee\ \<
    # is_null_propagation.slt
    | isnull\(\#0\)
    # src/transform/tests/test_transforms/column_knowledge.spec
    | \(\#1\)\ IS\ NULL
    # test/sqllogictest/cockroach/*.slt
    | cockroach\#
    | Liquibase
    # ci/test/lint-buf/README.md
    | Ignore\ because\ of\ database-issues#99999
    # src/storage-client/src/controller.rs
    | issues/20211\>
    # src/sql/src/plan/statement.rs
    | issues/20019\>
    # src/storage/src/storage_state.rs
    | \#19907$
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

COMMENT_RE = re.compile(r"#|//")

IGNORE_FILENAME_RE = re.compile(
    r"""
    ( .*\.(svg|png|jpg|jpeg|avro|ico)
    | doc/developer/design/20230223_stabilize_with_mutually_recursive.md
    )
    """,
    re.VERBOSE,
)

FILENAME_REFERENCE_RE = re.compile(r".*\.(td|slt|test)\.gh(?P<ambiguous>[0-9]+)")


@dataclass
class IssueRef:
    repository: str | None
    issue_id: int
    filename: str
    line_number: int
    text: str | None


@dataclass
class CommentBlock:
    char: str
    pos: int
    text: str
    line_number: int


def comment_blocks(file: IO) -> Iterator[tuple[int, str]]:
    comment: CommentBlock | None = None

    for line_number, line in enumerate(file):
        if comment_match := COMMENT_RE.search(line):
            char = comment_match.group(0)
            pos = comment_match.span()[0]
            if comment is None:
                comment = CommentBlock(char, pos, line, line_number + 1)
                continue
            if char == comment.char and pos == comment.pos:
                comment.text += line
                continue
            yield (comment.line_number, comment.text)
            comment = CommentBlock(char, pos, line, line_number + 1)
            continue
        if comment is not None:
            yield (comment.line_number, comment.text)
            comment = None
        yield (line_number + 1, line)

    if comment is not None:
        yield (comment.line_number, comment.text)


def detect_referenced_issues(filename: str) -> list[IssueRef]:
    issue_refs: list[IssueRef] = []

    with open(filename) as file:
        for line_number, text in comment_blocks(file):
            if not REFERENCE_RE.search(text) or IGNORE_RE.search(text):
                continue

            offset = 0
            while issue_match := ISSUE_RE.search(text, offset):
                offset = issue_match.span()[1]

                groups = [
                    (key, value)
                    for key, value in issue_match.groupdict().items()
                    if value
                ]
                assert len(groups) == 1, f"Expected only 1 element in {groups}"
                group, issue_id = groups[0]

                is_referenced_with_url = "issues/" in issue_match.group(0)

                # Explain plans can look like issue references
                if (
                    group == "ambiguous"
                    and int(issue_id) < 100
                    and not is_referenced_with_url
                ):
                    continue

                issue_refs.append(
                    IssueRef(
                        GROUP_REPO[group],
                        int(issue_id),
                        filename,
                        line_number,
                        text.strip(),
                    )
                )

    return issue_refs


def is_issue_closed_on_github(repository: str | None, issue_id: int) -> bool:
    assert repository
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    if token := os.getenv("GITHUB_CI_ISSUE_REFERENCE_CHECKER_TOKEN") or os.getenv(
        "GITHUB_TOKEN"
    ):
        headers["Authorization"] = f"Bearer {token}"

    url = f"https://api.github.com/repos/{repository}/issues/{issue_id}"
    response = requests.get(url, headers=headers)

    if response.status_code == 404 and not os.getenv("CI"):
        print(
            f"Can't check issue #{issue_id} in {repository} repo, set GITHUB_TOKEN environment variable or run this check in CI"
        )
        return False
    if response.status_code != 200:
        raise ValueError(
            f"Bad return code from GitHub on {url}: {response.status_code}: {response.text}"
        )

    issue_json = response.json()
    # We can't check the issue number anymore because issues can have moved
    return issue_json["state"] == "closed"


def filter_changed_lines(issue_refs: list[IssueRef]) -> list[IssueRef]:
    changed_lines = buildkite.find_modified_lines()
    return [
        issue_ref
        for issue_ref in issue_refs
        if issue_ref.text is not None
        and any(
            (issue_ref.filename, issue_ref.line_number + i) in changed_lines
            for i in range(issue_ref.text.count("\n"))
        )
    ]


def filter_ambiguous_issues(
    issue_refs: list[IssueRef],
) -> tuple[list[IssueRef], list[IssueRef]]:
    return [issue_ref for issue_ref in issue_refs if issue_ref.repository], [
        issue_ref for issue_ref in issue_refs if not issue_ref.repository
    ]


def filter_closed_issues(issue_refs: list[IssueRef]) -> list[IssueRef]:
    issues = {(issue_ref.repository, issue_ref.issue_id) for issue_ref in issue_refs}
    closed_issues = {
        (repository, issue)
        for repository, issue in issues
        if is_issue_closed_on_github(repository, issue)
    }
    return [
        issue_ref
        for issue_ref in issue_refs
        if (issue_ref.repository, issue_ref.issue_id) in closed_issues
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ci-closed-issues-detect",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="ci-closed-issues-detect detects references to already closed GitHub issues.",
    )

    parser.add_argument(
        "--changed-lines-only",
        action="store_true",
        help="only report issues in changed files/lines",
    )

    args = parser.parse_args()

    filenames = spawn.capture(
        ["git", "ls-tree", "--full-tree", "-r", "--name-only", "HEAD"]
    )

    issue_refs: list[IssueRef] = []
    for filename in filenames.splitlines():
        if issue_match := FILENAME_REFERENCE_RE.search(filename):
            groups = [
                (key, value) for key, value in issue_match.groupdict().items() if value
            ]
            assert len(groups) == 1, f"Expected only 1 element in {groups}"
            group, issue_id = groups[0]
            issue_refs.append(
                IssueRef(
                    GROUP_REPO[group],
                    int(issue_id),
                    filename,
                    0,
                    None,
                )
            )
        # Files without any ending can be interesting datadriven test files
        if (
            not IGNORE_FILENAME_RE.match(filename)
            and not os.path.isdir(filename)
            and not os.path.islink(filename)
        ):
            issue_refs.extend(detect_referenced_issues(filename))

    issue_refs, ambiguous_refs = filter_ambiguous_issues(issue_refs)

    if args.changed_lines_only:
        issue_refs = filter_changed_lines(issue_refs)
        ambiguous_refs = filter_changed_lines(ambiguous_refs)

    issue_refs = filter_closed_issues(issue_refs)

    for issue_ref in ambiguous_refs:
        print(f"--- Ambiguous issue reference: #{issue_ref.issue_id}")
        if issue_ref.text is not None:
            print(f"{issue_ref.filename}:{issue_ref.line_number}:")
            print(issue_ref.text)
        else:
            print(f"{issue_ref.filename} (filename)")
        print(
            f"Use database-issues#{issue_ref.issue_id} or materialize#{issue_ref.issue_id} instead to have an unambiguous reference"
        )

    for issue_ref in issue_refs:
        url = buildkite.inline_link(
            f"https://github.com/{issue_ref.repository}/issues/{issue_ref.issue_id}",
            f"{issue_ref.repository}#{issue_ref.issue_id}",
        )
        print(f"--- Issue is referenced in comment but already closed: {url}")
        if issue_ref.text is not None:
            print(f"{issue_ref.filename}:{issue_ref.line_number}:")
            print(issue_ref.text)
        else:
            print(f"{issue_ref.filename} (filename)")

    return 1 if issue_refs + ambiguous_refs else 0


if __name__ == "__main__":
    sys.exit(main())
