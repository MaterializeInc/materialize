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
from dataclasses import dataclass

import requests

from materialize import buildkite, spawn

ISSUE_RE = re.compile(
    r"""
    ( TimelyDataflow/timely-dataflow\#(?P<timelydataflow>[0-9]+)
    | ( materialize\# | materialize/issues/ | \# ) (?P<materialize>[0-9]+)
    )
    """,
    re.VERBOSE,
)

GROUP_REPO = {
    "timelydataflow": "TimelyDataflow/timely-dataflow",
    "materialize": "MaterializeInc/materialize",
}

REFERENCE_RE = re.compile(
    r"""
    ( reenable
    | TODO
    # Used in Buildkite pipeline config files
    | skip:
    # Used in platform-checks
    | @disabled
    # Used in pytest
    | @pytest.mark.skip
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

IGNORE_RE = re.compile(
    r"""
    # is_null_propagation.slt
    ( isnull\(\#0\)
    # src/transform/tests/test_transforms/column_knowledge.spec
    | \(\#1\)\ IS\ NULL
    # test/sqllogictest/cockroach/*.slt
    | cockroach\#
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)

IGNORE_FILE_NAME_RE = re.compile(
    r"""
    ( .*\.(svg|png|jpg|jpeg|avro|ico)
    | doc/developer/design/20230223_stabilize_with_mutually_recursive.md
    )
    """,
    re.VERBOSE,
)


@dataclass
class IssueRef:
    repository: str
    issue_id: int
    filename: str
    line_number: int
    line: str


def detect_closed_issues(filename: str) -> list[IssueRef]:
    issue_refs: list[IssueRef] = []

    with open(filename) as file:
        for line_number, line in enumerate(file):
            if REFERENCE_RE.search(line) and not IGNORE_RE.search(line):
                if issue_match := ISSUE_RE.search(line):
                    groups = [
                        (key, value)
                        for key, value in issue_match.groupdict().items()
                        if value
                    ]
                    assert len(groups) == 1, f"Expected only 1 element in {groups}"
                    group, issue_id = groups[0]
                    (
                        "TimelyDataflow/timely-dataflow"
                        if issue_match.group("timelydataflow")
                        else "MaterializeInc/materialize"
                    )
                    issue_refs.append(
                        IssueRef(
                            GROUP_REPO[group],
                            int(issue_id),
                            filename,
                            line_number + 1,
                            line.strip(),
                        )
                    )

    return issue_refs


def is_issue_closed_on_github(repository: str, issue_id: int) -> bool:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"

    url = f"https://api.github.com/repos/{repository}/issues/{issue_id}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise ValueError(
            f"Bad return code from GitHub on {url}: {response.status_code}: {response.text}"
        )

    issue_json = response.json()
    assert (
        issue_json["number"] == issue_id
    ), f"Returned issue number {issue_json['number']} is not the expected issue number {issue_id}"
    return issue_json["state"] == "closed"


def filter_changed_lines(issue_refs: list[IssueRef]) -> list[IssueRef]:
    changed_lines = buildkite.find_modified_lines()
    return [
        issue_ref
        for issue_ref in issue_refs
        if (issue_ref.filename, issue_ref.line_number) in changed_lines
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
        # Files without any ending can be interesting datadriven test files
        if (
            not IGNORE_FILE_NAME_RE.match(filename)
            and not os.path.isdir(filename)
            and not os.path.islink(filename)
        ):
            issue_refs.extend(detect_closed_issues(filename))

    if args.changed_lines_only:
        issue_refs = filter_changed_lines(issue_refs)

    issue_refs = filter_closed_issues(issue_refs)

    for issue_ref in issue_refs:
        url = buildkite.inline_link(
            f"https://github.com/{issue_ref.repository}/issues/{issue_ref.issue_id}",
            f"#{issue_ref.issue_id}",
        )
        print(f"--- Issue is referenced in comment but already closed: {url}")
        print(f"{issue_ref.filename}:{issue_ref.line_number}: {issue_ref.line}")

    return 1 if len(issue_refs) else 0


if __name__ == "__main__":
    sys.exit(main())
