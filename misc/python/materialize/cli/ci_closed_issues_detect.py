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

ISSUE_RE = re.compile(r"\#([0-9]+) | materialize/issues/([0-9]+)", re.VERBOSE)

FILE_ENDINGS = [".py", ".rs", ".yml", ".td", ".slt", ".md"]

REFERENCE_RE = re.compile(
    r"""
    # This block contains expected references, don't report them
    ^((?!.*
    # Only track issues in Materialize repo
    ( timely-dataflow\#
    ))
    .)*
    # This block contains unexpected references to issues, report them
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


@dataclass
class IssueRef:
    issue_id: int
    filename: str
    line_number: int
    line: str


def detect_closed_issues(filename: str) -> list[IssueRef]:
    issue_refs: list[IssueRef] = []

    with open(filename) as file:
        for line_number, line in enumerate(file):
            if REFERENCE_RE.search(line):
                if issue_match := ISSUE_RE.search(line):
                    issue_refs.append(
                        IssueRef(
                            int(issue_match.group(1) or issue_match.group(2)),
                            filename,
                            line_number + 1,
                            line.strip(),
                        )
                    )

    return issue_refs


def is_issue_closed_on_github(issue_id: int) -> bool:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(
        f"https://api.github.com/repos/MaterializeInc/materialize/issues/{issue_id}",
        headers=headers,
    )

    if response.status_code != 200:
        raise ValueError(
            f"Bad return code from GitHub: {response.status_code}: {response.text}"
        )

    issue_json = response.json()
    assert (
        issue_json["number"] == issue_id
    ), f"Returned issue number {issue_json['number']} is not the expected issue number {issue_id}"
    return issue_json["state"] == "closed"


def filter_changed(issue_refs: list[IssueRef]) -> list[IssueRef]:
    merge_base = buildkite.get_merge_base()
    print(f"Merge base: {merge_base}")
    result = spawn.capture(["git", "diff", "-U0", merge_base])
    file_path = None
    changed_lines = set()

    for line in result.splitlines():
        # +++ b/src/adapter/src/coord/command_handler.rs
        if line.startswith("+++"):
            file_path = line.removeprefix("+++ b/")
        # @@ -641,7 +640,6 @@ impl Coordinator {
        elif line.startswith("@@ "):
            # We only care about the second value ("+640,6" in the example),
            # which contains the line number and length of the modified block
            # in new code state.
            parts = line.split(" ")[2]
            if "," in parts:
                start, length = map(int, parts.split(","))
            else:
                start = int(parts)
                length = 1
            for line_nr in range(start, start + length):
                changed_lines.add((file_path, line_nr))

    return [
        issue_ref
        for issue_ref in issue_refs
        if (issue_ref.filename, issue_ref.line_number) in changed_lines
    ]


def filter_closed(issue_refs: list[IssueRef]) -> list[IssueRef]:
    issues = {issue_ref.issue_id for issue_ref in issue_refs}
    closed_issues = {issue for issue in issues if is_issue_closed_on_github(issue)}
    return [
        issue_ref for issue_ref in issue_refs if issue_ref.issue_id in closed_issues
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
            any([filename.endswith(ending) for ending in FILE_ENDINGS])
            or "." not in filename
        ) and os.path.isfile(filename):
            try:
                issue_refs.extend(detect_closed_issues(filename))
            except UnicodeDecodeError:
                # Not all files are source code
                pass

    if args.changed_lines_only:
        issue_refs = filter_changed(issue_refs)

    issue_refs = filter_closed(issue_refs)

    for issue_ref in issue_refs:
        url = buildkite.inline_link(
            f"https://github.com/MaterializeInc/materialize/issues/{issue_ref.issue_id}",
            f"#{issue_ref.issue_id}",
        )
        print(f"--- Issue is referenced in comment but already closed: {url}")
        print(f"{issue_ref.filename}:{issue_ref.line_number}: {issue_ref.line}")

    return 1 if len(issue_refs) else 0


if __name__ == "__main__":
    sys.exit(main())
