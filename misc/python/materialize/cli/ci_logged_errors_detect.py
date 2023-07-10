# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# ci_logged_errors_detect.py - Detect errors in log files during CI and find
# associated open Github issues in Materialize repository.

import argparse
import os
import re
import sys
from typing import Any, List, Optional, Set, Tuple

import requests

from materialize import ci_util, spawn

CI_RE = re.compile("ci-regexp: (.*)")
CI_APPLY_TO = re.compile("ci-apply-to: (.*)")
ERROR_RE = re.compile(
    r"""
    ( panicked\ at
    | segfault\ at
    | internal\ error:
    | \*\ FATAL:
    | [Oo]ut\ [Oo]f\ [Mm]emory
    | cannot\ migrate\ from\ catalog
    | halting\ process: # Rust unwrap
    | fatal runtime error: # stack overflow
    | \[SQLsmith\] # Unknown errors are logged
    | \[SQLancer\] # Unknown errors are logged
    # From src/testdrive/src/action/sql.rs
    | column\ name\ mismatch
    | non-matching\ rows:
    | wrong\ row\ count:
    | wrong\ hash\ value:
    | expected\ one\ statement
    | query\ succeeded,\ but\ expected
    | expected\ .*,\ got\ .*
    | expected\ .*,\ but\ found\ none
    | unsupported\ SQL\ type\ in\ testdrive:
    | environmentd:\ fatal: # startup failure
    | clusterd:\ fatal: # startup failure
    | error:\ Found\ argument\ '.*'\ which\ wasn't\ expected,\ or\ isn't\ valid\ in\ this\ context
    )
    # Emitted by tests employing explicit mz_panic()
    (?!.*forced\ panic)
    # Expected once compute cluster has panicked, brings no new information
    (?!.*timely\ communication\ error:)
    # Expected once compute cluster has panicked, only happens in CI
    (?!.*aborting\ because\ propagate_crashes\ is\ enabled)
    """,
    re.VERBOSE,
)


class KnownIssue:
    def __init__(self, regex: str, apply_to: Optional[str], info: Any):
        self.regex = re.compile(regex)
        self.apply_to = apply_to
        self.info = info


class ErrorLog:
    def __init__(self, line: str, file: str, line_nr: int):
        self.line = line
        self.file = file
        self.line_nr = line_nr


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ci-logged-errors-detect",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
ci-logged-errors-detect detects errors in log files during CI and finds
associated open Github issues in Materialize repository.""",
    )

    parser.add_argument("log_files", nargs="+", help="log files to search in")
    args = parser.parse_args()

    annotate_logged_errors(args.log_files)
    return 0


def annotate_errors(errors: List[str], title: str, style: str) -> None:
    if not errors:
        return

    suite_name = os.getenv("BUILDKITE_LABEL") or "Logged Errors"

    error_str = "\n".join(f"* {error}" for error in errors)

    if style == "info":
        markdown = f"""<details><summary>{suite_name}: {title}</summary>

{error_str}
</details>"""
    else:
        markdown = f"""{suite_name}: {title}

{error_str}"""

    spawn.runv(
        [
            "buildkite-agent",
            "annotate",
            f"--style={style}",
            f"--context={os.environ['BUILDKITE_JOB_ID']}-{style}",
        ],
        stdin=markdown.encode(),
    )


def annotate_logged_errors(log_files: List[str]) -> None:
    error_logs = get_error_logs(log_files)

    if not error_logs:
        return

    step_key: str = os.getenv("BUILDKITE_STEP_KEY", "")
    buildkite_label: str = os.getenv("BUILDKITE_LABEL", "")

    (known_issues, unknown_errors) = get_known_issues_from_github()

    artifacts = ci_util.get_artifacts()
    job = os.getenv("BUILDKITE_JOB_ID")

    known_errors: List[str] = []

    # Keep track of known errors so we log each only once
    already_reported_issue_numbers: Set[int] = set()

    for error in error_logs:
        for artifact in artifacts:
            if artifact["job_id"] == job and artifact["path"] == error.file:
                org = os.environ["BUILDKITE_ORGANIZATION_SLUG"]
                pipeline = os.environ["BUILDKITE_PIPELINE_SLUG"]
                build = os.environ["BUILDKITE_BUILD_NUMBER"]
                linked_file = f'[{error.file}](https://buildkite.com/organizations/{org}/pipelines/{pipeline}/builds/{build}/jobs/{artifact["job_id"]}/artifacts/{artifact["id"]})'
                break
        else:
            linked_file = error.file

        for issue in known_issues:
            match = issue.regex.search(error.line)
            if match and issue.info["state"] == "open":
                if issue.apply_to and issue.apply_to not in (
                    step_key.lower(),
                    buildkite_label.lower(),
                ):
                    continue

                if issue.info["number"] not in already_reported_issue_numbers:
                    known_errors.append(
                        f"[{issue.info['title']} (#{issue.info['number']})]({issue.info['html_url']}) in {linked_file}:{error.line_nr}:  \n``{error.line}``"
                    )
                    already_reported_issue_numbers.add(issue.info["number"])
                break
        else:
            for issue in known_issues:
                match = issue.regex.search(error.line)
                if match and issue.info["state"] == "closed":
                    if issue.apply_to and issue.apply_to not in (
                        step_key.lower(),
                        buildkite_label.lower(),
                    ):
                        continue

                    if issue.info["number"] not in already_reported_issue_numbers:
                        unknown_errors.append(
                            f"Potential regression [{issue.info['title']} (#{issue.info['number']}, closed)]({issue.info['html_url']}) in {linked_file}:{error.line_nr}:  \n``{error.line}``"
                        )
                        already_reported_issue_numbers.add(issue.info["number"])
                    break
            else:
                unknown_errors.append(
                    f"Unknown error in {linked_file}:{error.line_nr}:  \n``{error.line}``"
                )

    annotate_errors(
        unknown_errors,
        "Unknown errors and regressions in logs (see [ci-regexp](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/ci-regexp.md))",
        "error",
    )
    annotate_errors(known_errors, "Known errors in logs, ignoring", "info")


def get_error_logs(log_files: List[str]) -> List[ErrorLog]:
    error_logs = []
    for log_file in log_files:
        with open(log_file) as f:
            for line_nr, line in enumerate(f):
                match = ERROR_RE.search(line)
                if match:
                    error_logs.append(ErrorLog(line, log_file, line_nr + 1))
    # TODO: Only report multiple errors once?
    return error_logs


def get_known_issues_from_github_page(page: int = 1) -> Any:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(
        f'https://api.github.com/search/issues?q=repo:MaterializeInc/materialize%20type:issue%20in:body%20"ci-regexp%3A"&per_page=100&page={page}',
        headers=headers,
    )

    if response.status_code != 200:
        raise ValueError(f"Bad return code from Github: {response.status_code}")

    issues_json = response.json()
    assert issues_json["incomplete_results"] == False
    return issues_json


def get_known_issues_from_github() -> Tuple[List[KnownIssue], List[str]]:
    page = 1
    issues_json = get_known_issues_from_github_page(page)
    while issues_json["total_count"] > len(issues_json["items"]):
        page += 1
        next_page_json = get_known_issues_from_github_page(page)
        if not next_page_json["items"]:
            break
        issues_json["items"].extend(next_page_json["items"])

    unknown_errors = []
    known_issues = []

    for issue in issues_json["items"]:
        matches = CI_RE.findall(issue["body"])
        matches_apply_to = CI_APPLY_TO.findall(issue["body"])
        for match in matches:
            if matches_apply_to:
                for match_apply_to in matches_apply_to:
                    try:
                        known_issues.append(
                            KnownIssue(
                                match.strip(), match_apply_to.strip().lower(), issue
                            )
                        )
                    except:
                        unknown_errors.append(
                            "[{issue.info['title']} (#{issue.info['number']})]({issue.info['html_url']}): Invalid regex in ci-regexp: {match.strip()}, ignoring"
                        )
            else:
                known_issues.append(KnownIssue(match.strip(), None, issue))

    return (known_issues, unknown_errors)


if __name__ == "__main__":
    sys.exit(main())
