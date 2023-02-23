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
from typing import Any, List

import junit_xml
import requests

from materialize import ROOT, ci_util

CI_RE = re.compile("ci-regexp: (.*)")
ERROR_RE = re.compile(
    r"(panicked at|internal error:|\* FATAL:|[Oo]ut [Oo]f [Mm]emory|cannot migrate from catalog)"
)


class KnownIssue:
    def __init__(self, regex: str, info: Any):
        self.regex = re.compile(regex)
        self.info = info


class ErrorLog:
    def __init__(self, line: str, file: str, line_nr: int):
        self.line = line
        self.file = file
        self.line_nr = line_nr


def main(argv: List[str]) -> int:
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


def annotate_logged_errors(log_files: List[str]) -> None:
    error_logs = get_error_logs(log_files)

    if not error_logs:
        return

    known_issues = get_known_issues_from_github()

    step_key = os.getenv("BUILDKITE_STEP_KEY")
    suite_name = step_key or "Logged Errors"
    junit_suite = junit_xml.TestSuite(suite_name)

    artifacts = ci_util.get_artifacts()
    job = os.environ["BUILDKITE_JOB_ID"]

    for i, error in enumerate(error_logs):
        for artifact in artifacts:
            if artifact["job_id"] == job and artifact["path"] == error.file:
                linked_file = f'<a href="{artifact["url"]}">{error.file}</a>'
                break
        else:
            linked_file = error.file

        for issue in known_issues:
            match = issue.regex.search(error.line)
            if match:
                test_case = junit_xml.TestCase(f"log error {i + 1} (known)", suite_name)
                test_case.add_failure_info(
                    message=f"Known error in logs: <a href=\"{issue.info['html_url']}\">{issue.info['title']} (#{issue.info['number']})</a><br/>In {linked_file}:{error.line_nr}:",
                    output=error.line,
                )
                break
        else:
            test_case = junit_xml.TestCase(f"log error {i + 1} (new)", suite_name)
            test_case.add_failure_info(
                message=f"Unknown error in logs<br/>In {linked_file}:{error.line_nr}:",
                output=error.line,
            )
        junit_suite.test_cases.append(test_case)

    junit_name = f"{step_key}_logged_errors" if step_key else "logged_errors"

    junit_report = ci_util.junit_report_filename(junit_name)
    with junit_report.open("w") as f:
        junit_xml.to_xml_report_file(f, [junit_suite])

    if "BUILDKITE_ANALYTICS_TOKEN_LOGGED_ERRORS" in os.environ:
        ci_util.upload_junit_report("logged_errors", ROOT / junit_report)


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


def get_known_issues_from_github() -> list[KnownIssue]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(
        f"https://api.github.com/search/issues?q=type:repo:MaterializeInc/materialize%20type:issue%20state:open%20label:ci-flake",
        headers=headers,
    )

    if response.status_code != 200:
        raise ValueError(f"Bad return code from Github: {response.status_code}")

    issues_json = response.json()
    assert issues_json["incomplete_results"] == False

    known_issues = []
    for issue in issues_json["items"]:
        match = CI_RE.search(issue["body"])
        if not match:
            continue
        known_issues.append(KnownIssue(match.group(1), issue))
    return known_issues


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
