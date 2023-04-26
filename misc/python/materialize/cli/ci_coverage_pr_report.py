# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import re
import subprocess
from collections import OrderedDict
from typing import Dict, Optional

import junit_xml

from materialize import ROOT, ci_util

# - None value indicates that this line is interesting, but we don't know yet
#   if it can actually be covered.
# - Positive values indicate that the line can be covered and how often is has
#   been covered in end-to-end tests.
# - Negative values indicate that the line has only been covered in unit tests.
Coverage = Dict[str, OrderedDict[int, Optional[int]]]
SOURCE_RE = re.compile(
    "^/var/lib/buildkite-agent/builds/buildkite-.*/materialize/coverage/(.*$)"
)


def find_modified_lines() -> Coverage:
    """
    Find each line that has been added or modified in the current pull request.
    """
    base_branch = os.getenv("BUILDKITE_PULL_REQUEST_BASE_BRANCH", "main") or os.getenv(
        "BUILDKITE_PIPELINE_DEFAULT_BRANCH", "main"
    )
    result = subprocess.run(
        ["git", "merge-base", "HEAD", f"origin/{base_branch}"],
        check=True,
        capture_output=True,
    )
    merge_base = result.stdout.strip()
    result = subprocess.run(
        ["git", "diff", "-U0", merge_base], check=True, capture_output=True
    )

    coverage: Coverage = {}
    file = None
    for line_raw in result.stdout.splitlines():
        line = line_raw.decode("utf-8")
        # +++ b/src/adapter/src/coord/command_handler.rs
        if line.startswith("+++"):
            file = line.removeprefix("+++ b/")
            if not line.endswith(".rs"):
                continue
            coverage[file] = OrderedDict()
        # @@ -641,7 +640,6 @@ impl Coordinator {
        elif line.startswith("@@ ") and file in coverage:
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
                coverage[file][line_nr] = None
    return coverage


unittests_have_run = False


def mark_covered_lines(
    lcov_file: str, coverage: Coverage, unittests: bool = False
) -> None:
    """
    For a description of the lcov tracing file format, see the bottom of
    https://linux.die.net/man/1/geninfo
    """
    global unittests_have_run
    if unittests:
        unittests_have_run = True
    else:
        assert (
            not unittests_have_run
        ), "Call mark_covered_lines for unit tests last in order to get correct code coverage reports"

    for line in open(lcov_file):
        line = line.strip()
        if not line:
            continue
        if line == "end_of_record":
            continue
        method, content = tuple(line.strip().split(":", 1))
        # SF:/var/lib/buildkite-agent/builds/buildkite-builders-d43b1b5-i-0193496e7aec9a4e3-1/materialize/coverage/src/transform/src/lib.rs
        if method == "SF":
            if content.startswith("src/"):  # for unit tests
                file = content
            else:
                result = SOURCE_RE.search(content)
                assert result, f"Unexpected file {content}"
                file = result.group(1)
        # DA:111,15524
        # DA:112,0
        # DA:113,15901
        elif method == "DA" and file in coverage:
            line_str, hit_str = content.split(",", 1)
            line_nr = int(line_str)
            hit = int(hit_str) if hit_str.isnumeric() else int(float(hit_str))
            if line_nr in coverage[file]:
                if unittests:
                    if not coverage[file][line_nr]:
                        coverage[file][line_nr] = (coverage[file][line_nr] or 0) - hit
                else:
                    coverage[file][line_nr] = (coverage[file][line_nr] or 0) + hit


def get_report(coverage: Coverage) -> str:
    """
    Remove uncovered lines in real files and print a git diff, then restore to
    original state. This is pretty messy, we might want to do it without git if
    it gets any more complex.
    """
    try:
        # Remove lines which are only covered in unit tests as sindicated by a
        # negative line count
        for file, lines in coverage.items():
            with open(file, "r+") as f:
                content = f.readlines()
                f.seek(0)
                for i, line in enumerate(content):
                    if (lines.get(i + 1) or 0) < 0:
                        f.write(line)
                f.truncate()

        subprocess.run(
            ["git", "config", "user.email", "coverage@materialize.com"],
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Code Coverage"],
            check=True,
        )
        subprocess.run(
            ["git", "commit", "--allow-empty", "-a", "-m", "Covered in unit test only"],
            check=True,
        )
        # Add the lines back so they show up with a "." marker
        subprocess.run(["git", "revert", "--no-commit", "HEAD"], check=True)

        # Remove lines which are not covered at all so they show up with "!" marker
        for file, lines in coverage.items():
            with open(file, "r+") as f:
                content = f.readlines()
                f.seek(0)
                for i, line in enumerate(content):
                    if (lines.get(i + 1) or 0) > 0:
                        f.write(line)
                f.truncate()

        result = subprocess.run(
            [
                "git",
                "diff",
                "--output-indicator-old=!",
                "--output-indicator-new=.",
                "HEAD",
            ],
            check=True,
            capture_output=True,
        )
        return result.stdout.decode("utf-8").strip()
    finally:
        # Restore the code into its original state
        subprocess.run(["git", "reset", "--hard", "HEAD~"], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ci-coverage-pr-report",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
ci-coverage-pr-report creates a code coverage report for CI.""",
    )

    parser.add_argument("--unittests", type=str, help="unit test lcov file")
    parser.add_argument("tests", nargs="+", help="all other lcov files from test runs")
    args = parser.parse_args()

    result = subprocess.run(["git", "diff"], check=True, capture_output=True)
    output = result.stdout.decode("utf-8").strip()
    assert not output, f"Has to run on clean git state: \n{output}"

    test_cases = []

    coverage = find_modified_lines()
    for lcov_file in args.tests:
        mark_covered_lines(lcov_file, coverage)
    if args.unittests:
        if os.path.isfile(args.unittests):
            mark_covered_lines(args.unittests, coverage, unittests=True)
        else:
            test_case = junit_xml.TestCase("Unit Tests", "Code Coverage")
            test_case.add_error_info(message="No coverage for unit tests available")
            test_cases.append(test_case)
    report = get_report(coverage)

    test_case = junit_xml.TestCase("Uncovered Lines in Pull Request", "Code Coverage")
    if len(report):
        # Buildkite interprets the +++ and --- chars at the start of line, put
        # in a zero-width space as a workaround.
        print(report.replace("\n+++", "\n\u200B+++").replace("\n---", "\n\u200B---"))
        test_case.add_error_info(
            message="Full coverage report is available in Buildkite. The following changed lines are uncovered:",
            output=report,
        )
    else:
        test_case.add_error_info(
            message="Full coverage report is available in Buildkite. All changed lines are covered."
        )
    test_cases.append(test_case)
    junit_suite = junit_xml.TestSuite("Code Coverage", test_cases)
    junit_report = ROOT / ci_util.junit_report_filename("coverage")
    with junit_report.open("w") as f:
        junit_xml.to_xml_report_file(f, [junit_suite])


if __name__ == "__main__":
    main()
