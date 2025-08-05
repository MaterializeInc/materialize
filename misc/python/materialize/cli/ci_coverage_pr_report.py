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
from collections.abc import Callable

import junit_xml

from materialize import MZ_ROOT, buildkite, ci_util

# - None value indicates that this line is interesting, but we don't know yet
#   if it can actually be covered.
# - Positive values indicate that the line can be covered and how often is has
#   been covered in end-to-end tests.
# - Negative values indicate that the line has only been covered in unit tests.
Coverage = dict[str, OrderedDict[int, int | None]]
SOURCE_RE = re.compile(
    r"""
    ( src/(.*$)"
    | bazel-out/.*/bin/(.*$)
    | external/(.*$)
    | /usr/local/lib/rustlib/(.*$)
    | /var/lib/buildkite-agent/builds/buildkite-.*/materialize/[^/]*/src/(.*$)
    )""",
    re.VERBOSE,
)
# * Deriving generates more code, but we don't expect to cover this in most
# cases, so ignore such lines.
# * Same for mz_ore::test
# * The await keyword is not properly supported
# (https://github.com/rust-lang/rust/issues/98712).
IGNORE_SRC_LINE_RE = re.compile(
    r"""
    ( \#\[derive\(.*\)\]
    | \#\[mz_ore::test.*\]
    | \.await
    )
    """,
    re.VERBOSE,
)

IGNORE_FILE_PATH_RE = re.compile(
    r"""
    ( /maelstrom/
    )
    """,
    re.VERBOSE,
)


def ignore_file_in_coverage_report(file_path: str) -> bool:
    if not file_path.endswith(".rs"):
        return True

    if IGNORE_FILE_PATH_RE.search(file_path):
        return True

    return False


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

    # There will always be an SF line specifying a file before a DA line
    # according to the lcov tracing file format definition
    file = None

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
                assert result, f"not found: {content}"
                file = result.group(1)
        # DA:111,15524
        # DA:112,0
        # DA:113,15901
        elif method == "DA":
            assert file, "file was not set by a SF line"
            if file in coverage:
                line_str, hit_str = content.split(",", 1)
                line_nr = int(line_str)
                hit = int(hit_str) if hit_str.isnumeric() else int(float(hit_str))
                if line_nr in coverage[file]:
                    if unittests:
                        if not coverage[file][line_nr]:
                            coverage[file][line_nr] = (
                                coverage[file][line_nr] or 0
                            ) - hit
                    else:
                        coverage[file][line_nr] = (coverage[file][line_nr] or 0) + hit


def get_report(
    coverage: Coverage, fn: Callable[[OrderedDict[int, int | None], int, str], bool]
) -> str:
    """
    Remove uncovered lines in real files and print a git diff, then restore to
    original state.
    The fn function determines when to keep a line. Everything not kept will
    show up in the diff.
    """
    try:
        # Remove lines which are not covered so they show up with "!" marker
        for file, lines in coverage.items():
            with open(file, "r+") as f:
                content = f.readlines()
                f.seek(0)
                for i, line in enumerate(content):
                    if fn(lines, i, line):
                        f.write(line)
                f.truncate()

        result = subprocess.run(
            [
                "git",
                "diff",
                # Spaces can be moved around, leading to confusing reports
                "--ignore-all-space",
                "--output-indicator-old=!",
                "HEAD",
            ],
            check=True,
            capture_output=True,
        )

        return result.stdout.decode("utf-8").strip()
    finally:
        # Restore the code into its original state
        subprocess.run(["git", "reset", "--hard"], check=True)


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

    coverage: Coverage = {}
    for file, line in buildkite.find_modified_lines():
        if not ignore_file_in_coverage_report(file):
            coverage.setdefault(file, OrderedDict())[line] = None

    for lcov_file in args.tests:
        mark_covered_lines(lcov_file, coverage)
    if args.unittests:
        if os.path.isfile(args.unittests):
            mark_covered_lines(args.unittests, coverage, unittests=True)
        else:
            test_case = junit_xml.TestCase("Unit Tests", "Code Coverage")
            test_case.add_error_info(message="No coverage for unit tests available")
            test_cases.append(test_case)

    unit_test_only_report = get_report(
        coverage,
        lambda lines, i, line: bool(
            (lines.get(i + 1) or 0) >= 0 or IGNORE_SRC_LINE_RE.search(line)
        ),
    )
    # If a line has "None" marker, then it can't be covered, print it out.
    # If a line has positive or negative coverage then it is
    # covered in normal tests or unit tests, print it out.
    # All remaining lines can be covered, but are not covered.
    uncovered_report = get_report(
        coverage,
        lambda lines, i, line: bool(
            lines.get(i + 1) is None
            or (lines.get(i + 1) or 0) != 0
            or IGNORE_SRC_LINE_RE.search(line)
        ),
    )

    test_case = junit_xml.TestCase("Uncovered Lines in PR", "Code Coverage")
    if len(uncovered_report):
        print("Uncovered Lines in PR")
        # Buildkite interprets the +++ and --- chars at the start of line, put
        # in a zero-width space as a workaround.
        ZWSP = "\u200B"
        print(
            uncovered_report.replace("\n+++", f"\n{ZWSP}+++").replace(
                "\n---", f"\n{ZWSP}---"
            )
        )
        test_case.add_error_info(
            message="The following changed lines are uncovered:",
            output=uncovered_report,
        )
    else:
        test_case.add_error_info(message="All changed lines are covered.")
    test_cases.append(test_case)

    test_case = junit_xml.TestCase(
        "Lines Covered only in Unit Tests in PR", "Code Coverage"
    )
    if len(unit_test_only_report):
        print("Lines Covered only in Unit Tests in PR")
        # Buildkite interprets the +++ and --- chars at the start of line, put
        # in a zero-width space as a workaround.
        print(
            unit_test_only_report.replace("\n+++", "\n\u200B+++").replace(
                "\n---", "\n\u200B---"
            )
        )
        test_case.add_error_info(
            message="The following changed lines are covered only in unit tests:",
            output=unit_test_only_report,
        )
    else:
        test_case.add_error_info(
            message="All changed, covered lines are covered outside of unit tests."
        )
    test_cases.append(test_case)

    junit_suite = junit_xml.TestSuite("Code Coverage", test_cases)
    junit_report = MZ_ROOT / ci_util.junit_report_filename("coverage")
    with junit_report.open("w") as f:
        junit_xml.to_xml_report_file(f, [junit_suite])


if __name__ == "__main__":
    main()
