# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Calculate the `namescore`---the perctange of column references with
name information---of an `EXPLAIN PLAN`. By default, runs on all SLT
files in $MZ_ROOT/test/sqllogictest."""

import argparse
import os
import re

from materialize import MZ_ROOT

SLT_ROOT = MZ_ROOT / "test" / "sqllogictest"

COLUMN_REF_RE = re.compile(
    r"""
    \#[0-9]+({[^}]+})?
    """,
    re.VERBOSE,
)


def find_slt_files() -> list[str]:
    """Find all .slt files in $MZ_ROOT/test/sqllogictest directory"""
    slt_files = []
    for root, _dirs, files in os.walk(SLT_ROOT):
        for file in files:
            if file.endswith(".slt"):
                slt_files.append(os.path.join(root, file))
    return slt_files


def namescore(filename: str) -> tuple[int, int, int]:
    """Calculate the namescore of a file"""
    named_refs = 0
    unknown_cols = 0
    refs = 0
    with open(filename) as f:
        content = f.read()
        for match in COLUMN_REF_RE.finditer(content):
            refs += 1

            if match.group(1):
                named_refs += 1
                if match.group(1) == '{"?column?"}':
                    unknown_cols += 1
    return (named_refs, unknown_cols, refs)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="namescore",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""
calculates the `namescore` (percentage of column references with names)
of given files (or all SLT files in $MZ_ROOT/test/sqllogictest by default)""",
    )

    parser.add_argument(
        "tests",
        nargs="*",
        help="explicit files to run on [default: all SLT files in $MZ_ROOT/test/sqllogictest]",
    )
    args = parser.parse_args()

    tests = args.tests or find_slt_files()

    named_refs = 0
    unknown_cols = 0
    refs = 0
    nonames = 0
    total = len(tests)
    for test in tests:
        nr, uc, r = namescore(test)
        if r == 0:
            assert nr == 0
            assert uc == 0
            nonames += 1
            continue

        print(
            f"{test.removeprefix(str(SLT_ROOT) + os.sep)}: {nr / r * 100:.2f}% ({nr} / {r}; {uc} unknown columns)"
        )
        named_refs += nr
        unknown_cols += uc
        refs += r
    print(
        f"\nOverall namescore: {named_refs / refs * 100:.2f}% ({named_refs} / {refs}; {unknown_cols} unknown columns); {nonames} files with no column references / {total} total files"
    )


if __name__ == "__main__":
    main()
