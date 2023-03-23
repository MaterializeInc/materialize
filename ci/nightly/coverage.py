# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Generator for the test CI pipeline.

This script takes pipeline.template.yml as input, possibly trims out jobs
whose inputs have not changed relative to the code on main, and uploads the
resulting pipeline to the Buildkite job that triggers this script.

On main and tags, all jobs are always run.

For details about how steps are trimmed, see the comment at the top of
pipeline.template.yml and the docstring on `trim_pipeline` below.
"""

import subprocess
import sys
from pathlib import Path

import materialize.cli.mzcompose


def main() -> int:
    # Just the fast sqllogictests, for now
    tests = [("sqllogictest", "default")]

    for (composition, workflow) in tests:
        materialize.cli.mzcompose.main(
            [
                "--find",
                composition,
                "--coverage",
                "run",
                workflow,
            ]
        )
        materialize.cli.mzcompose.main(
            [
                "--find",
                composition,
                "down",
                "-v",
            ]
        )

    # NB: mzcompose _munge_services() sets LLVM_PROFILE_FILE, so that
    # output will go to a special coverage volume, but as a
    # conesquence we can't really distinguish between sqllogictest and
    # clusterd's output.
    subprocess.run(
        [
            "rust-profdata",
            "merge",
            "-sparse",
            *Path("test/sqllogictest/coverage").glob("sqllogictest*.profraw"),
            "-o",
            "sqllogictest.profdata",
        ]
    )
    with open("coverage-sqllogictest.json", "w") as out:
        subprocess.run(
            [
                "rust-cov",
                "export",
                "./target-xcompile/x86_64-unknown-linux-gnu/release/sqllogictest",
                "--instr-profile=sqllogictest.profdata",
            ],
            stdout=out,
        )
    with open("coverage-clusterd.json", "w") as out:
        subprocess.run(
            [
                "rust-cov",
                "export",
                "./target-xcompile/x86_64-unknown-linux-gnu/release/clusterd",
                "--instr-profile=sqllogictest.profdata",
            ],
            stdout=out,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
