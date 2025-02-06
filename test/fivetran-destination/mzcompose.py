# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests for the Materialize Fivetran destination.

This composition is a lightweight test harness for the Materialize Fivetran
destination.

Each test is structured as a directory within whose name begins with `test-`.
Each test directory may contain:

  * any number of Testdrive scripts, which are files whose name ends in `.td`
  * any number of Fivetran Destination Tester scripts, which are files whose
    name ends in `.json`
  * a README file, which must be named `00-README`

The test harness boots Materialize and the Materialize Fivetran Destination
server. At the start of each test, the harness creates a database named `test`
that is owned by the `materialize` user. Then, the test harness runs each
Testdrive and Fivetran Destination Tester script within the test directory in
lexicographic order. If a script fails, the test is marked as failed and no
further scripts from the test are executed.

A script is normally considered to fail if Testdrive or the Fivetran Destination
Tester exit with a non-zero code. However, if the last line of a Fivetran
Destination Tester script matches the pattern `// FAIL: <message>`, the test
harness will expect the Fivetran Destination Tester to exit with a non-zero code
and with `<message>` printed to stdout; the test script will be marked as failed
if it exits with code zero or if `<message>` is not printed to stdout.

For details on Testdrive, consult doc/developer/testdrive.md.

For details on the Fivetran Destination Tester, which is a tool provided by
Fivetran, consult misc/fivetran-sdk/tools/README.md.

To invoke the test harness locally:

  $ cd test/fivetran-destination
  $ ./mzcompose [--dev] run default -- [FILTER]

The optional FILTER argument indicates a pattern which limits which test cases
are run. A pattern matches a test case if the pattern is contained within the
name of the test directory.
"""

import shutil
from pathlib import Path

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.fivetran_destination import FivetranDestination
from materialize.mzcompose.services.fivetran_destination_tester import (
    FivetranDestinationTester,
)
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.testdrive import Testdrive

ROOT = Path(__file__).parent

SERVICES = [
    Mz(app_password=""),
    Materialized(),
    Testdrive(
        no_reset=True,
        default_timeout="5s",
    ),
    FivetranDestination(
        volumes_extra=["./data:/data"],
    ),
    FivetranDestinationTester(
        destination_host="fivetran-destination",
        destination_port=6874,
        volumes_extra=["./data:/data"],
    ),
]

# Tests that are currently broken because the Fivetran Tester seems to do the wrong thing.
BROKEN_TESTS = []


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("filter", nargs="?")
    args = parser.parse_args()

    c.up("materialized", "fivetran-destination")

    for path in ROOT.iterdir():
        if path.name.startswith("test-"):
            if args.filter and args.filter not in path.name:
                print(f"Test case {path.name!r} does not match filter; skipping...")
                continue
            if path.name in BROKEN_TESTS:
                print(f"Test case {path.name!r} is currently broken; skipping...")
                continue
            with c.test_case(path.name):
                _run_test_case(c, path)


def _run_test_case(c: Composition, path: Path):
    c.sql("DROP DATABASE IF EXISTS test")
    c.sql("CREATE DATABASE test")
    c.sql('DROP CLUSTER IF EXISTS "name with space" CASCADE')
    c.sql("CREATE CLUSTER \"name with space\" SIZE '1'")
    for test_file in sorted(p for p in path.iterdir()):
        test_file = test_file.relative_to(ROOT)
        if test_file.suffix == ".td":
            c.run_testdrive_files(str(test_file))
        elif test_file.suffix == ".json":
            _run_destination_tester(c, test_file)
        elif test_file.name != "00-README":
            raise RuntimeError(f"unexpected test file: {test_file}")


# Run the Fivetran Destination Tester with a single file.
def _run_destination_tester(c: Composition, test_file: Path):
    # The Fivetran Destination tester operates on an entire directory at a time. We run
    # individual test cases by copying everything into a single "data" directory which
    # automatically gets cleaned up at the start and end of every run.
    with DataDirGuard(ROOT / "data") as data_dir:
        test_file = ROOT / test_file
        shutil.copy(test_file, data_dir.path())

        last_line = test_file.read_text().splitlines()[-1]
        if last_line.startswith("// FAIL: "):
            expected_failure = last_line.removeprefix("// FAIL: ")
        else:
            expected_failure = None

        if expected_failure:
            ret = c.run("fivetran-destination-tester", check=False, capture=True)
            print("stdout:")
            print(ret.stdout)
            assert (
                ret.returncode != 0
            ), f"destination tester did not fail with expected message {expected_failure!r}"
            assert (
                expected_failure in ret.stdout
            ), f"destination tester did not fail with expected message {expected_failure!r}"
        else:
            c.run("fivetran-destination-tester")


# Type that implements the Context Protocol that makes it easy to automatically clean up our data
# directory before and after every test run.
class DataDirGuard:
    def __init__(self, dir: Path):
        self._dir = dir

    def clean(self):
        for file in self._dir.iterdir():
            if file.name in ("configuration.json", ".gitignore"):
                continue
            file.unlink()

    def path(self) -> Path:
        return self._dir

    def __enter__(self):
        self.clean()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.clean()
