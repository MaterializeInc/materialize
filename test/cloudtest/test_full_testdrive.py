# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import argparse

import pytest

from materialize.cloudtest.app.materialize_application import MaterializeApplication


@pytest.mark.long
def test_full_testdrive(mz: MaterializeApplication) -> None:
    parser = argparse.ArgumentParser(
        prog="test-full-testdrive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--file-pattern", default="*.td", type=str)
    args, _ = parser.parse_known_args()

    file_pattern = args.file_pattern
    print(f"File pattern: {file_pattern}")

    mz.testdrive.copy("test/testdrive", "/workdir")
    mz.testdrive.run(f"testdrive/{file_pattern}")
