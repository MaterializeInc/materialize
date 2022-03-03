# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Skips unselected tests in the nightly pipeline."""

import subprocess
import sys
from pathlib import Path

import yaml

from materialize import spawn


def main() -> int:
    # If the test filter metadata doesn't exist, run all tests.
    exists = subprocess.run(["buildkite-agent", "meta-data", "exists", "tests"])
    if exists.returncode == 100:
        return 0

    # Otherwise, filter down to the selected tests.
    with open(Path(__file__).parent / "pipeline.yml") as f:
        pipeline = yaml.safe_load(f.read())
    selected_tests = set(
        spawn.capture(["buildkite-agent", "meta-data", "get", "tests"]).splitlines()
    )
    new_steps = []
    for step in pipeline["steps"]:
        if "id" in step and step["id"] in selected_tests:
            del step["id"]
            new_steps.append(step)
    spawn.runv(
        ["buildkite-agent", "pipeline", "upload", "--replace"],
        stdin=yaml.dump(new_steps).encode(),
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
