# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Skips unselected tests in the pipeline.template.yml in the ci subdirectory provided as argument."""

import argparse
import subprocess
import sys
from typing import Any

import yaml

from materialize import MZ_ROOT, spawn


def permit_rerunning_successful_steps(pipeline: Any) -> None:
    def visit(step: Any) -> None:
        step.setdefault("retry", {}).setdefault("manual", {}).setdefault(
            "permit_on_passed", True
        )

    for config in pipeline["steps"]:
        if "trigger" in config or "wait" in config or "block" in config:
            continue
        if "group" in config:
            for inner_config in config.get("steps", []):
                visit(inner_config)
            continue
        visit(config)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("pipeline")
    args = parser.parse_args()

    # If the test filter metadata doesn't exist, run all tests.
    exists = subprocess.run(["buildkite-agent", "meta-data", "exists", "tests"])
    if exists.returncode == 100:
        return 0

    # Otherwise, filter down to the selected tests.
    with open(MZ_ROOT / "ci" / args.pipeline / "pipeline.template.yml") as f:
        pipeline = yaml.safe_load(f.read())
    selected_tests = set(
        spawn.capture(["buildkite-agent", "meta-data", "get", "tests"]).splitlines()
    )
    new_steps = []
    for step in pipeline["steps"]:
        # Always run analyze step in the end
        if "id" in step and (step["id"] in selected_tests or step["id"] == "analyze"):
            del step["id"]
            new_steps.append(step)
        if "wait" in step:
            new_steps.append(step)
        # Groups can't be nested, so handle them explicitly here instead of recursing
        if "group" in step:
            new_inner_steps = []
            for inner_step in step.get("steps", []):
                if "id" in inner_step and inner_step["id"] in selected_tests:
                    del inner_step["id"]
                    new_inner_steps.append(inner_step)
            # There must be at least 1 step in a group
            if new_inner_steps:
                step["steps"] = new_inner_steps
                del step["key"]
                new_steps.append(step)

    permit_rerunning_successful_steps(pipeline)

    spawn.runv(
        ["buildkite-agent", "pipeline", "upload", "--replace"],
        stdin=yaml.dump(new_steps).encode(),
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
