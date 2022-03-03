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

import sys
from pathlib import Path

import yaml

import materialize.cli.mzcompose


def main() -> int:
    with open(Path(__file__).parent.parent / "test" / "pipeline.template.yml") as f:
        pipeline = yaml.safe_load(f)

    tests = []

    for step in pipeline["steps"]:
        for plugin in step.get("plugins", []):
            for plugin_name, plugin_config in plugin.items():
                if plugin_name == "./ci/plugins/mzcompose":
                    tests.append((plugin_config["composition"], plugin_config["run"]))

    for (composition, workflow) in tests:
        print(f"==> Running workflow {workflow} in {composition}")
        materialize.cli.mzcompose.main(
            [
                "run",
                workflow,
                "--coverage",
                "--find",
                composition,
            ]
        )
        materialize.cli.mzcompose.main(
            [
                "down",
                "-v",
                "--coverage",
                "--find",
                composition,
            ]
        )

    # TODO: gather and combine coverage information.

    return 0


if __name__ == "__main__":
    sys.exit(main())
