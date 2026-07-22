# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# lint_test_flags.py - Check that new configs are used in parallel workload and other tests

import argparse
import random
import re
import sys
from pathlib import Path

from materialize.mz_version import MzVersion
from materialize.mzcompose import (
    UNINTERESTING_SYSTEM_PARAMETERS,
    get_default_system_parameters,
)
from materialize.parallel_workload.action import FlipFlagsAction

# Match both the bare `Config::new(` (imported via `use mz_dyncfg::Config`) and
# the qualified `mz_dyncfg::Config::new(`, without matching unrelated third-party
# Config types like `tokio_postgres::Config::new(`.
CONFIG_REGEX = re.compile(r'(?: |mz_dyncfg::)Config::new\(\s*"([^"]+)"', re.MULTILINE)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="lint-tests-flags",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Check that new configs are used in parallel workload and other tests",
    )
    parser.parse_args()

    configs = []

    for path in Path("src").rglob("*.rs"):
        if path in [
            Path("src/dyncfg/src/lib.rs"),
            Path("src/dyncfg-file/src/lib.rs"),
            # Has a local `Config` type unrelated to dyncfg.
            Path("src/orchestratord/src/gcp_node_upgrade.rs"),
        ]:
            continue  # contains tests
        with path.open(encoding="utf-8") as file:
            content = file.read()
            if matches := CONFIG_REGEX.findall(content):
                configs.extend(matches)

    action = FlipFlagsAction(random.Random(), None)
    parallel_workload_known_flags = set(action.flags_with_values).union(
        action.uninteresting_flags
    )
    mzcompose_known_flags = set(
        get_default_system_parameters(MzVersion.parse_cargo()).keys()
    ).union(UNINTERESTING_SYSTEM_PARAMETERS)
    found = False

    for config in configs:
        if config not in parallel_workload_known_flags:
            print(
                f'Configuration flag "{config}" seems to have been introduced/changed. Make sure parallel-workload\'s FlipFlagsAction in misc/python/materialize/parallel_workload/action.py knows about some valid values for it'
            )
            found = True

    for config in configs:
        if config not in mzcompose_known_flags:
            print(
                f'Configuration flag "{config}" seems to have been introduced/changed. Make sure get_variable_system_parameters/get_minimal_system_parameters/UNINTERESTING_SYSTEM_PARAMETERS in misc/python/materialize/mzcompose/__init__.py knows about some valid values for it'
            )
            found = True

    return int(found)


if __name__ == "__main__":
    sys.exit(main())
