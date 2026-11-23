# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# clear_corrupted_cargo_target_dir.py - Detect build failures that a retry
# recovers from in the given log, and remove the Cargo target directories when
# corrupted incremental build artifacts are to blame. Exits 199 on a match.

import argparse
from pathlib import Path

from materialize.mzbuild import (
    CargoRegistryFetchFailure,
    RustIncrementalBuildFailure,
    exit_for_retryable_build_failure,
    raise_if_retryable_build_failure,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="clear-corrupted-cargo-target-dir",
        description="Clear the cargo target directories and exit 199 if the log shows a build failure that a retry recovers from.",
    )
    parser.add_argument("log", type=Path, help="output of the failed cargo invocation")
    args = parser.parse_args()

    try:
        raise_if_retryable_build_failure(args.log.read_text(errors="replace"))
    except (RustIncrementalBuildFailure, CargoRegistryFetchFailure) as e:
        exit_for_retryable_build_failure(e)


if __name__ == "__main__":
    main()
