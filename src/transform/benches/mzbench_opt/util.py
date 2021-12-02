# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

import numpy as np

from . import Scenario


def str_to_ns(time: str) -> np.timedelta64:
    """Parses a time format `hh:mm:ss.up_to_9_digits` to a `np.timedelta64`."""
    h, m, s = time.split(":")
    s, ns = s.split(".")
    ns = ns.ljust(9, "0")
    ns = map(
        lambda t, unit: np.timedelta64(t, unit), [h, m, s, ns], ["h", "m", "s", "ns"]
    )
    return sum(ns)


def results_path(repository: Path, scenario: Scenario, mz_version: str) -> None:
    mz_version = mz_version.replace(" ", "-")
    mz_version = mz_version.replace("(", "")
    mz_version = mz_version.replace(")", "")
    file = f"mzbench-opt-{scenario}-{mz_version}.csv"
    return repository / file
