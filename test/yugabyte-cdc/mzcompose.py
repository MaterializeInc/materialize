# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Native Yugabyte source tests, functional.
"""

import random

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.yugabyte import Yugabyte

SERVICES = [
    Yugabyte(),
    Materialized(),
    Testdrive(),
]


#
# Test that Yugabyte ingestion works
#
def workflow_default(c: Composition) -> None:
    c.up("yugabyte", "materialized")
    seed = random.getrandbits(16)
    c.run_testdrive_files(
        "--no-reset",
        "--max-errors=1",
        f"--seed={seed}",
        f"--var=default-replica-size=scale={Materialized.Size.DEFAULT_SIZE},workers={Materialized.Size.DEFAULT_SIZE}",
        "yugabyte-cdc.td",
    )
