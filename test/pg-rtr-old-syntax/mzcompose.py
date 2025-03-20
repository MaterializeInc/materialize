# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Functional test for Postgres source with real-time recency enabled. Queries
should block until results are available instead of returning out of date
results.
"""

import random

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy

SERVICES = [
    Postgres(),
    Materialized(),
    Toxiproxy(),
    Testdrive(),
]


#
# Test that real-time recency works w/ slow ingest of upstream data.
#
def workflow_default(c: Composition) -> None:
    c.up("postgres", "materialized", "toxiproxy")
    seed = random.getrandbits(16)
    c.run_testdrive_files(
        "--no-reset",
        "--max-errors=1",
        f"--seed={seed}",
        "rtr/toxiproxy-setup.td",
        "rtr/mz-setup.td",
        "rtr/verify-rtr.td",
    )
