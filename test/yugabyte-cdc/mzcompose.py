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

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.yugabyte import Yugabyte

SERVICES = [
    Yugabyte(),
    Materialized(),
    # Overridden below
    Testdrive(),
]


#
# Test that Yugabyte ingestion works
#
def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="*",
        default=["*.td"],
        help="limit to only the files matching filter",
    )

    parser.add_argument(
        "--default-timeout",
        type=str,
        help="set the default timeout for Testdrive",
    )

    args = parser.parse_args()

    c.up("yugabyte", "materialized")
    seed = random.getrandbits(16)
    with c.override(Testdrive(default_timeout=args.default_timeout)):
        c.run_testdrive_files(
            "--no-reset",
            "--max-errors=1",
            f"--seed={seed}",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            *args.filter,
        )
