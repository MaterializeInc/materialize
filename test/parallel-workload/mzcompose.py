# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Cockroach, Materialized
from materialize.parallel_workload.parallel_workload import run

SERVICES = [
    Cockroach(setup_materialize=True),
    Materialized(external_cockroach=True, restart="on-failure"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument("--seed", type=str, default=str(int(time.time())))
    parser.add_argument("--runtime", default=3600, type=int)
    parser.add_argument("--complexity", default="ddl", type=str, choices=["dml", "ddl"])
    parser.add_argument("--threads", type=int)

    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    c.up("cockroach", "materialized")
    run(
        "localhost",
        c.default_port("materialized"),
        args.seed,
        args.runtime,
        args.complexity,
        args.threads,
    )
    # Check if catalog is corrupted, can Mz come up again?
    c.down()
    c.up("cockroach", "materialized")
