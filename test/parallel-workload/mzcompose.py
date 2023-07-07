# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Cockroach, Materialized
from materialize.parallel_workload.parallel_workload import parse_common_args, run

SERVICES = [
    Cockroach(setup_materialize=True),
    Materialized(external_cockroach=True, restart="on-failure"),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parse_common_args(parser)
    args = parser.parse_args()

    print(f"--- Random seed is {args.seed}")
    c.up("cockroach", "materialized")
    run(
        "localhost",
        c.default_port("materialized"),
        args.seed,
        args.runtime,
        args.complexity,
        args.scenario,
        args.threads,
    )
    # Restart mz
    c.kill("materialized")
    c.up("materialized")
    # Verify that things haven't blown up
    c.sql("SELECT 1")
