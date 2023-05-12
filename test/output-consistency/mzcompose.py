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
from materialize.output_consistency.output_consistency import (
    run_output_consistency_tests,
)

SERVICES = [
    Cockroach(setup_materialize=True),
    # We use mz_panic() in some test scenarios, so environmentd must stay up.
    Materialized(propagate_crashes=False, external_cockroach=True),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Test the output consistency of different query evaluation strategies (e.g., dataflow rendering and constant folding).
    """

    parser.add_argument("--runtime", default=600, type=int)
    parser.add_argument("--seed", default=0, type=int)
    parser.add_argument("--dry-run", default=False, type=bool)
    parser.add_argument("--fail-fast", default=False, type=bool)
    parser.add_argument("--execute-setup", default=True, type=bool)
    parser.add_argument("--verbose", default=False, type=bool)
    args = parser.parse_args()

    c.down(destroy_volumes=True)

    c.up("materialized")
    cursor = c.sql_cursor()

    test_summary = run_output_consistency_tests(
        cursor,
        args.runtime,
        args.seed,
        args.dry_run,
        args.fail_fast,
        args.execute_setup,
        args.verbose,
    )

    assert test_summary.all_passed(), "At least one test failed"
