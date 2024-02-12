# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.output_consistency.output_consistency_test import OutputConsistencyTest

SERVICES = [
    Cockroach(setup_materialize=True),
    Materialized(propagate_crashes=True, external_cockroach=True),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """
    Test the output consistency of different query evaluation strategies (e.g., dataflow rendering and constant folding).
    """

    c.down(destroy_volumes=True)

    c.up("materialized")

    test = OutputConsistencyTest()
    args = test.parse_output_consistency_input_args(parser)
    connection = c.sql_connection()

    test_summary = test.run_output_consistency_tests(connection, args)

    if not test_summary.all_passed():
        raise FailedTestExecutionError(
            "At least one test failed", errors=test_summary.failures
        )
