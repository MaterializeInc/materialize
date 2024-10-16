# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test the output consistency of different query evaluation strategies (e.g.,
dataflow rendering and constant folding).
"""

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import PostgresAsCockroach
from materialize.mzcompose.test_result import FailedTestExecutionError
from materialize.output_consistency.execution.query_output_mode import QueryOutputMode
from materialize.output_consistency.output_consistency_test import (
    OutputConsistencyTest,
    upload_output_consistency_results_to_test_analytics,
)

SERVICES = [
    PostgresAsCockroach(),
    Materialized(propagate_crashes=True, external_cockroach=True),
    Mz(app_password=""),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.down(destroy_volumes=True)

    c.up("materialized")

    test = OutputConsistencyTest()
    args = test.parse_output_consistency_input_args(parser)
    default_connection = c.sql_connection()
    mz_system_connection = c.sql_connection(user="mz_system", port=6877)

    test_summary = test.run_output_consistency_tests(
        default_connection,
        mz_system_connection,
        args,
        query_output_mode=QueryOutputMode.SELECT,
    )

    upload_output_consistency_results_to_test_analytics(c, test_summary)

    if not test_summary.all_passed():
        raise FailedTestExecutionError(errors=test_summary.failures)
