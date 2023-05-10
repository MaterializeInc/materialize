# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Clusterd,
    Cockroach,
    Kafka,
    Localstack,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)
from materialize.output_consistency.output_consistency import (
    run_output_consistency_tests,
)

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Cockroach(setup_materialize=True),
    Clusterd(name="clusterd1"),
    # We use mz_panic() in some test scenarios, so environmentd must stay up.
    Materialized(propagate_crashes=False, external_cockroach=True),
    Testdrive(
        volume_workdir="../testdrive:/workdir/testdrive",
        volumes_extra=[".:/workdir/smoke"],
        materialize_params={"cluster": "cluster1"},
    ),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    test_name = "test-output-consistency"

    with c.test_case(test_name):
        c.workflow(test_name)


def workflow_test_output_consistency(c: Composition) -> bool:
    """
    Test the output consistency of different query evaluation strategies (e.g., dataflow rendering and constant folding).
    """

    c.down(destroy_volumes=True)
    with c.override(
        Testdrive(no_reset=True),
    ):
        c.up("testdrive", persistent=True)
        c.up("materialized")

    test_summary = run_output_consistency_tests(c)
    return test_summary.all_passed()
