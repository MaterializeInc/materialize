# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.cloudtest.app.materialize_application import MaterializeApplication


def test_replica_metrics(mz: MaterializeApplication) -> None:
    mz.testdrive.run(
        input=dedent(
            """
            > CREATE CLUSTER my_cluster REPLICAS (my_replica (SIZE '4-4'))

            > SELECT COUNT(*) FROM mz_internal.mz_cluster_replica_metrics m JOIN mz_cluster_replicas cr ON m.replica_id = cr.id WHERE cr.name = 'my_replica' AND m.cpu_nano_cores IS NOT NULL AND m.memory_bytes IS NOT NULL
            4

            > DROP CLUSTER my_cluster
            """
        ),
    )
