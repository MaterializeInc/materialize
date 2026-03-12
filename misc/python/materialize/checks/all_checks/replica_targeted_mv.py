# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class ReplicaTargetedMaterializedViews(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.16.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE replica_targeted_mv_t (x INT);
                > INSERT INTO replica_targeted_mv_t SELECT generate_series FROM generate_series(1, 100);

                > CREATE CLUSTER replica_targeted_mv_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1'));

                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_replica_targeted_materialized_views = true

                > CREATE MATERIALIZED VIEW replica_targeted_mv_1 IN CLUSTER replica_targeted_mv_cluster REPLICA r1 AS SELECT count(*) AS cnt FROM replica_targeted_mv_t;
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_replica_targeted_materialized_views = true

                > INSERT INTO replica_targeted_mv_t SELECT generate_series FROM generate_series(101, 200);

                > CREATE MATERIALIZED VIEW replica_targeted_mv_2 IN CLUSTER replica_targeted_mv_cluster REPLICA r2 AS SELECT count(*) AS cnt FROM replica_targeted_mv_t;
                """,
                """
                $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER SYSTEM SET enable_replica_targeted_materialized_views = true

                > INSERT INTO replica_targeted_mv_t SELECT generate_series FROM generate_series(201, 300);

                > CREATE MATERIALIZED VIEW replica_targeted_mv_3 IN CLUSTER replica_targeted_mv_cluster REPLICA r1 AS SELECT count(*) AS cnt FROM replica_targeted_mv_t;

                > SELECT cnt FROM replica_targeted_mv_1;
                300
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT cnt FROM replica_targeted_mv_1;
                300

                > SELECT cnt FROM replica_targeted_mv_2;
                300

                > SELECT cnt FROM replica_targeted_mv_3;
                300
           """
            )
        )
