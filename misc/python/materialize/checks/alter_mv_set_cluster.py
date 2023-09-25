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
from materialize.util import MzVersion


class AlterMvSetCluster(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse("0.70.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE alter_mv_set_cluster_table (f1 INTEGER);
                > CREATE CLUSTER alter_mv_set_cluster_cluster1 REPLICAS (replica1 (SIZE '1'));
                > CREATE MATERIALIZED VIEW alter_mv_set_cluster_view1 AS SELECT f1 + 1 AS f1 FROM alter_mv_set_cluster_table;
                > CREATE DEFAULT INDEX ON alter_mv_set_cluster_view1;
                > INSERT INTO alter_mv_set_cluster_table VALUES (1);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(
                dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    ALTER SYSTEM SET enable_alter_set_cluster = true

                    > CREATE MATERIALIZED VIEW alter_mv_set_cluster_view2 AS SELECT f1 + 1 AS f1 FROM alter_mv_set_cluster_view1;
                    > CREATE DEFAULT INDEX ON alter_mv_set_cluster_view2;
                    > ALTER MATERIALIZED VIEW alter_mv_set_cluster_view1 SET CLUSTER alter_mv_set_cluster_cluster1;
                    > INSERT INTO alter_mv_set_cluster_table VALUES (2);

                    > CREATE CLUSTER alter_mv_set_cluster_cluster2 REPLICAS (replica1 (SIZE '1'));
                    """
                )
            ),
            Testdrive(
                dedent(
                    """
                    > CREATE MATERIALIZED VIEW alter_mv_set_cluster_view3 AS SELECT f1 + 1 AS f1 FROM alter_mv_set_cluster_view2;
                    > CREATE DEFAULT INDEX ON alter_mv_set_cluster_view3;

                    > ALTER MATERIALIZED VIEW alter_mv_set_cluster_view1 SET CLUSTER alter_mv_set_cluster_cluster2;
                    > ALTER MATERIALIZED VIEW alter_mv_set_cluster_view2 SET CLUSTER alter_mv_set_cluster_cluster2;
                    > ALTER MATERIALIZED VIEW alter_mv_set_cluster_view3 SET CLUSTER alter_mv_set_cluster_cluster2;

                    > INSERT INTO alter_mv_set_cluster_table VALUES (3);
                    """
                )
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM alter_mv_set_cluster_view1;
                2
                3
                4

                > SELECT * FROM alter_mv_set_cluster_view2;
                3
                4
                5

                > SELECT * FROM alter_mv_set_cluster_view3;
                4
                5
                6

                > SHOW MATERIALIZED VIEWS LIKE 'alter_mv_set_cluster_view%';
                alter_mv_set_cluster_view1 alter_mv_set_cluster_cluster2
                alter_mv_set_cluster_view2 alter_mv_set_cluster_cluster2
                alter_mv_set_cluster_view3 alter_mv_set_cluster_cluster2

                # Indexes remain where they were
                > SELECT mz_clusters.name
                  FROM mz_indexes
                  JOIN mz_clusters ON (cluster_id = mz_clusters.id)
                  WHERE mz_indexes.name like 'alter_mv_set_cluster_view%';
                default
                default
                default
                """
            )
        )
