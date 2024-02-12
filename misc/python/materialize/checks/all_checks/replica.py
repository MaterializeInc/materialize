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
from materialize.checks.checks import TESTDRIVE_NOP, Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class CreateReplica(Check):
    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE create_replica_table (f1 INTEGER);
                > INSERT INTO create_replica_table VALUES (123);

                > CREATE CLUSTER create_replica REPLICAS ()

                > SET cluster=create_replica
                > CREATE DEFAULT INDEX ON create_replica_table;
                > CREATE MATERIALIZED VIEW create_replica_view AS SELECT SUM(f1) FROM create_replica_table;

                > CREATE CLUSTER REPLICA create_replica.replica1 SIZE '2-2'
                """,
                """
                > CREATE CLUSTER REPLICA create_replica.replica2 SIZE '2-2'
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=create_replica

                > SELECT * FROM create_replica_table;
                123
                > SELECT * FROM create_replica_view;
                123

                # Confirm that all replica_ids have been migrated to the uXXX/sXXX format
                > SELECT COUNT(*)
                  FROM mz_cluster_replicas
                  WHERE id NOT LIKE 's%'
                  AND id NOT LIKE 'u%';
                0

                # Confirm that there are CREATE events for all replicas with new-format IDs
                # resultset should not contain any NULLs.
                # System and unmanaged replicas have no audit log entries, so we need to exclude
                # those.
                > SELECT DISTINCT event_type
                  FROM mz_cluster_replicas
                  LEFT JOIN mz_audit_events ON (
                    mz_cluster_replicas.id = mz_audit_events.details->>'replica_id'
                    AND mz_audit_events.event_type = 'create'
                  )
                  WHERE
                    mz_cluster_replicas.id LIKE 'u%'
                    AND mz_cluster_replicas.size IS NOT NULL;
                create
                """
                + """
                # Confirm that there are DROP events for replicas with old-format IDs
                > SELECT COUNT(*) >= 2 FROM mz_audit_events
                  WHERE object_type = 'cluster-replica'
                  AND event_type = 'drop'
                  AND details->>'replica_id' NOT LIKE 's%'
                  AND details->>'replica_id' NOT LIKE 'u%';
                true
                """
                if self.base_version < MzVersion.parse_mz("v0.66.0-dev")
                else TESTDRIVE_NOP
            )
        )


class DropReplica(Check):
    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE drop_replica_table (f1 INTEGER);
                > INSERT INTO drop_replica_table VALUES (1);

                > CREATE CLUSTER drop_replica REPLICAS ();

                > SET cluster=drop_replica
                > CREATE DEFAULT INDEX ON drop_replica_table;
                > CREATE MATERIALIZED VIEW drop_replica_view AS SELECT COUNT(f1) FROM drop_replica_table;

                > INSERT INTO drop_replica_table VALUES (2);
                > CREATE CLUSTER REPLICA drop_replica.replica1 SIZE '2-2';
                > INSERT INTO drop_replica_table VALUES (3);
                > CREATE CLUSTER REPLICA drop_replica.replica2 SIZE '2-2';
                > INSERT INTO drop_replica_table VALUES (4);
                > DROP CLUSTER REPLICA drop_replica.replica1;
                """,
                """
                > INSERT INTO drop_replica_table VALUES (5);
                > DROP CLUSTER REPLICA drop_replica.replica2;
                > CREATE CLUSTER REPLICA drop_replica.replica1 SIZE '2-2';
                > INSERT INTO drop_replica_table VALUES (6);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SET cluster=drop_replica

                > SELECT * FROM drop_replica_table;
                1
                2
                3
                4
                5
                6

                > SELECT * FROM drop_replica_view;
                6
           """
            )
        )


class ReplicaAnnotations(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.71.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE CLUSTER replica_annotations REPLICAS ()
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(
                dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    CREATE CLUSTER REPLICA replica_annotations.internal_r1 SIZE '1', INTERNAL, BILLED AS 'free';
                    """
                )
            ),
            Testdrive(
                dedent(
                    """
                    $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
                    CREATE CLUSTER REPLICA replica_annotations.internal_r2 SIZE '1', INTERNAL;
                    """
                )
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT name
                  FROM mz_internal.mz_internal_cluster_replicas
                  JOIN mz_cluster_replicas USING (id);
                internal_r1
                internal_r2

                > SELECT details->>'replica_name', details->>'billed_as', details->>'internal'
                  FROM mz_audit_events
                  WHERE event_type = 'create'
                  AND object_type = 'cluster-replica'
                  AND details->'cluster_name' = '"replica_annotations"';
                internal_r1 free true
                internal_r2 <null> true

                > SET cluster=replica_annotations

                > CREATE MATERIALIZED VIEW replica_annotations_mv AS SELECT name FROM mz_tables LIMIT 1;
                > CREATE DEFAULT INDEX ON replica_annotations_mv;

                > SELECT COUNT(*) > 0 FROM replica_annotations_mv;
                true

                > ALTER CLUSTER replica_annotations SET (MANAGED, SIZE '2');

                > DROP MATERIALIZED VIEW replica_annotations_mv;

                > ALTER CLUSTER replica_annotations SET (MANAGED false);
                """
            )
        )
