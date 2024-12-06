# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class UnifiedCluster(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            """
            > CREATE CLUSTER shared_cluster_compute_first SIZE '1', REPLICATION FACTOR 1;
            > CREATE CLUSTER shared_cluster_storage_first SIZE '1', REPLICATION FACTOR 1;
            """
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            # Create either a source or a view as first object in cluster
            Testdrive(
                """
                > CREATE SOURCE shared_cluster_storage_first_source
                  IN CLUSTER shared_cluster_storage_first
                  FROM LOAD GENERATOR COUNTER

                > CREATE MATERIALIZED VIEW shared_cluster_compute_first_mv
                  IN CLUSTER shared_cluster_compute_first
                  AS SELECT COUNT(*) AS cnt FROM shared_cluster_storage_first_source

                > CREATE DEFAULT INDEX
                  IN CLUSTER shared_cluster_compute_first
                  ON shared_cluster_compute_first_mv
                """
            ),
            # Create the other type of object as a second object in the cluster that
            # now already contains an object
            Testdrive(
                """
                > CREATE SOURCE shared_cluster_compute_first_source
                  IN CLUSTER shared_cluster_compute_first
                  FROM LOAD GENERATOR COUNTER

                > CREATE MATERIALIZED VIEW shared_cluster_storage_first_mv
                  IN CLUSTER shared_cluster_storage_first
                  AS SELECT COUNT(*) AS cnt FROM shared_cluster_compute_first_source

                > CREATE DEFAULT INDEX
                  IN CLUSTER shared_cluster_storage_first
                  ON shared_cluster_storage_first_mv
                """
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            """
            > SELECT COUNT(*) > 0 FROM shared_cluster_storage_first_source;
            true

            > SELECT cnt > 0 FROM shared_cluster_storage_first_mv;
            true

            > SELECT COUNT(*) > 0 FROM shared_cluster_compute_first_source;
            true

            > SELECT cnt > 0 FROM shared_cluster_compute_first_mv;
            true

            > SET cluster = shared_cluster_compute_first;
            > SELECT COUNT(*) > 0 FROM mz_tables;
            true

            > SET cluster = shared_cluster_storage_first;
            > SELECT COUNT(*) > 0 FROM mz_tables;
            true

            > SET cluster = default

            ! DROP CLUSTER shared_cluster_compute_first;
            contains: cannot drop cluster "shared_cluster_compute_first" because other objects depend on it

            ! DROP CLUSTER shared_cluster_storage_first;
            contains: cannot drop cluster "shared_cluster_storage_first" because other objects depend on it

            ! ALTER CLUSTER shared_cluster_compute_first SET (REPLICATION FACTOR 2);
            contains: cannot create more than one replica of a cluster containing sources or sinks

            ! ALTER CLUSTER shared_cluster_storage_first SET (REPLICATION FACTOR 2);
            contains: cannot create more than one replica of a cluster containing sources or sinks

            """
        )
