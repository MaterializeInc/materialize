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


class BuiltinClusterReplicationFactor(Check):
    """A builtin cluster's replication factor owns its replica set across restarts.

    `mz_support` is the interesting cluster. It runs nothing by default, and it is
    the break-glass path a support engineer scales up by hand. A catalog open that
    does not read the cluster's replication factor tears that replica down, leaving
    the cluster reporting a factor it is not honoring.

    The replica arrives asynchronously: the cluster controller materializes it a
    tick after the ALTER commits. Every assertion below is a retrying testdrive
    query for that reason.
    """

    def _can_run(self, e: Executor) -> bool:
        # Only versions that reconcile the replica set against the cluster's own
        # replication factor keep this replica across a restart. Confine the check
        # to scenarios whose boots are all the current version: the upgrade
        # scenarios restart on released binaries, and `manipulate` and `validate`
        # both run on those.
        return self.base_version >= MzVersion.parse_cargo()

    def manipulate(self) -> list[Testdrive]:
        # `mz_support` owns its own cluster, so the ALTER has to run as that role.
        # Its sessions default to `mz_catalog_server`, which has a replica, so a
        # statement from this connection is safe to issue.
        #
        # A restart separates the two phases, so phase 2 asserting the replica is
        # still there is the regression assertion: the replica set has to be
        # rederived from the cluster's factor rather than from a fixed list.
        return [
            Testdrive(dedent(s))
            for s in [
                """
                $ postgres-execute connection=postgres://mz_support:materialize@${testdrive.materialize-internal-sql-addr}
                ALTER CLUSTER mz_support SET (REPLICATION FACTOR 1);
                """,
                """
                > SELECT c.replication_factor, count(r.id)
                  FROM mz_clusters c
                  LEFT JOIN mz_cluster_replicas r ON r.cluster_id = c.id
                  WHERE c.name = 'mz_support'
                  GROUP BY c.replication_factor
                1 1
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT c.replication_factor, count(r.id)
              FROM mz_clusters c
              LEFT JOIN mz_cluster_replicas r ON r.cluster_id = c.id
              WHERE c.name = 'mz_support'
              GROUP BY c.replication_factor
            1 1

            # Named by the managed-cluster rule, and owned by the cluster's owner
            # rather than by mz_system.
            > SELECT r.name, o.name
              FROM mz_cluster_replicas r
              JOIN mz_clusters c ON c.id = r.cluster_id
              JOIN mz_roles o ON o.id = r.owner_id
              WHERE c.name = 'mz_support'
            r1 mz_support
            """))
