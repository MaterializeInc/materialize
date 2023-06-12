# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

import pg8000.exceptions

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Cockroach,
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

testdrive_no_reset = Testdrive(name="testdrive_no_reset", no_reset=True)

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
    testdrive_no_reset,
    Cockroach(setup_materialize=True),
]


def workflow_github_8021(c: Composition) -> None:
    c.up("materialized")
    c.run("testdrive", "github-8021.td")

    # Ensure MZ can boot
    c.kill("materialized")
    c.up("materialized")
    c.kill("materialized")


# Test that `mz_internal.mz_object_dependencies` re-populates.
def workflow_github_17578(c: Composition) -> None:
    c.up("testdrive_no_reset", persistent=True)
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > CREATE SOURCE with_subsources FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

            > SELECT
              top_level_s.name as source,
              s.name AS subsource
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id
              JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id
              WHERE top_level_s.name = 'with_subsources';
            source          subsource
            -------------------------
            with_subsources accounts
            with_subsources auctions
            with_subsources bids
            with_subsources organizations
            with_subsources users
            with_subsources with_subsources_progress
            """
        ),
    )

    # Restart mz
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT
              top_level_s.name as source,
              s.name AS subsource
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id
              JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id
              WHERE top_level_s.name = 'with_subsources';
            source          subsource
            -------------------------
            with_subsources accounts
            with_subsources auctions
            with_subsources bids
            with_subsources organizations
            with_subsources users
            with_subsources with_subsources_progress
            """
        ),
    )

    c.kill("materialized")


def workflow_audit_log(c: Composition) -> None:
    c.up("materialized")

    # Create some audit log entries.
    c.sql("CREATE TABLE t (i INT)")
    c.sql("CREATE DEFAULT INDEX ON t")

    log = c.sql_query("SELECT * FROM mz_audit_events ORDER BY id")

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify the audit log entries are still present and have not changed.
    restart_log = c.sql_query("SELECT * FROM mz_audit_events ORDER BY id")
    if log != restart_log or not log:
        print("initial audit log:", log)
        print("audit log after restart:", restart_log)
        raise Exception("audit logs emtpy or not equal after restart")


# Test for GitHub issue #13726
def workflow_timelines(c: Composition) -> None:
    for _ in range(3):
        c.up("zookeeper", "kafka", "schema-registry", "materialized")
        c.run("testdrive", "timelines.td")
        c.rm(
            "zookeeper",
            "kafka",
            "schema-registry",
            "materialized",
            destroy_volumes=True,
        )


def workflow_stash(c: Composition) -> None:
    c.rm(
        "testdrive",
        "materialized",
        stop=True,
        destroy_volumes=True,
    )
    c.rm_volumes("mzdata", force=True)

    with c.override(Materialized(external_cockroach=True)):
        c.up("cockroach")

        c.up("materialized")

        cursor = c.sql_cursor()
        cursor.execute("CREATE TABLE a (i INT)")

        c.stop("cockroach")
        c.up("cockroach")

        cursor.execute("CREATE TABLE b (i INT)")

        c.rm("cockroach")
        c.up("cockroach")

        # CockroachDB cleared its database, so this should fail.
        #
        # Depending on timing, this can fail in one of two ways. The stash error
        # comes from the stash complaining. The network error comes from pg8000
        # complaining because Materialize panicked.
        try:
            # Reusing the existing connection means we don't need to worry about
            # detecting `ConnectionRefused` errors
            cursor.execute("CREATE TABLE c (i INT)")
            raise Exception("expected unreachable")
        except pg8000.exceptions.InterfaceError as e:
            if str(e) != "network error":
                raise e


def workflow_storage_managed_collections(c: Composition) -> None:
    c.up("materialized")

    # Create some storage shard entries.
    c.sql("CREATE TABLE t (i INT)")

    # Storage collections are eventually consistent, so loop to be sure updates
    # have made it.

    user_shards = None
    while user_shards == None:
        user_shards = c.sql_query(
            "SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%';"
        )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify the shard mappings are still present and have not changed.
    restart_user_shards = None
    while restart_user_shards == None:
        restart_user_shards = c.sql_query(
            "SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%';"
        )

    if user_shards != restart_user_shards or not user_shards:
        print("initial user shards:", user_shards)
        print("user shards after restart:", restart_user_shards)
        raise Exception("user shards empty or not equal after restart")


def workflow_allowed_cluster_replica_sizes(c: Composition) -> None:
    c.up("testdrive_no_reset", persistent=True)
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # We can create a cluster with sizes '1' and '2'
            > CREATE CLUSTER test REPLICAS (r1 (SIZE '1'), r2 (SIZE '2'))

            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 1 true
            test r2 2 true

            # We cannot create replicas with size '2' after restricting allowed_cluster_replica_sizes to '1'
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET allowed_cluster_replica_sizes = '1'

            ! CREATE CLUSTER REPLICA test.r3 SIZE '2'
            contains:unknown cluster replica size 2
            """
        ),
    )

    # Assert that mz restarts successfully even in the presence of replica sizes that are not allowed
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # Cluster replica of disallowed sizes still exist
            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 1 true
            test r2 2 true

            # We cannot create replicas with size '2' (system parameter value persists across restarts)
            ! CREATE CLUSTER REPLICA test.r3 SIZE '2'
            contains:unknown cluster replica size 2

            # We can create replicas with size '2' after listing that size as allowed
            $ postgres-execute connection=mz_system
            ALTER SYSTEM SET allowed_cluster_replica_sizes = '1', '2'

            > CREATE CLUSTER REPLICA test.r3 SIZE '2'

            > SHOW CLUSTER REPLICAS WHERE cluster = 'test'
            test r1 1 true
            test r2 2 true
            test r3 2 true
            """
        ),
    )

    # Assert that the persisted allowed_cluster_replica_sizes (a setting that
    # supports multiple values) is correctly restored on restart.
    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SHOW allowed_cluster_replica_sizes
            "\\"1\\", \\"2\\""
            """
        ),
    )


def workflow_drop_materialize_database(c: Composition) -> None:
    c.up("materialized")

    # Drop materialize database
    c.sql(
        "DROP DATABASE materialize",
        port=6877,
        user="mz_system",
    )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify that materialize hasn't blown up
    c.sql("SELECT 1")


def workflow_default(c: Composition) -> None:
    c.workflow("github-17578")
    c.workflow("github-8021")
    c.workflow("audit-log")
    c.workflow("timelines")
    c.workflow("stash")
    c.workflow("allowed-cluster-replica-sizes")
    c.workflow("drop-materialize-database")
