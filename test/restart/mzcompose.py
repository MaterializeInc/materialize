# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import json
import time
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.ui import UIError

testdrive_no_reset = Testdrive(name="testdrive_no_reset", no_reset=True)

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Materialized(),
    Testdrive(
        entrypoint_extra=[
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
        ],
    ),
    testdrive_no_reset,
    Cockroach(setup_materialize=True),
]


def workflow_retain_history(c: Composition) -> None:
    def check_retain_history(name: str):
        start = time.time()
        while True:
            ts = c.sql_query(
                f"EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM retain_{name}"
            )
            ts = ts[0][0]
            ts = json.loads(ts)
            source = ts["sources"][0]
            since = source["read_frontier"][0]
            upper = source["write_frontier"][0]
            if upper - since > 2000:
                break
            end = time.time()
            # seconds since start
            elapsed = end - start
            if elapsed > 10:
                raise UIError("timeout hit while waiting for retain history")
            time.sleep(0.5)

    def check_retain_history_for(names: list[str]):
        for name in names:
            check_retain_history(name)

    c.up("materialized")
    c.sql(
        "ALTER SYSTEM SET enable_logical_compaction_window = true",
        port=6877,
        user="mz_system",
    )
    c.sql("CREATE TABLE retain_t (i INT)")
    c.sql("INSERT INTO retain_t VALUES (1)")
    c.sql(
        "CREATE MATERIALIZED VIEW retain_mv WITH (RETAIN HISTORY = FOR '2s') AS SELECT * FROM retain_t"
    )
    c.sql(
        "CREATE SOURCE retain_s FROM LOAD GENERATOR COUNTER WITH (RETAIN HISTORY = FOR '5s')"
    )
    names = ["mv", "s"]
    check_retain_history_for(names)

    # Ensure that RETAIN HISTORY is respected on boot.
    c.kill("materialized")
    c.up("materialized")
    check_retain_history_for(names)

    c.kill("materialized")


def workflow_github_8021(c: Composition) -> None:
    c.up("materialized")
    c.run_testdrive_files("github-8021.td")

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

            > SELECT DISTINCT
              top_level_s.name as source,
              s.name AS subsource
              FROM mz_internal.mz_object_dependencies AS d
              JOIN mz_sources AS s ON s.id = d.referenced_object_id OR s.id = d.object_id
              JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id OR top_level_s.id = d.referenced_object_id
              WHERE top_level_s.name = 'with_subsources' AND (s.type = 'progress' OR s.type = 'subsource');
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
              JOIN mz_sources AS s ON s.id = d.referenced_object_id OR s.id = d.object_id
              JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id OR top_level_s.id = d.referenced_object_id
              WHERE top_level_s.name = 'with_subsources' AND (s.type = 'progress' OR s.type = 'subsource');
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
        c.run_testdrive_files("timelines.td")
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

        # No implicit restart as sanity check here, will panic:
        # https://github.com/MaterializeInc/materialize/issues/20510
        c.down(sanity_restart_mz=False)


def workflow_storage_managed_collections(c: Composition) -> None:
    c.up("materialized")

    # Create some storage shard entries.
    c.sql("CREATE TABLE t (i INT)")

    # Storage collections are eventually consistent, so loop to be sure updates
    # have made it.

    user_shards: list[str] = []
    while len(user_shards) == 0:
        user_shards = c.sql_query(
            "SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id LIKE 'u%';"
        )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify the shard mappings are still present and have not changed.
    restart_user_shards: list[str] = []
    while len(restart_user_shards) == 0:
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

            $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

            # Reset for following tests
            $ postgres-execute connection=mz_system
            ALTER SYSTEM RESET allowed_cluster_replica_sizes
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

    # Restore for next tests
    c.sql(
        "CREATE DATABASE materialize",
        port=6877,
        user="mz_system",
    )
    c.sql(
        "GRANT ALL PRIVILEGES ON SCHEMA materialize.public TO materialize",
        port=6877,
        user="mz_system",
    )


def workflow_bound_size_mz_status_history(c: Composition) -> None:
    c.up("zookeeper", "kafka", "schema-registry", "materialized")
    c.up("testdrive_no_reset", persistent=True)

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            $ kafka-create-topic topic=status-history

            > CREATE CONNECTION kafka_conn
              TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

            > CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
                URL '${testdrive.schema-registry-url}'
              );

            > CREATE SOURCE kafka_source
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-status-history-${testdrive.seed}')
              FORMAT TEXT

            > CREATE SINK kafka_sink
              FROM kafka_source
              INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-kafka-sink-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM

            $ kafka-verify-topic sink=materialize.public.kafka_sink
            """
        ),
    )

    # Fill mz_source_status_history and mz_sink_status_history up with enough events
    for i in range(5):
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent(
                """
                > ALTER CONNECTION kafka_conn SET (BROKER 'dne') WITH (VALIDATE = false);
                > ALTER CONNECTION kafka_conn SET (BROKER '${testdrive.kafka-addr}') WITH (VALIDATE = true);
                """
            ),
        )

    # Verify that we have enough events so that they can be truncated
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT COUNT(*) > 7 FROM mz_internal.mz_source_status_history
            true

            > SELECT COUNT(*) > 7 FROM mz_internal.mz_sink_status_history
            true
            """
        ),
    )

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")

    # Verify that we have fewer events now
    # 7 because the truncation default is 5, and the restarted
    # objects produce a new starting and running event.
    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > SELECT COUNT(*) FROM mz_internal.mz_source_status_history
            7

            > SELECT COUNT(*) FROM mz_internal.mz_sink_status_history
            7
            """
        ),
    )


def workflow_index_compute_dependencies(c: Composition) -> None:
    """
    Assert that materialized views and index catalog items see and use only
    indexes created before them upon restart.

    Various parts of the optimizer internals and tooling, such as

    - `EXPLAIN REPLAN`
    - `bin/mzcompose clone defs`

    are currently depending on the fact that the `GlobalId` ordering respects
    dependency ordering. In other words, if an index `i` is created after a
    catalog item `x`, then `x` cannot use `i` even after restart.

    This test should codify this assumption so we can get an early signal if
    this is broken for some reason in the future.
    """
    c.up("testdrive_no_reset", persistent=True)
    c.up("materialized")

    def depends_on(c: Composition, obj_name: str, dep_name: str, expected: bool):
        """Check whether `(obj_name, dep_name)` is a compute dependency or not."""
        c.testdrive(
            service="testdrive_no_reset",
            input=dedent(
                f"""
                > (
                    SELECT
                      true
                    FROM
                      mz_catalog.mz_objects as obj
                    WHERE
                      obj.name = '{obj_name}' AND
                      obj.id IN (
                        SELECT
                          cd.object_id
                        FROM
                          mz_internal.mz_compute_dependencies cd JOIN
                          mz_objects dep ON (cd.dependency_id = dep.id)
                        WHERE
                          dep.name = '{dep_name}'
                      )
                  ) UNION (
                    SELECT
                      false
                    FROM
                      mz_catalog.mz_objects as obj
                    WHERE
                      obj.name = '{obj_name}' AND
                      obj.id NOT IN (
                        SELECT
                          cd.object_id
                        FROM
                          mz_internal.mz_compute_dependencies cd JOIN
                          mz_objects dep ON (cd.dependency_id = dep.id)
                        WHERE
                          dep.name = '{dep_name}'
                      )
                  );
                {str(expected).lower()}
                """
            ),
        )

    c.testdrive(
        service="testdrive_no_reset",
        input=dedent(
            """
            > DROP TABLE IF EXISTS t1 CASCADE;
            > DROP TABLE IF EXISTS t2 CASCADE;

            > CREATE TABLE t1(x int, y int);
            > CREATE TABLE t2(y int, z int);

            > CREATE INDEX ON t1(y);

            > CREATE VIEW v1 AS SELECT * FROM t1 JOIN t2 USING (y);
            > CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM v1;
            > CREATE INDEX ix1 ON v1(x);

            > CREATE INDEX ON t2(y);

            > CREATE VIEW v2 AS SELECT * FROM t2 JOIN t1 USING (y);
            > CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM v2;
            > CREATE INDEX ix2 ON v2(x);
            """
        ),
    )

    # Verify that mv1 and ix1 depend on t1_y_idx but not on t2_y_idx.
    depends_on(c, "mv1", "t1_y_idx", True)
    depends_on(c, "mv1", "t2_y_idx", False)
    depends_on(c, "ix1", "t1_y_idx", True)
    depends_on(c, "ix1", "t2_y_idx", False)
    # Verify that mv2 and ix2 depend on both t1_y_idx and t2_y_idx.
    depends_on(c, "mv2", "t1_y_idx", True)
    depends_on(c, "mv2", "t2_y_idx", True)
    depends_on(c, "ix2", "t1_y_idx", True)
    depends_on(c, "ix2", "t2_y_idx", True)

    # Restart mz. We expect the index on t2(y) to not be visible to ix1 and mv1
    # after the restart as well.
    c.kill("materialized")
    c.up("materialized")

    # Verify that mv1 and ix1 depend on t1_y_idx but not on t2_y_idx.
    depends_on(c, "mv1", "t1_y_idx", True)
    depends_on(c, "mv1", "t2_y_idx", False)
    depends_on(c, "ix1", "t1_y_idx", True)
    depends_on(c, "ix1", "t2_y_idx", False)
    # Verify that mv2 and ix2 depend on both t1_y_idx and t2_y_idx.
    depends_on(c, "mv2", "t1_y_idx", True)
    depends_on(c, "mv2", "t2_y_idx", True)
    depends_on(c, "ix2", "t1_y_idx", True)
    depends_on(c, "ix2", "t2_y_idx", True)


def workflow_default(c: Composition) -> None:
    for name in c.workflows:
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)
