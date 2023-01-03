# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
    Cockroach(),
]


def workflow_github_8021(c: Composition) -> None:
    c.up("materialized")
    c.wait_for_materialized("materialized")
    c.run("testdrive", "github-8021.td")

    # Ensure MZ can boot
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")
    c.kill("materialized")


def workflow_audit_log(c: Composition) -> None:
    c.up("materialized")
    c.wait_for_materialized(service="materialized")

    # Create some audit log entries.
    c.sql("CREATE TABLE t (i INT)")
    c.sql("CREATE DEFAULT INDEX ON t")

    log = c.sql_query("SELECT * FROM mz_audit_events ORDER BY id")

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()

    # Verify the audit log entries are still present and have not changed.
    restart_log = c.sql_query("SELECT * FROM mz_audit_events ORDER BY id")
    if log != restart_log or not log:
        print("initial audit log:", log)
        print("audit log after restart:", restart_log)
        raise Exception("audit logs emtpy or not equal after restart")


# Test for GitHub issue #13726
def workflow_timelines(c: Composition) -> None:
    for _ in range(3):
        c.start_and_wait_for_tcp(
            services=[
                "zookeeper",
                "kafka",
                "schema-registry",
                "materialized",
            ]
        )
        c.wait_for_materialized()
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
    c.rm_volumes("mzdata", "pgdata", force=True)

    materialized = Materialized(
        options=[
            "--adapter-stash-url=postgres://root@cockroach:26257?options=--search_path=adapter",
            "--storage-stash-url=postgres://root@cockroach:26257?options=--search_path=storage",
            "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus",
        ],
    )
    cockroach = Cockroach()

    with c.override(materialized, cockroach):
        c.up("cockroach")
        c.wait_for_cockroach()

        c.sql("CREATE SCHEMA adapter", service="cockroach", user="root")
        c.sql("CREATE SCHEMA storage", service="cockroach", user="root")
        c.sql("CREATE SCHEMA consensus", service="cockroach", user="root")

        c.start_and_wait_for_tcp(services=["materialized"])
        c.wait_for_materialized("materialized")

        c.sql("CREATE TABLE a (i INT)")

        c.stop("cockroach")
        c.up("cockroach")
        c.wait_for_cockroach()

        c.sql("CREATE TABLE b (i INT)")

        c.rm("cockroach")
        c.up("cockroach")
        c.wait_for_cockroach()

        # CockroachDB cleared its database, so this should fail.
        try:
            c.sql("CREATE TABLE c (i INT)")
            raise Exception("expected unreachable")
        except Exception as e:
            # Depending on timing, either of these errors can occur. The stash error comes
            # from the stash complaining. The network error comes from pg8000 complaining
            # because materialize panic'd.
            if "stash error: postgres: db error" not in str(
                e
            ) and "network error" not in str(e):
                raise e


def workflow_storage_managed_collections(c: Composition) -> None:
    c.up("materialized")
    c.wait_for_materialized(service="materialized")

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
    c.wait_for_materialized()

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


def workflow_default(c: Composition) -> None:
    c.workflow("github-8021")
    c.workflow("audit-log")
    c.workflow("timelines")
    c.workflow("stash")
