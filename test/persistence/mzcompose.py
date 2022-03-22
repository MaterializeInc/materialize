# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import time

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

mz_options = "--persistent-user-tables --persistent-kafka-sources --disable-persistent-system-tables-test=true"

mz_default = Materialized(options=mz_options)

mz_logical_compaction_window_off = Materialized(
    # We need to use 1s and not 100ms here as otherwise validate_timestamp_bindings()
    # dominates the CPU see #10740
    timestamp_frequency="1s",
    options=f"{mz_options} --logical-compaction-window=off",
)

# TODO: add back mz_logical_compaction_window_off in the line below.
# See: https://github.com/MaterializeInc/materialize/issues/10488
mz_configurations = [mz_default]

prerequisites = ["zookeeper", "kafka", "schema-registry"]

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    mz_default,
    Testdrive(no_reset=True),
]

td_test = os.environ.pop("TD_TEST", "*")


def workflow_default(c: Composition) -> None:
    for mz in mz_configurations:
        with c.override(mz):
            workflow_kafka_sources(c)
            workflow_user_tables(c)

    workflow_disable_user_indexes(c)
    workflow_compaction(c)


def workflow_kafka_sources(c: Composition) -> None:
    seed = round(time.time())

    c.start_and_wait_for_tcp(services=prerequisites)

    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.run("testdrive-svc", f"--seed={seed}", f"kafka-sources/*{td_test}*-before.td")

    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    # And restart again, for extra stress
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.run("testdrive-svc", f"--seed={seed}", f"kafka-sources/*{td_test}*-after.td")

    # Do one more restart, just in case and just confirm that Mz is able to come up
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.kill("materialized")
    c.rm("materialized", "testdrive-svc", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_user_tables(c: Composition) -> None:
    seed = round(time.time())

    c.up("materialized")
    c.wait_for_materialized()

    c.run(
        "testdrive-svc",
        f"--seed={seed}",
        f"user-tables/table-persistence-before-{td_test}.td",
    )

    c.kill("materialized")
    c.up("materialized")

    c.run(
        "testdrive-svc",
        f"--seed={seed}",
        f"user-tables/table-persistence-after-{td_test}.td",
    )

    c.kill("materialized")
    c.rm("materialized", "testdrive-svc", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_failpoints(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=prerequisites)

    for mz in mz_configurations:
        with c.override(mz):
            for failpoint in [
                "fileblob_set_sync",
                "fileblob_delete_before",
                "fileblob_delete_after",
                "insert_timestamp_bindings_before",
                "insert_timestamp_bindings_after",
            ]:
                for action in ["return", "panic", "sleep(1000)"]:
                    run_one_failpoint(c, failpoint, action)


def run_one_failpoint(c: Composition, failpoint: str, action: str) -> None:
    print(f">>> Running failpoint test for failpoint {failpoint} with action {action}")

    seed = round(time.time())

    c.up("materialized")
    c.wait_for_materialized()

    c.run(
        "testdrive-svc",
        f"--seed={seed}",
        f"--var=failpoint={failpoint}",
        f"--var=action={action}",
        "failpoints/before.td",
    )

    time.sleep(2)
    # kill Mz if the failpoint has not killed it
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()

    c.run("testdrive-svc", f"--seed={seed}", "failpoints/after.td")

    c.kill("materialized")
    c.rm("materialized", "testdrive-svc", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_disable_user_indexes(c: Composition) -> None:
    seed = round(time.time())

    c.start_and_wait_for_tcp(services=prerequisites)

    c.up("materialized")
    c.wait_for_materialized()

    c.run("testdrive-svc", f"--seed={seed}", "disable-user-indexes/before.td")

    c.kill("materialized")

    with c.override(
        Materialized(
            options=f"{mz_options} --disable-user-indexes",
        )
    ):
        c.up("materialized")
        c.wait_for_materialized()

        c.run("testdrive-svc", f"--seed={seed}", "disable-user-indexes/after.td")

        c.kill("materialized")

    c.rm("materialized", "testdrive-svc", destroy_volumes=True)

    c.rm_volumes("mzdata")


def workflow_compaction(c: Composition) -> None:
    with c.override(
        Materialized(
            options=f"{mz_options} --metrics-scraping-interval=1s",
        )
    ):
        c.up("materialized")
        c.wait_for_materialized()

        c.run("testdrive-svc", "compaction/compaction.td")

        c.kill("materialized")

    c.rm("materialized", "testdrive-svc", destroy_volumes=True)

    c.rm_volumes("mzdata")
