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

mz_options = "--log-filter=dataflow::source=trace,persist::operators::stream=trace --persistent-user-tables --persistent-kafka-upsert-source --disable-persistent-system-tables-test"

mz_default = Materialized(timestamp_frequency="100ms", options=mz_options)

mz_logical_compaction_window_off = Materialized(
    options=f"{mz_options} --logical-compaction-window=off"
)

mz_disable_user_indexes = Materialized(
    options=f"{mz_options} --disable-user-indexes",
)

prerequisites = ["zookeeper", "kafka", "schema-registry"]

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    mz_default,
    Testdrive(no_reset=True),
]

td_test = os.environ.pop("TD_TEST", "*")


def workflow_persistence(c: Composition) -> None:
    for mz in [mz_default, mz_logical_compaction_window_off]:
        with c.override(mz):
            workflow_kafka_sources(c)
            workflow_user_tables(c)
            workflow_failpoints(c)

    workflow_disable_user_indexes(c)


def workflow_kafka_sources(c: Composition) -> None:
    seed = round(time.time())

    c.start_and_wait_for_tcp(services=prerequisites)

    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.run("testdrive-svc", f"--seed {seed} kafka-sources/*{td_test}*-before.td")

    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    # And restart again, for extra stress
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")

    c.run("testdrive-svc", f"--seed {seed} kafka-sources/*{td_test}*-after.td")

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
        f"--seed {seed} user-tables/table-persistence-before-{td_test}.td",
    )

    c.kill("materialized")
    c.up("materialized")

    c.run(
        "testdrive-svc",
        f"--seed {seed} user-tables/table-persistence-after-{td_test}.td",
    )

    c.kill("materialized")
    c.rm("materialized", "testdrive-svc", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_failpoints(c: Composition) -> None:
    seed = round(time.time())

    c.up("materialized")
    c.wait_for_materialized()

    c.run("testdrive-svc", f"--seed {seed} failpoints/{td_test}.td")

    c.kill("materialized")
    c.rm("materialized", "testdrive-svc", destroy_volumes=True)
    c.rm_volumes("mzdata")


def workflow_disable_user_indexes(c: Composition) -> None:
    seed = round(time.time())

    c.start_and_wait_for_tcp(services=prerequisites)

    c.up("materialized")
    c.wait_for_materialized()

    c.run("testdrive-svc", f"--seed {seed} disable-user-indexes/before.td")

    c.kill("materialized")

    with c.override(mz_disable_user_indexes):
        c.up("materialized")
        c.wait_for_materialized()

        c.run("testdrive-svc", f"--seed {seed} disable-user-indexes/after.td")

        c.kill("materialized")

    c.rm("materialized", "testdrive-svc", destroy_volumes=True)

    c.rm_volumes("mzdata")
