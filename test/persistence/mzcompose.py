# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

DEFAULT_MZ_OPTIONS = "--persistent-user-tables --persistent-kafka-upsert-source --disable-persistent-system-tables-test"

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Materialized(options=DEFAULT_MZ_OPTIONS),
    Testdrive(
        no_reset=True,
        consistent_seed=True,
        depends_on=["kafka", "schema-registry", "materialized"],
    ),
]


def workflow_all(c: Composition) -> None:
    c.workflow("kafka-sources")
    c.workflow("user-tables")
    c.workflow("failpoints")
    c.workflow("disable-user-indexes")


def workflow_kafka_sources(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="?",
        default="*",
        help="run only files matching the specified filter",
    )
    args = parser.parse_args()

    reset(c)
    c.run("testdrive-svc", f"kafka-sources/*{args.filter}*-before.td")

    c.kill("materialized")
    c.up("materialized")

    # And restart again, for extra stress
    c.kill("materialized")
    c.up("materialized")

    c.run("testdrive-svc", f"kafka-sources/*{args.filter}*-after.td")

    # Do one more restart, just in case and just confirm that Mz is able to come up
    c.kill("materialized")
    c.up("materialized")


def workflow_user_tables(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="?",
        default="*",
        help="run only files matching the specified filter",
    )
    args = parser.parse_args()

    reset(c)
    c.run("testdrive-svc", f"user-tables/table-persistence-before-{args.filter}.td")
    c.kill("materialized")


def workflow_failpoints(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "filter",
        nargs="?",
        default="*",
        help="run only files matching the specified filter",
    )
    args = parser.parse_args()

    reset(c)
    # By using --disable-persistent-system-tables-test
    # we ensure that only testdrive-initiated actions cause I/O. The --workers 1 is used due to #8739
    materialized = Materialized(
        options="--persistent-user-tables --disable-persistent-system-tables-test --workers 1"
    )
    with c.override(materialized):
        c.run("testdrive-svc", f"failpoints/{args.filter}.td")


def workflow_disable_user_indexes(c: Composition) -> None:
    reset(c)
    c.run("testdrive-svc", "disable-user-indexes/before.td")
    c.kill("materialized")
    with c.override(
        Materialized(options=DEFAULT_MZ_OPTIONS + " --disable-user-indexes")
    ):
        c.run("testdrive-svc", "disable-user-indexes/after.td")


def reset(c: Composition) -> None:
    c.rm("materialized", "testdrive-svc")
    c.rm_volumes("mzdata", "tmp")
