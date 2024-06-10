# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

import requests

from materialize import buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.kafka import Kafka
from materialize.mzcompose.services.localstack import Localstack
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.toxiproxy import Toxiproxy
from materialize.mzcompose.services.zookeeper import Zookeeper

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Clusterd(),
    Materialized(),
    Redpanda(),
    Toxiproxy(),
    Testdrive(materialize_params={"cluster": "cluster"}, no_reset=True),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    for name in buildkite.shard_list(
        list(c.workflows.keys()), lambda workflow: workflow
    ):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_compute(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Verify that compute parts are read-only when Mz is restarted in read-only-mode"""
    c.down(destroy_volumes=True)
    c.up(
        "zookeeper",
        "kafka",
        "schema-registry",
        "localstack",
        "materialized",
        "clusterd",
    )
    c.up("testdrive", persistent=True)

    # Make sure cluster is owned by the system so it doesn't get dropped
    # between testdrive runs.
    c.sql(
        """
        ALTER SYSTEM SET enable_unorchestrated_cluster_replicas = true;
        DROP CLUSTER IF EXISTS cluster CASCADE;
        CREATE CLUSTER cluster REPLICAS (
            replica1 (
                STORAGECTL ADDRESSES ['clusterd:2100'],
                STORAGE ADDRESSES ['clusterd:2103'],
                COMPUTECTL ADDRESSES ['clusterd:2101'],
                COMPUTE ADDRESSES ['clusterd:2102'],
                WORKERS 2
            )
        );
        GRANT ALL ON CLUSTER cluster TO materialize;
        ALTER SYSTEM SET cluster = cluster;
    """,
        port=6877,
        user="mz_system",
    )

    c.testdrive(
        dedent(
            """
        > SET CLUSTER = cluster;
        > CREATE table t (a int, b int);
        > INSERT INTO t VALUES (1, 2);
        > CREATE INDEX t_idx ON t (a, b);
        > CREATE MATERIALIZED VIEW mv AS SELECT sum(a) FROM t;
        > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
        > SELECT * FROM mv;
        1
        > SELECT max(b) FROM t;
        2
        """
        )
    )

    c.kill("materialized")
    c.kill("clusterd")
    with c.override(Materialized(read_only_controllers=True)):
        c.up("materialized")
        c.up("clusterd")
        c.testdrive(
            dedent(
                """
            > SET CLUSTER = cluster;
            > SELECT 1
            1
            > INSERT INTO t VALUES (3, 4);
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            1
            > SELECT max(b) FROM t;
            4
            > SELECT mz_unsafe.mz_sleep(5)
            <null>
            > INSERT INTO t VALUES (5, 6);
            > SELECT * FROM mv;
            1
            > SELECT max(b) FROM t;
            6
            > DROP INDEX t_idx
            > CREATE INDEX t_idx ON t (a, b)
            > SELECT max(b) FROM t;
            6
            > CREATE MATERIALIZED VIEW mv2 AS SELECT sum(a) FROM t;

            $ set-regex match=(s\\d+|\\d{13}|[ ]{12}0|u\\d{1,3}|\\(\\d+-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\d\\)) replacement=<>

            > EXPLAIN TIMESTAMP FOR SELECT * FROM mv2;
            "                query timestamp: <> <>\\nlargest not in advance of upper: <> <>\\n                          upper:[<> <>]\\n                          since:[<> <>]\\n        can respond immediately: false\\n                       timeline: Some(EpochMilliseconds)\\n              session wall time: <> <>\\n\\nsource materialize.public.mv2 (<>, storage):\\n                  read frontier:[<> <>]\\n                 write frontier:[<> <>]\\n"
            """
            )
        )

        port = c.port("materialized", 6878)
        resp = requests.post(f"http://localhost:{port}/api/control/allow-writes")
        assert resp.status_code == 200, resp
        print(resp.text)

        c.testdrive(
            dedent(
                """
            > SET CLUSTER = cluster;
            > SET TRANSACTION_ISOLATION TO 'SERIALIZABLE';
            > SELECT * FROM mv;
            9
            > SELECT * FROM mv2;
            9
            > SELECT max(b) FROM t;
            6
            > INSERT INTO t VALUES (7, 8);
            > SELECT * FROM mv;
            16
            > SELECT * FROM mv2;
            16
            > SELECT max(b) FROM t;
            8
            """
            )
        )
