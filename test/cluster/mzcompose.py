# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from pathlib import Path
from textwrap import dedent

from pg8000.dbapi import ProgrammingError

from materialize import spawn
from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Computed,
    Kafka,
    Localstack,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Toxiproxy,
    Zookeeper,
)

PROXIES = {
    "computed_1:2100": "toxiproxy:3001",
    "computed_2:2100": "toxiproxy:3002",
    "computed_3:2100": "toxiproxy:3003",
    "computed_4:2100": "toxiproxy:3004",
}

SERVICES = [
    Zookeeper(),
    Kafka(),
    SchemaRegistry(),
    Localstack(),
    Computed(
        name="computed_1",
        options="--workers 2 --process 0 computed_1:2102 computed_2:2102",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_2",
        options="--workers 2 --process 1 computed_1:2102 computed_2:2102",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_3",
        options="--workers 2 --process 0 computed_3:2102 computed_4:2102 --linger --reconcile",
        ports=[2100, 2102],
    ),
    Computed(
        name="computed_4",
        options="--workers 2 --process 1 computed_3:2102 computed_4:2102 --linger --reconcile",
        ports=[2100, 2102],
    ),
    Materialized(),
    Toxiproxy(),
    Testdrive(
        volumes=[
            "mzdata:/mzdata",
            "tmp:/share/tmp",
            ".:/workdir/smoke",
            "../testdrive:/workdir/testdrive",
        ],
        materialized_params={"cluster": "cluster1"},
    ),
]


def toxiproxy_setup(c: Composition) -> None:
    for dest, listen in PROXIES.items():
        dest_host, _ = dest.split(":")
        listen_host, listen_port = listen.split(":")
        assert listen_host == "toxiproxy"
        x = dedent(
            f"""
        $ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
        {{
            "name": "{dest_host}",
            "listen": "0.0.0.0:{listen_port}",
            "upstream": "{dest}"
        }}"""
        )

        # Override to disable the cluster argument
        with c.override(
            Testdrive(
                validate_data_dir=False,
                no_reset=True,
            )
        ):
            c.run("testdrive", stdin=x)


def toxiproxy_set_enabled(c: Composition, name: str, enabled: bool) -> None:
    with c.override(
        Testdrive(
            validate_data_dir=False,
            no_reset=True,
        )
    ):
        enable_json = "true" if enabled else "false"
        td = dedent(
            f"""
            $ http-request method=POST url=http://toxiproxy:8474/proxies/{name} content-type=application/json
            {{
              "enabled": {enable_json}
            }}
            """
        )
        c.run("testdrive", stdin=td)


def workflow_default(c: Composition) -> None:
    test_cluster(c, "smoke/*.td")
    test_github_12251(c)


def workflow_nightly(c: Composition) -> None:
    """Run cluster testdrive"""
    c.start_and_wait_for_tcp(
        services=["zookeeper", "kafka", "schema-registry", "localstack"]
    )
    # Skip tests that use features that are not supported yet.
    files = spawn.capture(
        [
            "sh",
            "-c",
            "grep -rLE 'mz_catalog|mz_records_' testdrive/*.td",
        ],
        cwd=Path(__file__).parent.parent,
    ).split()
    test_cluster(c, *files)


def test_cluster(c: Composition, *glob: str) -> None:
    c.start_and_wait_for_tcp(services=["materialized", "toxiproxy"])
    c.up("testdrive", persistent=True)
    toxiproxy_setup(c)

    # Create a remote cluster and verify that tests pass.
    c.up("computed_1")
    c.up("computed_2")
    c.sql("DROP CLUSTER IF EXISTS cluster1 CASCADE;")
    c.sql(
        f"CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ('{PROXIES['computed_1:2100']}', '{PROXIES['computed_2:2100']}')));"
    )
    c.run("testdrive", *glob)

    # Add a replica to that remote cluster and verify that tests still pass.
    c.up("computed_3")
    c.up("computed_4")
    c.sql(
        f"CREATE CLUSTER REPLICA cluster1.replica2 REMOTE ('{PROXIES['computed_3:2100']}', '{PROXIES['computed_4:2100']}')"
    )
    c.run("testdrive", *glob)

    # Kill one of the nodes in the first replica of the compute cluster and
    # verify that tests still pass.
    c.kill("computed_1")
    c.run("testdrive", *glob)

    # Leave only replica 2 up and verify that tests still pass.
    c.sql("DROP CLUSTER REPLICA cluster1.replica1")
    c.run("testdrive", *glob)

    # Test re-connection to replica2
    toxiproxy_set_enabled(c, "computed_3", False)
    toxiproxy_set_enabled(c, "computed_3", True)
    c.run("testdrive", *glob)


# This tests that the client does not wait indefinitely on a resource that crashed
def test_github_12251(c: Composition) -> None:
    c.up("materialized")
    c.wait_for_materialized()
    c.up("computed_1")
    c.sql(
        """
        DROP CLUSTER IF EXISTS cluster1 CASCADE;
        CREATE CLUSTER cluster1 REPLICAS (replica1 (REMOTE ('computed_1:2100')));
        SET cluster = cluster1;
        """
    )
    start_time = time.process_time()
    try:
        c.sql(
            """
        SET statement_timeout = '1 s';
        CREATE TABLE IF NOT EXISTS log_table (f1 TEXT);
        CREATE TABLE IF NOT EXISTS panic_table (f1 TEXT);
        INSERT INTO panic_table VALUES ('panic!');
        -- Crash loop the cluster with the table's index
        INSERT INTO log_table SELECT mz_internal.mz_panic(f1) FROM panic_table;
        """
        )
    except ProgrammingError as e:
        # Ensure we received the correct error message
        assert "statement timeout" in e.args[0]["M"], e
        # Ensure the statemenet_timeout setting is ~honored
        assert (
            time.process_time() - start_time < 2
        ), "idle_in_transaction_session_timeout not respected"
    else:
        assert False, "unexpected success in test_github_12251"

    # Ensure we can select from tables after cancellation.
    c.sql("SELECT * FROM log_table;")
