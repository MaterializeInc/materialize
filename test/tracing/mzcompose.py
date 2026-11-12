# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Tests the dynamic tracing setup on environmentd"""

import os
import time

import requests

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.clusterd import Clusterd, native_catalog_options
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.mz import Mz

SENTRY_DSN = os.getenv("BUILDKITE_SENTRY_DSN")

SERVICES = [
    Mz(app_password=""),
    Materialized(
        options=[
            "--opentelemetry-endpoint=whatever:7777",
            f"--sentry-dsn={SENTRY_DSN}",
            "--sentry-environment=development",
        ],
        support_external_clusterd=True,
    ),
    Clusterd(name="clusterd"),
]


def workflow_default(c: Composition) -> None:
    def process(name: str) -> None:
        if name == "default":
            return

        with c.test_case(name):
            c.workflow(name)

    c.test_parts(list(c.workflows.keys()), process)


def workflow_with_everything(c: Composition) -> None:
    c.up("materialized")
    port = c.port("materialized", 6878)

    # Start with fastpath
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "info"

    # update the stderr config
    c.sql(
        "ALTER SYSTEM SET log_filter = 'foo=debug,info'",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "debug"

    # update the otel config
    c.sql(
        "ALTER SYSTEM SET opentelemetry_filter = 'foo=trace,info'",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "trace"

    # revert the otel config and make sure we go back
    c.sql(
        "ALTER SYSTEM SET opentelemetry_filter = 'off'",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "debug"

    # update the sentry directives
    c.sql(
        "ALTER SYSTEM SET sentry_filters = 'foo=trace'",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "trace"

    # revert the sentry directives and make sure we go back
    c.sql(
        "ALTER SYSTEM RESET sentry_filters",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "debug"

    # make sure we can go allll the way back
    c.sql(
        "ALTER SYSTEM SET log_filter = 'info'",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "info"


def workflow_basic(c: Composition) -> None:
    with c.override(Materialized()):
        c.up("materialized")
        port = c.port("materialized", 6878)

        # Start with fastpath
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "info"

        # update the stderr config
        c.sql(
            "ALTER SYSTEM SET log_filter = 'foo=debug,info'",
            user="mz_system",
            port=6877,
            print_statement=False,
        )
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "debug"

        # make sure we can go back to normal
        c.sql(
            "ALTER SYSTEM SET log_filter = 'info'",
            user="mz_system",
            port=6877,
            print_statement=False,
        )
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "info"

        # Assert `EXPLAIN` doesn't break things in steady-state.
        c.sql(
            "EXPLAIN SELECT 1",
            print_statement=False,
        )
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "info"


def workflow_clusterd(c: Composition) -> None:
    c.up("materialized")

    c.sql(
        "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.sql("""
        CREATE CLUSTER c REPLICAS (r1 (
            STORAGECTL ADDRESSES ['clusterd:2100'],
            STORAGE ADDRESSES ['clusterd:2103'],
            COMPUTECTL ADDRESSES ['clusterd:2101'],
            COMPUTE ADDRESSES ['clusterd:2102'],
            WORKERS 1
        ))
    """)

    [(cluster_id, replica_id)] = c.sql_query("""
        SELECT c.id, r.id FROM mz_clusters c
        JOIN mz_cluster_replicas r ON r.cluster_id = c.id
        WHERE c.name = 'c' AND r.name = 'r1'
    """)
    with c.override(
        Clusterd(
            options=[
                f"--catalog-cluster-id={cluster_id}",
                f"--catalog-replica-id={replica_id}",
                *native_catalog_options(c),
            ]
        )
    ):
        c.up("clusterd")
        port = c.port("clusterd", 6878)

        # HTTP availability precedes native catalog bootstrap. Establish that
        # this replica can serve its logging indexes before timing a config change.
        with c.sql_cursor(
            startup_params={
                "cluster": "c",
                "cluster_replica": "r1",
                "statement_timeout": "60s",
            }
        ) as cursor:
            cursor.execute(
                "SELECT count(*) FROM mz_introspection.mz_dataflow_operators"
            )
            cursor.fetchall()

        c.sql(
            "ALTER SYSTEM SET log_filter = 'foo=debug,info'",
            user="mz_system",
            port=6877,
            print_statement=False,
        )

        start = time.time()
        timeout = 10
        is_debug = False

        # the updated configuration is sent to clusterd asynchronously,
        # spin here until the new tracing level is observed.
        while time.time() - start < timeout:
            info = requests.get(f"http://localhost:{port}/api/tracing").json()
            if info["current_level_filter"] == "debug":
                is_debug = True
                break

        assert is_debug

        # Reset
        c.sql(
            "ALTER SYSTEM SET log_filter = 'info'",
            user="mz_system",
            port=6877,
            print_statement=False,
        )
        port = c.port("materialized", 6878)
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "info"
