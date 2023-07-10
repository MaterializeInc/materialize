# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import time

import requests

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Clusterd, Materialized

SERVICES = [
    Materialized(
        options=[
            "--opentelemetry-endpoint=whatever:7777",
        ]
    ),
    Clusterd(name="clusterd"),
]


def workflow_default(c: Composition) -> None:
    """Tests the dynamic tracing setup on environmentd"""
    c.workflow("with-otel")
    c.workflow("without-otel")
    c.workflow("clusterd")


def workflow_with_otel(c: Composition) -> None:
    c.up("materialized")
    port = c.port("materialized", 6878)

    # Start with fastpath
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "info"

    # update the stderr config
    c.sql(
        "ALTER SYSTEM SET log_filter = 'debug'",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "debug"

    # update the otel config
    c.sql(
        "ALTER SYSTEM SET opentelemetry_filter = 'trace'",
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

    # make sure we can go allll the way back
    c.sql(
        "ALTER SYSTEM SET log_filter = 'info'",
        user="mz_system",
        port=6877,
        print_statement=False,
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "info"


def workflow_without_otel(c: Composition) -> None:
    with c.override(Materialized()):
        c.up("materialized")
        port = c.port("materialized", 6878)

        # Start with fastpath
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "info"

        # update the stderr config
        c.sql(
            "ALTER SYSTEM SET log_filter = 'debug'",
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


def workflow_clusterd(c: Composition) -> None:
    c.up("materialized", "clusterd")
    port = c.port("clusterd", 6878)

    c.sql(
        "ALTER SYSTEM SET enable_unmanaged_cluster_replicas = true;",
        port=6877,
        user="mz_system",
    )

    c.sql(
        """
        CREATE CLUSTER c REPLICAS (r1 (
            STORAGECTL ADDRESSES ['clusterd:2100'],
            STORAGE ADDRESSES ['clusterd:2103'],
            COMPUTECTL ADDRESSES ['clusterd:2101'],
            COMPUTE ADDRESSES ['clusterd:2102'],
            WORKERS 1
        ))
    """
    )

    c.sql(
        "ALTER SYSTEM SET log_filter = 'debug'",
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
