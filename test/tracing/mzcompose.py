# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO(guswynn): test that the setup forwards to compute and storage correctly

import requests

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Materialized

SERVICES = [
    Materialized(
        options=[
            "--opentelemetry-endpoint=whatever:7777",
            "--opentelemetry-enabled=false",
            "--opentelemetry-filter=trace",
        ]
    ),
]


def workflow_default(c: Composition) -> None:
    """Tests the dynamic tracing setup on environmentd"""

    c.workflow("with-otel")
    c.workflow("without-otel")


def workflow_with_otel(c: Composition) -> None:
    c.start_and_wait_for_tcp(services=["materialized"])
    port = c.port("materialized", 6878)

    # Start with fastpath
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "info"

    # update the stderr config
    requests.put(
        f"http://localhost:{port}/api/stderr/config",
        json={"targets": "debug"},
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "debug"

    # update the otel config
    requests.put(
        f"http://localhost:{port}/api/opentelemetry/config",
        json={"targets": "trace"},
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "trace"

    # revert the otel config and make sure we go back
    requests.put(
        f"http://localhost:{port}/api/opentelemetry/config",
        json={"targets": "off"},
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "debug"

    # make sure we can go allll the way back
    requests.put(
        f"http://localhost:{port}/api/stderr/config",
        json={"targets": "info"},
    )
    info = requests.get(f"http://localhost:{port}/api/tracing").json()
    assert info["current_level_filter"] == "info"


def workflow_without_otel(c: Composition) -> None:
    with c.override(Materialized()):
        c.start_and_wait_for_tcp(services=["materialized"])
        port = c.port("materialized", 6878)

        # Start with fastpath
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "info"

        # update the stderr config
        requests.put(
            f"http://localhost:{port}/api/stderr/config",
            json={"targets": "debug"},
        )
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "debug"

        # make sure we can go back to normal
        requests.put(
            f"http://localhost:{port}/api/stderr/config",
            json={"targets": "info"},
        )
        info = requests.get(f"http://localhost:{port}/api/tracing").json()
        assert info["current_level_filter"] == "info"
