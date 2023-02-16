# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import Cockroach, Materialized, Testdrive, Service

COCKROACH_HEALTHCHECK_DISABLED = {
    "test": "/bin/true",
    "interval": "1s",
    "start_period": "30s",
}


SERVICES = [
    Materialized(
        depends_on={
            "cockroach1": {"condition": "service_healthy"},
            "cockroach2": {"condition": "service_healthy"},
            "cockroach3": {"condition": "service_healthy"},
            "jaeger": {"condition": "service_healthy"},
        },
        options=[
            "--adapter-stash-url=postgres://root@cockroach:26257?options=--search_path=adapter&connect_timeout=2",
            "--storage-stash-url=postgres://root@cockroach:26257?options=--search_path=storage&connect_timeout=2",
            "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus&connect_timeout=2",
            "--opentelemetry-endpoint=http://jaeger:4317/"
        ],
    ),
    Service(
        "jaeger",
        {
            "image": "jaegertracing/all-in-one:1.36",
            "ports": ["16686:16686", "4317:4317", "4318:4318", 14268, 14250],
            "command": ["--collector.grpc-server.max-message-size=16777216"],
            "environment": ["COLLECTOR_OTLP_ENABLED=true"],
            "allow_host_ports": True,
        },
    ),
    Cockroach(
        setup_materialize=True,
        name="cockroach1",
        command=[
            "start",
            "--insecure",
            "--store=cockroach1",
            "--listen-addr=0.0.0.0:26257",
            "--advertise-addr=cockroach1:26257",
            "--http-addr=0.0.0.0:8080",
            "--join=cockroach1:26257,cockroach2:26257,cockroach3:26257",
        ],
        healthcheck=COCKROACH_HEALTHCHECK_DISABLED,
    ),
    Cockroach(
        name="cockroach2",
        command=[
            "start",
            "--insecure",
            "--store=cockroach2",
            "--listen-addr=0.0.0.0:26257",
            "--advertise-addr=cockroach2:26257",
            "--http-addr=0.0.0.0:8080",
            "--join=cockroach1:26257,cockroach2:26257,cockroach3:26257",
        ],
        healthcheck=COCKROACH_HEALTHCHECK_DISABLED,
    ),
    Cockroach(
        name="cockroach3",
        command=[
            "start",
            "--insecure",
            "--store=cockroach3",
            "--listen-addr=0.0.0.0:26257",
            "--advertise-addr=cockroach3:26257",
            "--http-addr=0.0.0.0:8080",
            "--join=cockroach1:26257,cockroach2:26257,cockroach3:26257",
        ],
        healthcheck=COCKROACH_HEALTHCHECK_DISABLED,
    ),
]


def workflow_default(c: Composition) -> None:
    c.up("cockroach1", "cockroach2", "cockroach3", "jaeger")

    c.exec("cockroach1", "cockroach", "init", "--insecure", "--host=localhost:26257")
    c.exec(
        "cockroach1",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "SET CLUSTER SETTING sql.stats.forecasts.enabled = false",
    )
    c.exec(
        "cockroach1",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "CREATE SCHEMA IF NOT EXISTS consensus",
    )
    c.exec(
        "cockroach1",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "CREATE SCHEMA IF NOT EXISTS storage",
    )
    c.exec(
        "cockroach1",
        "cockroach",
        "sql",
        "--insecure",
        "-e",
        "CREATE SCHEMA IF NOT EXISTS adapter",
    )

    c.up("materialized")
