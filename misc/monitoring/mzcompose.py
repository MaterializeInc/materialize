# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service

SERVICES = [
    Service(
        "prometheus",
        {
            # renovate: datasource=docker depName=prom/prometheus versioning=docker
            "image": "prom/prometheus:v2.46.0@sha256:d6ead9daf2355b9923479e24d7e93f246253ee6a5eb18a61b0f607219f341a80",
            "ports": ["9090:9090"],
            "volumes": [
                "./prometheus.yml:/etc/prometheus/prometheus.yml",
                "../../mzdata/prometheus:/mnt/services",
            ],
            "command": [
                "--config.file=/etc/prometheus/prometheus.yml",
                "--web.enable-remote-write-receiver",
            ],
            "extra_hosts": ["host.docker.internal:host-gateway"],
            "allow_host_ports": True,
        },
    ),
    Service(
        "tempo",
        {
            # renovate: datasource=docker depName=grafana/tempo versioning=docker
            "image": "grafana/tempo:2.2.0@sha256:0f77cc5f7a3941a903ddaebe8990c82f43b5021b65ec45cdfc7471defce481ca",
            "ports": ["4317:4317", "3200:3200"],
            "volumes": [
                "./tempo.yml:/etc/tempo.yml",
                "../../mzdata/tempo:/tmp/tempo",
            ],
            "command": ["-config.file=/etc/tempo.yml"],
            "allow_host_ports": True,
        },
    ),
    Service(
        "grafana",
        {
            # renovate: datasource=docker depName=grafana/grafana versioning=docker
            "image": "grafana/grafana:10.0.3@sha256:423040d62678074111e4e72d7dcef23480a94eb4f21b9173204d1a5ee972ec59",
            "ports": ["3000:3000"],
            "environment": [
                "GF_AUTH_ANONYMOUS_ENABLED=true",
                "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin",
            ],
            "volumes": [
                "./grafana/datasources:/etc/grafana/provisioning/datasources",
            ],
            "allow_host_ports": True,
        },
    ),
]


def workflow_default(c: Composition) -> None:
    # Create the `mzdata/prometheus|tempo` directories that will be bind mounted into
    # the containers before invoking Docker Compose, since otherwise the Docker daemon
    # will create the directory as root, and `environmentd` won't be able to write to them.
    (MZ_ROOT / "mzdata" / "prometheus").mkdir(parents=True, exist_ok=True)
    (MZ_ROOT / "mzdata" / "tempo").mkdir(parents=True, exist_ok=True)
    c.up()
    print(f"Prometheus running at http://localhost:{c.default_port('prometheus')}")
    print(f"Tempo running at http://localhost:{c.default_port('tempo')}")
    print(f"Grafana running at http://localhost:{c.default_port('grafana')}")
