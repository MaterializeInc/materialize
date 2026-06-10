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
            "image": "prom/prometheus:v2.46.0",
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
            "image": "grafana/tempo:2.2.0",
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
            "image": "grafana/grafana:10.0.3",
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
