# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import ROOT
from materialize.mzcompose import Composition
from materialize.mzcompose.services import Service

SERVICES = [
    Service(
        "prometheus",
        {
            "image": "prom/prometheus:v2.41.0",
            "ports": ["9090:9090"],
            "volumes": [
                "./prometheus.yml:/etc/prometheus/prometheus.yml",
                "../../mzdata/prometheus:/mnt/services",
            ],
            "extra_hosts": ["host.docker.internal:host-gateway"],
            "allow_host_ports": True,
        },
    ),
    Service(
        "grafana",
        {
            "image": "grafana/grafana:9.3.2",
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
    # Create the `mzdata/prometheus` directory that will be bind mounted into
    # the container before invoking Docker Compose, since otherwise the Docker
    # daemon will create the directory as root, and `environmentd` won't be
    # able to write to it.
    (ROOT / "mzdata" / "prometheus").mkdir(parents=True, exist_ok=True)
    c.up()
    print(f"Prometheus running at http://localhost:{c.default_port('prometheus')}")
    print(f"Grafana running at http://localhost:{c.default_port('grafana')}")
