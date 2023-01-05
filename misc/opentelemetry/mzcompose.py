# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.services import Service

SERVICES = [
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
]
