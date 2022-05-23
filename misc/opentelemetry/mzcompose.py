# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from typing import Dict, List, Optional

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Service

SERVICES = [
    Service(
        "jaeger-all-in-one",
        {
            "image": "jaegertracing/all-in-one:latest",
            "ports": [16686, 14268, 14250],
        },
    ),
    Service(
        "otel-collector",
        {
            "image": "otel/opentelemetry-collector:latest",
            "command": "--config=/etc/otel-collector-config.yaml",
            "ports": [
                1888,  # pprof
                13133,  # health_check
                4317,  # otlp grpc
                4318,  # otlp http
                55670,  # zpages
            ],
            "volumes": ["./otel-collector-config.yaml:/etc/otel-collector-config.yaml"],
            "depends_on": ["jaeger-all-in-one"],
        },
    ),
]
