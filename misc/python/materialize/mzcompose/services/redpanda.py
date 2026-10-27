# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)

REDPANDA_VERSION = "v26.1.8"


class Redpanda(Service):
    def __init__(
        self,
        name: str = "redpanda",
        version: str = REDPANDA_VERSION,
        auto_create_topics: bool = False,
        image: str | None = None,
        aliases: list[str] | None = None,
        ports: list[int] | None = None,
        extra_cluster_settings: dict[str, str] | None = None,
    ) -> None:
        if image is None:
            image = f"redpandadata/redpanda:{version}"

        if ports is None:
            ports = [9092, 8081]

        # The Redpanda container provides both a Kafka and a Schema Registry replacement
        if aliases is None:
            aliases = ["kafka", "schema-registry"]

        # Most of these options are simply required when using Redpanda in Docker.
        # See: https://docs.redpanda.com/current/get-started/quick-start/#Single-command-for-a-1-node-cluster
        # The `enable_transactions` and `enable_idempotence` feature flags enable
        # features Materialize requires that are present by default in Apache Kafka
        # but not in Redpanda.

        command_list = [
            "redpanda",
            "start",
            "--overprovisioned",
            "--smp=1",
            "--memory=1G",
            "--reserve-memory=0M",
            "--node-id=0",
            "--check=false",
            "--set",
            "redpanda.enable_transactions=true",
            "--set",
            "redpanda.enable_idempotence=true",
            "--set",
            f"redpanda.auto_create_topics_enabled={auto_create_topics}",
            # Only require 4KB per topic partition rather than 4MiB.
            "--set",
            "redpanda.topic_memory_per_partition=4096",
        ]
        # Callers can inject additional `redpanda.<key>=<value>` cluster
        # settings. We can't apply such settings broadly because the same
        # `Redpanda()` is instantiated across `kafka-matrix`, which exercises
        # historical Redpanda versions that don't recognize newer keys (a
        # bad key crashes the container at startup).
        for key, value in (extra_cluster_settings or {}).items():
            command_list += ["--set", f"redpanda.{key}={value}"]
        command_list += [
            "--set",
            f"--advertise-kafka-addr=kafka:{ports[0]}",
        ]

        config: ServiceConfig = {
            "image": image,
            "ports": ports,
            "command": command_list,
            "networks": {"default": {"aliases": aliases}},
            "healthcheck": {
                "test": ["CMD", "curl", "-f", "localhost:9644/v1/status/ready"],
                "interval": "1s",
                "start_period": "120s",
            },
        }

        super().__init__(name=name, config=config)
