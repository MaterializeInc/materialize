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


class Postgres(Service):
    def __init__(
        self,
        name: str = "postgres",
        mzbuild: str = "postgres",
        image: str | None = None,
        ports: list[str] = ["5432"],
        extra_command: list[str] = [],
        environment: list[str] = ["POSTGRESDB=postgres", "POSTGRES_PASSWORD=postgres"],
        volumes: list[str] = [],
        max_wal_senders: int = 100,
        max_replication_slots: int = 100,
    ) -> None:
        command: list[str] = [
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            f"max_wal_senders={max_wal_senders}",
            "-c",
            f"max_replication_slots={max_replication_slots}",
            "-c",
            "max_connections=5000",
        ] + extra_command
        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}

        config.update(
            {
                "command": command,
                "allow_host_ports": True,
                "ports": ports,
                "environment": environment,
                "healthcheck": {
                    "test": ["CMD", "pg_isready"],
                    "interval": "1s",
                    "start_period": "30s",
                },
                "volumes": volumes,
            }
        )
        super().__init__(name=name, config=config)
