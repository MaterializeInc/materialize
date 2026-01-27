# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os

from materialize import MZ_ROOT
from materialize.mzcompose import loader
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.foundationdb import FoundationDB


class Postgres(Service):
    def __init__(
        self,
        name: str = "postgres",
        mzbuild: str = "postgres",
        image: str | None = None,
        ports: list[str] = ["5432"],
        extra_command: list[str] = [],
        environment: list[str] = [
            "POSTGRESDB=postgres",
            "POSTGRES_PASSWORD=postgres",
            "LD_PRELOAD=libeatmydata.so",
        ],
        volumes: list[str] = [],
        max_wal_senders: int = 100,
        max_replication_slots: int = 100,
        setup_materialize: bool = False,
        restart: str = "no",
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

        if setup_materialize:
            path = os.path.relpath(
                MZ_ROOT / "misc" / "postgres" / "setup_materialize.sql",
                loader.composition_path,
            )
            volumes = volumes + [
                f"{path}:/docker-entrypoint-initdb.d/z_setup_materialize.sql"
            ]

            environment = environment + ["PGPORT=26257"]

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
                "restart": restart,
                "volumes": volumes,
            }
        )
        super().__init__(name=name, config=config)


class PostgresMetadata(Postgres):
    def __init__(self, restart: str = "no") -> None:
        super().__init__(
            name="postgres-metadata",
            setup_materialize=True,
            ports=["26257"],
            restart=restart,
        )


def determine_external_metadata_store() -> (
    type[Cockroach | FoundationDB | PostgresMetadata]
):
    external_metadata_store = os.getenv("EXTERNAL_METADATA_STORE")
    if external_metadata_store == "cockroach":
        return Cockroach
    elif external_metadata_store == "foundationdb":
        return FoundationDB
    elif external_metadata_store == "postgres-metadata":
        return PostgresMetadata
    elif external_metadata_store is not None:
        raise ValueError(f"Unknown EXTERNAL_METADATA_STORE: {external_metadata_store}")
    return Cockroach if os.getenv("BUILDKITE_TAG", "") != "" else PostgresMetadata


CockroachOrPostgresMetadata = determine_external_metadata_store()


def _determine_metadata_store_name() -> str:
    if CockroachOrPostgresMetadata == Cockroach:
        return "cockroach"
    elif CockroachOrPostgresMetadata == PostgresMetadata:
        return "postgres-metadata"
    elif CockroachOrPostgresMetadata == FoundationDB:
        return "foundationdb"
    else:
        raise ValueError("Unknown metadata store")


METADATA_STORE: str = _determine_metadata_store_name()

FORCE_EXTERNAL_METADATA_STORE: bool = (
    os.getenv("EXTERNAL_METADATA_STORE") is not None
    or os.getenv("BUILDKITE_TAG") is not None
)
