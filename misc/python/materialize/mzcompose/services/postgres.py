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


# Legacy switch to select between CockroachDB and Postgres for metadata storage.
CockroachOrPostgresMetadata = (
    Cockroach if os.getenv("BUILDKITE_TAG", "") != "" else PostgresMetadata
)


def metadata_store_name() -> str:
    """
    Determines the metadata store to use.

    Defaults to 'postgres-metadata' unless the BUILDKITE_TAG environment variable is set,
    in which case 'cockroach' is used, or the value of the EXTERNAL_METADATA_STORE environment variable if set.
    """
    if os.getenv("BUILDKITE_TAG", "") != "":
        return "cockroach"
    return os.getenv("EXTERNAL_METADATA_STORE", "postgres-metadata")


def metadata_store_service(
    metadata_store: str,
) -> type[Cockroach | FoundationDB | PostgresMetadata] | None:
    """
    Returns the service class corresponding to the specified metadata store, or None if an internal store is used.
    :param metadata_store: The name of the metadata store.
    """
    match metadata_store:
        case "cockroach":
            return Cockroach
        case "foundationdb":
            return FoundationDB
        case "postgres-metadata":
            return PostgresMetadata
        case "postgres-internal":
            return None
        case _:
            raise ValueError(f"Unknown METADATA_STORE: {metadata_store}")


def is_external_metadata_store(metadata_store: str) -> bool:
    """
    Determines if the specified metadata store is external, i.e., it runs in a different container than
    `environmentd`.
    :param metadata_store: The name of the metadata store.
    """
    match metadata_store:
        case "cockroach" | "foundationdb" | "postgres-metadata":
            return True
        case "postgres-internal":
            return False
        case _:
            raise ValueError(f"Unknown METADATA_STORE: {metadata_store}")


METADATA_STORE: str = metadata_store_name()
""" Global metadata store configuration. """

METADATA_STORE_SERVICE = metadata_store_service(METADATA_STORE)
""" Global metadata store service class. None if an internal store is used. """

# We'd like this to be `is_external_metadata_store(METADATA_STORE)`, but
# can't as `METADATA_STORE` is always set.
REQUIRES_EXTERNAL_METADATA_STORE: bool = is_external_metadata_store(
    os.getenv("EXTERNAL_METADATA_STORE", "postgres-internal")
)
""" Global flag indicating if an external metadata store is used. """


def metadata_store_service_list(
    *args, **kwargs
) -> list[Cockroach | FoundationDB | PostgresMetadata]:
    """
    Returns a list containing the metadata store service instance if an external metadata store is used,
    otherwise returns an empty list. Useful to construct a `SERVICES` list in a composition.
    :param args: Passed through to the metadata store service constructor.
    :param kwargs: Passed through to the metadata store service constructor.
    """
    return [METADATA_STORE_SERVICE(*args, **kwargs)] if METADATA_STORE_SERVICE else []
