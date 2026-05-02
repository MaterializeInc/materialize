# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from materialize import MZ_ROOT
from materialize.mzcompose import loader
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)
from materialize.mzcompose.services.minio import minio_blob_uri

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition


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
                    "test": ["CMD", "pg_isready", "-U", "postgres"],
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

    @staticmethod
    def backup(c: Composition) -> None:
        backup = c.exec(
            "postgres-metadata",
            "pg_dumpall",
            "--user",
            "postgres",
            capture=True,
        ).stdout
        with open("backup.sql", "w") as f:
            f.write(backup)

    @staticmethod
    def restore(
        c: Composition, mz_service: str = "materialized", restart_mz: bool = True
    ) -> None:
        c.kill(mz_service)
        c.kill("postgres-metadata")
        c.rm("postgres-metadata")
        c.up("postgres-metadata")
        with open("backup.sql") as f:
            backup = f.read()
        c.exec(
            "postgres-metadata",
            "psql",
            "--user",
            "postgres",
            "--file",
            "-",
            stdin=backup,
        )
        from materialize.mzcompose.composition import Service as ServiceName

        c.up(ServiceName("persistcli", idle=True))
        c.exec(
            "persistcli",
            "persistcli",
            "admin",
            "--commit",
            "restore-blob",
            f"--blob-uri={minio_blob_uri()}",
            "--consensus-uri=postgres://root@postgres-metadata:26257?options=--search_path=consensus",
        )
        if restart_mz:
            c.up(mz_service)
