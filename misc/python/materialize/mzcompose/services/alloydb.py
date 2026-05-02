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
)
from materialize.mzcompose.services.minio import minio_blob_uri

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition


class AlloyDB(Service):
    def __init__(
        self,
        name: str = "alloydb",
        image: str = "google/alloydbomni:latest",
        restart: str = "no",
    ) -> None:
        path = os.path.relpath(
            MZ_ROOT / "misc" / "alloydb" / "setup_materialize.sql",
            loader.composition_path,
        )
        volumes = [f"{path}:/docker-entrypoint-initdb.d/setup_materialize.sql"]

        command = [
            "postgres",
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
            "-c",
            "wal_level=minimal",
            "-c",
            "max_wal_senders=0",
            "-c",
            "max_connections=5000",
        ]

        super().__init__(
            name=name,
            config={
                "image": image,
                "command": command,
                "ports": [26257],
                "environment": [
                    "POSTGRES_PASSWORD=postgres",
                    "POSTGRES_HOST_AUTH_METHOD=trust",
                    "PGPORT=26257",
                ],
                "volumes": volumes,
                "healthcheck": {
                    "test": [
                        "CMD-SHELL",
                        "psql -U root -d root -p 26257 -c 'SELECT 1' >/dev/null 2>&1",
                    ],
                    "interval": "1s",
                    "start_period": "30s",
                },
                "restart": restart,
            },
        )

    @staticmethod
    def backup(c: Composition) -> None:
        backup = c.exec(
            "alloydb",
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
        c.kill("alloydb")
        c.rm("alloydb")
        c.up("alloydb")
        with open("backup.sql") as f:
            backup = f.read()
        c.exec(
            "alloydb",
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
            "--consensus-uri=postgres://root@alloydb:26257?options=--search_path=consensus",
        )
        if restart_mz:
            c.up(mz_service)
