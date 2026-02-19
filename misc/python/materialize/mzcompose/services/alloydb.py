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
)


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
