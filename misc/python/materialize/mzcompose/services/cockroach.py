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
from materialize.mzcompose import (
    DEFAULT_CRDB_ENVIRONMENT,
    loader,
)
from materialize.mzcompose.service import (
    Service,
    ServiceHealthcheck,
)
from materialize.mzcompose.services.minio import minio_blob_uri

if TYPE_CHECKING:
    from materialize.mzcompose.composition import Composition


class Cockroach(Service):
    # TODO(def-): Bump when https://github.com/cockroachdb/cockroach/issues/158051 is fixed
    DEFAULT_COCKROACH_TAG = "v24.2.0"

    def __init__(
        self,
        name: str = "cockroach",
        aliases: list[str] = ["cockroach"],
        image: str | None = None,
        command: list[str] | None = None,
        setup_materialize: bool = True,
        in_memory: bool = False,
        healthcheck: ServiceHealthcheck | None = None,
        # Workaround for database-issues#5898, should be "no" otherwise
        restart: str = "on-failure:5",
        stop_grace_period: str = "120s",
    ):
        volumes = []

        if image is None:
            image = f"cockroachdb/cockroach:{Cockroach.DEFAULT_COCKROACH_TAG}"

        if command is None:
            command = ["start-single-node", "--insecure"]

        if setup_materialize:
            path = os.path.relpath(
                MZ_ROOT / "misc" / "cockroach" / "setup_materialize.sql",
                loader.composition_path,
            )
            volumes += [f"{path}:/docker-entrypoint-initdb.d/setup_materialize.sql"]

        if in_memory:
            command.append("--store=type=mem,size=2G")

        if healthcheck is None:
            healthcheck = {
                # init_success is a file created by the Cockroach container entrypoint
                "test": "[ -f init_success ] && curl --fail 'http://localhost:8080/health?ready=1'",
                "interval": "1s",
                "start_period": "30s",
            }

        super().__init__(
            name=name,
            config={
                "image": image,
                "networks": {"default": {"aliases": aliases}},
                "ports": [26257],
                "command": command,
                "volumes": volumes,
                "init": True,
                "healthcheck": healthcheck,
                "restart": restart,
                "environment": DEFAULT_CRDB_ENVIRONMENT,
                "stop_grace_period": stop_grace_period,
            },
        )

    @staticmethod
    def backup(c: Composition) -> None:
        from materialize.mzcompose.composition import Service as ServiceName

        c.up(ServiceName("mc", idle=True))
        c.exec("mc", "mc", "mb", "--ignore-existing", "persist/crdb-backup")
        c.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
                CREATE EXTERNAL CONNECTION backup_bucket
                AS 's3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
                BACKUP INTO 'external://backup_bucket';
                DROP EXTERNAL CONNECTION backup_bucket;
            """,
        )

    @staticmethod
    def restore(
        c: Composition, mz_service: str = "materialized", restart_mz: bool = True
    ) -> None:
        c.kill(mz_service)
        c.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
                DROP DATABASE defaultdb;
                CREATE EXTERNAL CONNECTION backup_bucket
                AS 's3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
                RESTORE DATABASE defaultdb
                FROM LATEST IN 'external://backup_bucket';
                DROP EXTERNAL CONNECTION backup_bucket;
            """,
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
            "--consensus-uri=postgres://root@cockroach:26257?options=--search_path=consensus",
        )
        if restart_mz:
            c.up(mz_service)
