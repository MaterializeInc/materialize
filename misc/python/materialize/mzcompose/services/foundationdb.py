# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize import MZ_ROOT
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)


class FoundationDB(Service):
    def __init__(
        self,
        name: str = "foundationdb",
        image: str | None = None,
        ports: list[str] = ["4500"],
        environment: list[str] = [
            "FDB_NETWORKING_MODE=container",
        ],
        volumes: list[str] = [],
        restart: str = "no",
        version: str = "7.3.71",
    ) -> None:
        # command: list[str] = [
        #     "postgres",
        #     "-c",
        #     "wal_level=logical",
        #     "-c",
        #     f"max_wal_senders={max_wal_senders}",
        #     "-c",
        #     f"max_replication_slots={max_replication_slots}",
        #     "-c",
        #     "max_connections=5000",
        # ] + extra_command

        # if setup_materialize:
        #     path = os.path.relpath(
        #         MZ_ROOT / "misc" / "postgres" / "setup_materialize.sql",
        #         loader.composition_path,
        #     )
        #     volumes = volumes + [
        #         f"{path}:/docker-entrypoint-initdb.d/z_setup_materialize.sql"
        #     ]
        #
        #     environment = environment + ["PGPORT=26257"]

        env_extra = [
            f"FDB_COORDINATOR_PORT={ports[0]}",
            f"FDB_PORT={ports[0]}",
        ]

        # command = dedent(
        #     """
        #     /usr/bin/tini -g -- /var/fdb/scripts/fdb.bash &
        #     sleep 5
        #     fdbcli -C /etc/foundationdb/fdb.cluster --exec "configure new single memory"
        #     fdbcli -C /etc/foundationdb/fdb.cluster --exec "status"
        #     wait
        # """
        # )

        if image is None:
            image = f"foundationdb/foundationdb:{version}"

        config: ServiceConfig = {"image": image}

        volumes += [f"{MZ_ROOT}/misc/foundationdb/:/etc/foundationdb/"]

        config.update(
            {
                "image": image,
                # "allow_host_ports": True,
                # "command": ["bash", "-c", command],
                "ports": ports,
                "environment": env_extra + environment,
                # "healthcheck": {
                #     "test": [
                #         "CMD",
                #         "fdbcli",
                #         "--exec",
                #         "configure single memory ; status",
                #     ],
                #     "interval": "1s",
                #     "start_period": "30s",
                # },
                "restart": restart,
                "volumes": volumes,
            }
        )
        super().__init__(name=name, config=config)


# class PostgresMetadata(Postgres):
#     def __init__(self, restart: str = "no") -> None:
#         super().__init__(
#             name="postgres-metadata",
#             setup_materialize=True,
#             ports=["26257"],
#             restart=restart,
#         )


# CockroachOrPostgresMetadata = (
#     Cockroach if os.getenv("BUILDKITE_TAG", "") != "" else PostgresMetadata
# )
#
# METADATA_STORE: str = (
#     "cockroach" if CockroachOrPostgresMetadata == Cockroach else "postgres-metadata"
# )
