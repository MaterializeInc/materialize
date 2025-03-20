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

YUGABYTE_VERSION = "2.23.0.0-b710"


class Yugabyte(Service):
    def __init__(
        self,
        name: str = "yugabyte",
        version: str = YUGABYTE_VERSION,
        image: str | None = None,
        ports: list[int] | None = None,
    ) -> None:
        if image is None:
            image = f"yugabytedb/yugabyte:{version}"

        if ports is None:
            ports = [5433]

        command_list = [
            "bin/yugabyted",
            "start",
            "--background=false",
            "--master_flags",
            ",".join(
                [
                    "ysql_yb_enable_replication_commands=true",
                    "ysql_cdc_active_replication_slot_window_ms=0",
                    "allowed_preview_flags_csv={ysql_yb_enable_replication_commands}",
                ]
            ),
            "--tserver_flags",
            "ysql_pg_conf=wal_level=logical",
        ]

        config: ServiceConfig = {
            "image": image,
            "ports": ports,
            "command": command_list,
            "healthcheck": {
                "test": [
                    "CMD",
                    "bash",
                    "-c",
                    '/home/yugabyte/bin/ysqlsh -h $(hostname) -p 5433 -U yugabyte -c "\\conninfo"',
                ],
                "interval": "1s",
                "start_period": "30s",
            },
        }

        super().__init__(name=name, config=config)
