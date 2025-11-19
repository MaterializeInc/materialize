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
)


def create_mysql_server_args(server_id: str, is_master: bool) -> list[str]:
    args = [
        "--log-bin=mysql-bin",
        "--gtid_mode=ON",
        "--enforce_gtid_consistency=ON",
        "--binlog-format=row",
        "--binlog-row-image=full",
        f"--server-id={server_id}",
    ]

    if not is_master:
        args.append("--log-slave-updates")
        args.append("--skip-replica-start")

    return args


class MySql(Service):
    DEFAULT_ROOT_PASSWORD = "p@ssw0rd"
    DEFAULT_VERSION = "9.5.0"

    DEFAULT_ADDITIONAL_ARGS = create_mysql_server_args(server_id="1", is_master=True)

    def __init__(
        self,
        root_password: str = DEFAULT_ROOT_PASSWORD,
        name: str = "mysql",
        version: str = DEFAULT_VERSION,
        port: int = 3306,
        volumes: list[str] = ["mydata:/var/lib/mysql-files"],
        additional_args: list[str] = DEFAULT_ADDITIONAL_ARGS,
    ) -> None:
        image = f"mysql:{version}"

        super().__init__(
            name=name,
            config={
                "image": image,
                "init": True,
                "ports": [port],
                "environment": [
                    f"MYSQL_ROOT_PASSWORD={root_password}",
                ],
                "command": [
                    "--secure-file-priv=/var/lib/mysql-files",
                    *additional_args,
                ],
                "healthcheck": {
                    "test": [
                        "CMD",
                        "mysqladmin",
                        "ping",
                        f"--password={root_password}",
                        "--protocol=TCP",
                    ],
                    "interval": "1s",
                    # MySQL can be slow to start up
                    "start_period": "180s",
                },
                "volumes": volumes,
            },
        )
