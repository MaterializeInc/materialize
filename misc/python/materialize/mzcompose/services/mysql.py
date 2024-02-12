# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Iterable

from materialize.mzcompose.service import (
    Service,
)


class MySql(Service):
    DEFAULT_ROOT_PASSWORD = "p@ssw0rd"

    def __init__(
        self,
        root_password: str = DEFAULT_ROOT_PASSWORD,
        name: str = "mysql",
        image: str = "mysql:8.0.35",
        port: int = 3306,
        volumes: list[str] = ["mydata:/var/lib/mysql-files"],
        additional_args: Iterable[str] = tuple(),
    ) -> None:
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
                    "--default-authentication-plugin=mysql_native_password",
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
                    "start_period": "60s",
                },
                "volumes": volumes,
            },
        )
