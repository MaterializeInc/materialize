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


class SqlServer(Service):
    DEFAULT_USER = "SA"
    DEFAULT_SA_PASSWORD = "RPSsql12345"

    def __init__(
        self,
        # The password must be at least 8 characters including uppercase,
        # lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        sa_password: str = DEFAULT_SA_PASSWORD,
        name: str = "sql-server",
        image: str = "mcr.microsoft.com/mssql/server",
        environment_extra: list[str] = [],
        volumes_extra: list[str] = [],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                # WARNING: The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested
                # See See https://github.com/microsoft/mssql-docker/issues/802 for current status
                "platform": "linux/amd64",
                "ports": [1433],
                "volumes": volumes_extra,
                "environment": [
                    "ACCEPT_EULA=Y",
                    "MSSQL_PID=Developer",
                    "MSSQL_AGENT_ENABLED=True",
                    f"SA_PASSWORD={sa_password}",
                    *environment_extra,
                ],
            },
        )
        self.sa_password = sa_password
