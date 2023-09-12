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
    DEFAULT_SA_PASSWORD = "RPSsql12345"

    def __init__(
        self,
        # The password must be at least 8 characters including uppercase,
        # lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        sa_password: str = DEFAULT_SA_PASSWORD,
        name: str = "sql-server",
        image: str = "mcr.microsoft.com/mssql/server",
        environment_extra: list[str] = [],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
                "ports": [1433],
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
