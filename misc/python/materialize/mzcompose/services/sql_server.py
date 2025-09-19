# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize import MZ_ROOT
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service


class SqlServer(Service):
    DEFAULT_USER = "SA"
    DEFAULT_SA_PASSWORD = "RPSsql12345"

    def __init__(
        self,
        # The password must be at least 8 characters including uppercase,
        # lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        sa_password: str = DEFAULT_SA_PASSWORD,
        name: str = "sql-server",
        environment_extra: list[str] = [],
        volumes_extra: list[str] = [],
    ) -> None:
        super().__init__(
            name=name,
            config={
                "mzbuild": "mssql-server",
                "ports": [1433],
                "volumes": volumes_extra,
                "environment": [
                    "ACCEPT_EULA=Y",
                    "MSSQL_PID=Developer",
                    "MSSQL_AGENT_ENABLED=True",
                    f"SA_PASSWORD={sa_password}",
                    "MSSQL_MEMORY_LIMIT_MB=2500",
                    *environment_extra,
                ],
                "healthcheck": {
                    "test": f"/opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P '{sa_password}' -Q 'SELECT 1'",
                    "interval": "1s",
                    "start_period": "30s",
                },
                "deploy": {"resources": {"limits": {"memory": "3G"}}},
                "cap_add": ["SYS_PTRACE"],
            },
        )
        self.sa_password = sa_password


def setup_sql_server_testing(c: Composition) -> None:
    with open(MZ_ROOT / "test" / "sql-server-cdc" / "setup" / "setup.td") as f:
        c.testdrive(
            f.read(),
            args=[
                "--max-errors=1",
                f"--var=default-sql-server-user={SqlServer.DEFAULT_USER}",
                f"--var=default-sql-server-password={SqlServer.DEFAULT_SA_PASSWORD}",
            ],
        )
