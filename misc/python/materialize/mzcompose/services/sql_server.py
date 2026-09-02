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
from materialize.mzcompose.service import Service, ServiceConfig


class SqlServer(Service):
    DEFAULT_USER = "SA"
    DEFAULT_SA_PASSWORD = "RPSsql12345"
    # When `version=None`, we use the locally-built mzbuild image (which
    # bakes in a pre-initialized data directory for faster startup). Passing
    # an explicit `version` opts into the upstream image without seed data,
    # which is what the version matrix in CI uses.
    DEFAULT_VERSION: str | None = None

    def __init__(
        self,
        # The password must be at least 8 characters including uppercase,
        # lowercase letters, base-10 digits and/or non-alphanumeric symbols.
        sa_password: str = DEFAULT_SA_PASSWORD,
        name: str = "sql-server",
        version: str | None = DEFAULT_VERSION,
        environment_extra: list[str] = [],
        volumes_extra: list[str] = [],
        enable_agent: bool = True,
    ) -> None:
        config: ServiceConfig = {
            "ports": [1433],
            "volumes": volumes_extra,
            "environment": [
                "ACCEPT_EULA=Y",
                "MSSQL_PID=Developer",
                f"MSSQL_AGENT_ENABLED={enable_agent}",
                f"SA_PASSWORD={sa_password}",
                *environment_extra,
            ],
            "healthcheck": {
                # Verify all system databases are ONLINE before declaring
                # healthy. Without this, the container can be marked
                # healthy while msdb is still recovering, causing error
                # 904 ("cannot be autostarted during server shutdown or
                # startup").
                # NOTE: -b is what turns the RAISERROR into a nonzero exit
                # code; without it sqlcmd exits 0 and the check degrades to
                # a plain connectivity probe.
                "test": (
                    "SQLCMD=/opt/mssql-tools18/bin/sqlcmd; "
                    '[ -x "$$SQLCMD" ] || SQLCMD=/opt/mssql-tools/bin/sqlcmd; '
                    f"\"$$SQLCMD\" -b -C -S localhost -U sa -P '{sa_password}' "
                    "-Q \"SET NOCOUNT ON; IF EXISTS (SELECT 1 FROM sys.databases WHERE state_desc != 'ONLINE') RAISERROR('not ready', 16, 1)\""
                ),
                # Recovering can take a while
                "start_period": "300s",
            },
        }

        if version is not None:
            config["image"] = f"mcr.microsoft.com/mssql/server:{version}"
        elif sa_password == self.DEFAULT_SA_PASSWORD:
            config["mzbuild"] = "mssql-server"
        else:
            raise ValueError(
                "SqlServer with a non-default sa_password requires an "
                "explicit `version` (the seeded mzbuild image can only be "
                "used with the default password)."
            )

        super().__init__(name=name, config=config)
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
