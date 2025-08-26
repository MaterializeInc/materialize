# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from textwrap import dedent

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.sql_server import (
    SqlServer,
    setup_sql_server_testing,
)
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import Action, Capabilities, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.sql_server_capabilities import (
    SqlServerRunning,
    SqlServerTableExists,
)


class SqlServerStart(Action):
    """Start a SQL Server instance."""

    def provides(self) -> list[Capability]:
        return [SqlServerRunning()]

    def run(self, c: Composition, state: State) -> None:
        c.up("sql-server")
        setup_sql_server_testing(c)

        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                $ sql-server-execute name=sql-server
                USE test;
                """
            ),
            mz_service=state.mz_service,
        )


class SqlServerStop(Action):
    """Stop the SQL Server instance."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {SqlServerRunning}

    def withholds(self) -> set[type[Capability]]:
        return {SqlServerRunning}

    def run(self, c: Composition, state: State) -> None:
        c.kill("sql-server")


class SqlServerRestart(Action):
    """Restart the SqlServer instance."""

    def run(self, c: Composition, state: State) -> None:
        c.kill("sql-server")
        c.up("sql-server")


class CreateSqlServerTable(Action):
    """Creates a table on the SqlServer instance. 50% of the tables have a PK."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, SqlServerRunning}

    def __init__(self, capabilities: Capabilities) -> None:
        this_sql_server_table = SqlServerTableExists(
            name="table" + str(random.randint(1, 10))
        )

        existing_sql_server_tables = [
            t
            for t in capabilities.get(SqlServerTableExists)
            if t.name == this_sql_server_table.name
        ]

        if len(existing_sql_server_tables) == 0:
            self.new_sql_server_table = True
            # A PK is now required for Debezium
            this_sql_server_table.has_pk = True

            self.sql_server_table = this_sql_server_table
        elif len(existing_sql_server_tables) == 1:
            self.new_sql_server_table = False
            self.sql_server_table = existing_sql_server_tables[0]
        else:
            raise RuntimeError("More than one table exists")

        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        if self.new_sql_server_table:
            primary_key = "PRIMARY KEY" if self.sql_server_table.has_pk else ""
            c.testdrive(
                dedent(
                    f"""
                    $ sql-server-connect name=sql-server
                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                    $ sql-server-execute name=sql-server
                    USE test;
                    CREATE TABLE {self.sql_server_table.name} (f1 INTEGER {primary_key});
                    EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = '{self.sql_server_table.name}', @role_name = 'SA', @supports_net_changes = 0;
                    INSERT INTO {self.sql_server_table.name} VALUES ({self.sql_server_table.watermarks.max});
                    """
                ),
                mz_service=state.mz_service,
            )

    def provides(self) -> list[Capability]:
        return [self.sql_server_table] if self.new_sql_server_table else []


class SqlServerDML(Action):
    """Performs an INSERT, DELETE or UPDATE against a SQL Server table."""

    # We use smaller batches in Pg then in Mz because Pg will fill up much faster
    MAX_BATCH_SIZE = 10000

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, SqlServerRunning, SqlServerTableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.sql_server_table = random.choice(capabilities.get(SqlServerTableExists))
        self.delta = random.randint(1, SqlServerDML.MAX_BATCH_SIZE)

        super().__init__(capabilities)

    def __str__(self) -> str:
        return f"{Action.__str__(self)} {self.sql_server_table.name}"


class SqlServerInsert(SqlServerDML):
    """Inserts rows into a SQL Server table."""

    def run(self, c: Composition, state: State) -> None:
        prev_max = self.sql_server_table.watermarks.max
        self.sql_server_table.watermarks.max = prev_max + self.delta
        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                $ sql-server-execute name=sql-server
                USE test;
                INSERT INTO {self.sql_server_table.name} (f1) SELECT n FROM (SELECT TOP ({self.sql_server_table.watermarks.max} - {prev_max}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {prev_max} AS n FROM sys.objects AS o1 CROSS JOIN sys.objects AS o2) AS numbers;
                """
            ),
            mz_service=state.mz_service,
        )


class SqlServerShiftForward(SqlServerDML):
    """Update all rows from a SQL Server table by incrementing their values by a constant (tables without a PK only)"""

    def run(self, c: Composition, state: State) -> None:
        if not self.sql_server_table.has_pk:
            self.sql_server_table.watermarks.shift(self.delta)
            c.testdrive(
                dedent(
                    f"""
                    $ sql-server-connect name=sql-server
                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                    $ sql-server-execute name=sql-server
                    USE test;
                    UPDATE {self.sql_server_table.name} SET f1 = f1 + {self.delta};
                    """
                ),
                mz_service=state.mz_service,
            )


class SqlServerShiftBackward(SqlServerDML):
    """Update all rows from a SQL Server table by decrementing their values by a constant (tables without a PK only)"""

    def run(self, c: Composition, state: State) -> None:
        if not self.sql_server_table.has_pk:
            self.sql_server_table.watermarks.shift(-self.delta)
            c.testdrive(
                dedent(
                    f"""
                    $ sql-server-connect name=sql-server
                    server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                    $ sql-server-execute name=sql-server
                    USE test;
                    UPDATE {self.sql_server_table.name} SET f1 = f1 - {self.delta};
                    """
                ),
                mz_service=state.mz_service,
            )


class SqlServerDeleteFromHead(SqlServerDML):
    """Delete the largest values from a SQL Server table"""

    def run(self, c: Composition, state: State) -> None:
        self.sql_server_table.watermarks.max = max(
            self.sql_server_table.watermarks.max - self.delta,
            self.sql_server_table.watermarks.min,
        )
        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                $ sql-server-execute name=sql-server
                USE test;
                DELETE FROM {self.sql_server_table.name} WHERE f1 > {self.sql_server_table.watermarks.max};
                """
            ),
            mz_service=state.mz_service,
        )


class SqlServerDeleteFromTail(SqlServerDML):
    """Delete the smallest values from a SQL Server table"""

    def run(self, c: Composition, state: State) -> None:
        self.sql_server_table.watermarks.min = min(
            self.sql_server_table.watermarks.min + self.delta,
            self.sql_server_table.watermarks.max,
        )
        c.testdrive(
            dedent(
                f"""
                $ sql-server-connect name=sql-server
                server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID={SqlServer.DEFAULT_USER};Password={SqlServer.DEFAULT_SA_PASSWORD}
                $ sql-server-execute name=sql-server
                USE test;
                DELETE FROM {self.sql_server_table.name} WHERE f1 < {self.sql_server_table.watermarks.min};
                """
            ),
            mz_service=state.mz_service,
        )
