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
from materialize.mzcompose.services.sql_server import SqlServer
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import Action, Capabilities, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.replica_capabilities import source_capable_clusters
from materialize.zippy.sql_server_capabilities import (
    SqlServerRunning,
    SqlServerTableExists,
)
from materialize.zippy.sql_server_cdc_capabilities import SqlServerCdcTableExists
from materialize.zippy.storaged_capabilities import StoragedRunning


class CreateSqlServerCdcTable(Action):
    """Creates a SQL Server CDC source in Materialized."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {
            BalancerdIsRunning,
            MzIsRunning,
            StoragedRunning,
            SqlServerRunning,
            SqlServerTableExists,
        }

    def __init__(self, capabilities: Capabilities) -> None:
        sql_server_table = random.choice(capabilities.get(SqlServerTableExists))
        sql_server_pg_cdc_name = f"sql_server_{sql_server_table.name}"
        this_sql_server_cdc_table = SqlServerCdcTableExists(name=sql_server_pg_cdc_name)
        cluster_name = random.choice(source_capable_clusters(capabilities))

        existing_sql_server_cdc_tables = [
            s
            for s in capabilities.get(SqlServerCdcTableExists)
            if s.name == this_sql_server_cdc_table.name
        ]

        if len(existing_sql_server_cdc_tables) == 0:
            self.new_sql_server_cdc_table = True

            self.sql_server_cdc_table = this_sql_server_cdc_table
            self.sql_server_cdc_table.sql_server_table = sql_server_table
            self.cluster_name = cluster_name
        elif len(existing_sql_server_cdc_tables) == 1:
            self.new_sql_server_cdc_table = False

            self.sql_server_cdc_table = existing_sql_server_cdc_tables[0]
        else:
            raise RuntimeError("More than one CDC table exists")

        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        if self.new_sql_server_cdc_table:
            assert self.sql_server_cdc_table is not None
            assert self.sql_server_cdc_table.sql_server_table is not None
            name = self.sql_server_cdc_table.name
            c.testdrive(
                dedent(
                    f"""
                    > CREATE SECRET {name}_password AS '{SqlServer.DEFAULT_SA_PASSWORD}'
                    > CREATE CONNECTION {name}_conn TO SQL SERVER (
                        HOST 'sql-server',
                        DATABASE test,
                        USER {SqlServer.DEFAULT_USER},
                        PASSWORD SECRET {name}_password
                      )

                    > CREATE SOURCE {name}_source
                      IN CLUSTER {self.cluster_name}
                      FROM SQL SERVER CONNECTION {name}_conn;

                    > CREATE TABLE {name} FROM SOURCE {name}_source (REFERENCE {self.sql_server_cdc_table.sql_server_table.name});
                    """
                ),
                mz_service=state.mz_service,
            )

    def provides(self) -> list[Capability]:
        return [self.sql_server_cdc_table] if self.new_sql_server_cdc_table else []
