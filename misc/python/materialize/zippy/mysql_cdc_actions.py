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
from materialize.mzcompose.services.mysql import MySql
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import Action, Capabilities, Capability, State
from materialize.zippy.mysql_capabilities import MySqlRunning, MySqlTableExists
from materialize.zippy.mysql_cdc_capabilities import MySqlCdcTableExists
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.replica_capabilities import source_capable_clusters
from materialize.zippy.storaged_capabilities import StoragedRunning


class CreateMySqlCdcTable(Action):
    """Creates a MySQL CDC source in Materialized."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {
            BalancerdIsRunning,
            MzIsRunning,
            StoragedRunning,
            MySqlRunning,
            MySqlTableExists,
        }

    def __init__(self, capabilities: Capabilities) -> None:
        mysql_table = random.choice(capabilities.get(MySqlTableExists))
        mysql_pg_cdc_name = f"mysql_{mysql_table.name}"
        this_mysql_cdc_table = MySqlCdcTableExists(name=mysql_pg_cdc_name)
        cluster_name = random.choice(source_capable_clusters(capabilities))

        existing_mysql_cdc_tables = [
            s
            for s in capabilities.get(MySqlCdcTableExists)
            if s.name == this_mysql_cdc_table.name
        ]

        if len(existing_mysql_cdc_tables) == 0:
            self.new_mysql_cdc_table = True

            self.mysql_cdc_table = this_mysql_cdc_table
            self.mysql_cdc_table.mysql_table = mysql_table
            self.cluster_name = cluster_name
        elif len(existing_mysql_cdc_tables) == 1:
            self.new_mysql_cdc_table = False

            self.mysql_cdc_table = existing_mysql_cdc_tables[0]
        else:
            raise RuntimeError("More than one CDC table exists")

        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        if self.new_mysql_cdc_table:
            assert self.mysql_cdc_table is not None
            assert self.mysql_cdc_table.mysql_table is not None
            name = self.mysql_cdc_table.name
            c.testdrive(
                dedent(
                    f"""
                    > CREATE SECRET {name}_password AS '{MySql.DEFAULT_ROOT_PASSWORD}'
                    > CREATE CONNECTION {name}_conn TO MYSQL (
                        HOST mysql,
                        USER root,
                        PASSWORD SECRET {name}_password
                      )

                    > CREATE SOURCE {name}_source
                      IN CLUSTER {self.cluster_name}
                      FROM MYSQL CONNECTION {name}_conn;

                    > CREATE TABLE {name} FROM SOURCE {name}_source (REFERENCE public.{self.mysql_cdc_table.mysql_table.name});
                    """
                ),
                mz_service=state.mz_service,
            )

    def provides(self) -> list[Capability]:
        return [self.mysql_cdc_table] if self.new_mysql_cdc_table else []
