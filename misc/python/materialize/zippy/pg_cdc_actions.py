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
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import Action, Capabilities, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.pg_cdc_capabilities import PostgresCdcTableExists
from materialize.zippy.postgres_capabilities import PostgresRunning, PostgresTableExists
from materialize.zippy.replica_capabilities import source_capable_clusters
from materialize.zippy.storaged_capabilities import StoragedRunning


class CreatePostgresCdcTable(Action):
    """Creates a Postgres CDC source in Materialized."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {
            BalancerdIsRunning,
            MzIsRunning,
            StoragedRunning,
            PostgresRunning,
            PostgresTableExists,
        }

    def __init__(self, capabilities: Capabilities) -> None:
        postgres_table = random.choice(capabilities.get(PostgresTableExists))
        postgres_pg_cdc_name = f"postgres_{postgres_table.name}"
        this_postgres_cdc_table = PostgresCdcTableExists(name=postgres_pg_cdc_name)
        cluster_name = random.choice(source_capable_clusters(capabilities))

        existing_postgres_cdc_tables = [
            s
            for s in capabilities.get(PostgresCdcTableExists)
            if s.name == this_postgres_cdc_table.name
        ]

        if len(existing_postgres_cdc_tables) == 0:
            self.new_postgres_cdc_table = True

            self.postgres_cdc_table = this_postgres_cdc_table
            self.postgres_cdc_table.postgres_table = postgres_table
            self.cluster_name = cluster_name
        elif len(existing_postgres_cdc_tables) == 1:
            self.new_postgres_cdc_table = False

            self.postgres_cdc_table = existing_postgres_cdc_tables[0]
        else:
            raise RuntimeError("More than one CDC table exists")

        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        if self.new_postgres_cdc_table:
            assert self.postgres_cdc_table is not None
            assert self.postgres_cdc_table.postgres_table is not None
            name = self.postgres_cdc_table.name
            c.testdrive(
                dedent(
                    f"""
                    $ postgres-execute connection=postgres://postgres:postgres@postgres

                    CREATE PUBLICATION {name}_publication FOR TABLE {self.postgres_cdc_table.postgres_table.name};


                    > CREATE SECRET {name}_password AS 'postgres';
                    > CREATE CONNECTION {name}_connection TO POSTGRES (
                        HOST postgres,
                        DATABASE postgres,
                        USER postgres,
                        PASSWORD SECRET {name}_password
                      );

                    > CREATE SOURCE {name}_source
                      IN CLUSTER {self.cluster_name}
                      FROM POSTGRES CONNECTION {name}_connection (PUBLICATION '{name}_publication');

                    > CREATE TABLE {name} FROM SOURCE {name}_source (REFERENCE {self.postgres_cdc_table.postgres_table.name});
                    """
                ),
                mz_service=state.mz_service,
            )

    def provides(self) -> list[Capability]:
        return [self.postgres_cdc_table] if self.new_postgres_cdc_table else []
