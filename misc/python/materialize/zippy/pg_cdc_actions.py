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
from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capabilities, Capability
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.pg_cdc_capabilities import PostgresCdcTableExists
from materialize.zippy.postgres_capabilities import PostgresRunning, PostgresTableExists


class CreatePostgresCdcTable(Action):
    """Creates a Postgres CDC source in Materialized."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, PostgresRunning, PostgresTableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        postgres_table = random.choice(capabilities.get(PostgresTableExists))
        postgres_pg_cdc_name = f"postgres_{postgres_table.name}"
        this_postgres_cdc_table = PostgresCdcTableExists(name=postgres_pg_cdc_name)

        existing_postgres_cdc_tables = [
            s
            for s in capabilities.get(PostgresCdcTableExists)
            if s.name == this_postgres_cdc_table.name
        ]

        if len(existing_postgres_cdc_tables) == 0:
            self.new_postgres_cdc_table = True

            self.postgres_cdc_table = this_postgres_cdc_table
            self.postgres_cdc_table.postgres_table = postgres_table
        elif len(existing_postgres_cdc_tables) == 1:
            self.new_postgres_cdc_table = False

            self.postgres_cdc_table = existing_postgres_cdc_tables[0]
        else:
            assert False

    def run(self, c: Composition) -> None:
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
                    > CREATE CONNECTION {name}_connection FOR POSTGRES
                      HOST postgres,
                      DATABASE postgres,
                      USER postgres,
                      PASSWORD SECRET {name}_password

                    > CREATE SOURCE {name}_source
                      FROM POSTGRES CONNECTION {name}_connection (PUBLICATION '{name}_publication')
                      FOR TABLES ({self.postgres_cdc_table.postgres_table.name} AS {name})
                    """
                )
            )

    def provides(self) -> List[Capability]:
        return [self.postgres_cdc_table] if self.new_postgres_cdc_table else []
