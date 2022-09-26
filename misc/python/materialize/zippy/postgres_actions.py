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
from materialize.zippy.postgres_capabilities import PostgresRunning, PostgresTableExists


class PostgresStart(Action):
    """Start a PostgresInstance instance."""

    def provides(self) -> List[Capability]:
        return [PostgresRunning()]

    def run(self, c: Composition) -> None:
        c.start_and_wait_for_tcp(services=["postgres"])


class PostgresStop(Action):
    """Stop the Postgres instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {PostgresRunning}

    def withholds(self) -> Set[Type[Capability]]:
        return {PostgresRunning}

    def run(self, c: Composition) -> None:
        c.kill("postgres")


class CreatePostgresTable(Action):
    """Creates a table on the Postgres instance. 50% of the tables have a PK."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, PostgresRunning}

    def __init__(self, capabilities: Capabilities) -> None:
        this_postgres_table = PostgresTableExists(
            name="table" + str(random.randint(1, 10))
        )

        existing_postgres_tables = [
            t
            for t in capabilities.get(PostgresTableExists)
            if t.name == this_postgres_table.name
        ]

        if len(existing_postgres_tables) == 0:
            self.new_postgres_table = True
            this_postgres_table.has_pk = random.choice([True, False])

            self.postgres_table = this_postgres_table
        elif len(existing_postgres_tables) == 1:
            self.new_postgres_table = False
            self.postgres_table = existing_postgres_tables[0]
        else:
            assert False

    def run(self, c: Composition) -> None:
        if self.new_postgres_table:
            primary_key = f"PRIMARY KEY" if self.postgres_table.has_pk else ""
            c.testdrive(
                dedent(
                    f"""
                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    CREATE TABLE {self.postgres_table.name} (f1 INTEGER {primary_key});
                    ALTER TABLE {self.postgres_table.name} REPLICA IDENTITY FULL;
                    INSERT INTO {self.postgres_table.name} VALUES ({self.postgres_table.watermarks.max});
                    """
                )
            )

    def provides(self) -> List[Capability]:
        return [self.postgres_table] if self.new_postgres_table else []


class PostgresDML(Action):
    """Performs an INSERT, DELETE or UPDATE against a Postgres table."""

    # We use smaller batches in Pg then in Mz because Pg will fill up much faster
    MAX_BATCH_SIZE = 10000

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, PostgresRunning, PostgresTableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.postgres_table = random.choice(capabilities.get(PostgresTableExists))
        self.delta = random.randint(1, PostgresDML.MAX_BATCH_SIZE)


class PostgresInsert(PostgresDML):
    """Inserts rows into a Postgres table."""

    def run(self, c: Composition) -> None:
        prev_max = self.postgres_table.watermarks.max
        self.postgres_table.watermarks.max = prev_max + self.delta
        c.testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                INSERT INTO {self.postgres_table.name} SELECT * FROM generate_series({prev_max + 1}, {self.postgres_table.watermarks.max});
                """
            )
        )


class PostgresShiftForward(PostgresDML):
    """Update all rows from a Postgres table by incrementing their values by a constant (tables with a PK only)"""

    def run(self, c: Composition) -> None:
        if not self.postgres_table.has_pk:
            self.postgres_table.watermarks.shift(self.delta)
            c.testdrive(
                dedent(
                    f"""
                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    UPDATE {self.postgres_table.name} SET f1 = f1 + {self.delta};
                    """
                )
            )


class PostgresShiftBackward(PostgresDML):
    """Update all rows from a Postgres table by decrementing their values by a constant (tables with a PK only)"""

    def run(self, c: Composition) -> None:
        if not self.postgres_table.has_pk:
            self.postgres_table.watermarks.shift(-self.delta)
            c.testdrive(
                dedent(
                    f"""
                    $ postgres-execute connection=postgres://postgres:postgres@postgres
                    UPDATE {self.postgres_table.name} SET f1 = f1 - {self.delta};
                    """
                )
            )


class PostgresDeleteFromHead(PostgresDML):
    """Delete the largest values from a Postgres table"""

    def run(self, c: Composition) -> None:
        self.postgres_table.watermarks.max = max(
            self.postgres_table.watermarks.max - self.delta,
            self.postgres_table.watermarks.min,
        )
        c.testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DELETE FROM {self.postgres_table.name} WHERE f1 > {self.postgres_table.watermarks.max};
                """
            )
        )


class PostgresDeleteFromTail(PostgresDML):
    """Delete the smallest values from a Postgres table"""

    def run(self, c: Composition) -> None:
        self.postgres_table.watermarks.min = min(
            self.postgres_table.watermarks.min + self.delta,
            self.postgres_table.watermarks.max,
        )
        c.testdrive(
            dedent(
                f"""
                $ postgres-execute connection=postgres://postgres:postgres@postgres
                DELETE FROM {self.postgres_table.name} WHERE f1 < {self.postgres_table.watermarks.min};
                """
            )
        )
