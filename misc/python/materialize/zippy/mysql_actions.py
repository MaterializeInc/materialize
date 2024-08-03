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
from materialize.zippy.mz_capabilities import MzIsRunning


class MySqlStart(Action):
    """Start a MySQL instance."""

    def provides(self) -> list[Capability]:
        return [MySqlRunning()]

    def run(self, c: Composition, state: State) -> None:
        c.up("mysql")

        c.testdrive(
            dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                DROP DATABASE IF EXISTS public;
                CREATE DATABASE public;
                USE public;
                """
            ),
            mz_service=state.mz_service,
        )


class MySqlStop(Action):
    """Stop the MySQL instance."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MySqlRunning}

    def withholds(self) -> set[type[Capability]]:
        return {MySqlRunning}

    def run(self, c: Composition, state: State) -> None:
        c.kill("mysql")


class MySqlRestart(Action):
    """Restart the MySql instance."""

    def run(self, c: Composition, state: State) -> None:
        c.kill("mysql")
        c.up("mysql")


class CreateMySqlTable(Action):
    """Creates a table on the MySql instance. 50% of the tables have a PK."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, MySqlRunning}

    def __init__(self, capabilities: Capabilities) -> None:
        this_mysql_table = MySqlTableExists(name="table" + str(random.randint(1, 10)))

        existing_mysql_tables = [
            t
            for t in capabilities.get(MySqlTableExists)
            if t.name == this_mysql_table.name
        ]

        if len(existing_mysql_tables) == 0:
            self.new_mysql_table = True
            # A PK is now required for Debezium
            this_mysql_table.has_pk = True

            self.mysql_table = this_mysql_table
        elif len(existing_mysql_tables) == 1:
            self.new_mysql_table = False
            self.mysql_table = existing_mysql_tables[0]
        else:
            raise RuntimeError("More than one table exists")

        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        if self.new_mysql_table:
            primary_key = "PRIMARY KEY" if self.mysql_table.has_pk else ""
            c.testdrive(
                dedent(
                    f"""
                    $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                    $ mysql-execute name=mysql
                    USE public;
                    CREATE TABLE {self.mysql_table.name} (f1 INTEGER {primary_key});
                    INSERT INTO {self.mysql_table.name} VALUES ({self.mysql_table.watermarks.max});
                    """
                ),
                mz_service=state.mz_service,
            )

    def provides(self) -> list[Capability]:
        return [self.mysql_table] if self.new_mysql_table else []


class MySqlDML(Action):
    """Performs an INSERT, DELETE or UPDATE against a MySQL table."""

    # We use smaller batches in Pg then in Mz because Pg will fill up much faster
    MAX_BATCH_SIZE = 10000

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, MySqlRunning, MySqlTableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.mysql_table = random.choice(capabilities.get(MySqlTableExists))
        self.delta = random.randint(1, MySqlDML.MAX_BATCH_SIZE)

        super().__init__(capabilities)

    def __str__(self) -> str:
        return f"{Action.__str__(self)} {self.mysql_table.name}"


class MySqlInsert(MySqlDML):
    """Inserts rows into a MySQL table."""

    def run(self, c: Composition, state: State) -> None:
        prev_max = self.mysql_table.watermarks.max
        self.mysql_table.watermarks.max = prev_max + self.delta
        c.testdrive(
            dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                SET @i:={prev_max};
                INSERT INTO {self.mysql_table.name} SELECT @i:=@i+1 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT {self.mysql_table.watermarks.max - prev_max};
                """
            ),
            mz_service=state.mz_service,
        )


class MySqlShiftForward(MySqlDML):
    """Update all rows from a MySQL table by incrementing their values by a constant (tables without a PK only)"""

    def run(self, c: Composition, state: State) -> None:
        if not self.mysql_table.has_pk:
            self.mysql_table.watermarks.shift(self.delta)
            c.testdrive(
                dedent(
                    f"""
                    $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                    $ mysql-execute name=mysql
                    USE public;
                    UPDATE {self.mysql_table.name} SET f1 = f1 + {self.delta};
                    """
                ),
                mz_service=state.mz_service,
            )


class MySqlShiftBackward(MySqlDML):
    """Update all rows from a MySQL table by decrementing their values by a constant (tables without a PK only)"""

    def run(self, c: Composition, state: State) -> None:
        if not self.mysql_table.has_pk:
            self.mysql_table.watermarks.shift(-self.delta)
            c.testdrive(
                dedent(
                    f"""
                    $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                    $ mysql-execute name=mysql
                    USE public;
                    UPDATE {self.mysql_table.name} SET f1 = f1 - {self.delta};
                    """
                ),
                mz_service=state.mz_service,
            )


class MySqlDeleteFromHead(MySqlDML):
    """Delete the largest values from a MySQL table"""

    def run(self, c: Composition, state: State) -> None:
        self.mysql_table.watermarks.max = max(
            self.mysql_table.watermarks.max - self.delta,
            self.mysql_table.watermarks.min,
        )
        c.testdrive(
            dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                DELETE FROM {self.mysql_table.name} WHERE f1 > {self.mysql_table.watermarks.max};
                """
            ),
            mz_service=state.mz_service,
        )


class MySqlDeleteFromTail(MySqlDML):
    """Delete the smallest values from a MySQL table"""

    def run(self, c: Composition, state: State) -> None:
        self.mysql_table.watermarks.min = min(
            self.mysql_table.watermarks.min + self.delta,
            self.mysql_table.watermarks.max,
        )
        c.testdrive(
            dedent(
                f"""
                $ mysql-connect name=mysql url=mysql://root@mysql password={MySql.DEFAULT_ROOT_PASSWORD}

                $ mysql-execute name=mysql
                USE public;
                DELETE FROM {self.mysql_table.name} WHERE f1 < {self.mysql_table.watermarks.min};
                """
            ),
            mz_service=state.mz_service,
        )
