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
from materialize.zippy.framework import Action, ActionFactory, Capabilities, Capability
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.table_capabilities import TableExists

MAX_ROWS_PER_ACTION = 10000


class CreateTableParameterized(ActionFactory):
    def __init__(
        self, max_tables: int = 10, max_rows_per_action: int = MAX_ROWS_PER_ACTION
    ) -> None:
        self.max_tables = max_tables
        self.max_rows_per_action = max_rows_per_action

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def new(self, capabilities: Capabilities) -> List[Action]:
        new_table_name = capabilities.get_free_capability_name(
            TableExists, self.max_tables
        )

        if new_table_name:
            return [
                CreateTable(
                    capabilities=capabilities,
                    table=TableExists(
                        name=new_table_name,
                        has_index=random.choice([True, False]),
                        max_rows_per_action=self.max_rows_per_action,
                    ),
                )
            ]
        else:
            return []


class CreateTable(Action):
    """Creates a table on the Mz instance. 50% of the tables have a default index."""

    def __init__(self, table: TableExists, capabilities: Capabilities) -> None:
        assert (
            table is not None
        ), "CreateTable Action can not be referenced directly, it is produced by CreateTableParameterized factory"
        self.table = table

    def run(self, c: Composition) -> None:
        index = (
            f"> CREATE DEFAULT INDEX ON {self.table.name}"
            if self.table.has_index
            else ""
        )
        c.testdrive(
            dedent(
                f"""
                > CREATE TABLE {self.table.name} (f1 INTEGER);
                {index}
                > INSERT INTO {self.table.name} VALUES ({self.table.watermarks.max});
                """
            )
        )

    def provides(self) -> List[Capability]:
        return [self.table]


class ValidateTable(Action):
    """Validates that a single table contains data that is consistent with the expected min/max watermark."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))

    def run(self, c: Composition) -> None:
        c.testdrive(
            dedent(
                f"""
                > SELECT MIN(f1), MAX(f1), COUNT(f1), COUNT(DISTINCT f1) FROM {self.table.name};
                {self.table.watermarks.min} {self.table.watermarks.max} {(self.table.watermarks.max-self.table.watermarks.min)+1} {(self.table.watermarks.max-self.table.watermarks.min)+1}
                """
            )
        )


class DML(Action):
    """Performs an INSERT, DELETE or UPDATE against a table."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))
        self.delta = random.randint(1, self.table.max_rows_per_action)


class Insert(DML):
    """Inserts rows into a table."""

    def run(self, c: Composition) -> None:
        prev_max = self.table.watermarks.max
        self.table.watermarks.max = prev_max + self.delta
        c.testdrive(
            f"> INSERT INTO {self.table.name} SELECT * FROM generate_series({prev_max + 1}, {self.table.watermarks.max});"
        )


class ShiftForward(DML):
    """Update all rows from a table by incrementing their values by a constant."""

    def run(self, c: Composition) -> None:
        self.table.watermarks.shift(self.delta)
        c.testdrive(f"> UPDATE {self.table.name} SET f1 = f1 + {self.delta};")


class ShiftBackward(DML):
    """Update all rows from a table by decrementing their values by a constant."""

    def run(self, c: Composition) -> None:
        self.table.watermarks.shift(-self.delta)
        c.testdrive(f"> UPDATE {self.table.name} SET f1 = f1 - {self.delta};")


class DeleteFromHead(DML):
    """Delete the largest values from a table"""

    def run(self, c: Composition) -> None:
        self.table.watermarks.max = max(
            self.table.watermarks.max - self.delta, self.table.watermarks.min
        )
        c.testdrive(
            f"> DELETE FROM {self.table.name} WHERE f1 > {self.table.watermarks.max};"
        )


class DeleteFromTail(DML):
    """Delete the smallest values from a table"""

    def run(self, c: Composition) -> None:
        self.table.watermarks.min = min(
            self.table.watermarks.min + self.delta, self.table.watermarks.max
        )
        c.testdrive(
            f"> DELETE FROM {self.table.name} WHERE f1 < {self.table.watermarks.min};"
        )
