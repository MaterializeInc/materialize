# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capabilities, Capability
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.table_capabilities import TableExists


class CreateTable(Action):
    """Creates a table on the Mz instance. 50% of the tables have a default index."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def __init__(self, capabilities: Capabilities) -> None:
        this_table = TableExists(name="table" + str(random.randint(1, 10)))

        existing_tables = [
            t for t in capabilities.get(TableExists) if t.name == this_table.name
        ]

        if len(existing_tables) == 0:
            self.new_table = True
            self.has_index = random.choice([True, False])
            self.table = this_table
        elif len(existing_tables) == 1:
            self.new_table = False
            self.table = existing_tables[0]
        else:
            assert False

    def run(self, c: Composition) -> None:
        if self.new_table:
            index = (
                f"> CREATE DEFAULT INDEX ON {self.table.name}" if self.has_index else ""
            )
            c.testdrive(
                f"""
> CREATE TABLE {self.table.name} (f1 INTEGER);
{index}
> INSERT INTO {self.table.name} VALUES ({self.table.watermarks.max});
"""
            )

    def provides(self) -> List[Capability]:
        return [self.table] if self.new_table else []


class ValidateTable(Action):
    """Validates that a single table contains data that is consistent with the expected min/max watermark."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))

    def run(self, c: Composition) -> None:
        c.testdrive(
            f"""
> SELECT MIN(f1), MAX(f1), COUNT(f1), COUNT(DISTINCT f1) FROM {self.table.name};
{self.table.watermarks.min} {self.table.watermarks.max} {(self.table.watermarks.max-self.table.watermarks.min)+1} {(self.table.watermarks.max-self.table.watermarks.min)+1}
"""
        )


class DML(Action):
    """Performs an INSERT, DELETE or UPDATE against a table."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))
        self.delta = random.randint(1, 100000)


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
