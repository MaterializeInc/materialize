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
> INSERT INTO {self.table.name} VALUES ({self.table.watermarks.high});
"""
            )

    def provides(self) -> List[Capability]:
        return [self.table] if self.new_table else []


class ValidateTable(Action):
    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))

    def run(self, c: Composition) -> None:
        c.testdrive(
            f"""
> SELECT MIN(f1), MAX(f1), COUNT(f1), COUNT(DISTINCT f1) FROM {self.table.name};
{self.table.watermarks.low} {self.table.watermarks.high} {(self.table.watermarks.high-self.table.watermarks.low)+1} {(self.table.watermarks.high-self.table.watermarks.low)+1}
"""
        )


class DML(Action):
    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))
        self.delta = random.randint(1, 100000)


class Insert(DML):
    def run(self, c: Composition) -> None:
        prev_high = self.table.watermarks.high
        self.table.watermarks.high = prev_high + self.delta
        c.testdrive(
            f"> INSERT INTO {self.table.name} SELECT * FROM generate_series({prev_high + 1}, {self.table.watermarks.high});"
        )


class ShiftForward(DML):
    def run(self, c: Composition) -> None:
        self.table.watermarks.shift(self.delta)
        c.testdrive(f"> UPDATE {self.table.name} SET f1 = f1 + {self.delta};")


class ShiftBackward(DML):
    def run(self, c: Composition) -> None:
        self.table.watermarks.shift(-self.delta)
        c.testdrive(f"> UPDATE {self.table.name} SET f1 = f1 - {self.delta};")


class DeleteFromHead(DML):
    def run(self, c: Composition) -> None:
        self.table.watermarks.high = max(
            self.table.watermarks.high - self.delta, self.table.watermarks.low
        )
        c.testdrive(
            f"> DELETE FROM {self.table.name} WHERE f1 > {self.table.watermarks.high};"
        )


class DeleteFromTail(DML):
    def run(self, c: Composition) -> None:
        self.table.watermarks.low = min(
            self.table.watermarks.low + self.delta, self.table.watermarks.high
        )
        c.testdrive(
            f"> DELETE FROM {self.table.name} WHERE f1 < {self.table.watermarks.low};"
        )
