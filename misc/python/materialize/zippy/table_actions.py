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
from materialize.zippy.framework import (
    Action,
    ActionFactory,
    Capabilities,
    Capability,
    State,
)
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
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning}

    def new(self, capabilities: Capabilities) -> list[Action]:
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

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning}

    def __init__(self, table: TableExists, capabilities: Capabilities) -> None:
        assert (
            table is not None
        ), "CreateTable Action can not be referenced directly, it is produced by CreateTableParameterized factory"
        self.table = table
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
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
            ),
            mz_service=state.mz_service,
        )

    def provides(self) -> list[Capability]:
        return [self.table]


class ValidateTable(Action):
    """Validates that a single table contains data that is consistent with the expected min/max watermark."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, TableExists}

    def __init__(
        self, capabilities: Capabilities, table: TableExists | None = None
    ) -> None:
        if table is not None:
            self.table = table
        else:
            self.table = random.choice(capabilities.get(TableExists))

        self.select_limit = random.choices([True, False], weights=[0.2, 0.8], k=1)[0]
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        # Validating via SELECT ... LIMIT is expensive as it requires creating a temporary table
        # Therefore, only use it in 20% of validations.
        if self.select_limit:
            c.testdrive(
                dedent(
                    f"""
                    > CREATE TEMPORARY TABLE {self.table.name}_select_limit (f1 INTEGER);
                    > INSERT INTO {self.table.name}_select_limit SELECT * FROM {self.table.name} LIMIT 999999999;
                    > SELECT MIN(f1), MAX(f1), COUNT(f1), COUNT(DISTINCT f1) FROM {self.table.name}_select_limit;
                    {self.table.watermarks.min} {self.table.watermarks.max} {(self.table.watermarks.max-self.table.watermarks.min)+1} {(self.table.watermarks.max-self.table.watermarks.min)+1}
                    > DROP TABLE {self.table.name}_select_limit
                    """
                ),
                mz_service=state.mz_service,
            )
        else:
            c.testdrive(
                dedent(
                    f"""
                    > SELECT MIN(f1), MAX(f1), COUNT(f1), COUNT(DISTINCT f1) FROM {self.table.name};
                    {self.table.watermarks.min} {self.table.watermarks.max} {(self.table.watermarks.max-self.table.watermarks.min)+1} {(self.table.watermarks.max-self.table.watermarks.min)+1}
                    """
                ),
                mz_service=state.mz_service,
            )


class DML(Action):
    """Performs an INSERT, DELETE or UPDATE against a table."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning, TableExists}

    def __init__(self, capabilities: Capabilities) -> None:
        self.table = random.choice(capabilities.get(TableExists))
        self.delta = random.randint(1, self.table.max_rows_per_action)
        super().__init__(capabilities)

    def __str__(self) -> str:
        return f"{Action.__str__(self)} {self.table.name}"


class Insert(DML):
    """Inserts rows into a table."""

    def run(self, c: Composition, state: State) -> None:
        prev_max = self.table.watermarks.max
        self.table.watermarks.max = prev_max + self.delta
        c.testdrive(
            f"> INSERT INTO {self.table.name} SELECT * FROM generate_series({prev_max + 1}, {self.table.watermarks.max});",
            mz_service=state.mz_service,
        )


class ShiftForward(DML):
    """Update all rows from a table by incrementing their values by a constant."""

    def run(self, c: Composition, state: State) -> None:
        self.table.watermarks.shift(self.delta)
        c.testdrive(
            f"> UPDATE {self.table.name} SET f1 = f1 + {self.delta};",
            mz_service=state.mz_service,
        )


class ShiftBackward(DML):
    """Update all rows from a table by decrementing their values by a constant."""

    def run(self, c: Composition, state: State) -> None:
        self.table.watermarks.shift(-self.delta)
        c.testdrive(
            f"> UPDATE {self.table.name} SET f1 = f1 - {self.delta};",
            mz_service=state.mz_service,
        )


class DeleteFromHead(DML):
    """Delete the largest values from a table"""

    def run(self, c: Composition, state: State) -> None:
        self.table.watermarks.max = max(
            self.table.watermarks.max - self.delta, self.table.watermarks.min
        )
        c.testdrive(
            f"> DELETE FROM {self.table.name} WHERE f1 > {self.table.watermarks.max};",
            mz_service=state.mz_service,
        )


class DeleteFromTail(DML):
    """Delete the smallest values from a table"""

    def run(self, c: Composition, state: State) -> None:
        self.table.watermarks.min = min(
            self.table.watermarks.min + self.delta, self.table.watermarks.max
        )
        c.testdrive(
            f"> DELETE FROM {self.table.name} WHERE f1 < {self.table.watermarks.min};",
            mz_service=state.mz_service,
        )
