# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List

import pg8000

from materialize.parallel_workload.database import Database, DBObject, Table
from materialize.parallel_workload.execute import execute

MAX_TABLES = 25
MAX_VIEWS = 25


class Action:
    rng: random.Random
    database: Database

    def __init__(self, rng: random.Random, database: Database):
        self.rng = rng
        self.database = database

    def run(self, cur: pg8000.Cursor) -> None:
        raise NotImplementedError


class SelectAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        tables_views: List[DBObject] = [*self.database.tables, *self.database.views]
        table = self.rng.choice(tables_views)
        # TODO: More interesting expressions
        expressions = ", ".join(
            str(column)
            for column in random.sample(
                table.columns, k=random.randint(1, len(table.columns))
            )
        )
        query = f"SELECT {expressions} FROM {table}"
        execute(cur, query)


class InsertAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        table = self.rng.choice(self.database.tables)
        column_names = ", ".join(str(column) for column in table.columns)
        column_values = ", ".join(column.value(self.rng) for column in table.columns)
        query = f"INSERT INTO {table} ({column_names}) VALUES ({column_values})"
        execute(cur, query)


class CreateIndexAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        tables_views: List[DBObject] = [*self.database.tables, *self.database.views]
        table = self.rng.choice(tables_views)
        columns = self.rng.sample(table.columns, len(table.columns))
        columns_str = "_".join(str(column) for column in columns)
        index_name = f"idx_{table}_{columns_str}"
        index_elems = []
        for i, column in enumerate(columns):
            order = self.rng.choice(["ASC", "DESC"])
            index_elems.append(f"{column} {order}")
        index_str = ", ".join(index_elems)
        query = f"CREATE INDEX {index_name} ON {table} ({index_str})"
        if execute(cur, query):
            with self.database.lock:
                self.database.indexes.add(index_name)


class DropIndexAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if not self.database.indexes:
                return
            index_name = self.rng.choice(list(self.database.indexes))
            query = f"DROP INDEX {index_name}"
            if execute(cur, query):
                self.database.indexes.remove(index_name)


class CreateTableAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if len(self.database.tables) > MAX_TABLES:
                return
            table = Table(self.rng, len(self.database.tables))
            if table.create(cur):
                self.database.tables.append(table)


class DropTableAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if len(self.database.tables) < 2:
                return
            table_id = self.rng.randrange(len(self.database.tables))
            table = self.database.tables[table_id]
            query = f"DROP TABLE {table} CASCADE"
            if execute(cur, query):
                del self.database.tables[table_id]


class RenameTableAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if not self.database.tables:
                return
            table = self.rng.choice(self.database.tables)
            old_name = str(table)
            table.rename += 1
            query = f"ALTER TABLE {old_name} RENAME TO {table}"
            if not execute(cur, query):
                table.rename -= 1


# TODO
# class CreateViewAction(Action):
# class DropViewAction(Action):

dml_actions = [
    (SelectAction, 90),
    (InsertAction, 10),
]

ddl_actions = [
    (CreateIndexAction, 1),
    (DropIndexAction, 1),
    (CreateTableAction, 1),
    (DropTableAction, 1),
    (RenameTableAction, 1),
]
