# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import threading
from typing import List, Optional, Set, Type

import pg8000

from materialize.parallel_workload.data_type import DATA_TYPES, DataType
from materialize.parallel_workload.execute import execute

MAX_COLUMNS = 20
MAX_INITIAL_TABLES = 10
MAX_INITIAL_VIEWS = 10


class Column:
    column_id: int
    data_type: Type[DataType]
    table: "Table"
    nullable: bool
    default: Optional[str]

    def __init__(
        self,
        rng: random.Random,
        column_id: int,
        data_type: Type[DataType],
        table: "Table",
    ):
        self.column_id = column_id
        self.data_type = data_type
        self.table = table
        self.nullable = rng.choice([True, False])
        self.default = rng.choice([None, str(data_type.value(rng))])

    def __str__(self) -> str:
        return f"c{self.column_id}_{self.data_type.name()}"

    def value(self, rng: random.Random) -> str:
        return str(self.data_type.value(rng))

    def create(self) -> str:
        result = f"{self} {self.data_type.name()}"
        if self.default:
            result += f" DEFAULT {self.default}"
        if not self.nullable:
            result += " NOT NULL"
        return result


class DBObject:
    columns: List[Column]


class Table(DBObject):
    table_id: int
    rename: int

    def __init__(self, rng: random.Random, table_id: int):
        self.table_id = table_id
        self.columns = [
            Column(rng, i, rng.choice(DATA_TYPES), self)
            for i in range(rng.randint(2, MAX_COLUMNS))
        ]
        self.rename = 0

    def __str__(self) -> str:
        if self.rename:
            return f"t{self.table_id}_{self.rename}"
        return f"t{self.table_id}"

    def create(self, cur: pg8000.Cursor) -> bool:
        query = f"CREATE TABLE {self}("
        query += ",\n    ".join(column.create() for column in self.columns)
        query += ")"
        return execute(cur, query)


class View(DBObject):
    view_id: int
    table: DBObject
    materialized: bool

    def __init__(self, rng: random.Random, view_id: int, table: DBObject):
        self.view_id = view_id
        self.table = table
        self.columns = [
            column
            for column in rng.sample(
                table.columns, k=rng.randint(1, len(table.columns))
            )
        ]
        self.materialized = rng.choice([True, False])

    def __str__(self) -> str:
        return f"v{self.view_id}"

    def create(self, cur: pg8000.Cursor) -> bool:
        if self.materialized:
            query = "CREATE MATERIALIZED VIEW"
        else:
            query = "CREATE VIEW"
        columns = ", ".join(str(column) for column in self.columns)
        query += f" {self} AS SELECT {columns} FROM {self.table}"
        return execute(cur, query)


class Database:
    seed: str
    tables: List[Table]
    views: List[View]
    indexes: Set[str]
    lock: threading.Lock

    def __init__(self, rng: random.Random, seed: str):
        self.seed = seed

        self.tables = [Table(rng, i) for i in range(rng.randint(2, MAX_INITIAL_TABLES))]
        self.views = []
        for i in range(rng.randint(2, MAX_INITIAL_VIEWS)):
            tables_views: List[DBObject] = [*self.tables, *self.views]
            view = View(rng, i, rng.choice(tables_views))
            self.views.append(view)
        self.indexes = set()
        self.lock = threading.Lock()

    def __str__(self) -> str:
        return f"db_pw_{self.seed}"

    def drop(self, cur: pg8000.Cursor) -> None:
        cur.execute(f"DROP DATABASE IF EXISTS {self}")

    def create(self, cur: pg8000.Cursor) -> bool:
        self.drop(cur)
        return execute(cur, f"CREATE DATABASE {self}")

    def create_relations(self, cur: pg8000.Cursor) -> None:
        for table in self.tables:
            table.create(cur)

        for view in self.views:
            view.create(cur)
