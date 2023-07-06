# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
from typing import List, Optional, Tuple, Type

import pg8000

from materialize.parallel_workload.database import Database, DBObject, Table, View
from materialize.parallel_workload.execute import QueryError, execute

MAX_TABLES = 20
MAX_VIEWS = 20


class Action:
    rng: random.Random
    database: Database
    complexity: str

    def __init__(self, rng: random.Random, database: Database, complexity: str):
        self.rng = rng
        self.database = database
        self.complexity = complexity

    def run(self, cur: pg8000.Cursor) -> None:
        raise NotImplementedError

    def errors_to_ignore(self) -> List[str]:
        if self.complexity == "ddl":
            return [
                "query could not complete",
                "cached plan must not change result type",
                "violates not-null constraint",
                "result exceeds max size of",
            ]
        return []


class SelectAction(Action):
    def errors_to_ignore(self) -> List[str]:
        result = [
            "in the same timedomain",
        ] + super().errors_to_ignore()

        if self.complexity == "ddl":
            result.extend(
                [
                    "unknown catalog item",
                    "does not exist",
                ]
            )
        return result

    def run(self, cur: pg8000.Cursor) -> None:
        tables_views: List[DBObject] = [*self.database.tables, *self.database.views]
        table = self.rng.choice(tables_views)
        column = self.rng.choice(table.columns)
        table2 = self.rng.choice(tables_views)
        table_name = str(table)
        table2_name = str(table2)
        columns = [c for c in table2.columns if c.data_type == column.data_type]
        if table_name != table2_name and columns:
            column2 = self.rng.choice(columns)
            query = f"SELECT * FROM {table_name} JOIN {table2_name} ON {column} = {column2} LIMIT 1"
        else:
            if self.rng.choice([True, False]):
                expressions = ", ".join(
                    str(column)
                    for column in random.sample(
                        table.columns, k=random.randint(1, len(table.columns))
                    )
                )
            else:
                expressions = "*"
            query = f"SELECT {expressions} FROM {table}"
        execute(cur, query)
        cur.fetchall()


class InsertAction(Action):
    def errors_to_ignore(self) -> List[str]:
        result = [
            # TODO: Remember table we used in current transaction in Executor,
            # then only run queries against it during the current transaction
            "writes to a single table",
        ] + super().errors_to_ignore()
        if self.complexity == "ddl":
            result.extend(
                [
                    "unknown catalog item",
                ]
            )
        return result

    def run(self, cur: pg8000.Cursor) -> None:
        table = self.rng.choice(self.database.tables)
        column_names = ", ".join(column.name() for column in table.columns)
        column_values = ", ".join(column.value(self.rng) for column in table.columns)
        query = f"INSERT INTO {table} ({column_names}) VALUES ({column_values})"
        execute(cur, query)


class CreateIndexAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "unknown catalog item",
            "already exists",
        ] + super().errors_to_ignore()

    def run(self, cur: pg8000.Cursor) -> None:
        tables_views: List[DBObject] = [*self.database.tables, *self.database.views]
        table = self.rng.choice(tables_views)
        columns = self.rng.sample(table.columns, len(table.columns))
        columns_str = "_".join(column.name() for column in columns)
        index_name = f"idx_{table}_{columns_str}"
        index_elems = []
        for i, column in enumerate(columns):
            order = self.rng.choice(["ASC", "DESC"])
            index_elems.append(f"{column.name()} {order}")
        index_str = ", ".join(index_elems)
        query = f"CREATE INDEX {index_name} ON {table} ({index_str})"
        execute(cur, query)
        with self.database.lock:
            self.database.indexes.add(index_name)


class DropIndexAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "unknown catalog item",
        ] + super().errors_to_ignore()

    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if not self.database.indexes:
                return
            index_name = self.rng.choice(list(self.database.indexes))
            query = f"DROP INDEX {index_name}"
            execute(cur, query)
            self.database.indexes.remove(index_name)


class CreateTableAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "already exists",
        ] + super().errors_to_ignore()

    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if len(self.database.tables) > MAX_TABLES:
                return
            table = Table(self.rng, len(self.database.tables))
            table.create(cur)
            self.database.tables.append(table)


class DropTableAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if len(self.database.tables) <= 2:
                return
            table_id = self.rng.randrange(len(self.database.tables))
            table = self.database.tables[table_id]
            query = f"DROP TABLE {table}"
            # TODO: Cascade
            execute(cur, query)
            del self.database.tables[table_id]


class RenameTableAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if not self.database.tables:
                return
            table = self.rng.choice(self.database.tables)
            old_name = str(table)
            table.rename += 1
            try:
                execute(cur, f"ALTER TABLE {old_name} RENAME TO {table}")
            finally:
                table.rename -= 1


class TransactionIsolationAction(Action):
    def run(self, cur: pg8000.Cursor) -> None:
        level = self.rng.choice(["SERIALIZABLE", "STRICT SERIALIZABLE"])
        execute(cur, f"SET TRANSACTION_ISOLATION TO '{level}'")


class CommitRollbackAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "unknown catalog item",  # https://github.com/MaterializeInc/materialize/issues/20381
        ]

    def run(self, cur: pg8000.Cursor) -> None:
        if self.rng.choice([True, False]):
            try:
                cur.connection.commit()
            except Exception as e:
                raise QueryError(str(e), "commit")
        else:
            cur.connection.rollback()


class CreateViewAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "already exists",
        ] + super().errors_to_ignore()

    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if len(self.database.views) > MAX_VIEWS:
                return
            # Only use tables for now since LIMIT 1 and statement_timeout are
            # not effective yet at preventing long-running queries and OoMs.
            base_object = self.rng.choice(self.database.tables)
            base_object2: Optional[Table] = self.rng.choice(self.database.tables)
            if self.rng.choice([True, False]) or base_object2 == base_object:
                base_object2 = None
            view = View(self.rng, len(self.database.views), base_object, base_object2)
            view.create(cur)
            self.database.views.append(view)


class DropViewAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "still depended upon by",
            "unknown catalog item",
        ] + super().errors_to_ignore()

    def run(self, cur: pg8000.Cursor) -> None:
        with self.database.lock:
            if not self.database.views:
                return
            view_id = self.rng.randrange(len(self.database.views))
            view = self.database.views[view_id]
            if view.materialized:
                query = f"DROP MATERIALIZED VIEW {view}"
            else:
                query = f"DROP VIEW {view}"
            # TODO: Cascade
            execute(cur, query)
            del self.database.views[view_id]


class ActionList:
    action_classes: List[Type[Action]]
    weights: List[int]
    autocommit: bool

    def __init__(
        self, action_classes_weights: List[Tuple[Type[Action], int]], autocommit: bool
    ):
        self.action_classes = [action[0] for action in action_classes_weights]
        self.weights = [action[1] for action in action_classes_weights]
        self.autocommit = autocommit


read_action_list = ActionList(
    [(SelectAction, 90), (CommitRollbackAction, 1)],
    autocommit=False,
)

write_action_list = ActionList(
    [(InsertAction, 10), (CommitRollbackAction, 1)],
    autocommit=False,
)

ddl_action_list = ActionList(
    [
        (CreateIndexAction, 2),
        (DropIndexAction, 1),
        (CreateTableAction, 2),
        (DropTableAction, 1),
        (CreateViewAction, 8),
        (DropViewAction, 4),
        # (RenameTableAction, 1),
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,
)
