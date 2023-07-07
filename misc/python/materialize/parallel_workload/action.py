# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time
from typing import List, Optional, Tuple, Type

from materialize.parallel_workload.database import (
    MAX_ROLES,
    MAX_TABLES,
    MAX_VIEWS,
    Database,
    DBObject,
    Role,
    Table,
    View,
)
from materialize.parallel_workload.executor import Executor


class Action:
    rng: random.Random
    db: Database

    def __init__(self, rng: random.Random, db: Database):
        self.rng = rng
        self.db = db

    def run(self, exe: Executor) -> None:
        raise NotImplementedError

    def errors_to_ignore(self) -> List[str]:
        result = []
        if self.db.complexity == "ddl":
            result.extend(
                [
                    "query could not complete",
                    "cached plan must not change result type",
                    "violates not-null constraint",
                    "result exceeds max size of",
                    "unknown catalog item",  # Expected, see #20381
                ]
            )
        if self.db.scenario == "cancel":
            result.extend(
                [
                    "canceling statement due to user request",
                ]
            )
        return result


class SelectAction(Action):
    def errors_to_ignore(self) -> List[str]:
        result = super().errors_to_ignore()
        if self.db.complexity in ("dml", "ddl"):
            result.extend(
                [
                    "in the same timedomain",
                ]
            )
        if self.db.complexity == "ddl":
            result.extend(
                [
                    "does not exist",
                ]
            )
        return result

    def run(self, exe: Executor) -> None:
        tables_views: List[DBObject] = [*self.db.tables, *self.db.views]
        table = self.rng.choice(tables_views)
        column = self.rng.choice(table.columns)
        table2 = self.rng.choice(tables_views)
        table_name = str(table)
        table2_name = str(table2)
        columns = [c for c in table2.columns if c.data_type == column.data_type]
        if table_name != table2_name and columns:
            column2 = self.rng.choice(columns)
            query = f"SELECT * FROM {table_name} JOIN {table2_name} ON {column} = {column2} LIMIT 1"
            exe.execute(query, explainable=True)
            exe.cur.fetchall()
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
            query = f"SELECT {expressions} FROM {table} LIMIT 1"
            exe.execute(query, explainable=True)
            exe.cur.fetchall()


class InsertAction(Action):
    def run(self, exe: Executor) -> None:
        table = None
        if exe.insert_table != None:
            for t in self.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    break
        if not table:
            table = self.rng.choice(self.db.tables)

        column_names = ", ".join(column.name() for column in table.columns)
        column_values = ", ".join(column.value(self.rng) for column in table.columns)
        query = f"INSERT INTO {table} ({column_names}) VALUES ({column_values})"
        exe.execute(query)
        exe.insert_table = table.table_id


class UpdateAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        table = None
        if exe.insert_table != None:
            for t in self.db.tables:
                if t.table_id == exe.insert_table:
                    table = t
                    break
        if not table:
            table = self.rng.choice(self.db.tables)

        column1 = table.columns[0]
        column2 = self.rng.choice(table.columns)
        query = f"UPDATE {table} SET {column2.name()} = {column2.value(self.rng, True)} WHERE {column1.name()} = {column1.value(self.rng, True)}"
        exe.execute(query)
        exe.insert_table = table.table_id


class DeleteAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "canceling statement due to statement timeout",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        table = self.rng.choice(self.db.tables)
        column = table.columns[0]
        query = f"DELETE FROM {table} WHERE {column.name()} = {column.value(self.rng, True)}"
        exe.execute(query)


class CreateIndexAction(Action):
    def run(self, exe: Executor) -> None:
        tables_views: List[DBObject] = [*self.db.tables, *self.db.views]
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
        exe.execute(query)
        with self.db.lock:
            self.db.indexes.add(index_name)


class DropIndexAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.indexes:
                return
            index_name = self.rng.choice(list(self.db.indexes))
            query = f"DROP INDEX {index_name}"
            exe.execute(query)
            self.db.indexes.remove(index_name)


class CreateTableAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.tables) > MAX_TABLES:
                return
            table_id = self.db.table_id
            self.db.table_id += 1
            table = Table(self.rng, table_id)
            table.create(exe)
            self.db.tables.append(table)


class DropTableAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.tables) <= 2:
                return
            table_id = self.rng.randrange(len(self.db.tables))
            table = self.db.tables[table_id]
            query = f"DROP TABLE {table}"
            # TODO: Cascade
            exe.execute(query)
            del self.db.tables[table_id]


class RenameTableAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.tables:
                return
            table = self.rng.choice(self.db.tables)
            old_name = str(table)
            table.rename += 1
            try:
                exe.execute(f"ALTER TABLE {old_name} RENAME TO {table}")
            finally:
                table.rename -= 1


class TransactionIsolationAction(Action):
    def run(self, exe: Executor) -> None:
        level = self.rng.choice(["SERIALIZABLE", "STRICT SERIALIZABLE"])
        exe.set_isolation(level)


class CommitRollbackAction(Action):
    def run(self, exe: Executor) -> None:
        if self.rng.choice([True, False]):
            exe.commit()
        else:
            exe.rollback()


class CreateViewAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.views) > MAX_VIEWS:
                return
            view_id = self.db.view_id
            self.db.view_id += 1
            # Only use tables for now since LIMIT 1 and statement_timeout are
            # not effective yet at preventing long-running queries and OoMs.
            base_object = self.rng.choice(self.db.tables)
            base_object2: Optional[Table] = self.rng.choice(self.db.tables)
            if self.rng.choice([True, False]) or base_object2 == base_object:
                base_object2 = None
            view = View(self.rng, view_id, base_object, base_object2)
            view.create(exe)
            self.db.views.append(view)


class DropViewAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "still depended upon by",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.views:
                return
            view_id = self.rng.randrange(len(self.db.views))
            view = self.db.views[view_id]
            if view.materialized:
                query = f"DROP MATERIALIZED VIEW {view}"
            else:
                query = f"DROP VIEW {view}"
            # TODO: Cascade
            exe.execute(query)
            del self.db.views[view_id]


class CreateRoleAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if len(self.db.roles) > MAX_ROLES:
                return
            role_id = self.db.role_id
            self.db.role_id += 1
            role = Role(role_id)
            role.create(exe)
            self.db.roles.append(role)


class DropRoleAction(Action):
    def errors_to_ignore(self) -> List[str]:
        return [
            "cannot be dropped because some objects depend on it",
        ] + super().errors_to_ignore()

    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.roles:
                return
            role_id = self.rng.randrange(len(self.db.roles))
            role = self.db.roles[role_id]
            query = f"DROP ROLE {role}"
            # TODO: Cascade
            exe.execute(query)
            del self.db.roles[role_id]


class GrantPrivilegesAction(Action):
    def run(self, exe: Executor) -> None:
        with self.db.lock:
            if not self.db.roles:
                return
            role = self.rng.choice(self.db.roles)
            privilege = self.rng.choice(["SELECT", "INSERT", "UPDATE", "ALL"])
            tables_views: List[DBObject] = [*self.db.tables, *self.db.views]
            table = self.rng.choice(tables_views)
            query = f"GRANT {privilege} ON {table} TO {role}"
            exe.execute(query)


class CancelAction(Action):
    worker_pids: List[int]

    def __init__(
        self,
        rng: random.Random,
        db: Database,
        worker_pids: List[int],
    ):
        super().__init__(rng, db)
        self.worker_pids = worker_pids

    def run(self, exe: Executor) -> None:
        pid = self.rng.choice(self.worker_pids)
        exe.execute(f"SELECT pg_cancel_backend({pid})")
        time.sleep(self.rng.uniform(0, 10))


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
    [(SelectAction, 100), (CommitRollbackAction, 1)],
    autocommit=False,
)

write_action_list = ActionList(
    [(InsertAction, 100), (CommitRollbackAction, 1)],
    autocommit=False,
)

dml_nontrans_action_list = ActionList(
    [
        (DeleteAction, 10),
        (UpdateAction, 10)
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,  # deletes can't be inside of transactions
)

ddl_action_list = ActionList(
    [
        (CreateIndexAction, 2),
        (DropIndexAction, 1),
        (CreateTableAction, 2),
        (DropTableAction, 1),
        (CreateViewAction, 8),
        (DropViewAction, 4),
        (CreateRoleAction, 2),
        (DropRoleAction, 1),
        (GrantPrivilegesAction, 2),
        # (RenameTableAction, 1),
        # (TransactionIsolationAction, 1),
    ],
    autocommit=True,
)
