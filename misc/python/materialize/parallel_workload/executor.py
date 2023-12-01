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
from typing import TYPE_CHECKING, TextIO

import pg8000

from materialize.data_ingest.query_error import QueryError

if TYPE_CHECKING:
    from materialize.parallel_workload.database import Database

logging: TextIO | None
lock: threading.Lock


def initialize_logging() -> None:
    global logging, lock
    logging = open("parallel-workload-queries.log", "w")
    lock = threading.Lock()


class Executor:
    rng: random.Random
    cur: pg8000.Cursor
    pg_pid: int
    # Used by INSERT action to prevent writing into different tables in the same transaction
    insert_table: int | None
    db: "Database"
    reconnect_next: bool
    rollback_next: bool
    last_log: str
    action_run_since_last_commit_rollback: bool

    def __init__(self, rng: random.Random, cur: pg8000.Cursor, db: "Database"):
        self.rng = rng
        self.cur = cur
        self.db = db
        self.pg_pid = -1
        self.insert_table = None
        self.reconnect_next = True
        self.rollback_next = True
        self.last_log = ""
        self.action_run_since_last_commit_rollback = False

    def set_isolation(self, level: str) -> None:
        self.execute(f"SET TRANSACTION_ISOLATION TO '{level}'")

    def commit(self) -> None:
        self.insert_table = None
        try:
            self.log("commit")
            self.cur._c.commit()
        except Exception as e:
            raise QueryError(str(e), "commit")

    def rollback(self) -> None:
        self.insert_table = None
        try:
            self.log("rollback")
            self.cur._c.rollback()
        except Exception as e:
            raise QueryError(str(e), "rollback")

    def log(self, msg: str) -> None:
        global logging, lock

        if not logging:
            return

        thread_name = threading.current_thread().getName()
        self.last_log = msg

        with lock:
            print(f"[{thread_name}] {msg}", file=logging)
            logging.flush()

    def execute(
        self, query: str, extra_info: str = "", explainable: bool = False
    ) -> None:
        if explainable and self.rng.choice([True, False]):
            query = f"EXPLAIN {query}"
        query += ";"
        extra_info_str = f" ({extra_info})" if extra_info else ""
        self.log(f"{query}{extra_info_str}")
        try:
            self.cur.execute(query)
        except Exception as e:
            raise QueryError(str(e), query)
        self.action_run_since_last_commit_rollback = True
