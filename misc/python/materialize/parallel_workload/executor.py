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
from typing import Optional, TextIO

import pg8000

log: Optional[TextIO]
lock: threading.Lock


def initialize_logging() -> None:
    global log, lock
    log = open("parallel-workload-queries.log", "w")
    lock = threading.Lock()


class QueryError(Exception):
    msg: str
    query: str

    def __init__(self, msg: str, query: str):
        self.msg = msg
        self.query = query


class Executor:
    rng: random.Random
    cur: pg8000.Cursor
    pg_pid: int
    insert_table: Optional[int]

    def __init__(self, rng: random.Random, cur: pg8000.Cursor):
        self.rng = rng
        self.cur = cur
        self.pg_pid = -1
        self.insert_table = None

    def set_isolation(self, level: str) -> None:
        self.execute(f"SET TRANSACTION_ISOLATION TO '{level}'")

    def commit(self) -> None:
        self.insert_table = None
        try:
            self.cur._c.commit()
        except Exception as e:
            raise QueryError(str(e), "commit")

    def rollback(self) -> None:
        self.insert_table = None
        try:
            self.cur._c.rollback()
        except Exception as e:
            raise QueryError(str(e), "rollback")

    def execute(self, query: str, explainable: bool = False) -> None:
        global log, lock
        if explainable and self.rng.choice([True, False]):
            query = f"EXPLAIN {query}"
        query += ";"
        thread_name = threading.current_thread().getName()
        if log:
            with lock:
                print(f"[{thread_name}] {query}", file=log)
                log.flush()
        try:
            self.cur.execute(query)
        except Exception as e:
            raise QueryError(str(e), query)
