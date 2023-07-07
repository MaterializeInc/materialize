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
import time
from collections import Counter, defaultdict
from typing import DefaultDict, List, Type

import pg8000

from materialize.parallel_workload.action import Action
from materialize.parallel_workload.executor import Executor, QueryError


class Worker:
    rng: random.Random
    actions: List[Action]
    weights: List[int]
    end_time: float
    num_queries: int
    autocommit: bool
    ignored_errors: DefaultDict[str, Counter[Type[Action]]]
    pg_pid: int

    def __init__(
        self,
        rng: random.Random,
        actions: List[Action],
        weights: List[int],
        end_time: float,
        autocommit: bool,
    ):
        self.rng = rng
        self.actions = actions
        self.weights = weights
        self.end_time = end_time
        self.num_queries = 0
        self.autocommit = autocommit
        self.ignored_errors = defaultdict(Counter)
        self.pg_pid = -1

    def run(self, host: str, port: int, database: str) -> None:
        self.conn = pg8000.connect(
            host=host, port=port, user="materialize", database=database
        )
        self.conn.autocommit = self.autocommit
        with self.conn.cursor() as cur:
            exe = Executor(self.rng, cur)
            exe.set_isolation("SERIALIZABLE")
            cur.execute("SELECT pg_backend_pid()")
            self.pg_pid = cur.fetchall()[0][0]
            while time.time() < self.end_time:
                action = self.rng.choices(self.actions, self.weights)[0]
                self.num_queries += 1
                try:
                    action.run(exe)
                except QueryError as e:
                    for error in action.errors_to_ignore():
                        if error in e.msg:
                            self.ignored_errors[error][type(action)] += 1
                            exe.rollback()
                            break
                    else:
                        thread_name = threading.current_thread().getName()
                        print(f"{thread_name} Query failed: {e.query} {e.msg}")
                        raise
