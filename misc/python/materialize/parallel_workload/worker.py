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
from typing import DefaultDict, List, Optional, Type

import pg8000

from materialize.parallel_workload.action import Action, ReconnectAction
from materialize.parallel_workload.executor import Executor, QueryError


class Worker:
    rng: random.Random
    actions: List[Action]
    weights: List[float]
    end_time: float
    num_queries: int
    autocommit: bool
    exe: Optional[Executor]
    ignored_errors: DefaultDict[str, Counter[Type[Action]]]

    def __init__(
        self,
        rng: random.Random,
        actions: List[Action],
        weights: List[float],
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
        self.exe = None

    def run(self, host: str, port: int, database: str) -> None:
        self.conn = pg8000.connect(
            host=host, port=port, user="materialize", database=database
        )
        self.conn.autocommit = self.autocommit
        cur = self.conn.cursor()
        self.exe = Executor(self.rng, cur)
        self.exe.set_isolation("SERIALIZABLE")
        cur.execute("SELECT pg_backend_pid()")
        self.exe.pg_pid = cur.fetchall()[0][0]
        rollback_next = True
        reconnect_next = True
        while time.time() < self.end_time:
            action = self.rng.choices(self.actions, self.weights)[0]
            self.num_queries += 1
            try:
                if rollback_next:
                    try:
                        self.exe.rollback()
                    except QueryError as e:
                        if (
                            "Please disconnect and re-connect" in e.msg
                            or "network error" in e.msg
                            or "Can't create a connection to host" in e.msg
                            or "Connection refused" in e.msg
                        ):
                            reconnect_next = True
                            rollback_next = False
                            continue
                    rollback_next = False
                if reconnect_next:
                    ReconnectAction(self.rng, action.db, random_role=False).run(
                        self.exe
                    )
                    reconnect_next = False
                action.run(self.exe)
            except QueryError as e:
                for error in action.errors_to_ignore():
                    if error in e.msg:
                        self.ignored_errors[error][type(action)] += 1
                        if (
                            "Please disconnect and re-connect" in e.msg
                            or "network error" in e.msg
                            or "Can't create a connection to host" in e.msg
                            or "Connection refused" in e.msg
                        ):
                            reconnect_next = True
                        else:
                            rollback_next = True
                        break
                else:
                    thread_name = threading.current_thread().getName()
                    print(f"{thread_name} Query failed: {e.query} {e.msg}")
                    raise
