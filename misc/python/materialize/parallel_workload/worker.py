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

import pg8000

from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload.action import Action, ReconnectAction
from materialize.parallel_workload.database import Database
from materialize.parallel_workload.executor import Executor


class Worker:
    rng: random.Random
    actions: list[Action]
    weights: list[float]
    end_time: float
    num_queries: int
    autocommit: bool
    system: bool
    exes: list[Executor]
    ignored_errors: defaultdict[str, Counter[type[Action]]]

    def __init__(
        self,
        rng: random.Random,
        actions: list[Action],
        weights: list[float],
        end_time: float,
        autocommit: bool,
        system: bool,
    ):
        self.rng = rng
        self.actions = actions
        self.weights = weights
        self.end_time = end_time
        self.num_queries = 0
        self.autocommit = autocommit
        self.system = system
        self.ignored_errors = defaultdict(Counter)
        self.exes = []

    def run(self, host: str, port: int, user: str, databases: list[Database]) -> None:
        self.conns = [
            pg8000.connect(host=host, port=port, user=user, database=database.name())
            for database in databases
        ]
        for database, conn in zip(databases, self.conns):
            conn.autocommit = self.autocommit
            cur = conn.cursor()
            exe = Executor(self.rng, cur, database)
            exe.set_isolation("SERIALIZABLE")
            cur.execute("SELECT pg_backend_pid()")
            exe.pg_pid = cur.fetchall()[0][0]
            self.exes.append(exe)

        while time.time() < self.end_time:
            exe = self.rng.choice(self.exes)
            action = self.rng.choices(self.actions, self.weights)[0]
            self.num_queries += 1
            try:
                if exe.rollback_next:
                    try:
                        exe.rollback()
                    except QueryError as e:
                        if (
                            "Please disconnect and re-connect" in e.msg
                            or "network error" in e.msg
                            or "Can't create a connection to host" in e.msg
                            or "Connection refused" in e.msg
                        ):
                            exe.reconnect_next = True
                            exe.rollback_next = False
                            continue
                    exe.rollback_next = False
                if exe.reconnect_next:
                    ReconnectAction(self.rng, random_role=False).run(exe)
                    exe.reconnect_next = False
                action.run(exe)
            except QueryError as e:
                for error in action.errors_to_ignore(exe):
                    if error in e.msg:
                        self.ignored_errors[error][type(action)] += 1
                        if (
                            "Please disconnect and re-connect" in e.msg
                            or "network error" in e.msg
                            or "Can't create a connection to host" in e.msg
                            or "Connection refused" in e.msg
                        ):
                            exe.reconnect_next = True
                        else:
                            exe.rollback_next = True
                        break
                else:
                    thread_name = threading.current_thread().getName()
                    print(
                        f"[{thread_name}][{exe.db.name()}] Query failed: {e.query} {e.msg}"
                    )
                    raise
