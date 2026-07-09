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

import psycopg
import websocket

from materialize.data_ingest.query_error import QueryError
from materialize.mzcompose.composition import Composition
from materialize.parallel_workload.action import (
    Action,
    ActionList,
    ReconnectAction,
    ws_connect,
)
from materialize.parallel_workload.database import Database
from materialize.parallel_workload.executor import Executor


class Worker:
    rng: random.Random
    action_list: ActionList | None
    actions: list[Action]
    weights: list[float]
    end_time: float
    num_queries: Counter[type[Action]]
    num_successes: Counter[type[Action]]
    num_skips: Counter[type[Action]]
    autocommit: bool
    system: bool
    exe: Executor | None
    ignored_errors: defaultdict[str, Counter[type[Action]]]
    composition: Composition | None
    occurred_exception: Exception | None

    def __init__(
        self,
        rng: random.Random,
        actions: list[Action],
        weights: list[float],
        end_time: float,
        autocommit: bool,
        system: bool,
        composition: Composition | None,
        action_list: ActionList | None = None,
    ):
        self.rng = rng
        self.action_list = action_list
        self.actions = actions
        self.weights = weights
        self.end_time = end_time
        self.num_queries = Counter()
        # Unlike num_queries, these are never cleared: they feed the
        # end-of-run action coverage check.
        self.num_successes = Counter()
        self.num_skips = Counter()
        self.autocommit = autocommit
        self.system = system
        self.ignored_errors = defaultdict(Counter)
        self.composition = composition
        self.occurred_exception = None
        self.exe = None

    def run(
        self, host: str, pg_port: int, http_port: int, user: str, database: Database
    ) -> None:
        # In scenarios with kills and deploys materialized can go down at any
        # point during the setup, keep retrying.
        for i in range(300):
            try:
                self.conn = psycopg.connect(
                    host=host, port=pg_port, user=user, dbname="materialize"
                )
                self.conn.autocommit = self.autocommit
                cur = self.conn.cursor()
                ws = websocket.WebSocket()
                ws_conn_id, ws_secret_key = ws_connect(ws, host, http_port, user)
                self.exe = Executor(self.rng, cur, ws, database, user=user)
                self.exe.set_isolation("SERIALIZABLE")
                cur.execute("SET auto_route_catalog_queries TO false")
                if self.exe.use_ws:
                    self.exe.pg_pid = ws_conn_id
                else:
                    cur.execute("SELECT pg_backend_pid()")
                    self.exe.pg_pid = cur.fetchall()[0][0]
            except Exception:
                if time.time() > self.end_time:
                    return
                if i == 299:
                    raise
                time.sleep(1)
            else:
                break
        assert self.exe

        while time.time() < self.end_time:
            action = self.rng.choices(self.actions, self.weights)[0]
            if not action.applicable(self.exe):
                continue
            if self.exe.rollback_next:
                try:
                    self.exe.rollback()
                except QueryError:
                    # ROLLBACK can itself fail, e.g. cancelled by
                    # `pg_cancel_backend` or on a broken WS session. Force a
                    # reconnect rather than leaving a session with an open
                    # aborted transaction behind.
                    self.exe.reconnect_next = True
                    self.exe.rollback_next = False
                    continue
                self.exe.rollback_next = False
            if self.exe.reconnect_next:
                self.exe.reconnect_next = False
                # Run as its own action so failures are attributed to
                # ReconnectAction, not to `action`, which hasn't run yet.
                self.run_action(
                    ReconnectAction(self.rng, self.composition, random_role=False)
                )
                if self.exe.reconnect_next or self.exe.rollback_next:
                    # Reconnecting failed with an ignored error. Always retry
                    # the reconnect, never fall through to the action: the
                    # old session may hold an aborted transaction that fails
                    # all statements.
                    self.exe.reconnect_next = True
                    self.exe.rollback_next = False
                    time.sleep(1)
                    continue
            self.run_action(action)

        self.exe.cur.connection.close()
        if self.exe.ws:
            self.exe.ws.close()

    def run_action(self, action: Action) -> None:
        assert self.exe
        try:
            if action.run(self.exe):
                self.num_queries[type(action)] += 1
                self.num_successes[type(action)] += 1
            else:
                self.num_skips[type(action)] += 1
        except QueryError as e:
            self.num_queries[type(action)] += 1
            # TODO(def-): Reduce number of errors for temp tables/views? At
            # least the errors will be fast, so maybe not worth it
            # if "temp" in e.msg:
            #     print(e.query)
            #     print(e.msg)
            for error_to_ignore in action.errors_to_ignore(self.exe):
                if error_to_ignore in e.msg:
                    self.ignored_errors[error_to_ignore][type(action)] += 1
                    if (
                        "Please disconnect and re-connect" in e.msg
                        or "server closed the connection unexpectedly" in e.msg
                        or "Can't create a connection to host" in e.msg
                        or "Connection refused" in e.msg
                        or "the connection is lost" in e.msg
                        or "connection in transaction status INERROR" in e.msg
                    ):
                        self.exe.reconnect_next = True
                    else:
                        self.exe.rollback_next = True
                    break
            else:
                thread_name = threading.current_thread().getName()
                self.occurred_exception = e
                print(f"+++ [{thread_name}] Query failed: {e.query} {e.msg}")
                raise
        except Exception as e:
            self.occurred_exception = e
            raise e
