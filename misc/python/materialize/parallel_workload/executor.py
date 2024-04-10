# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
import threading
from enum import Enum
from typing import TYPE_CHECKING, TextIO

import pg8000
import requests
import websocket

from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload.settings import Scenario

if TYPE_CHECKING:
    from materialize.parallel_workload.database import Database

logging: TextIO | None
lock: threading.Lock
websocket.enableTrace(True)


def initialize_logging() -> None:
    global logging, lock
    logging = open("parallel-workload-queries.log", "w")
    lock = threading.Lock()


class Http(Enum):
    NO = 0
    MAYBE = 1
    YES = 2


class Executor:
    rng: random.Random
    cur: pg8000.Cursor
    ws: websocket.WebSocket | None
    pg_pid: int
    # Used by INSERT action to prevent writing into different tables in the same transaction
    insert_table: int | None
    db: "Database"
    reconnect_next: bool
    rollback_next: bool
    last_log: str
    action_run_since_last_commit_rollback: bool

    def __init__(
        self,
        rng: random.Random,
        cur: pg8000.Cursor,
        ws: websocket.WebSocket | None,
        db: "Database",
    ):
        self.rng = rng
        self.cur = cur
        self.ws = ws
        self.db = db
        self.pg_pid = -1
        self.insert_table = None
        self.reconnect_next = True
        self.rollback_next = True
        self.last_log = ""
        self.action_run_since_last_commit_rollback = False
        self.use_ws = self.rng.choice([True, False]) if self.ws else False

    def set_isolation(self, level: str) -> None:
        self.execute(f"SET TRANSACTION_ISOLATION TO '{level}'")

    def commit(self) -> None:
        self.insert_table = None
        try:
            self.log("commit")
            if self.use_ws:
                self.execute("commit")
            else:
                self.cur._c.commit()
        except Exception as e:
            raise QueryError(str(e), "commit")
        self.use_ws = self.rng.choice([True, False]) if self.ws else False

    def rollback(self) -> None:
        self.insert_table = None
        try:
            self.log("rollback")
            if self.use_ws:
                self.execute("rollback")
            else:
                self.cur._c.rollback()
        except Exception as e:
            raise QueryError(str(e), "rollback")
        self.use_ws = self.rng.choice([True, False]) if self.ws else False

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
        self,
        query: str,
        extra_info: str = "",
        explainable: bool = False,
        http: Http = Http.NO,
        fetch: bool = False,
    ) -> None:
        is_http = (
            http == Http.MAYBE and self.rng.choice([True, False])
        ) or http == Http.YES
        if explainable and self.rng.choice([True, False]):
            query = f"EXPLAIN {query}"
        query += ";"
        extra_info_str = f" ({extra_info})" if extra_info else ""
        http_str = " (HTTP)" if is_http else ""
        self.log(f"{query}{extra_info_str}{http_str}")
        if not is_http:
            try:
                if self.use_ws and self.ws:
                    self.ws.send(json.dumps({"queries": [{"query": query}]}))
                else:
                    self.cur.execute(query)
            except Exception as e:
                raise QueryError(str(e), query)

            self.action_run_since_last_commit_rollback = True

            if fetch:
                if self.use_ws and self.ws:
                    self.ws.recv()
                else:
                    self.cur.fetchall()

            return

        try:
            result = requests.post(
                f"http://{self.db.host}:{self.db.ports['http']}/api/sql",
                data=json.dumps({"query": query}),
                headers={"content-type": "application/json"},
                timeout=self.rng.uniform(0, 10),
            )
            if result.status_code != 200:
                raise QueryError(
                    f"{result.status_code}: {result.text}", f"HTTP query: {query}"
                )
            r = result.json()["results"]
            assert len(r) == 1, r
            if "error" in r[0]:
                raise QueryError(
                    f"HTTP {r[0]['error']['code']}: {r[0]['error']['message']}\n{r[0]['error'].get('detail', '')}",
                    query,
                )
        except requests.exceptions.ReadTimeout:
            pass
        except requests.exceptions.ConnectionError:
            # Expected when Mz is killed
            if self.db.scenario not in (
                Scenario.Kill,
                Scenario.TogglePersistTxn,
                Scenario.BackupRestore,
            ):
                raise
