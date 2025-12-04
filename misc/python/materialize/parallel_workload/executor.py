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
from typing import TYPE_CHECKING, Any, TextIO

import psycopg
import requests
import websocket

from materialize.data_ingest.query_error import QueryError
from materialize.parallel_workload.settings import Scenario

if TYPE_CHECKING:
    from materialize.parallel_workload.database import Database

logging: TextIO | None
lock: threading.Lock


def initialize_logging() -> None:
    global logging, lock
    logging = open("parallel-workload-queries.log", "w")
    lock = threading.Lock()


class Http(Enum):
    NO = 0
    RANDOM = 1
    YES = 2


class Executor:
    rng: random.Random
    cur: psycopg.Cursor
    ws: websocket.WebSocket | None
    pg_pid: int
    # Used by INSERT action to prevent writing into different tables in the same transaction
    insert_table: int | None
    db: "Database"
    reconnect_next: bool
    rollback_next: bool
    last_log: str
    last_status: str
    action_run_since_last_commit_rollback: bool
    autocommit: bool
    allow_bad: bool

    def __init__(
        self,
        rng: random.Random,
        cur: psycopg.Cursor,
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
        self.last_status = ""
        self.action_run_since_last_commit_rollback = False
        self.use_ws = self.rng.choice([True, False]) if self.ws else False
        self.autocommit = cur.connection.autocommit
        self.mz_service = "materialized"
        self.allow_bad = False

        self.keywords = [
            "+",
            "-",
            "*",
            "/",
            "%",
            "+",
            "-",
            "~",
            "=",
            "<>",
            "!=",
            "<",
            "<=",
            ">",
            ">=",
            "&",
            "|",
            "#",
            "^",
            "<<",
            ">>",
            "||",
            "~",
            "!~",
            "~*",
            "!~*",
            "@>",
            "<@",
            "&&",
            "(",
            ")",
            ";",
            "0",
            '""',
            "NULL",
        ]
        with open("src/sql-lexer/src/keywords.txt") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                self.keywords.append(line.upper())

    def set_isolation(self, level: str) -> None:
        self.execute(f"SET TRANSACTION_ISOLATION TO '{level}'")

    def commit(self, http: Http = Http.RANDOM) -> None:
        self.insert_table = None
        self.execute("commit")
        # TODO(def-): Enable when things are stable
        # self.use_ws = self.rng.choice([True, False]) if self.ws else False

    def rollback(self, http: Http = Http.RANDOM) -> None:
        self.insert_table = None
        try:
            if self.use_ws and http != Http.NO:
                self.execute("rollback")
            else:
                self.log("rollback")
                self.cur.connection.rollback()
        except QueryError:
            raise
        except Exception as e:
            raise QueryError(str(e), "rollback")
        # TODO(def-): Enable when things are stable
        # self.use_ws = self.rng.choice([True, False]) if self.ws else False

    def log(self, msg: str) -> None:
        global logging, lock

        if not logging:
            return

        thread_name = threading.current_thread().getName()
        self.last_log = msg
        self.last_status = "logged"

        with lock:
            print(f"[{thread_name}][{self.mz_service}] {msg}", file=logging)
            logging.flush()

    def copy(
        self,
        query: str,
        rows: list[Any],
        cluster_replica: str | None = None,
    ) -> None:
        query += ";"
        self.log(f"{query} ({rows})")

        try:
            try:
                if cluster_replica:
                    self.cur.execute(
                        f"SET cluster_replica = {cluster_replica}".encode()
                    )
                with self.cur.copy(query.encode()) as copy:
                    for row in rows:
                        copy.write_row(row)
            except Exception as e:
                raise QueryError(str(e), query)

            self.action_run_since_last_commit_rollback = True
        finally:
            self.last_status = "finished"
            if cluster_replica:
                self.cur.execute("RESET cluster_replica")

    def execute(
        self,
        query: str,
        extra_info: str = "",
        explainable: bool = False,
        http: Http = Http.NO,
        fetch: bool = False,
        cluster_replica: str | None = None,
    ) -> None:
        is_http = (
            http == Http.RANDOM and self.rng.choice([True, False])
        ) or http == Http.YES
        if explainable and self.rng.choice([True, False]):
            query = f"EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR {query}"
        query += ";"
        extra_info_str = f" ({extra_info})" if extra_info else ""
        use_ws = self.use_ws and http != Http.NO
        http_str = " [HTTP]" if is_http else " [WS]" if use_ws and self.ws else ""
        self.log(f"{query}{extra_info_str}{http_str}")
        self.last_status = "running"

        if self.allow_bad and self.rng.choice([True, False]):
            parts = query.split(" ")
            idx = self.rng.randrange(len(parts))
            parts[idx] = str(
                self.rng.choice(
                    self.keywords
                    if self.rng.choice([True, False])
                    else self.db.db_objects()
                )
            )
            query = " ".join(parts)

        try:
            if not is_http:
                if use_ws and self.ws:
                    try:
                        self.ws.send(json.dumps({"queries": [{"query": query}]}))
                    except Exception as e:
                        raise QueryError(str(e), query)
                else:
                    try:
                        if cluster_replica:
                            self.cur.execute(
                                f"SET cluster_replica = {cluster_replica}".encode()
                            )
                        if query == "commit;":
                            self.log("commit")
                            self.cur.connection.commit()
                        elif query == "rollback;":
                            self.log("rollback")
                            self.cur.connection.rollback()
                        else:
                            self.cur.execute(query.encode())
                    except Exception as e:
                        raise QueryError(str(e), query)

                self.action_run_since_last_commit_rollback = True

                if use_ws and self.ws:
                    error = None
                    while True:
                        try:
                            result = json.loads(self.ws.recv())
                        except (
                            websocket._exceptions.WebSocketConnectionClosedException
                        ) as e:
                            raise QueryError(str(e), query)

                        result_type = result["type"]

                        if result_type in (
                            "CommandStarting",
                            "CommandComplete",
                            "Notice",
                            "Rows",
                            "Row",
                            "ParameterStatus",
                        ):
                            continue
                        elif result_type == "Error":
                            error = QueryError(
                                f"""WS {result["payload"]["code"]}: {result["payload"]["message"]}
    {result["payload"].get("details", "")}""",
                                query,
                            )
                        elif result_type == "ReadyForQuery":
                            if error:
                                raise error
                            break
                        else:
                            raise RuntimeError(
                                f"Unexpected result type: {result_type} in: {result}"
                            )

                if fetch and not use_ws:
                    try:
                        self.cur.fetchall()
                    except psycopg.DataError:
                        # We don't care about psycopg being unable to parse, examples:
                        # date too large (after year 10K): '97940-08-25'
                        # timestamp too large (after year 10K): '10876-06-20 00:00:00'
                        # can't parse interval '-178956970 years -8 months -2147483648 days -2562047788:00:54.775808': days=1252674755; must have magnitude <= 999999999
                        pass

                return

            try:
                result = requests.post(
                    f"http://{self.db.host}:{self.db.ports['http' if self.mz_service == 'materialized' else 'http2']}/api/sql",
                    data=json.dumps({"query": query}),
                    headers={"content-type": "application/json"},
                    timeout=self.rng.uniform(0, 10),
                )
                if result.status_code != 200:
                    raise QueryError(
                        f"{result.status_code}: {result.text}", f"HTTP query: {query}"
                    )
                for result in result.json()["results"]:
                    if "error" in result:
                        raise QueryError(
                            f"HTTP {result['error']['code']}: {result['error']['message']}\n{result['error'].get('detail', '')}",
                            query,
                        )
            except requests.exceptions.ReadTimeout as e:
                raise QueryError(f"HTTP read timeout: {e}", query)
            except requests.exceptions.ConnectionError:
                # Expected when Mz is killed
                if self.db.scenario not in (
                    Scenario.Kill,
                    Scenario.BackupRestore,
                    Scenario.ZeroDowntimeDeploy,
                ):
                    raise
        finally:
            self.last_status = "finished"
            if cluster_replica:
                self.cur.execute("RESET cluster_replica")
