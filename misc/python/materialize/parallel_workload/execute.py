# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import threading
from typing import Optional, TextIO

import pg8000

log: Optional[TextIO] = None
lock = threading.Lock()


def initialize_logging() -> None:
    global log
    log = open("parallel-workload-queries.log", "w")


class QueryError(Exception):
    msg: str
    query: str

    def __init__(self, msg: str, query: str):
        self.msg = msg
        self.query = query


def execute(cur: pg8000.Cursor, query: str) -> None:
    query += ";"
    if log:
        thread_name = threading.current_thread().getName()
        with lock:
            print(f"[{thread_name}] {query}", file=log)
    try:
        cur.execute(query)
    except Exception as e:
        raise QueryError(str(e), query)
