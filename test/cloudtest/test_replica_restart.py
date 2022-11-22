# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import threading
import time
from io import StringIO

from pg8000 import Connection

from materialize.cloudtest.application import MaterializeApplication


def query(conn: Connection, sql: str) -> None:
    # Wrap all exceptions so that when the connection is closed from the other
    # thread we don't panic the test.
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
    except:
        pass


def copy(conn: Connection, sql: str) -> None:
    try:
        conn.run(sql, stream=StringIO())
    except:
        pass


# Returns and consumes notices on conn until one contains `contains`.
def assert_notice(conn: Connection, contains: bytes) -> None:
    while True:
        try:
            notice = conn.notices.pop()
            if contains in notice[b"M"]:
                return
        except IndexError:
            pass
        time.sleep(0.2)


# Test that a crashed (and restarted) computed replica generates expected notice
# events.
def test_crash_computed(mz: MaterializeApplication) -> None:
    mz.environmentd.sql("DROP TABLE IF EXISTS t1 CASCADE")
    mz.environmentd.sql("CREATE TABLE t1 (f1 TEXT)")

    # For various query contexts, create a connection, run a query that'll never
    # finish in another thread, and examine its notices from this thread since
    # the queries block forever. The contexts here (SELECT stuck in pending,
    # direct SUBSCRIBE, SUBSCRIBE via COPY) are all separately implemented, so
    # need to be separately tested.
    c_select = mz.environmentd.sql_conn()
    t_select = threading.Thread(
        target=query,
        args=(
            c_select,
            "SELECT * FROM t1 AS OF 18446744073709551615",
        ),
    )
    t_select.start()

    c_subscribe = mz.environmentd.sql_conn()
    t_subscribe = threading.Thread(
        target=query,
        args=(
            c_subscribe,
            "SUBSCRIBE t1",
        ),
    )
    t_subscribe.start()

    c_copy = mz.environmentd.sql_conn()
    t_copy = threading.Thread(
        target=copy,
        args=(
            c_copy,
            "COPY (SUBSCRIBE t1) TO STDOUT",
        ),
    )
    t_copy.start()

    # Wait a teeny bit for the queries to be receiving notices.
    time.sleep(1)

    c_select.notices.clear()
    c_subscribe.notices.clear()
    c_copy.notices.clear()

    # Simulate an unexpected computed crash.
    pods = mz.kubectl("get", "pods", "-o", "custom-columns=:metadata.name")
    podcount = 0
    for pod in pods.splitlines():
        if "compute-cluster" in pod:
            mz.kubectl("delete", "pod", pod)
            podcount += 1
    assert podcount > 0

    # Wait for expected notices on all connections.
    msg = b"cluster replica default.r1 changed status to: NotReady"
    assert_notice(c_select, msg)
    assert_notice(c_subscribe, msg)
    assert_notice(c_copy, msg)

    # Cleanup for other tests.
    mz.environmentd.sql("DROP TABLE t1")

    # We need all the above threads to finish for the test to succeed.Close the
    # connections from this thread because pg8000 doesn't support cancellation
    # and dropping the table in mz doesn't complete the queries either.
    c_select.close()
    c_subscribe.close()
    c_copy.close()
