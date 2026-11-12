# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Compaction evidence and stalled SQL transport regression checks."""

import importlib.util
import socket
import struct
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location(
    "replica_isolation_workflow", Path(__file__).with_name("mzcompose.py")
)
assert spec is not None and spec.loader is not None
workflow = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workflow)


class CompactionTest(unittest.TestCase):
    def test_every_object_must_advance_from_a_known_baseline(self):
        check = workflow.CompactionCheck("cluster.replica")
        check.sample = Mock(
            side_effect=[
                {"a": None, "b": 10},
                {"a": 10, "b": 10},
                {"a": 10, "b": 10},
                {"a": 11, "b": None},
                {"a": 11, "b": 10},
                {"a": 11, "b": 11},
            ]
        )
        self.assertEqual([check.check(None) for _ in range(6)], [False] * 5 + [True])

    def test_missing_catalog_mapping_is_rejected(self):
        check = workflow.ArrangedIntro("cluster.replica")
        check.query = Mock(side_effect=[[("u1",)], [("si1", "si2"), ("si3", None)]])
        with self.assertRaisesRegex(AssertionError, "Missing global ID mappings"):
            check.prepare(None, time.monotonic() + 10)

    def test_sql_deadline_closes_without_waiting_for_server_cancellation(self):
        # Complete startup, then never answer the query or a cancel request.
        # A peer-side timeout prevents a broken client deadline hanging this test.
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(5)
        received_query = threading.Event()
        closed = threading.Event()
        errors = []

        def serve():
            try:
                with listener.accept()[0] as conn:
                    conn.settimeout(5)
                    with conn.makefile("rb") as stream:
                        length = struct.unpack("!I", stream.read(4))[0]
                        self.assertEqual(len(stream.read(length - 4)), length - 4)
                        startup = b"R" + struct.pack("!II", 8, 0)
                        for name, value in (
                            (b"server_version", b"17.0"),
                            (b"client_encoding", b"UTF8"),
                        ):
                            payload = name + b"\0" + value + b"\0"
                            startup += (
                                b"S" + struct.pack("!I", len(payload) + 4) + payload
                            )
                        # A cancellation-capable connection. A second connection
                        # would enter the listener backlog but receive no reply.
                        startup += b"K" + struct.pack("!III", 12, 1, 2)
                        conn.sendall(startup + b"Z" + struct.pack("!I", 5) + b"I")
                        received = bytearray()
                        while chunk := stream.read1(4096):
                            received.extend(chunk)
                            if b"SELECT 1" in received:
                                received_query.set()
                        closed.set()
            except BaseException as error:
                errors.append(error)

        server = threading.Thread(target=serve)
        server.start()
        try:
            check = workflow.CompactionCheck("cluster.replica")
            started = time.monotonic()
            check.deadline = started + 1
            c = SimpleNamespace(port=lambda *args: listener.getsockname()[1])
            with self.assertRaises(TimeoutError):
                check.query(c, "SELECT 1")
            self.assertLess(time.monotonic() - started, 3)
            self.assertTrue(received_query.is_set())
        finally:
            server.join(6)
            listener.close()
        self.assertFalse(server.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(closed.is_set())


if __name__ == "__main__":
    unittest.main()
