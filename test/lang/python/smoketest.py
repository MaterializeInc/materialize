# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import unittest

import psycopg2  # type: ignore
import psycopg3  # type: ignore
import sqlalchemy  # type: ignore
from psycopg3.oids import builtins  # type: ignore

MATERIALIZED_URL = "postgresql://materialize@materialized:6875/materialize"


class SmokeTest(unittest.TestCase):
    def test_custom_types(self) -> None:
        with psycopg3.connect(MATERIALIZED_URL, autocommit=True) as conn:
            # Text encoding of lists and maps is supported...
            with conn.cursor() as cur:
                cur.execute("SELECT LIST[1, 2, 3]")
                row = cur.fetchone()
                self.assertEqual(row, ("{1,2,3}",))

                cur.execute("SELECT '{a => 1, b => 2}'::map[text => int]")
                row = cur.fetchone()
                self.assertEqual(row, ("{a=>1,b=>2}",))

            # ...but binary encoding is not.
            with conn.cursor(binary=True) as cur:
                with self.assertRaisesRegex(
                    psycopg3.errors.ProtocolViolation,
                    "binary encoding of list types is not implemented",
                ):
                    cur.execute("SELECT LIST[1, 2, 3]")

                with self.assertRaisesRegex(
                    psycopg3.errors.ProtocolViolation,
                    "binary encoding of map types is not implemented",
                ):
                    cur.execute("SELECT '{a => 1, b => 2}'::map[text => int]")

    def test_sqlalchemy(self) -> None:
        engine = sqlalchemy.engine.create_engine(MATERIALIZED_URL)
        results = [[c1, c2] for c1, c2 in engine.execute("VALUES (1, 2), (3, 4)")]
        self.assertEqual(results, [[1, 2], [3, 4]])

    def test_psycopg2_tail(self) -> None:
        """Test TAIL with psycopg2 via server cursors."""
        with psycopg2.connect(MATERIALIZED_URL) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                # Create a table with one row of data.
                cur.execute("CREATE TABLE psycopg2_tail (a int, b text)")
                cur.execute("INSERT INTO psycopg2_tail VALUES (1, 'a')")
                conn.set_session(autocommit=False)

                # Start a tail using the binary copy protocol.
                cur.execute("DECLARE cur CURSOR FOR TAIL psycopg2_tail")
                cur.execute("FETCH ALL cur")

                # Validate the first row, but ignore the timestamp column.
                (ts, diff, a, b) = cur.fetchone()
                self.assertEqual(diff, 1)
                self.assertEqual(a, 1)
                self.assertEqual(b, "a")
                self.assertEqual(cur.fetchone(), None)

                # Insert another row from another connection to simulate an
                # update arriving.
                with psycopg2.connect(MATERIALIZED_URL) as conn2:
                    conn2.set_session(autocommit=True)
                    with conn2.cursor() as cur2:
                        cur2.execute("INSERT INTO psycopg2_tail VALUES (2, 'b')")

                # Validate the new row, again ignoring the timestamp column.
                cur.execute("FETCH ALL cur")
                (ts, diff, a, b) = cur.fetchone()
                self.assertEqual(diff, 1)
                self.assertEqual(a, 2)
                self.assertEqual(b, "b")
                self.assertEqual(cur.fetchone(), None)

    def test_psycopg3_tail_copy(self) -> None:
        """Test tail with psycopg3 via its new binary COPY decoding support."""
        with psycopg3.connect(MATERIALIZED_URL) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Create a table with one row of data.
                cur.execute("CREATE TABLE psycopg3_tail_copy (a int, b text)")
                cur.execute("INSERT INTO psycopg3_tail_copy VALUES (1, 'a')")
                conn.autocommit = False

                # Start a tail using the binary copy protocol.
                with cur.copy(
                    "COPY (TAIL psycopg3_tail_copy) TO STDOUT (FORMAT BINARY)"
                ) as copy:
                    copy.set_types(
                        [
                            builtins["numeric"].oid,  # timestamp
                            builtins["int8"].oid,  # diff
                            builtins["int4"].oid,  # a column
                            builtins["text"].oid,  # b column
                        ]
                    )

                    # Validate the first row, but ignore the timestamp column.
                    (ts, diff, a, b) = copy.read_row()
                    self.assertEqual(diff, 1)
                    self.assertEqual(a, 1)
                    self.assertEqual(b, "a")

                    # Insert another row from another connection to simulate an
                    # update arriving.
                    with psycopg3.connect(MATERIALIZED_URL) as conn2:
                        conn2.autocommit = True
                        with conn2.cursor() as cur2:
                            cur2.execute(
                                "INSERT INTO psycopg3_tail_copy VALUES (2, 'b')"
                            )

                    # Validate the new row, again ignoring the timestamp column.
                    (ts, diff, a, b) = copy.read_row()
                    self.assertEqual(diff, 1)
                    self.assertEqual(a, 2)
                    self.assertEqual(b, "b")

                    # The tail won't end until we send a cancel request.
                    conn.cancel()
                    with self.assertRaises(Exception) as context:
                        copy.read_row()
                    self.assertTrue(
                        "canceling statement due to user request"
                        in str(context.exception)
                    )

    # There might be problem with stream and the cancellation message. Skip until
    # resolved.
    @unittest.skip("https://github.com/psycopg/psycopg3/issues/30")
    def test_psycopg3_tail_stream(self) -> None:
        """Test tail with psycopg3 via its new streaming query support."""
        with psycopg3.connect(MATERIALIZED_URL) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Create a table with one row of data.
                cur.execute("CREATE TABLE psycopg3_tail_stream (a int, b text)")
                cur.execute("INSERT INTO psycopg3_tail_stream VALUES (1, 'a')")
                conn.autocommit = False

                # Start a tail using the streaming query API.
                stream = cur.stream("TAIL psycopg3_tail_stream")

                # Validate the first row, but ignore the timestamp column.
                (ts, diff, a, b) = next(stream)
                self.assertEqual(diff, 1)
                self.assertEqual(a, 1)
                self.assertEqual(b, "a")

                # Insert another row from another connection to simulate an
                # update arriving.
                with psycopg3.connect(MATERIALIZED_URL) as conn2:
                    conn2.autocommit = True
                    with conn2.cursor() as cur2:
                        cur2.execute("INSERT INTO psycopg3_tail_stream VALUES (2, 'b')")

                # Validate the new row, again ignoring the timestamp column.
                (ts, diff, a, b) = next(stream)
                self.assertEqual(diff, 1)
                self.assertEqual(a, 2)
                self.assertEqual(b, "b")

                # The tail won't end until we send a cancel request.
                conn.cancel()
                self.assertEqual(next(stream, None), None)
