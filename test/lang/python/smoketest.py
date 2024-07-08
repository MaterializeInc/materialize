# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import unittest

import psycopg
import psycopg2  # type: ignore
import sqlalchemy  # type: ignore

MATERIALIZED_URL = "postgresql://materialize@materialized:6875/materialize"


class SmokeTest(unittest.TestCase):
    def test_connection_options(self) -> None:
        with psycopg.connect(MATERIALIZED_URL + "?options=--cluster%3Db") as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW cluster")
                row = cur.fetchone()
                self.assertEqual(row, ("b",))

    def test_custom_types(self) -> None:
        with psycopg.connect(MATERIALIZED_URL, autocommit=True) as conn:
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
                    psycopg.errors.ProtocolViolation,
                    "binary encoding of list types is not implemented",
                ):
                    cur.execute("SELECT LIST[1, 2, 3]")

                with self.assertRaisesRegex(
                    psycopg.errors.ProtocolViolation,
                    "binary encoding of map types is not implemented",
                ):
                    cur.execute("SELECT '{a => 1, b => 2}'::map[text => int]")

    def test_arrays(self) -> None:
        with psycopg.connect(MATERIALIZED_URL, autocommit=True) as conn:
            # Text roundtripping of a one-dimensional integer array is supported.
            with conn.cursor() as cur:
                cur.execute("SELECT %t", ([1, 2, 3],))
                row = cur.fetchone()
                self.assertEqual(row, ([1, 2, 3],))

            # Text roundtripping of a two-dimensional integer array is
            # not supported.
            with conn.cursor() as cur:
                cur.execute("SELECT %t", ([[1], [2], [3]],))
                row = cur.fetchone()
                self.assertEqual(row, ([[1], [2], [3]],))

            # Binary roundtripping is not.
            with conn.cursor(binary=True) as cur:
                with self.assertRaisesRegex(
                    psycopg.errors.InvalidParameterValue,
                    "input of array types is not implemented",
                ):
                    cur.execute("SELECT %b", ([1, 2, 3],))
                    row = cur.fetchone()
                    self.assertEqual(row, ([1, 2, 3],))

    def test_sqlalchemy(self) -> None:
        engine = sqlalchemy.engine.create_engine(MATERIALIZED_URL)
        results = [[c1, c2] for c1, c2 in engine.execute("VALUES (1, 2), (3, 4)")]
        self.assertEqual(results, [[1, 2], [3, 4]])

    def test_psycopg2_subscribe(self) -> None:
        """Test SUBSCRIBE with psycopg2 via server cursors."""
        conn = psycopg2.connect(MATERIALIZED_URL)
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            # Create a table with one row of data.
            cur.execute("CREATE TABLE psycopg2_subscribe (a int, b text)")
            cur.execute("INSERT INTO psycopg2_subscribe VALUES (1, 'a')")
            conn.set_session(autocommit=False)

            # Start SUBSCRIBE using the binary copy protocol.
            cur.execute("DECLARE cur CURSOR FOR SUBSCRIBE psycopg2_subscribe")
            cur.execute("FETCH ALL cur")

            # Validate the first row, but ignore the timestamp column.
            row = cur.fetchone()
            if row is not None:
                (ts, diff, a, b) = row
                self.assertEqual(diff, 1)
                self.assertEqual(a, 1)
                self.assertEqual(b, "a")
            else:
                self.fail("row is None")

            self.assertEqual(cur.fetchone(), None)

            # Insert another row from another connection to simulate an
            # update arriving.
            with psycopg2.connect(MATERIALIZED_URL) as conn2:
                conn2.set_session(autocommit=True)
                with conn2.cursor() as cur2:
                    cur2.execute("INSERT INTO psycopg2_subscribe VALUES (2, 'b')")

            # Validate the new row, again ignoring the timestamp column.
            cur.execute("FETCH ALL cur")
            row = cur.fetchone()
            assert row is not None

            (ts, diff, a, b) = row
            self.assertEqual(diff, 1)
            self.assertEqual(a, 2)
            self.assertEqual(b, "b")

            self.assertEqual(cur.fetchone(), None)

    def test_psycopg3_subscribe_copy(self) -> None:
        """Test SUBSCRIBE with psycopg3 via its new binary COPY decoding support."""
        with psycopg.connect(MATERIALIZED_URL) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Create a table with one row of data.
                cur.execute("CREATE TABLE psycopg3_subscribe_copy (a int, b text)")
                cur.execute("INSERT INTO psycopg3_subscribe_copy VALUES (1, 'a')")
                conn.autocommit = False

                # Start a subscribe using the binary copy protocol.
                with cur.copy(
                    "COPY (SUBSCRIBE psycopg3_subscribe_copy) TO STDOUT (FORMAT BINARY)"
                ) as copy:
                    copy.set_types(
                        [
                            psycopg.adapters.types["numeric"].oid,  # timestamp
                            psycopg.adapters.types["int8"].oid,  # diff
                            psycopg.adapters.types["int4"].oid,  # a column
                            psycopg.adapters.types["text"].oid,  # b column
                        ]
                    )

                    # Validate the first row, but ignore the timestamp column.
                    row = copy.read_row()
                    assert row is not None
                    (ts, diff, a, b) = row
                    self.assertEqual(diff, 1)
                    self.assertEqual(a, 1)
                    self.assertEqual(b, "a")

                    # Insert another row from another connection to simulate an
                    # update arriving.
                    with psycopg.connect(MATERIALIZED_URL) as conn2:
                        conn2.autocommit = True
                        with conn2.cursor() as cur2:
                            cur2.execute(
                                "INSERT INTO psycopg3_subscribe_copy VALUES (2, 'b')"
                            )

                    # Validate the new row, again ignoring the timestamp column.
                    row = copy.read_row()
                    assert row is not None
                    (ts, diff, a, b) = row
                    self.assertEqual(diff, 1)
                    self.assertEqual(a, 2)
                    self.assertEqual(b, "b")

                    # The subscribe won't end until we send a cancel request.
                    conn.cancel()
                    with self.assertRaises(psycopg.errors.QueryCanceled):
                        copy.read_row()

    def test_psycopg3_subscribe_stream(self) -> None:
        """Test subscribe with psycopg3 via its new streaming query support."""
        with psycopg.connect(MATERIALIZED_URL) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Create a table with one row of data.
                cur.execute("CREATE TABLE psycopg3_subscribe_stream (a int, b text)")
                cur.execute("INSERT INTO psycopg3_subscribe_stream VALUES (1, 'a')")
                conn.autocommit = False

                # Start a subscribe using the streaming query API.
                stream = cur.stream("SUBSCRIBE psycopg3_subscribe_stream")

                # Validate the first row, but ignore the timestamp column.
                (ts, diff, a, b) = next(stream)
                self.assertEqual(diff, 1)
                self.assertEqual(a, 1)
                self.assertEqual(b, "a")

                # Insert another row from another connection to simulate an
                # update arriving.
                with psycopg.connect(MATERIALIZED_URL) as conn2:
                    conn2.autocommit = True
                    with conn2.cursor() as cur2:
                        cur2.execute(
                            "INSERT INTO psycopg3_subscribe_stream VALUES (2, 'b')"
                        )

                # Validate the new row, again ignoring the timestamp column.
                (ts, diff, a, b) = next(stream)
                self.assertEqual(diff, 1)
                self.assertEqual(a, 2)
                self.assertEqual(b, "b")

                # The subscribe won't end until we send a cancel request.
                conn.cancel()
                with self.assertRaises(psycopg.errors.QueryCanceled):
                    next(stream)

    def test_psycopg3_subscribe_terminate_connection(self) -> None:
        """Test terminating a bare subscribe with psycopg3.

        This test ensures that Materialize notices a TCP connection close when a
        bare SUBSCRIBE statement (i.e., one not wrapped in a COPY statement) is
        producing no rows.
        """

        # Create two connections: one to create a subscription and one to
        # query metadata about the subscription.
        with psycopg.connect(MATERIALIZED_URL) as metadata_conn:
            with psycopg.connect(MATERIALIZED_URL) as subscribe_conn:
                try:
                    metadata_session_id = metadata_conn.pgconn.backend_pid
                    subscribe_session_id = subscribe_conn.pgconn.backend_pid

                    # Subscribe to the list of active subscriptions in
                    # Materialize.
                    metadata = metadata_conn.cursor().stream(
                        "SUBSCRIBE (SELECT s.connection_id FROM mz_internal.mz_subscriptions b JOIN mz_internal.mz_sessions s ON s.id = b.session_id)"
                    )

                    # Ensure we see our own subscription in `mz_subscriptions`.
                    (_ts, diff, pid) = next(metadata)
                    self.assertEqual(int(pid), metadata_session_id)
                    self.assertEqual(diff, 1)

                    # Create a dummy subscribe that we know will only ever
                    # produce a single row, but, as far as Materialize can tell,
                    # has the potential to produce future updates. This ensures
                    # the SUBSCRIBE operation will be blocked inside of
                    # Materialize waiting for more rows.
                    #
                    # IMPORTANT: this must use a bare `SUBSCRIBE` statement,
                    # rather than a `SUBSCRIBE` inside of a `COPY` operation, to
                    # test the code path that previously had the bug.
                    stream = subscribe_conn.cursor().stream(
                        "SUBSCRIBE (SELECT * FROM mz_tables LIMIT 1)"
                    )
                    next(stream)

                    # Ensure we see the dummy subscription added to
                    # `mz_subscriptions`.
                    (_ts, diff, pid) = next(metadata)
                    self.assertEqual(int(pid), subscribe_session_id)
                    self.assertEqual(diff, 1)

                    # Kill the dummy subscription by forcibly closing the
                    # connection.
                    subscribe_conn.close()

                    # Ensure we see the dummy subscription removed from
                    # `mz_subscriptions`.
                    (_ts, diff, pid) = next(metadata)
                    self.assertEqual(int(pid), subscribe_session_id)
                    self.assertEqual(diff, -1)

                finally:
                    # Ensure the connections are always closed, even if an
                    # assertion fails partway through the test, as otherwise the
                    # `with` context manager will hang forever waiting for the
                    # subscribes to gracefully terminate, which they never will.
                    subscribe_conn.close()
                    metadata_conn.close()
