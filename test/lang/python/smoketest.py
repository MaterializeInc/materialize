# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from psycopg3.oids import builtins
import psycopg2
import psycopg3
import sqlalchemy
import unittest

MATERIALIZED_URL = "postgresql://localhost:6875/materialize"


class SmokeTest(unittest.TestCase):
    def test_sqlalchemy(self):
        engine = sqlalchemy.engine.create_engine(MATERIALIZED_URL)
        results = [[c1, c2] for c1, c2 in engine.execute("VALUES (1, 2), (3, 4)")]
        self.assertEqual(results, [[1, 2], [3, 4]])

    def test_psycopg2_tail(self):
        """Test TAIL with psycopg2 via server cursors."""
        with psycopg2.connect(MATERIALIZED_URL) as conn:
            with conn.cursor() as cur:
                # Create a table with one row of data.
                cur.execute("CREATE TABLE psycopg2_tail (a int, b text)")
                cur.execute("INSERT INTO psycopg2_tail VALUES (1, 'a')")

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
                    with conn2.cursor() as cur2:
                        cur2.execute("INSERT INTO psycopg2_tail VALUES (2, 'b')")

                # Validate the new row, again ignoring the timestamp column.
                cur.execute("FETCH ALL cur")
                (ts, diff, a, b) = cur.fetchone()
                self.assertEqual(diff, 1)
                self.assertEqual(a, 2)
                self.assertEqual(b, "b")
                self.assertEqual(cur.fetchone(), None)

    def test_psycopg3_tail(self):
        """Test tail with psycopg3 via its new binary COPY decoding support."""
        conn = psycopg3.connect(MATERIALIZED_URL)
        with conn.cursor() as cur:
            # Create a table with one row of data.
            cur.execute("CREATE TABLE psycopg3_tail (a int, b text)")
            cur.execute("INSERT INTO psycopg3_tail VALUES (1, 'a')")

            # Start a tail using the binary copy protocol.
            with cur.copy(
                "COPY (TAIL psycopg3_tail) TO STDOUT (FORMAT BINARY)"
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
                    with conn2.cursor() as cur2:
                        cur2.execute("INSERT INTO psycopg3_tail VALUES (2, 'b')")

                # Validate the new row, again ignoring the timestamp column.
                (ts, diff, a, b) = copy.read_row()
                self.assertEqual(diff, 1)
                self.assertEqual(a, 2)
                self.assertEqual(b, "b")

                # The tail won't end until we send a cancel request.
                conn.cancel()
                self.assertEqual(copy.read_row(), None)

        # TODO(benesch): it should be possible to use a `with` context manager
        # here, but doing so triggers a commit when the context manager exits,
        # and that commit fails with a libpq "operation already in progress"
        # error.
        conn.close()
