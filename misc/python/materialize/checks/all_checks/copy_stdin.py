# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from typing import Any

from materialize.checks.actions import PyAction
from materialize.checks.checks import Check


class CopyFromStdin(Check):
    """COPY ... FROM STDIN (TEXT and CSV formats with options) and COPY ...
    TO STDOUT, which speak the pgwire COPY protocol and are therefore driven
    through a direct connection instead of testdrive."""

    def initialize(self) -> PyAction:
        def run(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE copy_stdin_table (f1 INT, f2 TEXT)")
                with cur.copy("COPY copy_stdin_table FROM STDIN") as copy:
                    copy.write("1\tone\n")
                    copy.write("2\ttwo\n")

        return PyAction(run)

    def manipulate(self) -> list[PyAction]:
        def phase0(conn: Any) -> None:
            with conn.cursor() as cur:
                with cur.copy(
                    "COPY copy_stdin_table FROM STDIN WITH (DELIMITER '|', NULL 'NIL')"
                ) as copy:
                    copy.write("3|three\n")
                    copy.write("4|NIL\n")

        def phase1(conn: Any) -> None:
            with conn.cursor() as cur:
                with cur.copy(
                    "COPY copy_stdin_table FROM STDIN WITH (FORMAT CSV)"
                ) as copy:
                    copy.write('5,"fi,ve"\n')
                    copy.write("6,\n")
                with cur.copy(
                    "COPY copy_stdin_table FROM STDIN WITH (FORMAT CSV, QUOTE 'q', ESCAPE 'e')"
                ) as copy:
                    # The input deliberately has no data after the closing
                    # quote (accepted instead of rejected, SS-352) and no
                    # escape character before an ordinary character (swallowed
                    # instead of literal, SS-353).
                    copy.write("7,qseqvq\n")

        return [PyAction(phase0), PyAction(phase1)]

    def validate(self) -> PyAction:
        def run(conn: Any) -> None:
            expected = [
                (1, "one"),
                (2, "two"),
                (3, "three"),
                (4, None),
                (5, "fi,ve"),
                # In CSV format an empty unquoted field is NULL.
                (6, None),
                # The escape character 'e' before the quote character 'q'
                # yields a literal q. See SS-352 and SS-353 for the input
                # shapes this row avoids.
                (7, "sqv"),
            ]
            with conn.cursor() as cur:
                cur.execute("SELECT f1, f2 FROM copy_stdin_table ORDER BY f1")
                rows = cur.fetchall()
                assert [
                    tuple(row) for row in rows
                ] == expected, f"unexpected rows: {rows}"

                with cur.copy(
                    "COPY (SELECT f1, f2 FROM copy_stdin_table WHERE f1 <= 2) TO STDOUT"
                ) as copy:
                    data = b"".join(bytes(chunk) for chunk in copy)
                assert sorted(data.splitlines()) == [
                    b"1\tone",
                    b"2\ttwo",
                ], f"unexpected TEXT output: {data!r}"

                with cur.copy(
                    "COPY (SELECT f1, f2 FROM copy_stdin_table WHERE f1 = 5) TO STDOUT WITH (FORMAT CSV)"
                ) as copy:
                    data = b"".join(bytes(chunk) for chunk in copy)
                assert data == b'5,"fi,ve"\n', f"unexpected CSV output: {data!r}"

        return PyAction(run)
