# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class ReadThenWriteForeignRead(Check):
    """DELETE, UPDATE and INSERT ... SELECT whose selection reads objects other
    than the write target.

    Which of the two sequencing paths runs a read-then-write is decided once per
    process at startup, so the phases of a restart or upgrade scenario can take
    different ones, and the final state has to be the same either way. Reading a
    second table and a materialized view is what gives the mutation read
    dependencies its write target does not have, the shape that puts a foreign
    collection under a statement that writes elsewhere."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                > CREATE TABLE rtw_target (key INTEGER, val INTEGER);
                > INSERT INTO rtw_target SELECT generate_series, 0 FROM generate_series(1, 1000);

                > CREATE TABLE rtw_filter (key INTEGER);
                > INSERT INTO rtw_filter SELECT generate_series * 4 FROM generate_series(1, 250);

                > CREATE MATERIALIZED VIEW rtw_filter_mv AS SELECT key FROM rtw_filter WHERE key % 8 = 0;
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > UPDATE rtw_target SET val = val + 1 WHERE key IN (SELECT key FROM rtw_filter_mv);

                > DELETE FROM rtw_target WHERE key IN (SELECT key FROM rtw_filter WHERE key % 8 = 4);
                """,
                """
                > UPDATE rtw_target SET val = val + 1 WHERE key IN (SELECT key FROM rtw_filter_mv);

                > INSERT INTO rtw_target SELECT key, 9 FROM rtw_filter_mv;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        # The 250 multiples of 4 split evenly: the odd multiples were deleted,
        # the even ones (the materialized view's rows) were updated twice and
        # then inserted a second time at val 9.
        return Testdrive(dedent("""
                > SELECT val, count(*), count(DISTINCT key) FROM rtw_target GROUP BY val ORDER BY val;
                0 750 750
                2 125 125
                9 125 125

                > SELECT count(*), min(key), max(key) FROM rtw_target;
                1000 1 1000
            """))
