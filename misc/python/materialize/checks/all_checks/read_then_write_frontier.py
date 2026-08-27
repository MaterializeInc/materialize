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

# NOTE: Dedicated schema. Until its near refresh the view below is unreadable,
# and a transaction's timedomain spans every collection in the queried schemas,
# so in the default schema that window would block unrelated checks.


class ReadThenWriteFarFrontier(Check):
    """Read-then-writes whose selection reads a far-future write frontier.

    A REFRESH materialized view settles until its next refresh, so its frontier
    legitimately sits far out while the target table's upper is near the clock.
    The write timestamp must still come from the timeline's oracle. Taking it
    from the frontier ratchets the oracle into the future, where it is monotone
    and durable, so every later write and strict-serializable read blocks until
    the clock catches up, restarts included.

    NOTE: that failure is environment-wide. A run where this check passes its
    write and every other check then times out is this check's finding.

    `INSERT ... SELECT` isolates it, since the target is written but not read.
    The `UPDATE` and `DELETE` read their target too, which pulls the frontier
    back to the clock: a far-future input must change neither the timestamp nor
    the answer. `serializable` never consults the oracle, so there the write is
    invisible rather than slow, which is what the read-back pins.
    """

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SCHEMA rtw_frontier_schema

            > CREATE TABLE rtw_frontier_schema.source (f1 INTEGER)
            > INSERT INTO rtw_frontier_schema.source VALUES (1), (2), (3)

            > CREATE TABLE rtw_frontier_schema.destination (f1 INTEGER, phase TEXT)

            > CREATE MATERIALIZED VIEW rtw_frontier_schema.frozen_mv
              WITH (REFRESH AT mz_now()::text::int8 + 2000, REFRESH AT '3000-01-01')
              AS SELECT f1 FROM rtw_frontier_schema.source

            # Parks until the near refresh, after which the contents are fixed
            # at these three rows: the only later refresh is in the year 3000.
            > SELECT count(*) FROM rtw_frontier_schema.frozen_mv
            3

            > INSERT INTO rtw_frontier_schema.destination SELECT f1, 'initialize' FROM rtw_frontier_schema.frozen_mv
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO rtw_frontier_schema.source VALUES (4), (5)

                > INSERT INTO rtw_frontier_schema.destination SELECT f1, 'manipulate1' FROM rtw_frontier_schema.frozen_mv

                # An UPDATE reads its target too, so the table pulls this
                # selection's frontier back to the clock.
                > UPDATE rtw_frontier_schema.destination SET f1 = f1 + 10
                  WHERE phase = 'initialize' AND f1 IN (SELECT f1 FROM rtw_frontier_schema.frozen_mv)
                """,
                """
                > INSERT INTO rtw_frontier_schema.source VALUES (6), (7)

                > INSERT INTO rtw_frontier_schema.destination SELECT f1, 'manipulate2' FROM rtw_frontier_schema.frozen_mv

                > DELETE FROM rtw_frontier_schema.destination
                  WHERE phase = 'manipulate1' AND f1 IN (SELECT f1 FROM rtw_frontier_schema.frozen_mv)

                # A serializable read picks a timestamp near the clock, so it
                # sees this write back only if the write landed near it too.
                > SET transaction_isolation = 'serializable'

                > INSERT INTO rtw_frontier_schema.destination SELECT f1, 'serializable' FROM rtw_frontier_schema.frozen_mv

                > SELECT count(*) FROM rtw_frontier_schema.destination WHERE phase = 'serializable'
                3

                > RESET transaction_isolation
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT phase, count(*), sum(f1) FROM rtw_frontier_schema.destination GROUP BY phase ORDER BY phase
            initialize 3 36
            manipulate2 3 6
            serializable 3 6

            # A write committed at the view's frontier leaves the oracle in the
            # year 3000, where this read blocks: a timeout is the same finding.
            > SELECT mz_now()::text::bigint - (extract(epoch FROM now()) * 1000)::bigint < 60000
            true

            # TEMPORARY so a second validate() repeats rather than accumulates.
            > CREATE TEMPORARY TABLE rtw_frontier_probe (f1 INTEGER)

            > INSERT INTO rtw_frontier_probe SELECT f1 FROM rtw_frontier_schema.frozen_mv

            > SELECT count(*), sum(f1) FROM rtw_frontier_probe
            3 6

            # The timeline still takes a blind write.
            > INSERT INTO rtw_frontier_probe VALUES (100)

            > SELECT count(*) FROM rtw_frontier_probe
            4

            > DROP TABLE rtw_frontier_probe
            """))
