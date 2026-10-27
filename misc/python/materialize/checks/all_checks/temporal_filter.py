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


class TemporalFilter(Check):
    """Temporal filters: mz_now() in WHERE clauses of maintained views, and
    the pushdown helper try_parse_monotonic_iso8601_timestamp.

    Determinism comes from using timestamps in the far past and far future,
    so where a row falls relative to mz_now() never changes during a test
    run.
    """

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE temporal_filter_table (id INT, expiry TIMESTAMPTZ, expiry_text STRING)
            > INSERT INTO temporal_filter_table VALUES
              (1, '2000-01-01 00:00:00+00', '2000-01-01T00:00:00.000Z'),
              (2, '2099-01-01 00:00:00+00', '2099-01-01T00:00:00.000Z')

            > CREATE MATERIALIZED VIEW temporal_filter_live1 AS
              SELECT id FROM temporal_filter_table
              WHERE mz_now() <= expiry
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO temporal_filter_table VALUES
                  (3, '2000-06-01 00:00:00+00', '2000-06-01T00:00:00.000Z'),
                  (4, '2099-06-01 00:00:00+00', '2099-06-01T00:00:00.000Z')

                # A row must be past its start and before its expiry. The
                # start is clamped to stay after the Unix epoch: casting
                # pre-1970 timestamps to mz_timestamp errors.
                > CREATE MATERIALIZED VIEW temporal_filter_window1 AS
                  SELECT id FROM temporal_filter_table
                  WHERE mz_now() >= greatest(expiry - INTERVAL '200 years', TIMESTAMPTZ '1970-01-02')
                    AND mz_now() < expiry
                """,
                """
                > INSERT INTO temporal_filter_table VALUES
                  (5, '2000-12-01 00:00:00+00', 'not-a-timestamp'),
                  (6, '2099-12-01 00:00:00+00', '2099-12-01T00:00:00.000Z')

                # try_parse_monotonic_iso8601_timestamp returns NULL for
                # unparseable input, dropping such rows from the filter.
                > CREATE MATERIALIZED VIEW temporal_filter_pushdown1 AS
                  SELECT id FROM temporal_filter_table
                  WHERE mz_now() <= try_parse_monotonic_iso8601_timestamp(expiry_text)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM temporal_filter_live1
            2
            4
            6

            > SELECT * FROM temporal_filter_window1
            2
            4
            6

            > SELECT * FROM temporal_filter_pushdown1
            2
            4
            6

            > SELECT id, try_parse_monotonic_iso8601_timestamp(expiry_text) IS NULL AS unparseable
              FROM temporal_filter_table
            1 false
            2 false
            3 false
            4 false
            5 true
            6 false
            """))
