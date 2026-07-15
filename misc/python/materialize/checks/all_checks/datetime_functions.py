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


class DateTimeFunctions(Check):
    """Date/time functions with dedicated documentation pages: extract,
    date_part, date_trunc, date_bin, datediff, to_char, AT TIME ZONE /
    timezone, and the justify_* family."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE datetime_fns_table (id INT, ts TIMESTAMP, iv INTERVAL)
            > INSERT INTO datetime_fns_table VALUES
              (1, '2024-02-15 13:45:30', '35 days'),
              (2, '2024-07-01 06:10:00', '27 hours')
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW datetime_fns_extract1 AS
                  SELECT
                    id,
                    EXTRACT(YEAR FROM ts) AS y,
                    EXTRACT(DOY FROM ts) AS doy,
                    EXTRACT(EPOCH FROM ts) AS epoch,
                    date_part('hour', ts) AS hr,
                    date_trunc('month', ts) AS mon,
                    date_bin('15 minutes', ts, '2001-01-01 00:00:00') AS bin
                  FROM datetime_fns_table

                > INSERT INTO datetime_fns_table VALUES (3, '2024-12-31 23:59:59', '1 month -1 hour')
                """,
                """
                > CREATE MATERIALIZED VIEW datetime_fns_convert1 AS
                  SELECT
                    id,
                    datediff('day', '2024-02-01 00:00:00'::timestamp, ts) AS dd,
                    to_char(ts, 'YYYY-MM-DD HH24:MI:SS') AS formatted,
                    (ts AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York' AS ny,
                    timezone('America/New_York', ts AT TIME ZONE 'UTC') AS ny2,
                    justify_days(iv) AS jd,
                    justify_hours(iv) AS jh,
                    justify_interval(iv) AS ji
                  FROM datetime_fns_table

                > INSERT INTO datetime_fns_table VALUES (4, '2025-01-01 00:00:00', '0 days')
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM datetime_fns_extract1
            1 2024 46 1708004730 13 "2024-02-01 00:00:00" "2024-02-15 13:45:00"
            2 2024 183 1719814200 6 "2024-07-01 00:00:00" "2024-07-01 06:00:00"
            3 2024 366 1735689599 23 "2024-12-01 00:00:00" "2024-12-31 23:45:00"
            4 2025 1 1735689600 0 "2025-01-01 00:00:00" "2025-01-01 00:00:00"

            > SELECT id, dd, formatted, ny, ny2 FROM datetime_fns_convert1
            1 14 "2024-02-15 13:45:30" "2024-02-15 08:45:30" "2024-02-15 08:45:30"
            2 151 "2024-07-01 06:10:00" "2024-07-01 02:10:00" "2024-07-01 02:10:00"
            3 334 "2024-12-31 23:59:59" "2024-12-31 18:59:59" "2024-12-31 18:59:59"
            4 335 "2025-01-01 00:00:00" "2024-12-31 19:00:00" "2024-12-31 19:00:00"

            > SELECT id, jd::text, jh::text, ji::text FROM datetime_fns_convert1
            1 "1 month 5 days" "35 days" "1 month 5 days"
            2 27:00:00 "1 day 03:00:00" "1 day 03:00:00"
            3 "1 month -01:00:00" "1 month -01:00:00" "29 days 23:00:00"
            4 00:00:00 00:00:00 00:00:00
            """))


class DateBinHopping(Check):
    """date_bin_hopping, which is gated behind its own feature flag."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET enable_date_bin_hopping = true

            > CREATE TABLE date_bin_hopping_table (ts TIMESTAMP, amount INT)
            > INSERT INTO date_bin_hopping_table VALUES ('2024-01-01 00:10:00', 1)
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO date_bin_hopping_table VALUES ('2024-01-01 00:20:00', 2)

                > CREATE MATERIALIZED VIEW date_bin_hopping_mv1 AS
                  SELECT hop, sum(amount) AS total
                  FROM date_bin_hopping_table,
                       date_bin_hopping('15 minutes', '30 minutes', ts) AS hop
                  GROUP BY hop
                """,
                """
                > INSERT INTO date_bin_hopping_table VALUES ('2024-01-01 00:40:00', 4)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT hop, total FROM date_bin_hopping_mv1
            "2023-12-31 23:45:00" 1
            "2024-01-01 00:00:00" 3
            "2024-01-01 00:15:00" 6
            "2024-01-01 00:30:00" 4
            """))
