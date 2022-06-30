# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class TemporalTypes(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE temporal_types (date_col DATE, time_col TIME, timestamp_col TIMESTAMP, timestamptz_col TIMESTAMPTZ, interval_col INTERVAL);
            > INSERT INTO temporal_types VALUES ('2010-10-10', '10:10:10', '2010-10-10 10:10:10+00','2010-10-10 10:10:10+00', INTERVAL '0 day');
        """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE MATERIALIZED VIEW date_view1 AS
                  SELECT
                  date_col, '2010-10-10'::date AS date_col2,
                  time_col, '10:10:10'::time AS time_col2,
                  timestamp_col, '2010-10-10 10:10:10+01'::timestamp AS timestamp_col2,
                  timestamptz_col, '2010-10-10 10:10:10+01'::timestamptz AS timestamptz_col2,
                  interval_col, INTERVAL '1 day' AS interval_col2
                  FROM temporal_types
                  WHERE date_col >= '2010-10-10'::DATE
                  AND time_col >= '10:10:10'::TIME
                  AND timestamp_col >= '2010-10-10 10:10:10+00'::TIMESTAMP
                  AND timestamptz_col >= '2010-10-10 10:10:10+00'::TIMESTAMPTZ
                  AND interval_col >= INTERVAL '0 day';

                > INSERT INTO temporal_types VALUES ('2011-11-11', '11:11:11', '2011-11-11 11:11:11+01', '2011-11-11 11:11:11+01', INTERVAL '1 day');
                """,
                """
                > CREATE MATERIALIZED VIEW date_view2 AS
                  SELECT
                  date_col, '2010-10-10'::date AS date_col2,
                  time_col, '10:10:10'::time AS time_col2,
                  timestamp_col, '2010-10-10 10:10:10+01'::timestamp AS timestamp_col2,
                  timestamptz_col, '2010-10-10 10:10:10+01'::timestamptz AS timestamptz_col2,
                  interval_col, INTERVAL '1 day' AS interval_col2
                  FROM temporal_types
                  WHERE date_col >= '2010-10-10'::DATE
                  AND time_col >= '10:10:10'::TIME
                  AND timestamp_col >= '2010-10-10 10:10:10+00'::TIMESTAMP
                  AND timestamptz_col >= '2010-10-10 10:10:10+00'::TIMESTAMPTZ
                  AND interval_col >= INTERVAL '0 day';

                > INSERT INTO temporal_types VALUES ('2012-12-12', '12:12:12', '2012-12-12 12:12:12+02', '2012-12-12 12:12:12+02', INTERVAL '2 day');
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM date_view1;
                2010-10-10 2010-10-10 10:10:10 10:10:10 "2010-10-10 10:10:10" "2010-10-10 10:10:10" "2010-10-10 10:10:10 UTC" "2010-10-10 09:10:10 UTC" 00:00:00 "1 day"
                2011-11-11 2010-10-10 11:11:11 10:10:10 "2011-11-11 11:11:11" "2010-10-10 10:10:10" "2011-11-11 10:11:11 UTC" "2010-10-10 09:10:10 UTC" "1 day" "1 day"
                2012-12-12 2010-10-10 12:12:12 10:10:10 "2012-12-12 12:12:12" "2010-10-10 10:10:10" "2012-12-12 10:12:12 UTC" "2010-10-10 09:10:10 UTC" "2 days" "1 day"

                > SELECT * FROM date_view2;
                2010-10-10 2010-10-10 10:10:10 10:10:10 "2010-10-10 10:10:10" "2010-10-10 10:10:10" "2010-10-10 10:10:10 UTC" "2010-10-10 09:10:10 UTC" 00:00:00 "1 day"
                2011-11-11 2010-10-10 11:11:11 10:10:10 "2011-11-11 11:11:11" "2010-10-10 10:10:10" "2011-11-11 10:11:11 UTC" "2010-10-10 09:10:10 UTC" "1 day" "1 day"
                2012-12-12 2010-10-10 12:12:12 10:10:10 "2012-12-12 12:12:12" "2010-10-10 10:10:10" "2012-12-12 10:12:12 UTC" "2010-10-10 09:10:10 UTC" "2 days" "1 day"
            """
            )
        )
