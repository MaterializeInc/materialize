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


class MaterializedViews(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE materialized_views_table (f1 STRING);
                > INSERT INTO materialized_views_table SELECT 'T1A' || generate_series FROM generate_series(1,10000);
                > INSERT INTO materialized_views_table SELECT 'T1B' || generate_series FROM generate_series(1,10000);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO materialized_views_table SELECT 'T2A' || generate_series FROM generate_series(1, 10000);

                > CREATE MATERIALIZED VIEW materialized_view1 AS SELECT LEFT(f1, 3), COUNT(*) FROM materialized_views_table GROUP BY LEFT(f1, 3);

                > DELETE FROM materialized_views_table WHERE LEFT(f1, 3) = 'T1A';

                > INSERT INTO materialized_views_table SELECT 'T2B' || generate_series FROM generate_series(1, 10000);


                """,
                """
                > DELETE FROM materialized_views_table WHERE LEFT(f1, 3) = 'T2A';

                > CREATE MATERIALIZED VIEW materialized_view2 AS SELECT LEFT(f1, 3), COUNT(*) FROM materialized_views_table GROUP BY LEFT(f1, 3);

                > INSERT INTO materialized_views_table SELECT 'T3B' || generate_series FROM generate_series(1, 10000);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM materialized_view1
                T1B 10000
                T2B 10000
                T3B 10000

                > SELECT * FROM materialized_view2
                T1B 10000
                T2B 10000
                T3B 10000
           """
            )
        )
