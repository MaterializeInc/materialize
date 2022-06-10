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


class InsertSelect(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE insert_select_destination (f1 STRING);

                > CREATE TABLE insert_select_source_table (f1 STRING);
                > INSERT INTO insert_select_source_table SELECT 'T1' || generate_series FROM generate_series(1,10000);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO insert_select_source_table SELECT 'T2' || generate_series FROM generate_series(1, 10000);

                > INSERT INTO insert_select_destination SELECT * FROM insert_select_source_table;
                """,
                """
                > INSERT INTO insert_select_source_table SELECT 'T3' || generate_series FROM generate_series(1, 10000);

                > INSERT INTO insert_select_destination SELECT * FROM insert_select_source_table;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT LEFT(f1, 2), COUNT(*), COUNT(DISTINCT f1) FROM insert_select_destination GROUP BY LEFT(f1, 2);
                T1 20000 10000
                T2 20000 10000
                T3 10000 10000
           """
            )
        )
