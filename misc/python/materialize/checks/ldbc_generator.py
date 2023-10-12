# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.util import MzVersion


class LdbcGenerator(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse("0.73.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            """
            > CREATE SCHEMA ldbc_generator;
            """
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(
                """
                > SET schema = ldbc_generator;
                > CREATE SOURCE gen FROM LOAD GENERATOR LDBC (SCALE FACTOR 1) FOR ALL TABLES;
                """,
            ),
            Testdrive(
                """
                # Do not burden the system with a second LDBC load generator, so do nothing in manipulate() #2
                > SELECT 1;
                1
                """
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            """
            > SET SCHEMA = ldbc_generator;

            # Confirm that the sub-sources exist without actually trying to query them.
            # Starting up the LDBC generator takes time and we can not afford to wait.
            ? EXPLAIN SELECT * FROM tagclass;
            Explained Query:
              ReadStorage materialize.ldbc_generator.tagclass
            """
        )
