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


class CheckSchemas(Check):
    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE SCHEMA to_be_created;

                > CREATE SCHEMA to_be_dropped;
                > CREATE TABLE to_be_dropped.t1 (f1 INTEGER);
                """,
                """
                > DROP SCHEMA to_be_dropped CASCADE;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW SCHEMAS;
                public
                to_be_created

                > SET DATABASE=to_be_dropped;

                > SET DATABASE=to_be_created;

                > CREATE TABLE t1 (f1 INTEGER);

                > DROP DATABASE to_be_created CASCADE;
                """
            )
        )
