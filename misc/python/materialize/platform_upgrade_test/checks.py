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

from materialize.mzcompose import Composition
from materialize.platform_upgrade_test.actions import Testdrive


class Check:
    def populate(self) -> List[Testdrive]:
        assert False

    def validate(self) -> Testdrive:
        assert False

    def run_populate(self, c: Composition) -> None:
        for action in self.populate():
            action.execute(c)

    def run_validate(self, c: Composition) -> None:
        self.validate().execute(c)


class DataTypes(Check):
    def populate(self) -> List[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                "> CREATE TABLE types_table (int_col INTEGER, dec_col DECIMAL, double_col DOUBLE)",
                "> INSERT INTO types_table VALUES (123, 123.234, 123.234)",
                "> INSERT INTO types_table VALUES (234, 234.345, 234.345)",
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM types_table;
                123 123.234 123.234
                234 234.345 234.345
                """
            )
        )
