# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import random
from typing import Any

from materialize.checks.actions import PyAction
from materialize.checks.checks import Check, externally_idempotent


@externally_idempotent(False)
class TableData(Check):
    data: list[int]

    def initialize(self) -> PyAction:
        self.data = []

        def run(conn: Any) -> None:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE table_data (f1 INTEGER)")

        return PyAction(run)

    def manipulate(self) -> list[PyAction]:
        def run(conn: Any) -> None:
            with conn.cursor() as cur:
                for _ in range(100):
                    value = random.randint(1, 1000000)
                    self.data.append(value)
                    cur.execute(f"INSERT INTO table_data VALUES ({value})")

        return [PyAction(run) for _ in range(2)]

    def validate(self) -> PyAction:
        def run(conn: Any) -> None:
            with conn.cursor() as cur:
                self.data.sort()
                cur.execute("SELECT f1 FROM table_data ORDER BY f1")
                for i, row in enumerate(cur.fetchall()):
                    assert row[0] == self.data[i]

        return PyAction(run)
