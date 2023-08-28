# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import time

from psycopg import Cursor

from materialize.scalability.operation import Operation


class EmptyOperation(Operation):
    def execute(self, cursor: Cursor) -> None:
        pass


class EmptySqlStatement(Operation):
    def sql_statement(self) -> str:
        return ""


class SleepInPython(Operation):
    def __init__(self, duration_in_sec: float) -> None:
        self.duration_in_sec = duration_in_sec

    def execute(self, cursor: Cursor) -> None:
        time.sleep(self.duration_in_sec)


class SleepInEnvironmentd(Operation):
    """Run mz_sleep() in a constant query so that the sleep actually happens in environmentd."""

    def __init__(self, duration_in_sec: float) -> None:
        self.duration_in_sec = duration_in_sec

    def sql_statement(self) -> str:
        return f"SELECT mz_internal.mz_sleep({self.duration_in_sec});"


class SleepInClusterd(Operation):
    """Run mz_sleep() in a manner that it will be run in clusterd.
    The first part of the UNION is what makes this query non-constant,
    but at the same time it matches no rows, so mz_sleep will only run once,
    with the input being the `1` from `SELECT 1`
    """

    def __init__(self, duration_in_sec: float) -> None:
        self.duration_in_sec = duration_in_sec

    def sql_statement(self) -> str:
        return f"""
            SELECT mz_internal.mz_sleep(f1 * {self.duration_in_sec})
            FROM (
                (SELECT * FROM t1 WHERE f1 < 0)
                UNION ALL
                (SELECT 1)
            );
        """
