# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import time

from materialize.scalability.operation.operation_data import OperationData
from materialize.scalability.operation.scalability_operation import (
    Operation,
    SimpleSqlOperation,
)


class EmptyOperation(Operation):
    def _execute(self, data: OperationData) -> OperationData:
        return data


class EmptySqlStatement(SimpleSqlOperation):
    def sql_statement(self) -> str:
        return ""


class SleepInPython(Operation):
    def __init__(self, duration_in_sec: float) -> None:
        self.duration_in_sec = duration_in_sec

    def _execute(self, data: OperationData) -> OperationData:
        time.sleep(self.duration_in_sec)
        return data


class SleepInEnvironmentd(SimpleSqlOperation):
    """Run mz_sleep() in a constant query so that the sleep actually happens in environmentd."""

    def __init__(self, duration_in_sec: float) -> None:
        self.duration_in_sec = duration_in_sec

    def sql_statement(self) -> str:
        return f"SELECT mz_unsafe.mz_sleep({self.duration_in_sec});"


class SleepInClusterd(SimpleSqlOperation):
    """Run mz_sleep() in a manner that it will be run in clusterd.
    The first part of the UNION is what makes this query non-constant,
    but at the same time it matches no rows, so mz_sleep will only run once,
    with the input being the `1` from `SELECT 1`
    """

    def __init__(self, duration_in_sec: float) -> None:
        self.duration_in_sec = duration_in_sec

    def sql_statement(self) -> str:
        return f"""
            SELECT mz_unsafe.mz_sleep(f1 * {self.duration_in_sec})
            FROM (
                (SELECT * FROM t1 WHERE f1 < 0)
                UNION ALL
                (SELECT 1)
            );
        """
