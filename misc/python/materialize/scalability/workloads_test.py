# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.scalability.operation import Operation
from materialize.scalability.operations_test import (
    EmptyOperation,
    EmptySqlStatement,
    SleepInClusterd,
    SleepInEnvironmentd,
    SleepInPython,
)
from materialize.scalability.workload import WorkloadSelfTest


class EmptyOperatorWorkload(WorkloadSelfTest):
    def operations(self) -> list["Operation"]:
        return [EmptyOperation()]


class EmptySqlStatementWorkload(WorkloadSelfTest):
    def operations(self) -> list["Operation"]:
        return [EmptySqlStatement()]


class Sleep10MsInEnvironmentdWorkload(WorkloadSelfTest):
    def operations(self) -> list["Operation"]:
        return [SleepInEnvironmentd(duration_in_sec=0.01)]


class Sleep10MsInClusterdWorkload(WorkloadSelfTest):
    def operations(self) -> list["Operation"]:
        return [SleepInClusterd(duration_in_sec=0.01)]


class Sleep10MsInPythonWorkload(WorkloadSelfTest):
    def operations(self) -> list["Operation"]:
        return [SleepInPython(duration_in_sec=0.01)]
