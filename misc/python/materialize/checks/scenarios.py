# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Type

from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.checks.mzcompose_actions import (
    DropCreateDefaultReplica as DropCreateDefaultReplicaAction,
)
from materialize.checks.mzcompose_actions import KillClusterdCompute
from materialize.checks.mzcompose_actions import (
    KillClusterdStorage as KillClusterdStorageAction,
)
from materialize.checks.mzcompose_actions import KillMz
from materialize.checks.mzcompose_actions import (
    RestartPostgresBackend as RestartPostgresBackendAction,
)
from materialize.checks.mzcompose_actions import (
    RestartRedpandaDebezium as RestartRedpandaDebeziumAction,
)
from materialize.checks.mzcompose_actions import (
    RestartSourcePostgres as RestartSourcePostgresAction,
)
from materialize.checks.mzcompose_actions import (
    StartClusterdCompute,
    StartMz,
    UseClusterdCompute,
)


class Scenario:
    def __init__(self, checks: List[Type[Check]], executor: Executor) -> None:
        self.checks = checks
        self.executor = executor

    def actions(self) -> List[Action]:
        assert False

    def run(self) -> None:
        for action in self.actions():
            action.execute(self.executor)


class NoRestartNoUpgrade(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            Manipulate(self.checks, phase=1),
            Manipulate(self.checks, phase=2),
            Validate(self.checks),
        ]


class RestartEntireMz(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            KillMz(),
            StartMz(),
            Manipulate(self.checks, phase=1),
            KillMz(),
            StartMz(),
            Manipulate(self.checks, phase=2),
            KillMz(),
            StartMz(),
            Validate(self.checks),
        ]


class DropCreateDefaultReplica(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            Manipulate(self.checks, phase=1),
            DropCreateDefaultReplicaAction(),
            Manipulate(self.checks, phase=2),
            Validate(self.checks),
        ]


class RestartClusterdCompute(Scenario):
    """Restart clusterd by having it run in a separate container that is then killed and restarted."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            StartClusterdCompute(),
            UseClusterdCompute(),
            Initialize(self.checks),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Manipulate(self.checks, phase=1),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Manipulate(self.checks, phase=2),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Validate(self.checks),
        ]


class RestartEnvironmentdClusterdStorage(Scenario):
    """Restart environmentd and storage clusterds (as spawned from it), while keeping computed running by placing it in a separate container."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            StartClusterdCompute(),
            UseClusterdCompute(),
            Initialize(self.checks),
            KillMz(),
            StartMz(),
            Manipulate(self.checks, phase=1),
            KillMz(),
            StartMz(),
            Manipulate(self.checks, phase=2),
            KillMz(),
            StartMz(),
            Validate(self.checks),
        ]


class KillClusterdStorage(Scenario):
    """Kill storage clusterd while it is running inside the enviromentd container. The process orchestrator will (try to) start it again."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            KillClusterdStorageAction(),
            Manipulate(self.checks, phase=1),
            KillClusterdStorageAction(),
            Manipulate(self.checks, phase=2),
            KillClusterdStorageAction(),
            Validate(self.checks),
        ]


class RestartPostgresBackend(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            RestartPostgresBackendAction(),
            Manipulate(self.checks, phase=1),
            RestartPostgresBackendAction(),
            Manipulate(self.checks, phase=2),
            RestartPostgresBackendAction(),
            Validate(self.checks),
        ]


class RestartSourcePostgres(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            RestartSourcePostgresAction(),
            Manipulate(self.checks, phase=1),
            RestartSourcePostgresAction(),
            Manipulate(self.checks, phase=2),
            RestartSourcePostgresAction(),
            Validate(self.checks),
        ]


class RestartRedpandaDebezium(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            RestartRedpandaDebeziumAction(),
            Manipulate(self.checks, phase=1),
            RestartRedpandaDebeziumAction(),
            Manipulate(self.checks, phase=2),
            RestartRedpandaDebeziumAction(),
            Validate(self.checks),
        ]
