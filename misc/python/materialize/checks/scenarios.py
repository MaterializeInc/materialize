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
from materialize.checks.mzcompose_actions import KillComputed, KillMz
from materialize.checks.mzcompose_actions import KillStoraged as KillStoragedAction
from materialize.checks.mzcompose_actions import (
    RestartPostgresBackend as RestartPostgresBackendAction,
)
from materialize.checks.mzcompose_actions import (
    RestartRedpanda as RestartRedpandaAction,
)
from materialize.checks.mzcompose_actions import (
    RestartSourcePostgres as RestartSourcePostgresAction,
)
from materialize.checks.mzcompose_actions import StartComputed, StartMz, UseComputed


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


class RestartComputed(Scenario):
    """Restart computed by having it run in a separate container that is then killed and restarted."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            StartComputed(),
            UseComputed(),
            Initialize(self.checks),
            KillComputed(),
            StartComputed(),
            Manipulate(self.checks, phase=1),
            KillComputed(),
            StartComputed(),
            Manipulate(self.checks, phase=2),
            KillComputed(),
            StartComputed(),
            Validate(self.checks),
        ]


class RestartEnvironmentdStoraged(Scenario):
    """Restart environmentd and storaged (as spawned from it), while keeping computed running by placing it in a separate container."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            StartComputed(),
            UseComputed(),
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


class KillStoraged(Scenario):
    """Kill storaged while it is running inside the enviromentd container. The process orchestrator will (try to) start it again."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            KillStoragedAction(),
            Manipulate(self.checks, phase=1),
            KillStoragedAction(),
            Manipulate(self.checks, phase=2),
            KillStoragedAction(),
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


class RestartRedpanda(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks),
            RestartRedpandaAction(),
            Manipulate(self.checks, phase=1),
            RestartRedpandaAction(),
            Manipulate(self.checks, phase=2),
            RestartRedpandaAction(),
            Validate(self.checks),
        ]
