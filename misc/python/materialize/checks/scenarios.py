# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import Random
from typing import List, Optional, Type

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
    RestartCockroach as RestartCockroachAction,
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
from materialize.util import MzVersion


class Scenario:
    def __init__(
        self, checks: List[Type[Check]], executor: Executor, seed: Optional[str] = None
    ) -> None:
        self._checks = checks
        self.executor = executor
        self.rng = None if seed is None else Random(seed)
        self.version_cargo = MzVersion.parse_cargo()

    def checks(self) -> List[Type[Check]]:
        if self.rng:
            self.rng.shuffle(self._checks)
        return self._checks

    def actions(self) -> List[Action]:
        assert False

    def run(self) -> None:
        for action in self.actions():
            action.execute(self.executor)


class NoRestartNoUpgrade(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks(), base_version=self.version_cargo),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class RestartEntireMz(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks(), base_version=self.version_cargo),
            KillMz(),
            StartMz(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            KillMz(),
            StartMz(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            KillMz(),
            StartMz(),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class DropCreateDefaultReplica(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks(), base_version=self.version_cargo),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            DropCreateDefaultReplicaAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class RestartClusterdCompute(Scenario):
    """Restart clusterd by having it run in a separate container that is then killed and restarted."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            StartClusterdCompute(),
            UseClusterdCompute(base_version=self.version_cargo),
            Initialize(self.checks(), base_version=self.version_cargo),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class RestartEnvironmentdClusterdStorage(Scenario):
    """Restart environmentd and storage clusterds (as spawned from it), while keeping computed running by placing it in a separate container."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            StartClusterdCompute(),
            UseClusterdCompute(base_version=self.version_cargo),
            Initialize(self.checks(), base_version=self.version_cargo),
            KillMz(),
            StartMz(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            KillMz(),
            StartMz(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            KillMz(),
            StartMz(),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class KillClusterdStorage(Scenario):
    """Kill storage clusterd while it is running inside the enviromentd container. The process orchestrator will (try to) start it again."""

    def actions(self) -> List[Action]:
        return [
            StartMz(),
            StartClusterdCompute(),
            UseClusterdCompute(base_version=self.version_cargo),
            Initialize(self.checks(), base_version=self.version_cargo),
            KillClusterdStorageAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            KillClusterdStorageAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            KillClusterdStorageAction(),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class RestartCockroach(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks(), base_version=self.version_cargo),
            RestartCockroachAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            RestartCockroachAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            RestartCockroachAction(),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class RestartSourcePostgres(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks(), base_version=self.version_cargo),
            RestartSourcePostgresAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            RestartSourcePostgresAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            RestartSourcePostgresAction(),
            Validate(self.checks(), base_version=self.version_cargo),
        ]


class RestartRedpandaDebezium(Scenario):
    def actions(self) -> List[Action]:
        return [
            StartMz(),
            Initialize(self.checks(), base_version=self.version_cargo),
            RestartRedpandaDebeziumAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=1),
            RestartRedpandaDebeziumAction(),
            Manipulate(self.checks(), base_version=self.version_cargo, phase=2),
            RestartRedpandaDebeziumAction(),
            Validate(self.checks(), base_version=self.version_cargo),
        ]
