# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from random import Random

from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.checks import Check
from materialize.checks.cloudtest_actions import ReplaceEnvironmentdStatefulSet
from materialize.checks.executors import Executor
from materialize.checks.features import Features
from materialize.checks.mzcompose_actions import (
    ConfigureMz,
    KillClusterdCompute,
    KillMz,
    SetupSqlServerTesting,
    StartClusterdCompute,
    StartMz,
    UseClusterdCompute,
)
from materialize.checks.mzcompose_actions import (
    DropCreateDefaultReplica as DropCreateDefaultReplicaAction,
)
from materialize.checks.mzcompose_actions import (
    KillClusterdStorage as KillClusterdStorageAction,
)
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
    SystemVarChange as SystemVarChangeAction,
)
from materialize.mz_version import MzVersion


class Scenario:
    def __init__(
        self,
        checks: list[type[Check]],
        executor: Executor,
        features: Features,
        seed: str | None = None,
    ) -> None:
        self._checks = checks
        self.executor = executor
        self.features = features
        self.rng = None if seed is None else Random(seed)
        self._base_version = MzVersion.parse_cargo()

        filtered_check_classes = []

        for check_class in self.checks():
            if self._include_check_class(check_class):
                filtered_check_classes.append(check_class)

        # Use base_version() here instead of _base_version so that overwriting
        # upgrade scenarios can specify another base version.
        self.check_objects = [
            check_class(self.base_version(), self.rng)
            for check_class in filtered_check_classes
        ]

    def checks(self) -> list[type[Check]]:
        if self.rng:
            self.rng.shuffle(self._checks)
        return self._checks

    def actions(self) -> list[Action]:
        raise NotImplementedError

    def base_version(self) -> MzVersion:
        return self._base_version

    def run(self) -> None:
        actions = self.actions()
        # Configure implicitly for cloud scenarios
        if not isinstance(actions[0], StartMz):
            actions.insert(0, ConfigureMz(self))

        sql_server_testing_setup = False
        for index, action in enumerate(actions):
            # Implicitly call configure to raise version-dependent limits
            if isinstance(action, StartMz) and not action.deploy_generation:
                actions.insert(
                    index + 1, ConfigureMz(self, mz_service=action.mz_service)
                )
                if not sql_server_testing_setup:
                    # Can only be run once
                    actions.insert(
                        index + 1,
                        SetupSqlServerTesting(self, mz_service=action.mz_service),
                    )
                    sql_server_testing_setup = True
            elif isinstance(action, ReplaceEnvironmentdStatefulSet):
                actions.insert(index + 1, ConfigureMz(self))

        for action in actions:
            action.execute(self.executor)
            action.join(self.executor)

    def requires_external_idempotence(self) -> bool:
        return False

    def _include_check_class(self, check_class: type[Check]) -> bool:
        return not check_class.__name__.endswith("Base") and (
            not self.requires_external_idempotence()
            or check_class.externally_idempotent
        )


class NoRestartNoUpgrade(Scenario):
    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            Initialize(self),
            Manipulate(self, phase=1),
            Manipulate(self, phase=2),
            Validate(self),
        ]


class RestartEntireMz(Scenario):
    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            Initialize(self),
            KillMz(),
            StartMz(self),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(self),
            Manipulate(self, phase=2),
            KillMz(),
            StartMz(self),
            Validate(self),
        ]


class DropCreateDefaultReplica(Scenario):
    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            Initialize(self),
            Manipulate(self, phase=1),
            DropCreateDefaultReplicaAction(self),
            Manipulate(self, phase=2),
            Validate(self),
        ]


class RestartClusterdCompute(Scenario):
    """Restart clusterd by having it run in a separate container that is then killed and restarted."""

    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            StartClusterdCompute(),
            UseClusterdCompute(self),
            Initialize(self),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Manipulate(self, phase=1),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Manipulate(self, phase=2),
            KillClusterdCompute(),
            StartClusterdCompute(),
            Validate(self),
        ]


class RestartEnvironmentdClusterdStorage(Scenario):
    """Restart environmentd and storage clusterds (as spawned from it), while keeping computed running by placing it in a separate container."""

    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            StartClusterdCompute(),
            UseClusterdCompute(self),
            Initialize(self),
            KillMz(),
            StartMz(self),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(self),
            Manipulate(self, phase=2),
            KillMz(),
            StartMz(self),
            Validate(self),
            # Validate again so that introducing non-idempotent validate()s
            # will cause the CI to fail.
            Validate(self),
        ]


class KillClusterdStorage(Scenario):
    """Kill storage clusterd while it is running inside the enviromentd container. The process orchestrator will (try to) start it again."""

    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            StartClusterdCompute(),
            UseClusterdCompute(self),
            Initialize(self),
            KillClusterdStorageAction(),
            Manipulate(self, phase=1),
            KillClusterdStorageAction(),
            Manipulate(self, phase=2),
            KillClusterdStorageAction(),
            Validate(self),
        ]


class RestartCockroach(Scenario):
    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            Initialize(self),
            RestartCockroachAction(),
            Manipulate(self, phase=1),
            RestartCockroachAction(),
            Manipulate(self, phase=2),
            RestartCockroachAction(),
            Validate(self),
        ]


class RestartSourcePostgres(Scenario):
    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            Initialize(self),
            RestartSourcePostgresAction(),
            Manipulate(self, phase=1),
            RestartSourcePostgresAction(),
            Manipulate(self, phase=2),
            RestartSourcePostgresAction(),
            Validate(self),
        ]


class RestartRedpandaDebezium(Scenario):
    def actions(self) -> list[Action]:
        return [
            StartMz(self),
            Initialize(self),
            RestartRedpandaDebeziumAction(),
            Manipulate(self, phase=1),
            RestartRedpandaDebeziumAction(),
            Manipulate(self, phase=2),
            RestartRedpandaDebeziumAction(),
            Validate(self),
        ]


@dataclass
class SystemVarChangeEntry:
    name: str
    value_for_manipulate_phase_1: str
    value_for_manipulate_phase_2: str

    def value_for_validation_1(self) -> str:
        return self.value_for_manipulate_phase_1

    def value_for_validation_2(self) -> str:
        return self.value_for_manipulate_phase_2


# This scenario is not instantiated. Create a subclass to use it.
class SystemVarChange(Scenario):
    def __init__(
        self,
        checks: list[type[Check]],
        executor: Executor,
        features: Features,
        seed: str | None,
        change_entries: list[SystemVarChangeEntry],
    ):
        super().__init__(checks, executor, features, seed)
        self.change_entries = change_entries

    def actions(self) -> list[Action]:
        validations = []

        for var in self.change_entries:
            validations.append(
                SystemVarChangeAction(
                    name=var.name, value=var.value_for_manipulate_phase_2
                )
            )
            validations.append(Validate(self))

        return [
            StartMz(self),
            Initialize(self),
            # manipulate phase 1
            *[
                SystemVarChangeAction(
                    name=var.name, value=var.value_for_manipulate_phase_1
                )
                for var in self.change_entries
            ],
            Manipulate(self, phase=1),
            # manipulate phase 2
            *[
                SystemVarChangeAction(
                    name=var.name, value=var.value_for_manipulate_phase_2
                )
                for var in self.change_entries
            ],
            Manipulate(self, phase=2),
            # validation 1
            *[
                SystemVarChangeAction(name=var.name, value=var.value_for_validation_1())
                for var in self.change_entries
            ],
            Validate(self),
            # validation 2
            *[
                SystemVarChangeAction(name=var.name, value=var.value_for_validation_2())
                for var in self.change_entries
            ],
            Validate(self),
        ]
