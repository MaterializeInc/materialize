# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from random import Random
from typing import TYPE_CHECKING

from materialize import buildkite
from materialize.buildkite import BuildkiteEnvVar
from materialize.checks.actions import Testdrive
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion

if TYPE_CHECKING:
    from materialize.checks.actions import Action

TESTDRIVE_NOP = "$ nop"


class Check:
    # Has to be set for the class already, not just in the constructor, so that
    # we can change the value for the entire class in the decorator
    enabled: bool = True
    externally_idempotent: bool = True

    def __init__(self, base_version: MzVersion, rng: Random | None) -> None:
        self.base_version = base_version
        self.rng = rng

    def _can_run(self, e: Executor) -> bool:
        return True

    def _kafka_broker(self) -> str:
        return "BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT"

    def _unsafe_schema(self) -> str:
        """
        :return: the schema containing unsafe functions, such as `mz_sleep`.
        """
        return "mz_unsafe"

    def _default_cluster(self) -> str:
        """
        :return: name of the cluster created in all environments.
        """
        return "quickstart"

    def initialize(self) -> Testdrive:
        return Testdrive(TESTDRIVE_NOP)

    def manipulate(self) -> list[Testdrive]:
        raise NotImplementedError

    def validate(self) -> Testdrive:
        """Note that the validation method may be invoked multiple times (depending on the scenario)."""
        raise NotImplementedError

    def start_initialize(self, e: Executor, a: "Action") -> None:
        if self._can_run(e) and self.enabled:
            self.current_version = e.current_mz_version
            self._initialize = self.initialize()
            self._initialize.execute(e, a.mz_service)

    def join_initialize(self, e: Executor) -> None:
        if self._can_run(e) and self.enabled:
            self._initialize.join(e)

    def start_manipulate(self, e: Executor, a: "Action") -> None:
        if self._can_run(e) and self.enabled:
            self.current_version = e.current_mz_version
            self._manipulate = self.manipulate()
            assert (
                len(self._manipulate) == 2
            ), f"manipulate() should return a list with exactly 2 elements, but actually returns {len(self._manipulate)} elements"

            assert a.phase is not None
            self._manipulate[a.phase].execute(e, a.mz_service)

    def join_manipulate(self, e: Executor, a: "Action") -> None:
        if self._can_run(e) and self.enabled:
            assert a.phase is not None
            self._manipulate[a.phase].join(e)

    def start_validate(self, e: Executor, a: "Action") -> None:
        if self._can_run(e) and self.enabled:
            self.current_version = e.current_mz_version
            self._validate = self.validate()
            self._validate.execute(e, a.mz_service)

    def join_validate(self, e: Executor) -> None:
        if self._can_run(e) and self.enabled:
            self._validate.join(e)

    def is_running_as_cloudtest(self) -> bool:
        return buildkite.get_var(BuildkiteEnvVar.BUILDKITE_STEP_KEY, "") in {
            "cloudtest-upgrade",
            "testdrive-in-cloudtest",
        }


def disabled(ignore_reason: str):
    def decorator(cls):
        cls.enabled = False
        return cls

    return decorator


def externally_idempotent(externally_idempotent: bool = True):
    def decorator(cls):
        cls.externally_idempotent = externally_idempotent
        return cls

    return decorator
