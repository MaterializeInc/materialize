# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.framework import Action, Capability
from materialize.zippy.mz_capabilities import MzIsRunning


class MzStart(Action):
    """Starts a Mz instance (all components are running in the same container)."""

    def run(self, c: Composition) -> None:
        c.up("materialized")
        c.wait_for_materialized()

    def provides(self) -> List[Capability]:
        return [MzIsRunning()]


class MzStop(Action):
    """Stops the entire Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition) -> None:
        c.kill("materialized")

    def removes(self) -> Set[Type[Capability]]:
        return {MzIsRunning}


class KillStoraged(Action):
    """Kills the storaged processes in the environmentd container. The process orchestrator will restart them."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition) -> None:
        # Depending on the workload, storaged may not be running, hence the || true
        c.exec("materialized", "bash", "-c", "kill -9 `pidof storaged` || true")
