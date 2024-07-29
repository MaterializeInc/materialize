# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.framework import Action, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning


class BalancerdStart(Action):
    """Starts balancerd"""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning}

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning}

    def run(self, c: Composition, state: State) -> None:
        with c.override(
            Balancerd(
                https_resolver_template=f"{state.mz_service}:6876",
                static_resolver_addr=f"{state.mz_service}:6875",
            )
        ):
            c.up("balancerd")

    def provides(self) -> list[Capability]:
        return [BalancerdIsRunning()]


class BalancerdStop(Action):
    """Stops balancerd"""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        # Technically speaking, we do not need Mz to be up in order to kill balancerd
        # However, without this protection we frequently end up in a situation where
        # both are down and Zippy enters a prolonged period of restarting one or the
        # other and no other useful work can be performed in the meantime.
        return {BalancerdIsRunning, MzIsRunning}

    def run(self, c: Composition, state: State) -> None:
        c.kill("balancerd")

    def withholds(self) -> set[type[Capability]]:
        return {BalancerdIsRunning}


class BalancerdRestart(Action):
    """Restarts balancerd"""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BalancerdIsRunning, MzIsRunning}

    def run(self, c: Composition, state: State) -> None:
        with c.override(
            Balancerd(
                https_resolver_template=f"{state.mz_service}:6876",
                static_resolver_addr=f"{state.mz_service}:6875",
            )
        ):
            c.kill("balancerd")
            c.up("balancerd")
