# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.mzcompose.composition import Composition
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, Capability, State


class CockroachStart(Action):
    """Starts a CockroachDB instance."""

    def run(self, c: Composition, state: State) -> None:
        c.up(c.metadata_store())

    def provides(self) -> list[Capability]:
        return [CockroachIsRunning()]


class CockroachRestart(Action):
    """Restart the CockroachDB instance."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {CockroachIsRunning}

    def run(self, c: Composition, state: State) -> None:
        c.kill(c.metadata_store())
        time.sleep(1)
        c.up(c.metadata_store())
