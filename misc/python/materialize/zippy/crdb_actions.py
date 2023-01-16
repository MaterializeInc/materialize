# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time
from typing import List, Set, Type

from materialize.mzcompose import Composition
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, Capability


class CockroachStart(Action):
    """Starts a CockroachDB instance."""

    def run(self, c: Composition) -> None:
        c.up("cockroach")

    def provides(self) -> List[Capability]:
        return [CockroachIsRunning()]


class CockroachRestart(Action):
    """Restart the CockroachDB instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {CockroachIsRunning}

    def run(self, c: Composition) -> None:
        c.kill("cockroach")
        time.sleep(1)
        c.up("cockroach")
