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
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, Capability
from materialize.zippy.minio_capabilities import MinioIsRunning
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.storaged_capabilities import StoragedRunning


class StoragedStart(Action):
    """Starts a storaged clusterd instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {CockroachIsRunning, MinioIsRunning}

    def run(self, c: Composition) -> None:
        c.start_and_wait_for_tcp(services=["storaged"])

    def provides(self) -> List[Capability]:
        return [StoragedRunning()]


class StoragedRestart(Action):
    """Restarts the entire storaged clusterd instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, StoragedRunning}

    def run(self, c: Composition) -> None:
        c.kill("storaged")
        c.start_and_wait_for_tcp(services=["storaged"])


class StoragedKill(Action):
    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MzIsRunning, StoragedRunning}

    def run(self, c: Composition) -> None:
        c.kill("storaged")

    def withholds(self) -> Set[Type[Capability]]:
        return {StoragedRunning}
