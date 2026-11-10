# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition
from materialize.zippy.blob_store_capabilities import BlobStoreIsRunning
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.storaged_capabilities import StoragedRunning


class StoragedStart(Action):
    """Make storage available, starting the standalone replica when configured."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {CockroachIsRunning, BlobStoreIsRunning}

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        return {StoragedRunning}

    def run(self, c: Composition, state: State) -> None:
        # Managed storage is started by MzStart. Keep its capability so source
        # actions remain eligible without launching an unused standalone process.
        if state.external_storage:
            c.up("storaged")

    def provides(self) -> list[Capability]:
        return [StoragedRunning()]


class StoragedRestart(Action):
    """Restarts the entire storaged clusterd instance."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning, StoragedRunning}

    def run(self, c: Composition, state: State) -> None:
        assert state.external_storage, "standalone disruption requires external storage"
        c.kill("storaged")
        c.up("storaged")


class StoragedKill(Action):
    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning, StoragedRunning}

    def run(self, c: Composition, state: State) -> None:
        assert state.external_storage, "standalone disruption requires external storage"
        c.kill("storaged")

    def withholds(self) -> set[type[Capability]]:
        return {StoragedRunning}
