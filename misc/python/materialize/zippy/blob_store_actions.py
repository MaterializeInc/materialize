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
from materialize.zippy.blob_store_capabilities import BlobStoreIsRunning
from materialize.zippy.framework import Action, Capability, State


class BlobStoreStart(Action):
    """Starts a BlobStore instance."""

    def run(self, c: Composition, state: State) -> None:
        c.up(c.blob_store())

    def provides(self) -> list[Capability]:
        return [BlobStoreIsRunning()]


class BlobStoreRestart(Action):
    """Restart the BlobStore instance."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {BlobStoreIsRunning}

    def run(self, c: Composition, state: State) -> None:
        blob_store = c.blob_store()
        c.kill(blob_store)
        time.sleep(1)
        c.up(blob_store)
