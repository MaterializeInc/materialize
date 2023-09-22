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
from materialize.zippy.framework import Action, Capability
from materialize.zippy.minio_capabilities import MinioIsRunning


class MinioStart(Action):
    """Starts a Minio instance."""

    def run(self, c: Composition) -> None:
        c.up("minio")

    def provides(self) -> list[Capability]:
        return [MinioIsRunning()]


class MinioRestart(Action):
    """Restart the Minio instance."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MinioIsRunning}

    def run(self, c: Composition) -> None:
        c.kill("minio")
        time.sleep(1)
        c.up("minio")
