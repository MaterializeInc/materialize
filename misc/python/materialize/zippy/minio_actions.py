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
from materialize.zippy.framework import Action, Capability
from materialize.zippy.minio_capabilities import MinioIsRunning


class MinioStart(Action):
    """Starts a Minio instance."""

    def run(self, c: Composition) -> None:
        c.start_and_wait_for_tcp(services=["minio"])

        # Minio is managed using a dedicated container
        c.up("minio_mc", persistent=True)

        # Create user
        c.exec(
            "minio_mc",
            "mc",
            "config",
            "host",
            "add",
            "myminio",
            "http://minio:9000",
            "minioadmin",
            "minioadmin",
        )

        # Create bucket
        c.exec("minio_mc", "mc", "mb", "myminio/persist"),

    def provides(self) -> List[Capability]:
        return [MinioIsRunning()]


class MinioRestart(Action):
    """Restart the Minio instance."""

    @classmethod
    def requires(self) -> Set[Type[Capability]]:
        return {MinioIsRunning}

    def run(self, c: Composition) -> None:
        c.kill("minio")
        time.sleep(1)
        c.up("minio")
