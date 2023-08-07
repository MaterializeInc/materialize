# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List, Optional

from materialize import ROOT
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s import K8sResource
from materialize.cloudtest.k8s.testdrive import Testdrive
from materialize.cloudtest.wait import wait


# TODO: move file to mz-repo
# TODO: merge with MaterializeApplication
class CloudtestApplication(MaterializeApplication):
    def __init__(
        self,
        release_mode: bool = True,
        tag: Optional[str] = None,
        aws_region: Optional[str] = None,
        log_filter: Optional[str] = None,
    ) -> None:
        self.root = ROOT
        self.release_mode = release_mode
        self.aws_region = aws_region
        self.testdrive = Testdrive(release_mode=release_mode, aws_region=aws_region)
        """It is necessary to wait for environmentd before running testdrive commands."""

        self.resources = self.get_resources(release_mode, log_filter, tag)
        self.images = self.get_images()

        self.create()

    def get_resources(
        self,
        release_mode: bool,
        log_filter: Optional[str],
        tag: Optional[str],
    ) -> List[K8sResource]:
        return [
            self.testdrive,
        ]

    def get_images(self) -> List[str]:
        return ["testdrive"]

    def wait_create_completed(self) -> None:
        wait(condition="condition=Ready", resource="pod/testdrive")
