# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.mzcompose_actions import KillMz, StartMz
from materialize.checks.scenarios import Scenario


class MigrateX86Aarch64(Scenario):
    """Migrate from x86-64 to aarch64 before the validation, Linux specific since we don't have x86-64 macOS images on Dockerhub"""

    def actions(self) -> list[Action]:
        return [
            StartMz(self, platform="linux/amd64"),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(capture_logs=True),
            StartMz(self, platform="linux/arm64/v8"),
            Manipulate(self, phase=2),
            Validate(self),
        ]


class MigrateAarch64X86(Scenario):
    """Migrate from aarch64 to x86-64 before the validation, Linux specific since we don't have x86-64 macOS images on Dockerhub"""

    def actions(self) -> list[Action]:
        return [
            StartMz(self, platform="linux/arm64/v8"),
            Initialize(self),
            Manipulate(self, phase=1),
            KillMz(capture_logs=True),
            StartMz(self, platform="linux/amd64"),
            Manipulate(self, phase=2),
            Validate(self),
        ]
