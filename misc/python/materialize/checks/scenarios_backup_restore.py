# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.backup_actions import (
    Backup,
    Restore,
)
from materialize.checks.mzcompose_actions import (
    KillMz,
    StartMz,
)
from materialize.checks.scenarios import Scenario


class BackupAndRestoreAfterManipulate(Scenario):
    """Backup and Restore Materialize after manipulate(phase=2) has run.
    Only validate() is run post-restore.
    """

    def actions(self) -> list[Action]:
        return [
            StartMz(),
            Initialize(self),
            Manipulate(self, phase=1),
            Manipulate(self, phase=2),
            Backup(),
            KillMz(),
            Restore(),
            StartMz(),
            Validate(self),
        ]


class BackupAndRestoreBeforeManipulate(Scenario):
    """Backup and Restore Materialize before manipulate(phase=2) has run."""

    def actions(self) -> list[Action]:
        return [
            StartMz(),
            Initialize(self),
            Manipulate(self, phase=1),
            Backup(),
            KillMz(),
            Restore(),
            StartMz(),
            Manipulate(self, phase=2),
            Validate(self),
        ]
