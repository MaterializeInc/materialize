# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, Capability
from materialize.zippy.mz_capabilities import MzIsRunning


class BackupAndRestore(Action):
    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning, CockroachIsRunning}

    def run(self, c: Composition) -> None:
        # Required because of #22762
        c.kill("storaged")

        c.backup_crdb()
        c.restore_mz()

        c.up("storaged")
