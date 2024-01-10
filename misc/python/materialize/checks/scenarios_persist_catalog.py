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


class PersistCatalogToggle(Scenario):
    """Toggle catalog_kind between `stash` and `persist`"""

    def actions(self) -> list[Action]:
        return [
            StartMz(self, catalog_store="stash"),
            Initialize(self),
            KillMz(),
            StartMz(self, catalog_store="persist"),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(self, catalog_store="stash"),
            Manipulate(self, phase=2),
            KillMz(),
            StartMz(self, catalog_store="persist"),
            Validate(self),
            KillMz(),
            StartMz(self, catalog_store="stash"),
            Validate(self),
        ]
