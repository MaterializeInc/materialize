# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.zippy.balancerd_actions import BalancerdIsRunning
from materialize.zippy.framework import (
    Action,
    ActionFactory,
    Capabilities,
    Capability,
)
from materialize.zippy.mz_actions import MzIsRunning
from materialize.zippy.table_actions import ValidateTable
from materialize.zippy.table_capabilities import TableExists
from materialize.zippy.view_actions import ValidateView
from materialize.zippy.view_capabilities import ViewExists


class ValidateAll(ActionFactory):
    """Emits ValidateView and ValidateTable for all eligible objects."""

    @classmethod
    def requires(cls) -> list[set[type[Capability]]]:
        return [
            {BalancerdIsRunning, MzIsRunning, TableExists},
            {BalancerdIsRunning, MzIsRunning, ViewExists},
        ]

    def new(self, capabilities: Capabilities) -> list[Action]:
        validations = []
        for view in capabilities.get(ViewExists):
            validations.append(ValidateView(capabilities=capabilities, view=view))

        for table in capabilities.get(TableExists):
            validations.append(ValidateTable(capabilities=capabilities, table=table))

        return validations
