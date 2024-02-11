# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.checks.actions import Action, Initialize, Manipulate, Validate
from materialize.checks.mzcompose_actions import Down, KillMz, StartMz
from materialize.checks.scenarios import Scenario


class PersistTxnToggle(Scenario):
    """Toggle persist_txn_tables between `off`, `eager` and `lazy`"""

    def actions(self) -> list[Action]:
        return [
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "off"},
                catalog_store="persist",
            ),
            Initialize(self),
            KillMz(capture_logs=True),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "eager"},
                catalog_store="persist",
            ),
            Manipulate(self, phase=1),
            KillMz(capture_logs=True),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "lazy"},
                catalog_store="persist",
            ),
            Manipulate(self, phase=2),
            KillMz(capture_logs=True),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "eager"},
                catalog_store="persist",
            ),
            Validate(self),
            KillMz(capture_logs=True),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "off"},
                catalog_store="persist",
            ),
            Validate(self),
        ]


class PersistTxnFencing(Scenario):
    """Switch between two instances with different persist_txn_tables settings.
    Fencing should kick in to prevent data corruption."""

    def actions(self) -> list[Action]:
        return [
            StartMz(self, mz_service="mz_txn_tables_default"),
            Initialize(self, mz_service="mz_txn_tables_default"),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "off"},
                mz_service="mz_txn_tables_off",
                catalog_store="persist",
            ),
            Manipulate(self, phase=1, mz_service="mz_txn_tables_off"),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "eager"},
                mz_service="mz_txn_tables_eager",
                catalog_store="persist",
            ),
            Manipulate(self, phase=2, mz_service="mz_txn_tables_eager"),
            Validate(self, mz_service="mz_txn_tables_eager"),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "lazy"},
                mz_service="mz_txn_tables_lazy",
                catalog_store="persist",
            ),
            Validate(self, mz_service="mz_txn_tables_lazy"),
            # Since we are creating Mz instances with a non-default name,
            # we need to perform explicit cleanup here. Some instances are
            # dead by now, but we still need to capture their logs
            KillMz(mz_service="mz_txn_tables_default"),
            KillMz(mz_service="mz_txn_tables_eager"),
            KillMz(mz_service="mz_txn_tables_lazy"),
            Down(),
        ]
