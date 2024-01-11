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
    """Toggle persist_txn_tables between `off` and `eager`"""

    def actions(self) -> list[Action]:
        return [
            StartMz(
                self, additional_system_parameter_defaults={"persist_txn_tables": "off"}
            ),
            Initialize(self),
            KillMz(),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "eager"},
            ),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(
                self, additional_system_parameter_defaults={"persist_txn_tables": "off"}
            ),
            Manipulate(self, phase=2),
            KillMz(),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "eager"},
            ),
            Validate(self),
            KillMz(),
            StartMz(
                self, additional_system_parameter_defaults={"persist_txn_tables": "off"}
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
            ),
            Manipulate(self, phase=1, mz_service="mz_txn_tables_off"),
            StartMz(
                self,
                additional_system_parameter_defaults={"persist_txn_tables": "eager"},
                mz_service="mz_txn_tables_eager",
            ),
            Manipulate(self, phase=2, mz_service="mz_txn_tables_eager"),
            Validate(self, mz_service="mz_txn_tables_eager"),
            StartMz(self, mz_service="mz_txn_tables_default"),
            Validate(self, mz_service="mz_txn_tables_default"),
            # Since we are creating Mz instances with a non-default name,
            # we need to perform explicit cleanup here
            KillMz(mz_service="mz_txn_tables_default"),
            Down(),
        ]


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


class TimestampOracleToggle(Scenario):
    """Toggle timestamp_oracle between `catalog` and `postgres`"""

    def actions(self) -> list[Action]:
        return [
            StartMz(
                self,
                additional_system_parameter_defaults={"timestamp_oracle": "catalog"},
            ),
            Initialize(self),
            KillMz(),
            StartMz(
                self,
                additional_system_parameter_defaults={"timestamp_oracle": "postgres"},
            ),
            Manipulate(self, phase=1),
            KillMz(),
            StartMz(
                self,
                additional_system_parameter_defaults={"timestamp_oracle": "catalog"},
            ),
            Manipulate(self, phase=2),
            KillMz(),
            StartMz(
                self,
                additional_system_parameter_defaults={"timestamp_oracle": "postgres"},
            ),
            Validate(self),
            KillMz(),
            StartMz(
                self,
                additional_system_parameter_defaults={"timestamp_oracle": "catalog"},
            ),
            Validate(self),
        ]
