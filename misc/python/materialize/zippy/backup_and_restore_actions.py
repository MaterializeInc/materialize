# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, Capability, State
from materialize.zippy.mz_capabilities import MzIsRunning


class BackupAndRestore(Action):
    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning, CockroachIsRunning}

    def run(self, c: Composition, state: State) -> None:
        # TODO: Support and test azurite backups
        if c.blob_store() == "azurite":
            return

        # Required because of database-issues#6880
        c.kill("storaged")

        c.backup()
        with c.override(
            Materialized(
                name=state.mz_service,
                external_blob_store=True,
                blob_store_is_azure=c.blob_store() == "azurite",
                external_metadata_store=True,
                deploy_generation=state.deploy_generation,
                system_parameter_defaults=state.system_parameter_defaults,
                sanity_restart=False,
                restart="on-failure",
                metadata_store="cockroach",
            )
        ):
            c.restore(state.mz_service)

        c.up("storaged")
