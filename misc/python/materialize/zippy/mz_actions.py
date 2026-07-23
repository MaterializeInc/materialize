# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import (
    LEADER_STATUS_HEALTHCHECK,
    DeploymentStatus,
    Materialized,
)
from materialize.zippy.balancerd_actions import restart_balancerd
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.blob_store_capabilities import BlobStoreIsRunning
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import (
    Action,
    Capability,
    Mz0dtDeployBaseAction,
    State,
)
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.table_capabilities import TableExists
from materialize.zippy.view_capabilities import ViewExists


class MzStart(Action):
    """Starts a Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {CockroachIsRunning, BlobStoreIsRunning}

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition, state: State) -> None:
        print(
            f"Starting Mz with additional_system_parameter_defaults = {state.additional_system_parameter_defaults}"
        )

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
                additional_system_parameter_defaults=state.additional_system_parameter_defaults,
                metadata_store="cockroach",
                default_replication_factor=2,
            )
        ):
            c.up(state.mz_service)

        for config_param in [
            "max_tables",
            "max_sources",
            "max_objects_per_schema",
            "max_materialized_views",
            "max_sinks",
        ]:
            c.sql(
                f"ALTER SYSTEM SET {config_param} TO 1000",
                user="mz_system",
                port=6877,
                print_statement=False,
                service=state.mz_service,
            )

        c.sql(
            """
            ALTER CLUSTER quickstart SET (MANAGED = false);
            """,
            user="mz_system",
            port=6877,
            service=state.mz_service,
        )

        # Make sure all eligible LIMIT queries use the PeekPersist optimization
        c.sql(
            "ALTER SYSTEM SET persist_fast_path_limit = 1000000000",
            user="mz_system",
            port=6877,
            service=state.mz_service,
        )

    def provides(self) -> list[Capability]:
        return [MzIsRunning()]


class MzStop(Action):
    """Stops the entire Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        # Technically speaking, we do not need balancerd to be up in order to kill Mz
        # However, without this protection we frequently end up in a situation where
        # both are down and Zippy enters a prolonged period of restarting one or the
        # other and no other useful work can be performed in the meantime.
        return {MzIsRunning, BalancerdIsRunning}

    def run(self, c: Composition, state: State) -> None:
        c.kill(state.mz_service)

    def withholds(self) -> set[type[Capability]]:
        return {MzIsRunning}


class MzRestart(Action):
    """Restarts the entire Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition, state: State) -> None:
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
                additional_system_parameter_defaults=state.additional_system_parameter_defaults,
                metadata_store="cockroach",
                default_replication_factor=2,
            )
        ):
            c.kill(state.mz_service)
            c.up(state.mz_service)


class Mz0dtDeploy(Mz0dtDeployBaseAction):
    """Switches Mz to a new deployment using 0dt."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition, state: State) -> None:
        state.deploy_generation += 1

        state.mz_service = (
            "materialized" if state.deploy_generation % 2 == 0 else "materialized2"
        )

        print(f"Deploying generation {state.deploy_generation} on {state.mz_service}")

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
                healthcheck=LEADER_STATUS_HEALTHCHECK,
                additional_system_parameter_defaults=state.additional_system_parameter_defaults,
                metadata_store="cockroach",
                default_replication_factor=2,
            ),
        ):
            c.up(state.mz_service, detach=True)
            c.await_mz_deployment_status(
                DeploymentStatus.READY_TO_PROMOTE, state.mz_service
            )
            c.promote_mz(state.mz_service)
            c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, state.mz_service)
            c.stop(
                (
                    "materialized2"
                    if state.mz_service == "materialized"
                    else "materialized"
                ),
                wait=True,
            )

        # Balancerd's resolver is fixed at startup and still points at the
        # previous generation. Re-point it at the new leader.
        if c.is_running("balancerd"):
            restart_balancerd(c, state)


class KillClusterd(Action):
    """Kills the clusterd processes in the environmentd container. The process orchestrator will restart them."""

    @classmethod
    def requires(cls) -> list[set[type[Capability]]]:
        # Only kill once a dataflow-bearing object exists, so the kill has
        # something to disrupt.
        return [{MzIsRunning, ViewExists}, {MzIsRunning, TableExists}]

    def run(self, c: Composition, state: State) -> None:
        # Depending on the workload, clusterd may not be running, hence the || true
        c.exec(state.mz_service, "bash", "-c", "kill -9 `pidof clusterd` || true")
