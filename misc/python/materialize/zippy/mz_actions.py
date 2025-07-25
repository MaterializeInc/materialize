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
    DeploymentStatus,
    Materialized,
    leader_status_healthcheck,
)
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.blob_store_capabilities import BlobStoreIsRunning
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import (
    Action,
    ActionFactory,
    Capabilities,
    Capability,
    Mz0dtDeployBaseAction,
    State,
)
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.view_capabilities import ViewExists


class MzStartParameterized(ActionFactory):
    """Starts a Mz instance with custom paramters."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {CockroachIsRunning, BlobStoreIsRunning}

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        return {MzIsRunning}

    def __init__(
        self, additional_system_parameter_defaults: dict[str, str] = {}
    ) -> None:
        self.additional_system_parameter_defaults = additional_system_parameter_defaults

    def new(self, capabilities: Capabilities) -> list[Action]:
        return [
            MzStart(
                capabilities=capabilities,
                additional_system_parameter_defaults=self.additional_system_parameter_defaults,
            )
        ]


class MzStart(Action):
    """Starts a Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {CockroachIsRunning, BlobStoreIsRunning}

    @classmethod
    def incompatible_with(cls) -> set[type[Capability]]:
        return {MzIsRunning}

    def __init__(
        self,
        capabilities: Capabilities,
        additional_system_parameter_defaults: dict[str, str] = {},
    ) -> None:
        self.additional_system_parameter_defaults = additional_system_parameter_defaults
        super().__init__(capabilities)

    def run(self, c: Composition, state: State) -> None:
        print(
            f"Starting Mz with additional_system_parameter_defaults = {self.additional_system_parameter_defaults}"
        )

        with c.override(
            Materialized(
                name=state.mz_service,
                ports=(
                    [16875, 16876, 16877, 16878, 16879]
                    if state.mz_service == "materialized2"
                    else [6875, 6876, 6877, 6878, 6879]
                ),
                external_blob_store=True,
                blob_store_is_azure=c.blob_store() == "azurite",
                external_metadata_store=True,
                deploy_generation=state.deploy_generation,
                system_parameter_defaults=state.system_parameter_defaults,
                sanity_restart=False,
                restart="on-failure",
                additional_system_parameter_defaults=self.additional_system_parameter_defaults,
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
                port=6877 if state.mz_service == "materialized" else 16877,
                print_statement=False,
                service=state.mz_service,
            )

        c.sql(
            """
            ALTER CLUSTER quickstart SET (MANAGED = false);
            """,
            user="mz_system",
            port=6877 if state.mz_service == "materialized" else 16877,
            service=state.mz_service,
        )

        # Make sure all eligible LIMIT queries use the PeekPersist optimization
        c.sql(
            "ALTER SYSTEM SET persist_fast_path_limit = 1000000000",
            user="mz_system",
            port=6877 if state.mz_service == "materialized" else 16877,
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
                ports=(
                    [16875, 16876, 16877, 16878, 16879]
                    if state.mz_service == "materialized2"
                    else [6875, 6876, 6877, 6878, 6879]
                ),
                external_blob_store=True,
                blob_store_is_azure=c.blob_store() == "azurite",
                external_metadata_store=True,
                deploy_generation=state.deploy_generation,
                system_parameter_defaults=state.system_parameter_defaults,
                sanity_restart=False,
                restart="on-failure",
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

        ports = (
            [16875, 16876, 16877, 16878, 16879]
            if state.mz_service == "materialized2"
            else [6875, 6876, 6877, 6878, 6879]
        )
        with c.override(
            Materialized(
                name=state.mz_service,
                ports=ports,
                external_blob_store=True,
                blob_store_is_azure=c.blob_store() == "azurite",
                external_metadata_store=True,
                deploy_generation=state.deploy_generation,
                system_parameter_defaults=state.system_parameter_defaults,
                sanity_restart=False,
                restart="on-failure",
                healthcheck=leader_status_healthcheck(ports[3]),
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


class KillClusterd(Action):
    """Kills the clusterd processes in the environmentd container. The process orchestrator will restart them."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning, ViewExists}

    def run(self, c: Composition, state: State) -> None:
        # Depending on the workload, clusterd may not be running, hence the || true
        c.exec(state.mz_service, "bash", "-c", "kill -9 `pidof clusterd` || true")
