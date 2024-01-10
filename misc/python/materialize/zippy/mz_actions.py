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
from materialize.zippy.balancerd_capabilities import BalancerdIsRunning
from materialize.zippy.crdb_capabilities import CockroachIsRunning
from materialize.zippy.framework import Action, ActionFactory, Capabilities, Capability
from materialize.zippy.minio_capabilities import MinioIsRunning
from materialize.zippy.mz_capabilities import MzIsRunning
from materialize.zippy.view_capabilities import ViewExists


class MzStartParameterized(ActionFactory):
    """Starts a Mz instance with custom paramters."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {CockroachIsRunning, MinioIsRunning}

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
        return {CockroachIsRunning, MinioIsRunning}

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

    def run(self, c: Composition) -> None:
        print(
            f"Starting Mz with additional_system_parameter_defaults = {self.additional_system_parameter_defaults}"
        )

        with c.override(
            Materialized(
                external_minio="toxiproxy",
                external_cockroach="toxiproxy",
                sanity_restart=False,
                additional_system_parameter_defaults=self.additional_system_parameter_defaults,
            )
        ):
            c.up("materialized")

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
            )

        c.sql(
            """
            ALTER CLUSTER quickstart SET (MANAGED = false);
            """,
            user="mz_system",
            port=6877,
        )

        # Make sure all eligible LIMIT queries use the PeekPersist optimization
        c.sql(
            "ALTER SYSTEM SET persist_fast_path_limit = 1000000000",
            user="mz_system",
            port=6877,
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

    def run(self, c: Composition) -> None:
        c.kill("materialized")

    def withholds(self) -> set[type[Capability]]:
        return {MzIsRunning}


class MzRestart(Action):
    """Restarts the entire Mz instance (all components are running in the same container)."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning}

    def run(self, c: Composition) -> None:
        c.kill("materialized")
        c.up("materialized")


class KillClusterd(Action):
    """Kills the clusterd processes in the environmentd container. The process orchestrator will restart them."""

    @classmethod
    def requires(cls) -> set[type[Capability]]:
        return {MzIsRunning, ViewExists}

    def run(self, c: Composition) -> None:
        # Depending on the workload, clusterd may not be running, hence the || true
        c.exec("materialized", "bash", "-c", "kill -9 `pidof clusterd` || true")
