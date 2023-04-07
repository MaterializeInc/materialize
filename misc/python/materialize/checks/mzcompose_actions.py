# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent
from typing import TYPE_CHECKING, Any, List, Optional

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mzcompose.services import Clusterd, Materialized
from materialize.util import MzVersion

if TYPE_CHECKING:
    from materialize.checks.scenarios import Scenario


class MzcomposeAction(Action):
    def join(self, e: Executor) -> None:
        # Most of these actions are already blocking
        pass


class StartMz(MzcomposeAction):
    def __init__(
        self, tag: Optional[MzVersion] = None, environment_extra: List[str] = []
    ) -> None:
        self.tag = tag
        self.environment_extra = environment_extra

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        image = f"materialize/materialized:{self.tag}" if self.tag is not None else None
        print(f"Starting Mz using image {image}")
        mz = Materialized(
            image=image,
            external_cockroach=True,
            environment_extra=self.environment_extra,
        )

        with c.override(mz):
            c.up("materialized")

        mz_version = MzVersion.parse_sql(c)
        if self.tag:
            assert (
                self.tag == mz_version
            ), f"Materialize version mismatch, expected {self.tag}, but got {mz_version}"
        else:
            version_cargo = MzVersion.parse_cargo()
            assert (
                version_cargo == mz_version
            ), f"Materialize version mismatch, expected {version_cargo}, but got {mz_version}"


class ConfigureMz(MzcomposeAction):
    def __init__(self, scenario: "Scenario") -> None:
        self.base_version = scenario.base_version()
        self.handle: Optional[Any] = None

    def execute(self, e: Executor) -> None:
        input = dedent(
            """
            # Run any query to have the materialize user implicitly created if
            # it didn't exist yet. Required for the ALTER ROLE later.
            > SELECT 1;
            1

            $ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
            ALTER SYSTEM SET max_tables = 1000;
            ALTER SYSTEM SET max_sinks = 1000;
            ALTER SYSTEM SET max_sources = 1000;
            ALTER SYSTEM SET max_materialized_views = 1000;
            ALTER SYSTEM SET max_objects_per_schema = 1000;
            ALTER SYSTEM SET max_secrets = 1000;
            # Since we already test with RBAC enabled, we have to give materialize
            # user the relevant permissions so the existing tests keep working.
            ALTER ROLE materialize CREATEROLE CREATEDB CREATECLUSTER;
            """
        )

        if self.base_version >= MzVersion(0, 47, 0):
            input += "ALTER SYSTEM SET enable_ld_rbac_checks TO true;\n"
            input += "ALTER SYSTEM SET enable_server_rbac_checks TO true;\n"

        self.handle = e.testdrive(input=input)

    def join(self, e: Executor) -> None:
        e.join(self.handle)


class KillMz(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.kill("materialized")


class UseClusterdCompute(MzcomposeAction):
    def __init__(self, scenario: "Scenario") -> None:
        self.base_version = scenario.base_version()

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        storage_addresses = (
            """STORAGECTL ADDRESSES ['clusterd_compute_1:2100'],
                STORAGE ADDRESSES ['clusterd_compute_1:2103']"""
            if self.base_version >= MzVersion(0, 44, 0)
            else "STORAGECTL ADDRESS 'clusterd_compute_1:2100'"
        )

        c.sql(
            f"""
            DROP CLUSTER REPLICA default.r1;
            CREATE CLUSTER REPLICA default.r1
                {storage_addresses},
                COMPUTECTL ADDRESSES ['clusterd_compute_1:2101'],
                COMPUTE ADDRESSES ['clusterd_compute_1:2102'],
                WORKERS 1;
            """,
            port=6877,
            user="mz_system",
        )


class KillClusterdCompute(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        with c.override(Clusterd(name="clusterd_compute_1")):
            c.kill("clusterd_compute_1")


class StartClusterdCompute(MzcomposeAction):
    def __init__(self, tag: Optional[MzVersion] = None) -> None:
        self.tag = tag

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        clusterd = Clusterd(name="clusterd_compute_1")
        if self.tag:
            clusterd = Clusterd(
                name="clusterd_compute_1",
                image=f"materialize/clusterd:{self.tag}",
            )
        print(f"Starting Compute using image {clusterd.config.get('image')}")

        with c.override(clusterd):
            c.up("clusterd_compute_1")


class RestartRedpandaDebezium(MzcomposeAction):
    """Restarts Redpanda and Debezium. Debezium is unable to survive Redpanda restarts so the two go together."""

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        for service in ["redpanda", "debezium"]:
            c.kill(service)
            c.up(service)


class RestartCockroach(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("cockroach")
        c.up("cockroach")


class RestartSourcePostgres(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.kill("postgres")
        c.up("postgres")


class KillClusterdStorage(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        # Depending on the workload, clusterd may not be running, hence the || true
        c.exec("materialized", "bash", "-c", "kill -9 `pidof clusterd` || true")


class DropCreateDefaultReplica(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            """
           DROP CLUSTER REPLICA default.r1;
           CREATE CLUSTER REPLICA default.r1 SIZE '1';
            """,
            port=6877,
            user="mz_system",
        )
