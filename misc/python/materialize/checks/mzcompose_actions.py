# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import time
from textwrap import dedent
from typing import TYPE_CHECKING, Any

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.ssh_bastion_host import (
    setup_default_ssh_test_connection,
)

if TYPE_CHECKING:
    from materialize.checks.scenarios import Scenario


class MzcomposeAction(Action):
    def join(self, e: Executor) -> None:
        # Most of these actions are already blocking
        pass


class StartMz(MzcomposeAction):
    def __init__(
        self,
        scenario: "Scenario",
        tag: MzVersion | None = None,
        environment_extra: list[str] = [],
        system_parameter_defaults: dict[str, str] | None = None,
        additional_system_parameter_defaults: dict[str, str] = {},
        mz_service: str | None = None,
        catalog_store: str | None = None,
        platform: str | None = None,
        healthcheck: list[str] | None = None,
    ) -> None:
        if healthcheck is None:
            healthcheck = ["CMD", "curl", "-f", "localhost:6878/api/readyz"]
        self.tag = tag
        self.environment_extra = environment_extra
        self.system_parameter_defaults = system_parameter_defaults
        self.additional_system_parameter_defaults = additional_system_parameter_defaults
        self.catalog_store = catalog_store or (
            "persist"
            if scenario.base_version() >= MzVersion.parse_mz("v0.82.0-dev")
            else "stash"
        )
        self.healthcheck = healthcheck
        self.mz_service = mz_service
        self.platform = platform

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        image = f"materialize/materialized:{self.tag}" if self.tag is not None else None
        print(f"Starting Mz using image {image}, mz_service {self.mz_service}")

        mz = Materialized(
            name=self.mz_service,
            image=image,
            external_cockroach=True,
            external_minio=True,
            environment_extra=self.environment_extra,
            system_parameter_defaults=self.system_parameter_defaults,
            additional_system_parameter_defaults=self.additional_system_parameter_defaults,
            sanity_restart=False,
            catalog_store=self.catalog_store,
            platform=self.platform,
            healthcheck=self.healthcheck,
        )

        with c.override(mz):
            c.up("materialized" if self.mz_service is None else self.mz_service)

            # If we start up Materialize with MZ_DEPLOY_GENERATION, then it
            # stays in a stuck state when the preflight-check is completed. So
            # we can't connect to it yet to run any commands.
            if any(
                env.startswith("MZ_DEPLOY_GENERATION=")
                for env in self.environment_extra
            ):
                return

            # This should live in ssh.py and alter_connection.py, but accessing the
            # ssh bastion host from inside a check is not possible currently.
            for i in range(4):
                ssh_tunnel_name = f"ssh_tunnel_{i}"
                setup_default_ssh_test_connection(
                    c, ssh_tunnel_name, mz_service=self.mz_service
                )

            mz_version = MzVersion.parse_mz(c.query_mz_version(service=self.mz_service))
            if self.tag:
                assert (
                    self.tag == mz_version
                ), f"Materialize version mismatch, expected {self.tag}, but got {mz_version}"
            else:
                version_cargo = MzVersion.parse_cargo()
                assert (
                    version_cargo == mz_version
                ), f"Materialize version mismatch, expected {version_cargo}, but got {mz_version}"

            e.current_mz_version = mz_version


class ConfigureMz(MzcomposeAction):
    def __init__(self, scenario: "Scenario", mz_service: str | None = None) -> None:
        self.handle: Any | None = None
        self.mz_service = mz_service
        self.scenario = scenario

    def execute(self, e: Executor) -> None:
        input = dedent(
            """
            # Run any query to have the materialize user implicitly created if
            # it didn't exist yet. Required for the GRANT later.
            > SELECT 1;
            1
            """
        )

        system_settings = {
            "ALTER SYSTEM SET max_tables = 1000;",
            "ALTER SYSTEM SET max_sinks = 1000;",
            "ALTER SYSTEM SET max_sources = 1000;",
            "ALTER SYSTEM SET max_materialized_views = 1000;",
            "ALTER SYSTEM SET max_objects_per_schema = 1000;",
            "ALTER SYSTEM SET max_secrets = 1000;",
            "ALTER SYSTEM SET max_clusters = 1000;",
        }

        # Since we already test with RBAC enabled, we have to give materialize
        # user the relevant attributes so the existing tests keep working.
        if (
            MzVersion.parse_mz("v0.45.0")
            <= e.current_mz_version
            < MzVersion.parse_mz("v0.59.0-dev")
        ):
            system_settings.add(
                "ALTER ROLE materialize CREATEROLE CREATEDB CREATECLUSTER;"
            )
        elif e.current_mz_version >= MzVersion.parse_mz("v0.59.0"):
            system_settings.add("GRANT ALL PRIVILEGES ON SYSTEM TO materialize;")

        if e.current_mz_version >= MzVersion.parse_mz("v0.47.0"):
            system_settings.add("ALTER SYSTEM SET enable_rbac_checks TO true;")

        if e.current_mz_version >= MzVersion.parse_mz(
            "v0.51.0-dev"
        ) and e.current_mz_version < MzVersion.parse_mz("v0.76.0-dev"):
            system_settings.add("ALTER SYSTEM SET enable_ld_rbac_checks TO true;")

        if e.current_mz_version >= MzVersion.parse_mz("v0.52.0-dev"):
            # Since we already test with RBAC enabled, we have to give materialize
            # user the relevant privileges so the existing tests keep working.
            system_settings.add("GRANT CREATE ON DATABASE materialize TO materialize;")
            system_settings.add(
                "GRANT CREATE ON SCHEMA materialize.public TO materialize;"
            )
            if self.scenario.base_version() >= MzVersion.parse_mz("v0.82.0-dev"):
                cluster_name = "quickstart"
            else:
                cluster_name = "default"
            system_settings.add(
                f"GRANT CREATE ON CLUSTER {cluster_name} TO materialize;"
            )

        if (
            MzVersion.parse_mz("v0.58.0-dev")
            <= e.current_mz_version
            <= MzVersion.parse_mz("v0.63.99")
        ):
            system_settings.add("ALTER SYSTEM SET enable_managed_clusters = on;")

        system_settings = system_settings - e.system_settings

        if system_settings:
            input += (
                "$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}\n"
                + "\n".join(system_settings)
            )

        kafka_broker = "BROKER '${testdrive.kafka-addr}'"
        print(e.current_mz_version)
        if e.current_mz_version >= MzVersion.parse_mz("v0.78.0-dev"):
            kafka_broker += ", SECURITY PROTOCOL PLAINTEXT"
        input += dedent(
            f"""
            > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA {kafka_broker}

            > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${{testdrive.schema-registry-url}}';
            """
        )

        self.handle = e.testdrive(input=input, mz_service=self.mz_service)
        e.system_settings.update(system_settings)

    def join(self, e: Executor) -> None:
        e.join(self.handle)


class KillMz(MzcomposeAction):
    def __init__(
        self, mz_service: str = "materialized", capture_logs: bool = False
    ) -> None:
        self.mz_service = mz_service
        self.capture_logs = capture_logs

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        with c.override(Materialized(name=self.mz_service)):
            c.kill(self.mz_service, wait=True)

            if self.capture_logs:
                c.capture_logs(self.mz_service)


class Down(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.down()


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

        if self.base_version >= MzVersion(0, 55, 0):
            param = "enable_unmanaged_cluster_replicas"
            if self.base_version >= MzVersion.parse_mz("v0.90.0-dev"):
                param = "enable_unorchestrated_cluster_replicas"
            c.sql(
                f"ALTER SYSTEM SET {param} = on;",
                port=6877,
                user="mz_system",
            )
        if self.base_version >= MzVersion.parse_mz("v0.82.0-dev"):
            cluster_name = "quickstart"
        else:
            cluster_name = "default"

        c.sql(
            f"""
            ALTER CLUSTER {cluster_name} SET (MANAGED = false);
            DROP CLUSTER REPLICA {cluster_name}.r1;
            CREATE CLUSTER REPLICA {cluster_name}.r1
                {storage_addresses},
                COMPUTECTL ADDRESSES ['clusterd_compute_1:2101'],
                COMPUTE ADDRESSES ['clusterd_compute_1:2102'],
                WORKERS 1;
            """,
            port=6877,
            user="mz_system",
        )


class KillClusterdCompute(MzcomposeAction):
    def __init__(self, capture_logs: bool = False) -> None:
        self.capture_logs = capture_logs

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        with c.override(Clusterd(name="clusterd_compute_1")):
            c.kill("clusterd_compute_1")

            if self.capture_logs:
                c.capture_logs("clusterd_compute_1")


class StartClusterdCompute(MzcomposeAction):
    def __init__(self, tag: MzVersion | None = None) -> None:
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
    def __init__(self, scenario: "Scenario") -> None:
        self.base_version = scenario.base_version()

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        if self.base_version >= MzVersion.parse_mz("v0.82.0-dev"):
            cluster_name = "quickstart"
        else:
            cluster_name = "default"

        c.sql(
            f"""
            ALTER CLUSTER {cluster_name} SET (MANAGED = false);
            DROP CLUSTER REPLICA {cluster_name}.r1;
            CREATE CLUSTER REPLICA {cluster_name}.r1 SIZE '1';
            """,
            port=6877,
            user="mz_system",
        )


class WaitReadyMz(MzcomposeAction):
    """Wait until environmentd is ready, see https://github.com/MaterializeInc/cloud/blob/main/doc/design/20230418_upgrade_orchestration.md#get-apileaderstatus"""

    def __init__(self, mz_service: str = "materialized") -> None:
        self.mz_service = mz_service

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        while True:
            result = json.loads(
                c.exec(
                    self.mz_service,
                    "curl",
                    "localhost:6878/api/leader/status",
                    capture=True,
                ).stdout
            )
            if result["status"] == "ReadyToPromote":
                return
            assert result["status"] == "Initializing", f"Unexpected status {result}"
            print("Not ready yet, waiting 1 s")
            time.sleep(1)


class PromoteMz(MzcomposeAction):
    """Promote environmentd to leader, see https://github.com/MaterializeInc/cloud/blob/main/doc/design/20230418_upgrade_orchestration.md#post-apileaderpromote"""

    def __init__(self, mz_service: str = "materialized") -> None:
        self.mz_service = mz_service

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        result = json.loads(
            c.exec(
                self.mz_service,
                "curl",
                "-X",
                "POST",
                "http://127.0.0.1:6878/api/leader/promote",
                capture=True,
            ).stdout
        )
        assert result["result"] == "Success", f"Unexpected result {result}"
