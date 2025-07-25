# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
from textwrap import dedent
from typing import TYPE_CHECKING, Any

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion
from materialize.mzcompose.services.clusterd import Clusterd
from materialize.mzcompose.services.materialized import DeploymentStatus, Materialized
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
        system_parameter_version: MzVersion | None = None,
        mz_service: str | None = None,
        platform: str | None = None,
        healthcheck: list[str] | None = None,
        deploy_generation: int | None = None,
        restart: str | None = None,
        force_migrations: str | None = None,
        publish: bool | None = None,
    ) -> None:
        self.healthcheck = healthcheck or [
            "CMD",
            "curl",
            "-f",
            "localhost:6878/api/readyz",
        ]
        self.tag = tag
        self.environment_extra = environment_extra
        self.system_parameter_defaults = system_parameter_defaults
        self.additional_system_parameter_defaults = additional_system_parameter_defaults
        self.system_parameter_version = system_parameter_version or tag
        self.mz_service = mz_service
        self.platform = platform
        self.deploy_generation = deploy_generation
        self.restart = restart
        self.force_migrations = force_migrations
        self.publish = publish
        self.scenario = scenario

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        image = f"materialize/materialized:{self.tag}" if self.tag is not None else None
        print(f"Starting Mz using image {image}, mz_service {self.mz_service}")

        mz = Materialized(
            name=self.mz_service,
            image=image,
            external_metadata_store=True,
            external_blob_store=True,
            blob_store_is_azure=self.scenario.features.azurite_enabled(),
            environment_extra=self.environment_extra,
            system_parameter_defaults=self.system_parameter_defaults,
            additional_system_parameter_defaults=self.additional_system_parameter_defaults,
            system_parameter_version=self.system_parameter_version,
            sanity_restart=False,
            platform=self.platform,
            healthcheck=self.healthcheck,
            deploy_generation=self.deploy_generation,
            restart=self.restart,
            force_migrations=self.force_migrations,
            publish=self.publish,
            default_replication_factor=2,
            support_external_clusterd=True,
        )

        # Don't fail since we are careful to explicitly kill and collect logs
        # of the services thus started
        with c.override(mz, fail_on_new_service=False):
            c.up("materialized" if self.mz_service is None else self.mz_service)

            # If we start up Materialize with a deploy-generation , then it
            # stays in a stuck state when the preflight-check is completed. So
            # we can't connect to it yet to run any commands.
            if self.deploy_generation:
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
        system_settings.add("GRANT ALL PRIVILEGES ON SYSTEM TO materialize;")

        # do not enable this by default for all checks
        system_settings.add("ALTER SYSTEM SET enable_rbac_checks TO false;")

        # Since we already test with RBAC enabled, we have to give materialize
        # user the relevant privileges so the existing tests keep working.
        system_settings.add("GRANT CREATE ON DATABASE materialize TO materialize;")
        system_settings.add("GRANT CREATE ON SCHEMA materialize.public TO materialize;")
        system_settings.add("GRANT CREATE ON CLUSTER quickstart TO materialize;")

        system_settings = system_settings - e.system_settings

        if system_settings:
            input += (
                "$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}\n"
                + "\n".join(system_settings)
            )

        kafka_broker = "BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT"
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

        # Don't fail since we are careful to explicitly kill and collect logs
        # of the services thus started
        with c.override(Materialized(name=self.mz_service), fail_on_new_service=False):
            c.kill(self.mz_service, wait=True)

            if self.capture_logs:
                c.capture_logs(self.mz_service)


class Stop(MzcomposeAction):
    def __init__(self, service: str = "materialized") -> None:
        self.service = service

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.stop(self.service, wait=True)


class Down(MzcomposeAction):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.down()


class UseClusterdCompute(MzcomposeAction):
    def __init__(self, scenario: "Scenario") -> None:
        self.base_version = scenario.base_version()

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            "ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = on;",
            port=6877,
            user="mz_system",
        )
        c.sql(
            """
            ALTER CLUSTER quickstart SET (MANAGED = false);
            DROP CLUSTER REPLICA quickstart.r1;
            CREATE CLUSTER REPLICA quickstart.r1
                STORAGECTL ADDRESSES ['clusterd_compute_1:2100'],
                STORAGE ADDRESSES ['clusterd_compute_1:2103'],
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

        c.kill(c.metadata_store())
        c.up(c.metadata_store())


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

        c.sql(
            """
            ALTER CLUSTER quickstart SET (MANAGED = false);
            DROP CLUSTER REPLICA quickstart.r1;
            CREATE CLUSTER REPLICA quickstart.r1 SIZE '1';
            """,
            port=6877,
            user="mz_system",
        )


class WaitReadyMz(MzcomposeAction):
    """Wait until environmentd is ready, see https://github.com/MaterializeInc/cloud/blob/main/doc/design/20230418_upgrade_orchestration.md#get-apileaderstatus"""

    def __init__(self, mz_service: str = "materialized") -> None:
        self.mz_service = mz_service

    def execute(self, e: Executor) -> None:
        e.mzcompose_composition().await_mz_deployment_status(
            DeploymentStatus.READY_TO_PROMOTE, self.mz_service
        )


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
                "-s",
                "-X",
                "POST",
                "http://127.0.0.1:6878/api/leader/promote",
                capture=True,
            ).stdout
        )
        assert result["result"] == "Success", f"Unexpected result {result}"

        # Wait until new Materialize is ready to handle queries
        c.await_mz_deployment_status(DeploymentStatus.IS_LEADER, self.mz_service)

        mz_version = MzVersion.parse_mz(c.query_mz_version(service=self.mz_service))
        e.current_mz_version = mz_version


class SystemVarChange(MzcomposeAction):
    """Changes a system var."""

    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value

    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()

        c.sql(
            f"ALTER SYSTEM SET {self.name} = {self.value};",
            port=6877,
            user="mz_system",
        )
