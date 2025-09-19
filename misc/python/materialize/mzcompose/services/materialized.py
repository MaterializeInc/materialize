# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import json
import os
import shutil
import tempfile
from enum import Enum
from typing import Any

from materialize import MZ_ROOT, docker
from materialize.mz_version import MzVersion
from materialize.mzcompose import (
    DEFAULT_CRDB_ENVIRONMENT,
    DEFAULT_MZ_ENVIRONMENT_ID,
    DEFAULT_MZ_VOLUMES,
    bootstrap_cluster_replica_size,
    cluster_replica_size_map,
    get_default_system_parameters,
)
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
    ServiceDeploy,
    ServiceDeployResources,
    ServiceDeployResourceLimits,
    ServiceDependency,
)
from materialize.mzcompose.services.azurite import azure_blob_uri
from materialize.mzcompose.services.minio import minio_blob_uri
from materialize.mzcompose.services.postgres import METADATA_STORE


class MaterializeEmulator(Service):
    """Just the Materialize Emulator with its defaults unchanged"""

    def __init__(self, image: str | None = None):
        name = "materialized"

        config: ServiceConfig = {
            "mzbuild": name,
            "ports": [6875, 6876, 6877, 6878, 26257],
            "healthcheck": {
                "test": ["CMD", "curl", "-f", "localhost:6878/api/readyz"],
                "interval": "1s",
                # A fully loaded Materialize can take a long time to start.
                "start_period": "600s",
            },
        }

        super().__init__(name=name, config=config)


class Materialized(Service):
    class Size:
        DEFAULT_SIZE = 4

    def __init__(
        self,
        name: str | None = None,
        image: str | None = None,
        environment_extra: list[str] = [],
        volumes_extra: list[str] = [],
        depends_on: list[str] = [],
        memory: str | None = None,
        cpu: str | None = None,
        options: list[str] = [],
        persist_blob_url: str | None = None,
        default_size: int | str = Size.DEFAULT_SIZE,
        environment_id: str | None = None,
        propagate_crashes: bool = True,
        external_metadata_store: str | bool = False,
        external_blob_store: str | bool = False,
        blob_store_is_azure: bool = False,
        unsafe_mode: bool = True,
        restart: str | None = None,
        use_default_volumes: bool = True,
        ports: list[str] | None = None,
        system_parameter_defaults: dict[str, str] | None = None,
        additional_system_parameter_defaults: dict[str, str] | None = None,
        system_parameter_version: MzVersion | None = None,
        soft_assertions: bool = True,
        sanity_restart: bool = True,
        platform: str | None = None,
        healthcheck: list[str] | None = None,
        deploy_generation: int | None = None,
        force_migrations: str | None = None,
        publish: bool | None = None,
        stop_grace_period: str = "120s",
        metadata_store: str = METADATA_STORE,
        cluster_replica_size: dict[str, dict[str, Any]] | None = None,
        bootstrap_replica_size: str | None = None,
        default_replication_factor: int = 1,
        listeners_config_path: str = f"{MZ_ROOT}/src/materialized/ci/listener_configs/no_auth.json",
        support_external_clusterd: bool = False,
    ) -> None:
        if name is None:
            name = "materialized"

        if healthcheck is None:
            healthcheck = ["CMD", "curl", "-f", "localhost:6878/api/readyz"]

        depends_graph: dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }

        if bootstrap_replica_size is None:
            bootstrap_replica_size = bootstrap_cluster_replica_size()
        if cluster_replica_size is None:
            cluster_replica_size = cluster_replica_size_map()

        environment = [
            "MZ_NO_TELEMETRY=1",
            # Not used in most tests
            "MZ_NO_BUILTIN_CONSOLE=1",
            # Test runs are discarded on system crash anyway
            "MZ_EAT_MY_DATA=1",
            "MZ_TEST_ONLY_DUMMY_SEGMENT_CLIENT=true",
            f"MZ_SOFT_ASSERTIONS={int(soft_assertions)}",
            # The following settings can not be baked in the default image, as they
            # are enabled for testing purposes only
            "MZ_ORCHESTRATOR_PROCESS_TCP_PROXY_LISTEN_ADDR=0.0.0.0",
            "MZ_ORCHESTRATOR_PROCESS_PROMETHEUS_SERVICE_DISCOVERY_DIRECTORY=/mzdata/prometheus",
            "MZ_BOOTSTRAP_ROLE=materialize",
            # TODO move this to the listener config?
            "MZ_INTERNAL_PERSIST_PUBSUB_LISTEN_ADDR=0.0.0.0:6879",
            "MZ_PERSIST_PUBSUB_URL=http://127.0.0.1:6879",
            "MZ_AWS_CONNECTION_ROLE_ARN=arn:aws:iam::123456789000:role/MaterializeConnection",
            "MZ_AWS_EXTERNAL_ID_PREFIX=eb5cb59b-e2fe-41f3-87ca-d2176a495345",
            # Always use the persist catalog if the version has multiple implementations.
            "MZ_CATALOG_STORE=persist",
            # Please think twice before forwarding additional environment
            # variables from the host, as it's easy to write tests that are
            # then accidentally dependent on the state of the host machine.
            #
            # To dynamically change the environment during a workflow run,
            # use Composition.override.
            "MZ_LOG_FILTER",
            "CLUSTERD_LOG_FILTER",
            f"MZ_CLUSTER_REPLICA_SIZES={json.dumps(cluster_replica_size)}",
            f"MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE={bootstrap_replica_size}",
            f"MZ_BOOTSTRAP_BUILTIN_SYSTEM_CLUSTER_REPLICA_SIZE={bootstrap_replica_size}",
            f"MZ_BOOTSTRAP_BUILTIN_PROBE_CLUSTER_REPLICA_SIZE={bootstrap_replica_size}",
            f"MZ_BOOTSTRAP_BUILTIN_SUPPORT_CLUSTER_REPLICA_SIZE={bootstrap_replica_size}",
            f"MZ_BOOTSTRAP_BUILTIN_CATALOG_SERVER_CLUSTER_REPLICA_SIZE={bootstrap_replica_size}",
            f"MZ_BOOTSTRAP_BUILTIN_ANALYTICS_CLUSTER_REPLICA_SIZE={bootstrap_replica_size}",
            # Note(SangJunBak): mz_system and mz_probe have no replicas by default in materialized
            # but we re-enable them here since many of our tests rely on them.
            f"MZ_BOOTSTRAP_BUILTIN_SYSTEM_CLUSTER_REPLICATION_FACTOR={default_replication_factor}",
            f"MZ_BOOTSTRAP_BUILTIN_PROBE_CLUSTER_REPLICATION_FACTOR={default_replication_factor}",
            f"MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICATION_FACTOR={default_replication_factor}",
            *environment_extra,
            *DEFAULT_CRDB_ENVIRONMENT,
        ]

        image_version = None
        if not image:
            image_version = MzVersion.parse_cargo()
        elif ":" in image:
            image_version_str = image.split(":")[1]
            if docker.is_image_tag_of_release_version(image_version_str):
                image_version = MzVersion.parse_mz(image_version_str)

        if system_parameter_defaults is None:
            system_parameter_defaults = get_default_system_parameters(
                system_parameter_version or image_version
            )

        system_parameter_defaults["default_cluster_replication_factor"] = str(
            default_replication_factor
        )
        if additional_system_parameter_defaults is not None:
            system_parameter_defaults.update(additional_system_parameter_defaults)

        if len(system_parameter_defaults) > 0:
            environment += [
                "MZ_SYSTEM_PARAMETER_DEFAULT="
                + ";".join(
                    f"{key}={value}" for key, value in system_parameter_defaults.items()
                )
            ]

        if not support_external_clusterd:
            environment.append("MZ_NO_EXTERNAL_CLUSTERD=1")

        command = []

        if unsafe_mode:
            command += ["--unsafe-mode"]

        if not environment_id:
            environment_id = DEFAULT_MZ_ENVIRONMENT_ID
        command += [f"--environment-id={environment_id}"]

        if external_blob_store:
            blob_store = "azurite" if blob_store_is_azure else "minio"
            depends_graph[blob_store] = {"condition": "service_healthy"}
            address = blob_store if external_blob_store == True else external_blob_store
            persist_blob_url = (
                azure_blob_uri(address)
                if blob_store_is_azure
                else minio_blob_uri(address)
            )

        if persist_blob_url:
            command.append(f"--persist-blob-url={persist_blob_url}")

        if propagate_crashes:
            command += ["--orchestrator-process-propagate-crashes"]

        if deploy_generation is not None:
            command += [f"--deploy-generation={deploy_generation}"]

        if force_migrations is not None and image is None:
            command += [
                f"--unsafe-builtin-table-fingerprint-whitespace={force_migrations}",
            ]
            if not unsafe_mode:
                command += ["--unsafe-mode"]

        self.default_storage_size = (
            "scale=1,workers=1"
            if default_size == 1
            else f"scale={default_size},workers=1"
        )

        self.default_replica_size = (
            "scale=1,workers=1"
            if default_size == 1
            else (
                f"scale={default_size},workers={default_size}"
                if isinstance(default_size, int)
                else default_size
            )
        )

        if external_metadata_store:
            address = (
                metadata_store
                if external_metadata_store == True
                else external_metadata_store
            )
            depends_graph[metadata_store] = {"condition": "service_healthy"}
            command += [
                f"--persist-consensus-url=postgres://root@{address}:26257?options=--search_path=consensus",
            ]
            environment += [
                f"MZ_TIMESTAMP_ORACLE_URL=postgres://root@{address}:26257?options=--search_path=tsoracle",
                "MZ_NO_BUILTIN_POSTGRES=1",
                # For older Materialize versions
                "MZ_NO_BUILTIN_COCKROACH=1",
                # Set the adapter stash URL for older environments that need it (versions before
                # v0.92.0).
                f"MZ_ADAPTER_STASH_URL=postgres://root@{address}:26257?options=--search_path=adapter",
            ]

        command += [
            "--orchestrator-process-tcp-proxy-listen-addr=0.0.0.0",
            "--orchestrator-process-prometheus-service-discovery-directory=/mzdata/prometheus",
        ]

        command += options

        config: ServiceConfig = {
            # Use the service name as the hostname so that it is stable across
            # container recreation. (The default hostname is the container ID,
            # which changes when the container is recreated.) This is important
            # when using `external_metadata_store=False`, as the consensus/blob URLs
            # refer to the container's hostname, and we don't want those URLs to
            # change when the container is recreated.
            "hostname": name,
        }

        if publish is not None:
            config["publish"] = publish

        if image:
            config["image"] = image
        else:
            config["mzbuild"] = "materialized"

        if restart:
            policy, _, max_tries = restart.partition(":")
            if policy == "on-failure":
                environment += ["MZ_RESTART_ON_FAILURE=1"]
                if max_tries:
                    environment += [f"MZ_RESTART_LIMIT={max_tries}"]
            elif policy == "no":
                pass
            else:
                raise RuntimeError(f"unknown restart policy: {policy}")

        # Depending on the Docker Compose version, this may either work or be
        # ignored with a warning. Unfortunately no portable way of setting the
        # memory limit is known.
        if memory or cpu:
            limits: ServiceDeployResourceLimits = {}
            if memory:
                limits["memory"] = memory
            if cpu:
                limits["cpus"] = cpu
            resources: ServiceDeployResources = {"limits": limits}
            deploy: ServiceDeploy = {"resources": resources}
            config["deploy"] = deploy

        if sanity_restart:
            # Workaround for https://github.com/docker/compose/issues/11133
            config["labels"] = {"sanity_restart": True}

        if platform:
            config["platform"] = platform

        volumes = []

        if image_version is None or image_version >= "v0.147.0-dev":
            assert os.path.exists(listeners_config_path)
            volumes.append(f"{listeners_config_path}:/listeners_config")
            environment.append("MZ_LISTENERS_CONFIG_PATH=/listeners_config")

        if image_version is None or image_version >= "v0.140.0-dev":
            if "MZ_CI_LICENSE_KEY" in os.environ:
                # We have to take care to write the license_key file atomically
                # so that it is always valid, even if multiple Materialized
                # objects are created concurrently.
                with tempfile.NamedTemporaryFile("w", delete=False) as tmp_file:
                    tmp_path = tmp_file.name
                    tmp_file.write(os.environ["MZ_CI_LICENSE_KEY"])
                os.chmod(tmp_path, 0o644)
                shutil.move(tmp_path, "license_key")

                environment += ["MZ_LICENSE_KEY=/license_key/license_key"]

                volumes += [f"{os.getcwd()}/license_key:/license_key/license_key"]

        if use_default_volumes:
            volumes += DEFAULT_MZ_VOLUMES
        volumes += volumes_extra

        config.update(
            {
                "depends_on": depends_graph,
                "command": command,
                "ports": [6875, 6876, 6877, 6878, 26257],
                "environment": environment,
                "volumes": volumes,
                "tmpfs": ["/tmp"],
                "healthcheck": {
                    "test": healthcheck,
                    "interval": "1s",
                    # A fully loaded Materialize can take a long time to start.
                    "start_period": "600s",
                },
                "stop_grace_period": stop_grace_period,
            }
        )

        if ports:
            config.update(
                {
                    "allow_host_ports": True,
                    "ports": ports,
                }
            )

        super().__init__(name=name, config=config)


class DeploymentStatus(Enum):
    """See DeploymentStateInner for reference"""

    INITIALIZING = "Initializing"
    READY_TO_PROMOTE = "ReadyToPromote"
    PROMOTING = "Promoting"
    IS_LEADER = "IsLeader"


LEADER_STATUS_HEALTHCHECK: list[str] = [
    "CMD",
    "curl",
    "-f",
    "localhost:6878/api/leader/status",
]
