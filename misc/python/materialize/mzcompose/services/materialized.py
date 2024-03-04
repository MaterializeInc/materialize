# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from copy import copy

from materialize.mz_version import MzVersion
from materialize.mzcompose import (
    DEFAULT_CRDB_ENVIRONMENT,
    DEFAULT_MZ_ENVIRONMENT_ID,
    DEFAULT_MZ_VOLUMES,
    DEFAULT_SYSTEM_PARAMETERS,
)
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
    ServiceDependency,
)
from materialize.mzcompose.services.minio import minio_blob_uri


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
        options: list[str] = [],
        persist_blob_url: str | None = None,
        default_size: int = Size.DEFAULT_SIZE,
        environment_id: str | None = None,
        propagate_crashes: bool = True,
        external_cockroach: str | bool = False,
        external_minio: str | bool = False,
        unsafe_mode: bool = True,
        restart: str | None = None,
        use_default_volumes: bool = True,
        ports: list[str] | None = None,
        system_parameter_defaults: dict[str, str] | None = None,
        additional_system_parameter_defaults: dict[str, str] | None = None,
        soft_assertions: bool = True,
        sanity_restart: bool = True,
        catalog_store: str | None = "persist",
        platform: str | None = None,
        healthcheck: list[str] | None = None,
    ) -> None:
        if name is None:
            name = "materialized"

        if healthcheck is None:
            healthcheck = ["CMD", "curl", "-f", "localhost:6878/api/readyz"]

        depends_graph: dict[str, ServiceDependency] = {
            s: {"condition": "service_started"} for s in depends_on
        }

        environment = [
            f"MZ_SOFT_ASSERTIONS={int(soft_assertions)}",
            # TODO(benesch): remove the following environment variables
            # after v0.38 ships, since these environment variables will be
            # baked into the Docker image.
            "MZ_ORCHESTRATOR=process",
            # The following settings can not be baked in the default image, as they
            # are enabled for testing purposes only
            "MZ_ORCHESTRATOR_PROCESS_TCP_PROXY_LISTEN_ADDR=0.0.0.0",
            "MZ_ORCHESTRATOR_PROCESS_PROMETHEUS_SERVICE_DISCOVERY_DIRECTORY=/mzdata/prometheus",
            "MZ_BOOTSTRAP_ROLE=materialize",
            "MZ_INTERNAL_PERSIST_PUBSUB_LISTEN_ADDR=0.0.0.0:6879",
            "MZ_AWS_CONNECTION_ROLE_ARN=arn:aws:iam::123456789000:role/MaterializeConnection",
            "MZ_AWS_EXTERNAL_ID_PREFIX=eb5cb59b-e2fe-41f3-87ca-d2176a495345",
            # Please think twice before forwarding additional environment
            # variables from the host, as it's easy to write tests that are
            # then accidentally dependent on the state of the host machine.
            #
            # To dynamically change the environment during a workflow run,
            # use Composition.override.
            "MZ_LOG_FILTER",
            "CLUSTERD_LOG_FILTER",
            *environment_extra,
            *DEFAULT_CRDB_ENVIRONMENT,
        ]

        if system_parameter_defaults is None:
            # Has to be copied so we later don't modify the
            # DEFAULT_SYSTEM_PARAMETERS dictionary
            system_parameter_defaults = copy(DEFAULT_SYSTEM_PARAMETERS)

        if additional_system_parameter_defaults is not None:
            system_parameter_defaults.update(additional_system_parameter_defaults)

        if len(system_parameter_defaults) > 0:
            environment += [
                "MZ_SYSTEM_PARAMETER_DEFAULT="
                + ";".join(
                    f"{key}={value}" for key, value in system_parameter_defaults.items()
                )
            ]

        if catalog_store:
            environment.append(f"MZ_CATALOG_STORE={catalog_store}")

        command = []

        if unsafe_mode:
            command += ["--unsafe-mode"]

        if not environment_id:
            environment_id = DEFAULT_MZ_ENVIRONMENT_ID
        command += [f"--environment-id={environment_id}"]

        if external_minio:
            depends_graph["minio"] = {"condition": "service_healthy"}
            address = "minio" if external_minio == True else external_minio
            persist_blob_url = minio_blob_uri(address)

        if persist_blob_url:
            command.append(f"--persist-blob-url={persist_blob_url}")

        if propagate_crashes:
            command += ["--orchestrator-process-propagate-crashes"]

        self.default_storage_size = (
            default_size
            if image
            and "latest" not in image
            and "devel" not in image
            and "unstable" not in image
            and MzVersion.parse_mz(image.split(":")[1]) < MzVersion.parse_mz("v0.41.0")
            else "1"
            if default_size == 1
            else f"{default_size}-1"
        )

        self.default_replica_size = (
            "1" if default_size == 1 else f"{default_size}-{default_size}"
        )
        command += [
            # Issue #15858 prevents the habitual use of large introspection
            # clusters, so we leave the builtin cluster replica size as is.
            # f"--bootstrap-builtin-cluster-replica-size={self.default_replica_size}",
            f"--bootstrap-default-cluster-replica-size={self.default_replica_size}",
            f"--default-storage-host-size={self.default_storage_size}",
        ]

        if external_cockroach:
            address = "cockroach" if external_cockroach == True else external_cockroach
            depends_graph["cockroach"] = {"condition": "service_healthy"}
            command += [
                f"--adapter-stash-url=postgres://root@{address}:26257?options=--search_path=adapter",
                f"--storage-stash-url=postgres://root@{address}:26257?options=--search_path=storage",
                f"--persist-consensus-url=postgres://root@{address}:26257?options=--search_path=consensus",
            ]
            environment += [
                f"MZ_TIMESTAMP_ORACLE_URL=postgres://root@{address}:26257?options=--search_path=tsoracle",
                "MZ_NO_BUILTIN_COCKROACH=1",
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
            # when using `external_cockroach=False`, as the consensus/blob URLs
            # refer to the container's hostname, and we don't want those URLs to
            # change when the container is recreated.
            "hostname": name,
        }

        if image:
            config["image"] = image
        else:
            config["mzbuild"] = "materialized"

        if restart:
            config["restart"] = restart

        # Depending on the Docker Compose version, this may either work or be
        # ignored with a warning. Unfortunately no portable way of setting the
        # memory limit is known.
        if memory:
            config["deploy"] = {"resources": {"limits": {"memory": memory}}}

        if sanity_restart:
            # Workaround for https://github.com/docker/compose/issues/11133
            config["labels"] = {"sanity_restart": True}

        if platform:
            config["platform"] = platform

        volumes = []
        if use_default_volumes:
            volumes += DEFAULT_MZ_VOLUMES
        volumes += volumes_extra

        config.update(
            {
                "depends_on": depends_graph,
                "command": command,
                "ports": [6875, 6876, 6877, 6878, 6880, 6881, 26257],
                "environment": environment,
                "volumes": volumes,
                "tmpfs": ["/tmp"],
                "healthcheck": {
                    "test": healthcheck,
                    "interval": "1s",
                    # A fully loaded Materialize can take a long time to start.
                    "start_period": "600s",
                },
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
