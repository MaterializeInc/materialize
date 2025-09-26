# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
from typing import Any

from materialize import buildkite, ui
from materialize.mzcompose import DEFAULT_MZ_VOLUMES, cluster_replica_size_map
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)
from materialize.mzcompose.services.azurite import azure_blob_uri
from materialize.mzcompose.services.minio import minio_blob_uri
from materialize.mzcompose.services.postgres import METADATA_STORE


class Testdrive(Service):
    def __init__(
        self,
        name: str = "testdrive",
        mzbuild: str = "testdrive",
        materialize_url: str = "postgres://materialize@materialized:6875",
        materialize_url_internal: str = "postgres://materialize@materialized:6877",
        materialize_use_https: bool = False,
        materialize_params: dict[str, str] = {},
        kafka_url: str = "kafka:9092",
        kafka_default_partitions: int | None = None,
        kafka_args: str | None = None,
        schema_registry_url: str = "http://schema-registry:8081",
        no_reset: bool = False,
        default_timeout: str | None = None,
        seed: int | None = None,
        consistent_seed: bool = False,
        validate_catalog_store: bool = False,
        entrypoint: list[str] | None = None,
        entrypoint_extra: list[str] = [],
        environment: list[str] | None = None,
        volumes_extra: list[str] = [],
        volume_workdir: str = ".:/workdir",
        propagate_uid_gid: bool = True,
        forward_buildkite_shard: bool = False,
        aws_region: str | None = None,
        aws_endpoint: str | None = "http://minio:9000",
        aws_access_key_id: str | None = "minioadmin",
        aws_secret_access_key: str | None = "minioadmin",
        no_consistency_checks: bool = False,
        external_metadata_store: bool = False,
        external_blob_store: bool = False,
        blob_store_is_azure: bool = False,
        fivetran_destination: bool = False,
        fivetran_destination_url: str = "http://fivetran-destination:6874",
        fivetran_destination_files_path: str = "/share/tmp",
        mz_service: str = "materialized",
        metadata_store: str = METADATA_STORE,
        stop_grace_period: str = "120s",
        cluster_replica_size: dict[str, dict[str, Any]] | None = None,
        network_mode: str | None = None,
        set_persist_urls: bool = True,
        backoff_factor: float = 1.0,
        networks: (
            dict[str, dict[str, list[str]]]
            | dict[str, dict[str, str]]
            | list[str]
            | None
        ) = None,
    ) -> None:
        if cluster_replica_size is None:
            cluster_replica_size = cluster_replica_size_map()

        if environment is None:
            environment = [
                "TMPDIR=/share/tmp",
                "MZ_SOFT_ASSERTIONS=1",
                # Please think twice before forwarding additional environment
                # variables from the host, as it's easy to write tests that are
                # then accidentally dependent on the state of the host machine.
                #
                # To pass arguments to a testdrive script, use the `--var` CLI
                # option rather than environment variables.
                "MZ_LOG_FILTER",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ]

        environment += [
            f"CLUSTER_REPLICA_SIZES={json.dumps(cluster_replica_size)}",
            "MZ_CI_LICENSE_KEY",
            "LD_PRELOAD=libeatmydata.so",
        ]

        volumes = [
            volume_workdir,
            *(v for v in DEFAULT_MZ_VOLUMES if v.startswith("tmp:")),
        ]
        if volumes_extra:
            volumes.extend(volumes_extra)

        if entrypoint is None:
            entrypoint = [
                "testdrive",
                f"--kafka-addr={kafka_url}",
                f"--schema-registry-url={schema_registry_url}",
                f"--materialize-url={materialize_url}",
                f"--materialize-internal-url={materialize_url_internal}",
                *(["--materialize-use-https"] if materialize_use_https else []),
                # Faster retries
            ]

        entrypoint.append(f"--backoff-factor={backoff_factor}")

        if aws_region:
            entrypoint.append(f"--aws-region={aws_region}")

        if aws_endpoint and not aws_region:
            entrypoint.append(f"--aws-endpoint={aws_endpoint}")
            entrypoint.append(f"--var=aws-endpoint={aws_endpoint}")

        if aws_access_key_id:
            entrypoint.append(f"--aws-access-key-id={aws_access_key_id}")
            entrypoint.append(f"--var=aws-access-key-id={aws_access_key_id}")

        if aws_secret_access_key:
            entrypoint.append(f"--aws-secret-access-key={aws_secret_access_key}")
            entrypoint.append(f"--var=aws-secret-access-key={aws_secret_access_key}")

        if validate_catalog_store:
            entrypoint.append("--validate-catalog-store")

        if no_reset:
            entrypoint.append("--no-reset")

        for k, v in materialize_params.items():
            entrypoint.append(f"--materialize-param={k}={v}")

        if default_timeout is None:
            default_timeout = (
                "120s" if ui.env_is_truthy("CI_COVERAGE_ENABLED") else "20s"
            )
        entrypoint.append(f"--default-timeout={default_timeout}")

        if kafka_default_partitions:
            entrypoint.append(f"--kafka-default-partitions={kafka_default_partitions}")

        if forward_buildkite_shard:
            shard = buildkite.get_parallelism_index()
            shard_count = buildkite.get_parallelism_count()
            entrypoint += [f"--shard={shard}", f"--shard-count={shard_count}"]

        if seed is not None and consistent_seed:
            raise RuntimeError("Can't pass `seed` and `consistent_seed` at same time")
        elif consistent_seed:
            entrypoint.append(f"--seed={random.getrandbits(32)}")
        elif seed is not None:
            entrypoint.append(f"--seed={seed}")

        if no_consistency_checks:
            entrypoint.append("--consistency-checks=disable")

        if fivetran_destination:
            entrypoint.append(f"--fivetran-destination-url={fivetran_destination_url}")
            entrypoint.append(
                f"--fivetran-destination-files-path={fivetran_destination_files_path}"
            )

        if set_persist_urls:
            if external_blob_store:
                blob_store = "azurite" if blob_store_is_azure else "minio"
                address = (
                    blob_store if external_blob_store == True else external_blob_store
                )
                persist_blob_url = (
                    azure_blob_uri(address)
                    if blob_store_is_azure
                    else minio_blob_uri(address)
                )
                entrypoint.append(f"--persist-blob-url={persist_blob_url}")
            else:
                entrypoint.append("--persist-blob-url=file:///mzdata/persist/blob")

            if external_metadata_store:
                entrypoint.append(
                    "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus"
                )
            else:
                entrypoint.append(
                    f"--persist-consensus-url=postgres://root@{mz_service}:26257?options=--search_path=consensus"
                )

        entrypoint.extend(entrypoint_extra)

        config: ServiceConfig = {
            "mzbuild": mzbuild,
            "entrypoint": entrypoint,
            "environment": environment,
            "volumes": volumes,
            "propagate_uid_gid": propagate_uid_gid,
            "init": True,
            "stop_grace_period": stop_grace_period,
        }
        if network_mode:
            config["network_mode"] = network_mode

        if networks:
            config["networks"] = networks

        super().__init__(
            name=name,
            config=config,
        )
