# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random

from materialize import buildkite
from materialize.mzcompose import DEFAULT_MZ_VOLUMES
from materialize.mzcompose.service import (
    Service,
    ServiceDependency,
)


class Testdrive(Service):
    def __init__(
        self,
        name: str = "testdrive",
        mzbuild: str = "testdrive",
        materialize_url: str = "postgres://materialize@materialized:6875",
        materialize_url_internal: str = "postgres://materialize@materialized:6877",
        materialize_params: dict[str, str] = {},
        kafka_url: str = "kafka:9092",
        kafka_default_partitions: int | None = None,
        kafka_args: str | None = None,
        schema_registry_url: str = "http://schema-registry:8081",
        no_reset: bool = False,
        default_timeout: str | None = None,
        seed: int | None = None,
        consistent_seed: bool = False,
        postgres_stash: str | None = None,
        validate_catalog_store: str | None = None,
        entrypoint: list[str] | None = None,
        entrypoint_extra: list[str] = [],
        environment: list[str] | None = None,
        volumes_extra: list[str] = [],
        volume_workdir: str = ".:/workdir",
        propagate_uid_gid: bool = True,
        forward_buildkite_shard: bool = False,
        aws_region: str | None = None,
        aws_endpoint: str | None = "http://localstack:4566",
        no_consistency_checks: bool = False,
        external_cockroach: bool = False,
        external_minio: bool = False,
        mz_service: str = "materialized",
    ) -> None:
        depends_graph: dict[str, ServiceDependency] = {}

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
            ]

        if aws_region:
            entrypoint.append(f"--aws-region={aws_region}")

        if aws_endpoint and not aws_region:
            entrypoint.append(f"--aws-endpoint={aws_endpoint}")

        if postgres_stash:
            entrypoint.append(
                f"--postgres-stash=postgres://root@{postgres_stash}:26257?options=--search_path=adapter"
            )

        if validate_catalog_store:
            entrypoint.append(f"--validate-catalog-store={validate_catalog_store}")

        if no_reset:
            entrypoint.append("--no-reset")

        for k, v in materialize_params.items():
            entrypoint.append(f"--materialize-param={k}={v}")

        if default_timeout is None:
            default_timeout = "120s"
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

        if external_minio:
            depends_graph["minio"] = {"condition": "service_healthy"}
            persist_blob_url = "s3://minioadmin:minioadmin@persist/persist?endpoint=http://minio:9000/&region=minio"
            entrypoint.append(f"--persist-blob-url={persist_blob_url}")
        else:
            entrypoint.append("--persist-blob-url=file:///mzdata/persist/blob")

        if external_cockroach:
            depends_graph["cockroach"] = {"condition": "service_healthy"}
            entrypoint.append(
                "--persist-consensus-url=postgres://root@cockroach:26257?options=--search_path=consensus"
            )
        else:
            entrypoint.append(
                f"--persist-consensus-url=postgres://root@{mz_service}:26257?options=--search_path=consensus"
            )

        entrypoint.extend(entrypoint_extra)

        super().__init__(
            name=name,
            config={
                "depends_on": depends_graph,
                "mzbuild": mzbuild,
                "entrypoint": entrypoint,
                "environment": environment,
                "volumes": volumes,
                "propagate_uid_gid": propagate_uid_gid,
                "init": True,
            },
        )
