# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
import hashlib
import os

from materialize import MZ_ROOT
from materialize.mzcompose import loader
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
)


class FoundationDB(Service):
    def __init__(
        self,
        name: str = "foundationdb",
        mzbuild: str = "foundationdb",
        image: str | None = None,
        ports: list[str] = ["4500"],
        allow_host_ports: bool = False,
        environment: list[str] = [
            "FDB_NETWORKING_MODE=container",
        ],
        volumes: list[str] = [],
        restart: str = "no",
    ) -> None:

        # Extract the container port from "host:container" or just "container" format
        container_port = ports[0].split(":")[-1] if ports else "4500"

        env_extra = [
            f"FDB_COORDINATOR_PORT={container_port}",
            f"FDB_PORT={container_port}",
        ]

        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}

        config.update(
            {
                "ports": ports,
                "allow_host_ports": allow_host_ports,
                "environment": env_extra + environment,
                "restart": restart,
                "volumes": volumes,
            }
        )
        super().__init__(name=name, config=config)


def fdb_cluster_file(
    metadata_store: str | None, external_metadata_store: str | bool
) -> list[str]:
    """
    Generate a FoundationDB cluster file if FoundationDB is used as the metadata store. The
    cluster file is created dynamically based on the external metadata store address. Returns
    an empty list if FoundationDB is not used.
    :param metadata_store: The type of metadata store being used.
    :param external_metadata_store: The address of the external metadata store, or False if not used.
    :return: List of volume mappings for the fdb.cluster file.
    """
    if metadata_store != "foundationdb" or external_metadata_store is False:
        return []
    # Generate fdb.cluster file dynamically based on the metadata store address
    fdb_host = (
        external_metadata_store
        if isinstance(external_metadata_store, str)
        else "foundationdb"
    )
    fdb_cluster_content = f"docker:docker@{fdb_host}:4500"
    fdb_cluster_hash = hashlib.sha256(fdb_cluster_content.encode()).hexdigest()
    fdb_cluster_path = (
        loader.composition_path or MZ_ROOT
    ) / f"fdb_{fdb_cluster_hash}.cluster"
    with open(fdb_cluster_path, "w") as f:
        f.write(fdb_cluster_content)
    os.chmod(fdb_cluster_path, 0o644)

    return [f"{fdb_cluster_path}:/etc/foundationdb/fdb.cluster"]
