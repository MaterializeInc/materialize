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
from textwrap import dedent

from materialize import MZ_ROOT
from materialize.mzcompose import loader
from materialize.mzcompose.service import (
    Service,
    ServiceConfig,
    ServiceDependency,
)

STANDALONE_INIT_SCRIPT = dedent(
    """
    set -e

    CLUSTER_FILE="${FDB_CLUSTER_FILE:-/var/fdb/fdb.cluster}"

    # Function to initialize the database
    initialize_database() {
        echo "Checking FoundationDB status..."

        # If status minimal succeeds, database is already configured
        if fdbcli -C "$CLUSTER_FILE" --exec "status minimal" --timeout 5 2>/dev/null; then
            echo "Database already configured"
            return 0
        fi

        # Configure the database
        echo "Configuring new single ssd database..."
        fdbcli -C "$CLUSTER_FILE" --exec "configure new single ssd" --timeout 30
        echo "Database configured successfully"
    }

    # Run initialization in background after a delay to let fdbserver start
    (
        sleep 1
        initialize_database
    ) &

    # Execute the original entrypoint
    exec /var/fdb/scripts/fdb.bash "$@"
    """
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
        cluster_file_contents: str | None = None,
        standalone: bool = True,
    ) -> None:
        """
        Create a FoundationDB server node service.

        :param name: Service name.
        :param cluster_file_contents: Contents of the FDB cluster file (e.g., "docker:docker@host1:4500,host2:4500").
                                     If None, the container will use FDB_COORDINATOR for single-node setup.
        """

        # Extract the container port from "host:container" or just "container" format
        container_port = ports[0].split(":")[-1] if ports else "4500"

        env_extra = [
            f"FDB_COORDINATOR_PORT={container_port}",
            f"FDB_PORT={container_port}",
        ]

        # If cluster file contents are specified, use FDB_CLUSTER_FILE_CONTENTS
        # This is the preferred way to configure multi-node clusters
        if cluster_file_contents:
            env_extra.append(f"FDB_CLUSTER_FILE_CONTENTS={cluster_file_contents}")

        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}

        if standalone:
            config.update(
                {
                    "entrypoint": ["bash", "-c", STANDALONE_INIT_SCRIPT],
                }
            )

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


class FoundationDBCluster(Service):
    def __init__(
        self,
        name: str = "foundationdb",
        mzbuild: str = "foundationdb",
        image: str | None = None,
        node_names: list[str] | None = None,
        cluster_file_contents: str | None = None,
    ) -> None:
        """
        Create a FoundationDB cluster initialization service.

        This service depends on all FDB server nodes, initializes the database once,
        and provides a healthcheck for other services to depend on.

        :param name: Service name (typically "foundationdb" for dependency compatibility).
        :param node_names: List of FDB server node service names to depend on.
        :param cluster_file_contents: Contents of the FDB cluster file.
        """
        node_names = node_names or []

        # Build depends_on with service_healthy condition for all nodes
        depends_on: dict[str, ServiceDependency] = {
            node: {"condition": "service_healthy"} for node in node_names
        }

        # Command to initialize the database and then sleep
        # Embed cluster file contents directly to avoid environment variable issues
        cluster_file = cluster_file_contents or ""
        init_script = (
            "set -e; "
            f"printf '%s' '{cluster_file}' > /var/fdb/fdb.cluster; "
            # Check if already configured
            "if fdbcli -C /var/fdb/fdb.cluster --exec 'status minimal' --timeout 5 2>/dev/null; then "
            "  echo 'Database already configured'; "
            "  exec sleep infinity; "
            "fi; "
            # Not configured - configure it now
            "echo 'Configuring new database...'; "
            "fdbcli -C /var/fdb/fdb.cluster --exec 'configure new single ssd' --timeout 30; "
            "echo 'Database configured successfully'; "
            "exec sleep infinity"
        )

        config: ServiceConfig = {"image": image} if image else {"mzbuild": mzbuild}

        config.update(
            {
                "depends_on": depends_on,
                "entrypoint": ["bash", "-c", init_script],
                "healthcheck": {
                    "test": [
                        "CMD",
                        "fdbcli",
                        "-C",
                        "/var/fdb/fdb.cluster",
                        "--exec",
                        "status minimal",
                        "--timeout",
                        "5",
                    ],
                    "interval": "1s",
                    "start_period": "30s",
                },
            }
        )
        super().__init__(name=name, config=config)


def foundationdb_services(
    num_nodes: int = 1,
    base_name: str = "foundationdb",
    mzbuild: str = "foundationdb",
    image: str | None = None,
    port: int = 4500,
    environment: list[str] = [
        "FDB_NETWORKING_MODE=container",
    ],
    restart: str = "no",
) -> list[Service]:
    """
    Create a list of FoundationDB services forming a cluster.

    Returns server nodes named "{base_name}-0", "{base_name}-1", etc.,
    plus a FoundationDBCluster service named "{base_name}" that handles initialization and
    serves as the dependency target for other services.

    :param num_nodes: Number of nodes in the cluster. Use 1 for single-node,
                     3 or 5 for fault-tolerant clusters.
    :param base_name: Base name for services. The cluster service uses this name directly.
    :param port: Container port for FDB.
    :return: List of FoundationDB Service instances. The last service is the
             FoundationDBCluster that other services should depend on.
    """
    if num_nodes < 1:
        raise ValueError("num_nodes must be at least 1")

    # Create server nodes with numbered names
    node_names = [f"{base_name}-{i}" for i in range(num_nodes)]
    coordinator_list = ",".join(f"{name}:{port}" for name in node_names)
    cluster_file_contents = f"docker:docker@{coordinator_list}"

    services: list[Service] = []
    for name in node_names:
        services.append(
            FoundationDB(
                name=name,
                mzbuild=mzbuild,
                image=image,
                ports=[str(port)],
                environment=environment,
                restart=restart,
                cluster_file_contents=cluster_file_contents,
                standalone=False,
            )
        )

    # Add the cluster service that handles initialization
    # This service has the base_name for dependency compatibility
    services.append(
        FoundationDBCluster(
            name=base_name,
            mzbuild=mzbuild,
            image=image,
            node_names=node_names,
            cluster_file_contents=cluster_file_contents,
        )
    )

    return services


def fdb_cluster_file(
    external_metadata_store: str | bool,
    num_nodes: int = 1,
    base_name: str = "foundationdb",
    port: int = 4500,
) -> list[str]:
    """
    Generate a FoundationDB cluster file if FoundationDB is used as the metadata store. The
    cluster file is created dynamically based on the external metadata store address. Returns
    an empty list if FoundationDB is not used.

    :param external_metadata_store: The address of the external metadata store, or False if not used.
    :param num_nodes: Number of nodes in the cluster (for internal FDB clusters).
    :param base_name: Base name for FDB services (for internal FDB clusters).
    :param port: Port for FDB services.
    :return: List of volume mappings for the fdb.cluster file.
    """

    # Generate fdb.cluster file dynamically based on the metadata store address
    if isinstance(external_metadata_store, str):
        # External metadata store - use the provided address
        # If the address contains ':', assume it already includes port info
        if ":" in external_metadata_store:
            fdb_cluster_content = f"docker:docker@{external_metadata_store}"
        else:
            fdb_cluster_content = f"docker:docker@{external_metadata_store}:{port}"
    else:
        # Internal cluster - always use numbered node names (foundationdb-0, foundationdb-1, etc.)
        coordinators = ",".join(f"{base_name}-{i}:{port}" for i in range(num_nodes))
        fdb_cluster_content = f"docker:docker@{coordinators}"

    fdb_cluster_hash = hashlib.sha256(fdb_cluster_content.encode()).hexdigest()
    fdb_cluster_path = (
        loader.composition_path or MZ_ROOT
    ) / f"fdb_{fdb_cluster_hash}.cluster"
    # Remove if it's a directory (can happen if Docker created it as a placeholder)
    if fdb_cluster_path.is_dir():
        fdb_cluster_path.rmdir()
    with open(fdb_cluster_path, "w") as f:
        f.write(fdb_cluster_content)
    os.chmod(fdb_cluster_path, 0o644)

    return [f"{fdb_cluster_path}:/etc/foundationdb/fdb.cluster"]


FDB_NUM_NODES: int = int(os.getenv("FDB_NUM_NODES", "1"))
""" Number of FoundationDB nodes to use. Set via FDB_NUM_NODES environment variable. """


def fdb_coordinator_addresses(
    num_nodes: int = FDB_NUM_NODES, base_name: str = "foundationdb", port: int = 4500
) -> str:
    """
    Returns the FDB coordinator addresses string for the cluster.
    This can be passed to Materialized/Testdrive's external_metadata_store parameter.

    The behavior must be compatible with [`foundationdb_services`].
    """
    node_names = [f"{base_name}-{i}" for i in range(num_nodes)]
    return ",".join(f"{name}:{port}" for name in node_names)
