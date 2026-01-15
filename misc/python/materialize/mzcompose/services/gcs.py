# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service, ServiceHealthcheck


# Google's storage-testbench uses separate ports for HTTP and gRPC
GCS_HTTP_PORT = 9000
GCS_GRPC_PORT = 9001


def gcs_blob_uri(bucket: str = "persist") -> str:
    return f"gcs://{bucket}/persist"


def gcs_emulator_host_http(address: str = "gcs-emulator") -> str:
    """Returns the HTTP/REST endpoint for the GCS emulator."""
    return f"http://{address}:{GCS_HTTP_PORT}"


def gcs_emulator_host_grpc(address: str = "gcs-emulator") -> str:
    """Returns the gRPC endpoint for the GCS emulator."""
    return f"http://{address}:{GCS_GRPC_PORT}"


class GcsEmulator(Service):
    """Google storage-testbench: Official GCS emulator for testing.

    Supports both REST and gRPC APIs.
    See: https://github.com/googleapis/storage-testbench
    """

    def __init__(
        self,
        name: str = "gcs-emulator",
        setup_materialize: bool = False,
        additional_buckets: list[str] | None = None,
        ports: list[int | str] | None = None,
        allow_host_ports: bool = False,
        healthcheck: ServiceHealthcheck | None = None,
        stop_grace_period: str = "120s",
    ):
        if ports is None:
            ports = [GCS_HTTP_PORT, GCS_GRPC_PORT]

        if additional_buckets is None:
            additional_buckets = []

        buckets_to_create = []
        if setup_materialize:
            buckets_to_create.append("persist")
        buckets_to_create.extend(additional_buckets)

        # Build bucket creation commands using the REST API
        # storage-testbench creates buckets via POST to /storage/v1/b?project=test
        bucket_commands = ""
        if buckets_to_create:
            bucket_commands = "\n".join(
                f'echo "Creating bucket {bucket}..." && '
                f'curl -sf -X POST "http://localhost:{GCS_HTTP_PORT}/storage/v1/b?project=test" '
                f'-H "Content-Type: application/json" '
                f"""-d '{{"name": "{bucket}"}}' """
                f'&& echo "Bucket {bucket} created"'
                for bucket in buckets_to_create
            )

        # The startup script:
        # 1. Starts storage-testbench in background
        # 2. Waits for HTTP server to be ready
        # 3. Starts gRPC server via HTTP request
        # 4. Creates buckets
        # 5. Waits for the background process
        startup_script = f"""
            cd /opt/storage-testbench && python3 testbench_run.py 0.0.0.0 {GCS_HTTP_PORT} 10 &
            PID=$!
            echo "Waiting for HTTP server..."
            until curl -s http://localhost:{GCS_HTTP_PORT}/ 2>&1 | grep -q "OK"; do
                sleep 0.5
            done
            echo "HTTP server ready, starting gRPC server..."
            curl -s "http://localhost:{GCS_HTTP_PORT}/start_grpc?port={GCS_GRPC_PORT}"
            echo "gRPC server started on port {GCS_GRPC_PORT}"
            {bucket_commands}
            echo "GCS emulator ready"
            wait $PID
        """

        if healthcheck is None:
            healthcheck = {
                "test": [
                    "CMD-SHELL",
                    f"curl -s http://localhost:{GCS_HTTP_PORT}/ | grep -q OK",
                ],
                "timeout": "5s",
                "interval": "1s",
                "start_period": "30s",
            }

        config: dict = {
            "mzbuild": "gcs-emulator",
            "ports": ports,
            "allow_host_ports": allow_host_ports,
            "healthcheck": healthcheck,
            "stop_grace_period": stop_grace_period,
            "init": True,
            "entrypoint": ["bash", "-c"],
            "command": [startup_script],
        }

        super().__init__(name=name, config=config)
