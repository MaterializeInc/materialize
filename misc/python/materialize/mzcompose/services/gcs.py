# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service, ServiceHealthcheck


GCS_HTTPS_PORT = 4443

def gcs_blob_uri(bucket: str = "persist") -> str:
    return f"gcs://{bucket}/persist"


def gcs_emulator_host(address: str = "gcs-emulator") -> str:
    return f"http://{address}:{GCS_HTTPS_PORT}"


class GcsEmulator(Service):
    """fake-gcs-server: A GCS-compatible emulator for testing.

    See: http://github.com/fsouza/fake-gcs-server
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
            ports = [GCS_HTTPS_PORT]
            # TODO handle setting port in gcs_emulator_host

        if additional_buckets is None:
            additional_buckets = []

        # fake-gcs-server creates buckets from directories in /data
        # We can use an entrypoint to create the necessary directories
        buckets_to_create = []
        if setup_materialize:
            buckets_to_create.append("persist")
        buckets_to_create.extend(additional_buckets)

        # Build the command to create bucket directories and start the server
        if buckets_to_create:
            mkdir_commands = " && ".join(
                f"mkdir -p /data/{bucket}" for bucket in buckets_to_create
            )
            command = [
                "sh",
                "-c",
                f"{mkdir_commands} && /bin/fake-gcs-server -scheme http -host 0.0.0.0 -port {GCS_HTTPS_PORT} -backend memory",
                #f"{mkdir_commands} && /bin/fake-gcs-server -host 0.0.0.0 -port {GCS_HTTPS_PORT} -backend memory",
            ]
            entrypoint = None
        else:
            command = [
                "-scheme",
                "http",
                "-host",
                "0.0.0.0",
                "-port",
                str(GCS_HTTPS_PORT),
                "-backend",
                "memory",
            ]
            entrypoint = None

        if healthcheck is None:
            healthcheck = {
                "test": [
                    "CMD",
                    "wget",
                    "-q",
                    #"--no-check-certificate",
                    "--spider",
                    f"http://localhost:{GCS_HTTPS_PORT}/storage/v1/b",
                ],
                "timeout": "5s",
                "interval": "1s",
                "start_period": "30s",
            }

        config = {
            "mzbuild": "gcs-emulator",
            "ports": ports,
            "allow_host_ports": allow_host_ports,
            "healthcheck": healthcheck,
            "stop_grace_period": stop_grace_period,
            "init": True,
        }

        if buckets_to_create:
            config["entrypoint"] = ["sh", "-c"]
            config["command"] = [
                f"{mkdir_commands} && /bin/fake-gcs-server -scheme http -host 0.0.0.0 -port {GCS_HTTPS_PORT} -backend memory"
                #f"{mkdir_commands} && /bin/fake-gcs-server -host 0.0.0.0 -port {GCS_HTTPS_PORT} -backend memory"
            ]
        else:
            config["command"] = command

        super().__init__(name=name, config=config)
