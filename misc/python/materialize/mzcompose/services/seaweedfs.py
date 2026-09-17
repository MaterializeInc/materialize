# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


import json

from materialize.mzcompose.service import Service

SEAWEEDFS_VERSION = "4.47"

# Without an S3 config seaweedfs accepts every request unauthenticated, which
# would skip the signature verification the other stores perform. Giving it a
# key keeps the comparison honest.
SEAWEEDFS_ACCESS_KEY = "minioadmin"
SEAWEEDFS_SECRET_KEY = "minioadmin"


def seaweedfs_blob_uri(address: str = "seaweedfs") -> str:
    return f"s3://{SEAWEEDFS_ACCESS_KEY}:{SEAWEEDFS_SECRET_KEY}@persist/persist?endpoint=http://{address}:8333/&region=seaweedfs"


class SeaweedFs(Service):
    """Single-node seaweedfs, an S3-compatible blob store, at `seaweedfs:8333`.

    Runs the all-in-one `mini` mode, which is a master, volume server, filer
    and S3 gateway in one process. The container is healthy only once the
    buckets exist, so start it before, or wait for it separately from, anything
    that depends on it with `service_started`.

    seaweedfs does not fsync on the write path by default, which is what the
    minio image's patch and garage's configuration are there to achieve, so
    nothing extra is needed for durability parity.
    """

    def __init__(
        self,
        name: str = "seaweedfs",
        image: str = f"chrislusf/seaweedfs:{SEAWEEDFS_VERSION}",
        setup_materialize: bool = False,
        additional_buckets: list[str] = [],
        ports: list[int | str] = [8333],
        allow_host_ports: bool = False,
    ) -> None:
        buckets = (["persist"] if setup_materialize else []) + additional_buckets
        s3_config = json.dumps(
            {
                "identities": [
                    {
                        "name": "persist",
                        "credentials": [
                            {
                                "accessKey": SEAWEEDFS_ACCESS_KEY,
                                "secretKey": SEAWEEDFS_SECRET_KEY,
                            }
                        ],
                        "actions": ["Admin", "Read", "Write", "List", "Tagging"],
                    }
                ]
            }
        )
        # The config has to exist before the server reads it, and writing it
        # here keeps the service self-contained, with no image to build and no
        # file for each composition to carry. The image's own entrypoint then
        # takes over, which is what drops privileges to the `seaweed` user.
        command = (
            f"printf '%s' '{s3_config}' > /tmp/seaweedfs-s3.json && "
            "exec /entrypoint.sh mini -dir=/data"
            f"{' -bucket=' + ','.join(buckets) if buckets else ''}"
            " -s3.port=8333"
            " -s3.config=/tmp/seaweedfs-s3.json"
            # 128MiB in `mini` mode, which would spread a benchmark's data over
            # dozens of volumes. This is what the image's own `server` mode uses.
            " -master.volumeSizeLimitMB=1024"
            # Auto-size the volume count from free disk, rather than capping it
            # at a default that a large benchmark would exhaust mid-run.
            " -volume.max=0"
        )
        # The filer serves a bucket as a directory and needs no credentials,
        # unlike the S3 API once a key is configured, so the healthcheck can
        # tell "started" from "ready to serve persist".
        bucket_checks = " && ".join(
            f"curl -sf -o /dev/null http://127.0.0.1:8888/buckets/{bucket}/"
            for bucket in buckets
        ) or "curl -sf -o /dev/null http://127.0.0.1:8333/"
        super().__init__(
            name=name,
            config={
                "image": image,
                "entrypoint": ["sh", "-c"],
                "command": [command],
                "ports": ports,
                "allow_host_ports": allow_host_ports,
                "healthcheck": {
                    "test": ["CMD-SHELL", bucket_checks],
                    "timeout": "5s",
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )
