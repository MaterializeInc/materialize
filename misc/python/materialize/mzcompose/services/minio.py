# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import (
    Service,
)


def minio_blob_uri(address: str = "minio") -> str:
    return f"s3://minioadmin:minioadmin@persist/persist?endpoint=http://{address}:9000/&region=minio"


class Minio(Service):
    def __init__(
        self,
        name: str = "minio",
        image: str = "minio/minio:RELEASE.2023-07-07T07-13-57Z",
        setup_materialize: bool = False,
    ) -> None:
        # We can pre-create buckets in minio by creating subdirectories in
        # /data. A bit gross to do this via a shell command, but it's net
        # less complicated than using a separate setup container that runs `mc`.
        command = "minio server /data --console-address :9001"
        if setup_materialize:
            command = f"mkdir -p /data/persist && {command}"
        super().__init__(
            name=name,
            config={
                "entrypoint": ["sh", "-c"],
                "command": [command],
                "image": image,
                "ports": [9000, 9001],
                "environment": ["MINIO_STORAGE_CLASS_STANDARD=EC:0"],
                "healthcheck": {
                    "test": [
                        "CMD",
                        "curl",
                        "--fail",
                        "http://localhost:9000/minio/health/live",
                    ],
                    "timeout": "5s",
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )


class Mc(Service):
    def __init__(
        self,
        name: str = "mc",
        image: str = "minio/mc:RELEASE.2023-07-07T05-25-51Z",
    ) -> None:
        super().__init__(
            name=name,
            config={
                "image": image,
            },
        )
