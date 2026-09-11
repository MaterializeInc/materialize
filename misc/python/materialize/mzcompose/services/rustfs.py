# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service

RUSTFS_VERSION = "1.0.0-rc.5"


def rustfs_blob_uri(address: str = "rustfs") -> str:
    return f"s3://minioadmin:minioadmin@persist/persist?endpoint=http://{address}:9000/&region=rustfs"


class RustFs(Service):
    """Single-node rustfs, an S3-compatible blob store, at `rustfs:9000`.

    The container is healthy only once the buckets exist, since they are
    created through the S3 API after the server is up. Start it before, or wait
    for it separately from, anything that depends on it with `service_started`.

    One volume, which is zero-parity erasure coding, matching the single drive
    and `EC:0` the minio image runs with. rustfs refuses several volumes that
    share a device, so more drives would need more devices, not just more
    directories.

    NOTE: rustfs 1.0.0-rc.5 answers `GetObject` with `partNumber` without the
    `x-amz-mp-parts-count` header, and its `HeadObject` ignores `partNumber`.
    Persist reads multipart-uploaded blobs part by part and falls back to byte
    ranges when the count is missing (see `S3Blob::get`), so reads work but
    cost the store one extra round trip per part beyond the first.
    """

    def __init__(
        self,
        name: str = "rustfs",
        image: str = f"rustfs/rustfs:{RUSTFS_VERSION}",
        setup_materialize: bool = False,
        additional_buckets: list[str] = [],
        ports: list[int | str] = [9000],
        allow_host_ports: bool = False,
    ) -> None:
        buckets = (["persist"] if setup_materialize else []) + additional_buckets
        # rustfs keeps objects in an erasure-coded layout, so buckets cannot be
        # pre-created as directories the way the minio image does it. Creation
        # is retried until the bucket is visible: rustfs answers its health
        # endpoint before it accepts bucket operations, and the PUT fails when
        # the bucket survived a restart.
        s3 = "curl -s -o /dev/null --aws-sigv4 aws:amz:rustfs:s3 --user minioadmin:minioadmin"
        create_buckets = "".join(
            f"until {s3} -f -I http://127.0.0.1:9000/{bucket}; do "
            f"{s3} -X PUT http://127.0.0.1:9000/{bucket}; sleep 0.5; done; "
            for bucket in buckets
        )
        # `$$` keeps docker compose from interpolating the shell variables.
        command = (
            "rustfs & pid=$$!; trap 'kill $$pid' TERM INT; "
            "until curl -sf -o /dev/null http://127.0.0.1:9000/health; do sleep 0.2; done; "
            f"{create_buckets}touch /tmp/rustfs-ready; "
            "wait $$pid"
        )
        super().__init__(
            name=name,
            config={
                "image": image,
                "entrypoint": ["sh", "-c"],
                "command": [command],
                "ports": ports,
                "allow_host_ports": allow_host_ports,
                "environment": [
                    "RUSTFS_ACCESS_KEY=minioadmin",
                    "RUSTFS_SECRET_KEY=minioadmin",
                    "RUSTFS_CONSOLE_ENABLE=false",
                    # Speed over durability, like the minio image's patched-out
                    # fdatasync and garage's fsync-off default. The new-bucket
                    # tier would otherwise override the process-wide mode.
                    "RUSTFS_DURABILITY_MODE=none",
                    "RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit",
                    # Background work no composition needs on a store that is
                    # thrown away at the end of the run. minio runs with
                    # MINIO_HEAL_DISABLE=on for the same reason.
                    #
                    # NOTE: in 1.0.0-rc.5 the two switches leave the startup
                    # logs unchanged, so they may gate less than their names
                    # suggest. The scanner preset is set as well, since that
                    # one documents what it controls (sleep factor, maximum
                    # sleep, cycle interval).
                    "RUSTFS_HEAL_ENABLED=false",
                    "RUSTFS_SCANNER_ENABLED=false",
                    "RUSTFS_SCANNER_SPEED=slowest",
                ],
                "healthcheck": {
                    "test": [
                        "CMD-SHELL",
                        "test -f /tmp/rustfs-ready && curl -sf -o /dev/null http://127.0.0.1:9000/health",
                    ],
                    "timeout": "5s",
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )
