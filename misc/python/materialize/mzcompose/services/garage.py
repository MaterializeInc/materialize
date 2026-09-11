# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from materialize.mzcompose.service import Service

# Garage only accepts keys shaped like the ones it generates: `GK` followed by
# 12 hex bytes, with a 32 hex byte secret. The entrypoint imports this pair so
# the blob URL can be a constant.
GARAGE_ACCESS_KEY_ID = "GK0123456789abcdef01234567"
GARAGE_SECRET_ACCESS_KEY = "0123456789abcdef" * 4


def garage_blob_uri(address: str = "garage") -> str:
    return f"s3://{GARAGE_ACCESS_KEY_ID}:{GARAGE_SECRET_ACCESS_KEY}@persist/persist?endpoint=http://{address}:3900/&region=garage"


class Garage(Service):
    """Single-node garage, an S3-compatible blob store, at `garage:3900`.

    The container is healthy only once the buckets exist, since garage creates
    them after the server is up. Start it before, or wait for it separately
    from, anything that depends on it with `service_started`.
    """

    def __init__(
        self,
        name: str = "garage",
        setup_materialize: bool = False,
        additional_buckets: list[str] = [],
        ports: list[int | str] = [3900],
        allow_host_ports: bool = False,
    ) -> None:
        buckets = (["persist"] if setup_materialize else []) + additional_buckets
        super().__init__(
            name=name,
            config={
                "mzbuild": "garage",
                "ports": ports,
                "allow_host_ports": allow_host_ports,
                "environment": [
                    f"GARAGE_ACCESS_KEY_ID={GARAGE_ACCESS_KEY_ID}",
                    f"GARAGE_SECRET_ACCESS_KEY={GARAGE_SECRET_ACCESS_KEY}",
                    f"GARAGE_BUCKETS={' '.join(buckets)}",
                ],
                "healthcheck": {
                    "test": [
                        "CMD-SHELL",
                        "test -f /var/lib/garage/meta/ready && garage status >/dev/null",
                    ],
                    "timeout": "5s",
                    "interval": "1s",
                    "start_period": "30s",
                },
            },
        )
