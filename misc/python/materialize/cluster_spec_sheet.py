# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Configuration and result validation for the cluster spec sheet QPS sweep."""

from dataclasses import dataclass
from math import isfinite
from urllib.parse import parse_qs

QPS_CONCURRENCIES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]


@dataclass(frozen=True)
class QpsSweep:
    concurrencies: list[int]
    protocols: list[str]
    clusters: int = 32
    duration: float = 20
    warmup: float = 5
    query_timeout: float = 30

    def __post_init__(self) -> None:
        if (
            not self.concurrencies
            or any(c < 1 for c in self.concurrencies)
            or len(set(self.concurrencies)) != len(self.concurrencies)
            or not self.protocols
            or any(p not in ("simple", "prepared") for p in self.protocols)
            or len(set(self.protocols)) != len(self.protocols)
            or self.clusters < 1
            or self.duration <= 0
            or self.warmup < 0
            or self.query_timeout <= 0
            or not all(
                isfinite(n) for n in (self.duration, self.warmup, self.query_timeout)
            )
        ):
            raise ValueError("Invalid QPS sweep configuration")

    def config(self, flags: list[str], concurrency: int, protocol: str) -> dict:
        options = dict(zip(flags[::2], flags[1::2]))
        params = parse_qs(options["-params"])

        def quote(s: str) -> str:
            return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

        common = {
            "host": options["-host"],
            "port": options["-port"],
            "user": options["-username"],
            "password": options.get("-password", ""),
            "dbname": options["-database"],
            "sslmode": params["sslmode"][0],
            "connect_timeout": str(int(self.query_timeout) + 1),
            "application_name": "cluster-spec-sheet-qps",
        }
        return {
            "dsns": [
                " ".join(
                    f"{k}={quote(v)}"
                    for k, v in (common | {"cluster": f"qps_{i}"}).items()
                )
                for i in range(self.clusters)
            ],
            "concurrency": concurrency,
            "protocol": protocol,
            "warmup_seconds": self.warmup,
            "duration_seconds": self.duration,
            "query_timeout_seconds": self.query_timeout,
            "query": "SELECT * FROM qps_gen_view",
            "expected_rows": 10,
        }


def validate_qps_result(result: dict, concurrency: int, protocol: str) -> None:
    if (
        result["concurrency"] != concurrency
        or result["protocol"] != protocol
        or result["errors"] != 0
        or result["queries"] <= 0
        or result["qps"] <= 0
        or result["mean_latency_ms"] <= 0
        or not all(
            isfinite(result[k]) for k in ("qps", "mean_latency_ms", "p99_latency_ms")
        )
    ):
        raise ValueError("Invalid or unsuccessful QPS measurement")
