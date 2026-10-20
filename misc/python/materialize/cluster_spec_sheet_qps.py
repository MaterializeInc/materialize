# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""QPS sweep configuration and result validation for the cluster spec sheet."""

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

        common = {
            "PGHOST": options["-host"],
            "PGPORT": options["-port"],
            "PGUSER": options["-username"],
            "PGPASSWORD": options.get("-password", ""),
            "PGDATABASE": options["-database"],
            "PGSSLMODE": params["sslmode"][0],
            "PGCONNECT_TIMEOUT": str(int(self.query_timeout) + 1),
            "PGAPPNAME": "cluster-spec-sheet-qps",
        }
        clusters = ["c" if i == 0 else f"qps_{i}" for i in range(self.clusters)]
        return {
            "libpq_envs": [
                common
                | {
                    "PGOPTIONS": f"-c cluster={cluster} -c statement_timeout={int(self.query_timeout * 1000)}"
                }
                for cluster in clusters
            ],
            "cluster_names": clusters,
            "concurrency": concurrency,
            "protocol": protocol,
            "warmup_seconds": self.warmup,
            "duration_seconds": self.duration,
            "query_timeout_seconds": self.query_timeout,
            "query": "SELECT * FROM qps_gen_view WHERE x = 5",
            "expected_rows": 1,
        }


def validate_qps_result(result: dict, concurrency: int, protocol: str) -> None:
    if (
        result["concurrency"] != concurrency
        or result["protocol"] != protocol
        or result["errors"] != 0
        or result["queries"] <= 0
        or result["qps"] <= 0
        or result["mean_latency_ms"] <= 0
        or not 0
        < result["p50_latency_ms"]
        <= result["p95_latency_ms"]
        <= result["p99_latency_ms"]
        <= result["max_latency_ms"]
        or result["mean_latency_ms"] > result["max_latency_ms"]
        or not all(
            isfinite(result[k])
            for k in (
                "qps",
                "mean_latency_ms",
                "p50_latency_ms",
                "p95_latency_ms",
                "p99_latency_ms",
                "max_latency_ms",
            )
        )
    ):
        raise ValueError("Invalid or unsuccessful QPS measurement")
