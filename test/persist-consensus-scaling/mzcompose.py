# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Experiment: persist consensus throughput against vanilla Postgres as the
number of shards scales.

Each shard is one sequential compare-and-set stream (persist never has more
than one CaS in flight per shard), so the shard count is the concurrency the
consensus backend sees. Traffic runs through toxiproxy with a configurable
added latency, standing in for the network RTT between Materialize and its
metadata Postgres in a real deployment. Postgres runs with real fsync (no
libeatmydata) so commits pay their true cost.

Run manually, e.g.:

    bin/mzcompose --find persist-consensus-scaling run default --shards 8 32 128 512
"""

import json
import threading
import time

import requests

from materialize.mzcompose.composition import (
    Composition,
    Service,
    WorkflowArgumentParser,
)
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.persistcli import Persistcli
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.toxiproxy import Toxiproxy

PROXY_PORT = 26258


def postgres_service(fsync: str = "on") -> Postgres:
    return Postgres(
        name="postgres-metadata",
        setup_materialize=True,
        ports=["26257"],
        # The stock service preloads libeatmydata (fsync disabled), which
        # would hide the commit-latency component this experiment measures.
        environment=[
            "POSTGRESDB=postgres",
            "POSTGRES_PASSWORD=postgres",
        ],
        # Allow SERIALIZABLE runs at high concurrency without exhausting the
        # SSI predicate-lock pool (mirrors PostgresMetadata).
        extra_command=[
            "-c",
            "max_pred_locks_per_transaction=1024",
            "-c",
            "max_pred_locks_per_relation=10000",
            "-c",
            "max_pred_locks_per_page=512",
            # The image's setup-postgres.sh bakes fsync=off into
            # postgresql.conf. Command-line flags take precedence. Real WAL
            # flushes are the one commit cost pipelining cannot hide, so runs
            # default to paying them. fsync=off approximates the fast-flush
            # regime (NVMe) on hosts with slow storage.
            "-c",
            f"fsync={fsync}",
            # Keep checkpoints out of the short measurement windows. A run
            # that randomly contains a checkpoint's fsync burst measures the
            # checkpoint, not the workload, and which run eats it is purely
            # an ordering artifact.
            "-c",
            "max_wal_size=8GB",
            "-c",
            "checkpoint_timeout=30min",
            "-c",
            "log_checkpoints=on",
        ],
    )


SERVICES = [
    postgres_service(),
    Cockroach(setup_materialize=True),
    Toxiproxy(),
    Persistcli(),
]


class ConnectionSampler(threading.Thread):
    """Samples the number of client connections the metadata store sees for
    the consensus user.

    Reports both the overall maximum (which includes the shard-initialization
    burst, a path that always uses exclusive pooled connections) and the
    steady-state maximum over the trailing samples, which reflects the
    benchmark's measured window."""

    def __init__(self, c: Composition, backend: str):
        super().__init__(daemon=True)
        self.c = c
        self.backend = backend
        self.samples: list[int] = []
        self._stop = threading.Event()

    def _sample(self) -> int:
        if self.backend == "postgres":
            out = self.c.exec(
                "postgres-metadata",
                "psql",
                "-U",
                "postgres",
                "-tAc",
                "SELECT count(*) FROM pg_stat_activity"
                " WHERE backend_type = 'client backend' AND usename = 'root'",
                capture=True,
            ).stdout.strip()
        else:
            out = self.c.exec(
                "cockroach",
                "cockroach",
                "sql",
                "--insecure",
                "--format=tsv",
                "--set=show_times=false",
                "-e",
                "SELECT count(*) FROM crdb_internal.cluster_sessions",
                capture=True,
            ).stdout.splitlines()[-1]
        return int(out)

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self.samples.append(self._sample())
            except Exception:
                pass
            time.sleep(1)

    @property
    def max_connections(self) -> int:
        return max(self.samples, default=0)

    @property
    def steady_connections(self) -> int:
        # The tail half of the samples excludes initialization and warmup.
        tail = self.samples[len(self.samples) // 2 :]
        return max(tail, default=0)

    def stop(self) -> None:
        self._stop.set()
        self.join()


def reset_consensus_table(c: Composition, backend: str) -> None:
    """Drop the consensus table (if it exists yet) so runs don't inherit
    bloat or rows from earlier runs. The benchmark's open() recreates it."""
    if backend == "postgres":
        c.exec(
            "postgres-metadata",
            "psql",
            "-U",
            "postgres",
            "-d",
            "root",
            "-c",
            "DROP TABLE IF EXISTS consensus.consensus",
        )
    else:
        c.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            "DROP TABLE IF EXISTS consensus.consensus",
        )


def setup_proxy(c: Composition, upstream: str, rtt_ms: int) -> None:
    url = f"http://localhost:{c.port('toxiproxy', 8474)}"
    # Idempotent across reruns against a still-running toxiproxy.
    requests.delete(f"{url}/proxies/postgres")
    r = requests.post(
        f"{url}/proxies",
        json={
            "name": "postgres",
            "listen": f"0.0.0.0:{PROXY_PORT}",
            "upstream": upstream,
        },
    )
    r.raise_for_status()
    if rtt_ms > 0:
        r = requests.post(
            f"{url}/proxies/postgres/toxics",
            json={
                "name": "rtt",
                "type": "latency",
                "stream": "downstream",
                "attributes": {"latency": rtt_ms, "jitter": 0},
            },
        )
        r.raise_for_status()


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--shards",
        nargs="+",
        type=int,
        default=[8, 32, 128, 512],
        help="shard counts to sweep",
    )
    parser.add_argument("--pool-size", type=int, default=50)
    parser.add_argument("--runtime", default="30s")
    parser.add_argument("--warmup", default="5s")
    parser.add_argument("--data-size", type=int, default=1024)
    parser.add_argument(
        "--rtt-ms",
        type=int,
        default=2,
        help="added response latency via toxiproxy, simulating network RTT",
    )
    parser.add_argument(
        "--serializable",
        action="store_true",
        help="run under SERIALIZABLE instead of READ COMMITTED",
    )
    parser.add_argument(
        "--pipeline-connections",
        type=int,
        default=None,
        help="cap on shared connections consensus ops run on, pipelining"
        " once all are busy (0 forces the exclusive-checkout pool, unset"
        " uses the production default)",
    )
    parser.add_argument(
        "--backend",
        choices=["postgres", "cockroach"],
        default="postgres",
    )
    parser.add_argument("--fsync", choices=["on", "off"], default="on")
    args = parser.parse_args()

    metadata_service = (
        "postgres-metadata" if args.backend == "postgres" else "cockroach"
    )
    with c.override(postgres_service(args.fsync)):
        results = run_benchmarks(c, args, metadata_service)

    print()
    header = (
        f"{'shards':>7} {'pipeline':>9} {'cas/s':>9} {'p50 ms':>8} {'p95 ms':>8}"
        f" {'p99 ms':>8} {'max ms':>9} {'errors':>7} {'conns':>6} {'steady':>7}"
    )
    print(header)
    for r in results:
        print(
            f"{r['num_shards']:>7} {r['pipeline_connections']:>9} {r['cas_per_s']:>9.0f}"
            f" {r['p50_ms']:>8.2f} {r['p95_ms']:>8.2f} {r['p99_ms']:>8.2f}"
            f" {r['max_ms']:>9.2f} {r['errors']:>7} {r['max_pg_connections']:>6}"
            f" {r['steady_pg_connections']:>7}"
        )


def run_benchmarks(c: Composition, args, metadata_service: str) -> list[dict]:
    c.up(metadata_service, "toxiproxy", Service("persistcli", idle=True))
    setup_proxy(c, f"{metadata_service}:26257", args.rtt_ms)

    results = []
    for num_shards in args.shards:
        reset_consensus_table(c, args.backend)
        sampler = ConnectionSampler(c, args.backend)
        sampler.start()
        cmd = [
            "persistcli",
            "consensus-bench",
            f"--consensus-uri=postgres://root@toxiproxy:{PROXY_PORT}"
            "?options=--search_path=consensus",
            f"--num-shards={num_shards}",
            f"--runtime={args.runtime}",
            f"--warmup={args.warmup}",
            f"--data-size={args.data_size}",
            f"--pool-size={args.pool_size}",
        ]
        if args.pipeline_connections is not None:
            cmd.append(f"--pipeline-connections={args.pipeline_connections}")
        # The READ COMMITTED query family is Postgres-only. CockroachDB always
        # runs the SERIALIZABLE CRDB_* queries.
        if not args.serializable and args.backend == "postgres":
            cmd.append("--read-committed")
        print(f"--- running {num_shards} shards")
        out = c.exec("persistcli", *cmd, capture=True).stdout
        sampler.stop()
        result = None
        for line in out.splitlines():
            if line.startswith("RESULT "):
                result = json.loads(line[len("RESULT ") :])
        assert result is not None, f"no RESULT line in output:\n{out}"
        result["max_pg_connections"] = sampler.max_connections
        result["steady_pg_connections"] = sampler.steady_connections
        print(f"--- {num_shards} shards: {result}")
        results.append(result)

    return results
