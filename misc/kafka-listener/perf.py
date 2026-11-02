# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Measures Kafka listener source write throughput at different replica sizes.

Run against an environmentd started with misc/kafka-listener/run-environmentd.sh:

    bin/pyactivate misc/kafka-listener/perf.py

For each size and number of producers, the script creates a cluster with one
Kafka listener source, produces JSON records from that many librdkafka
producers, each in its own process with its own connection, for a fixed
duration, and reports the rate of records acknowledged as durable. It then
waits until Materialize holds exactly the acknowledged records, and reports how
long after the last ack that took.

With `--rate`, each producer offers that many records a second, so the offered
load grows with the number of producers. The acknowledged rate then shows where
the listener stops keeping up, and the ack latency, which librdkafka measures
from `produce()` to the acknowledgement, shows how close it is. The script also
reports the CPU that the replica and the producers used, in cores, since both
run on this machine.
"""

import argparse
import json
import multiprocessing
import random
import re
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

import psutil
import psycopg
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parent))
from demo import MZ_ROOT, kafka_endpoint, sql  # noqa: E402

TOPIC = "events"

PRODUCER_CONFIG = {
    "acks": "all",
    "compression.type": "none",
    # Large batches keep the number of requests, and so of log appends, low.
    "linger.ms": 50,
    "batch.num.messages": 50_000,
    "batch.size": 8 * 1024 * 1024,
    "message.max.bytes": 16 * 1024 * 1024,
    "max.in.flight.requests.per.connection": 16,
    # Small enough that queued messages are sent before message.timeout.ms when the source is
    # the bottleneck.
    "queue.buffering.max.messages": 500_000,
    "queue.buffering.max.kbytes": 1024 * 1024,
    "message.timeout.ms": 60_000,
}


def payloads(n: int) -> list[bytes]:
    customers = ["ada", "grace", "edsger", "barbara", "ken", "dennis", "frances"]
    return [
        json.dumps(
            {
                "id": i,
                "customer": random.choice(customers),
                "amount": random.randrange(5, 500),
                "status": random.choice(["pending", "paid", "shipped"]),
                "ts": "2026-10-31T12:00:00Z",
            }
        ).encode()
        for i in range(n)
    ]


# One in this many acknowledged records contributes its latency to the percentiles.
LATENCY_SAMPLE = 50


def produce_for(
    bootstrap: str,
    seconds: float,
    idempotent: bool,
    rate: float | None,
    queue: multiprocessing.Queue,
) -> None:
    values = payloads(10_000)
    acked = 0
    acked_bytes = 0
    failed = 0
    latencies: list[float] = []

    def on_delivery(err, msg):
        nonlocal acked, acked_bytes, failed
        if err is None:
            acked += 1
            acked_bytes += len(msg.value())
            if acked % LATENCY_SAMPLE == 0 and msg.latency() is not None:
                latencies.append(msg.latency())
        else:
            failed += 1

    config = {**PRODUCER_CONFIG, "bootstrap.servers": bootstrap}
    if idempotent:
        # Idempotent producers keep at most 5 requests in flight.
        config["enable.idempotence"] = True
        config["max.in.flight.requests.per.connection"] = 5
    producer = Producer(config)
    started = time.time()
    deadline = started + seconds
    i = 0
    while True:
        now = time.time()
        if now >= deadline:
            break
        if rate is not None and i >= rate * (now - started):
            producer.poll(0.001)
            continue
        try:
            producer.produce(
                TOPIC, value=values[i % len(values)], on_delivery=on_delivery
            )
            i += 1
        except BufferError:
            producer.poll(0.01)
        if i % 1_000 == 0:
            producer.poll(0)
    producer.flush(120)
    queue.put((acked, acked_bytes, failed, started, time.time(), latencies))


def replica_process(cluster_id: str) -> psutil.Process | None:
    """The process of the cluster's replica, if the process orchestrator runs it locally."""
    env = (MZ_ROOT / "mzdata" / "environment-id").read_text().strip()
    pid_files = list(
        Path(tempfile.gettempdir()).glob(
            f"environmentd-{env}/cluster-{cluster_id}-replica-*/0.pid"
        )
    )
    if len(pid_files) != 1:
        return None
    try:
        return psutil.Process(int(pid_files[0].read_text().split()[0]))
    except (ValueError, psutil.Error):
        return None


class CpuSampler(threading.Thread):
    """Samples the CPU, in cores, and the peak resident memory of groups of processes, once a
    second."""

    def __init__(self, groups: dict[str, list[psutil.Process]]):
        super().__init__(daemon=True)
        self.groups = groups
        self.samples: dict[str, list[float]] = {name: [] for name in groups}
        self.peak_rss: dict[str, int] = {name: 0 for name in groups}
        self.stop = threading.Event()

    def run(self) -> None:
        for procs in self.groups.values():
            for p in procs:
                try:
                    p.cpu_percent()
                except psutil.Error:
                    pass
        while not self.stop.wait(1):
            for name, procs in self.groups.items():
                total = 0.0
                rss = 0
                for p in procs:
                    try:
                        total += p.cpu_percent()
                        rss += p.memory_info().rss
                    except psutil.Error:
                        pass
                self.samples[name].append(total / 100)
                self.peak_rss[name] = max(self.peak_rss[name], rss)

    def mean(self, name: str) -> float:
        # The first and last samples cover start-up and shutdown.
        samples = self.samples[name][1:-1] or self.samples[name]
        return statistics.mean(samples) if samples else float("nan")


def run(conn: psycopg.Connection, size: str, producers: int, args) -> dict:
    cluster = "perf_" + "".join(c if c.isalnum() else "_" for c in size.lower())
    sql(conn, f"DROP CLUSTER IF EXISTS {cluster} CASCADE")
    sql(conn, f"CREATE CLUSTER {cluster} SIZE '{size}'")
    sql(
        conn,
        f"CREATE SOURCE {cluster}_events IN CLUSTER {cluster} "
        f"FROM KAFKA LISTENER (TOPIC '{TOPIC}') FORMAT JSON ENVELOPE NONE"
        + (
            f" WITH (TIMESTAMP INTERVAL '{args.timestamp_interval}')"
            if args.timestamp_interval
            else ""
        ),
    )
    (cluster_id,) = sql(conn, f"SELECT id FROM mz_clusters WHERE name = '{cluster}'")[0]
    bootstrap = kafka_endpoint(cluster_id)
    # Wait for the source to claim its topic.
    for _ in range(120):
        rows = sql(
            conn,
            "SELECT st.status FROM mz_internal.mz_source_statuses st "
            f"JOIN mz_sources s ON s.id = st.id WHERE s.name = '{cluster}_events'",
        )
        if rows and rows[0][0] == "running":
            break
        time.sleep(0.5)

    queue: multiprocessing.Queue = multiprocessing.Queue()
    procs = [
        multiprocessing.Process(
            target=produce_for,
            args=(bootstrap, args.seconds, args.idempotent, args.rate, queue),
        )
        for _ in range(producers)
    ]
    for p in procs:
        p.start()
    replica = replica_process(cluster_id)
    sampler = CpuSampler(
        {
            "replica": [replica] if replica else [],
            "producers": [psutil.Process(p.pid) for p in procs],
        }
    )
    sampler.start()
    results = [queue.get() for _ in procs]
    sampler.stop.set()
    for p in procs:
        p.join()
    latencies = sorted(lat for r in results for lat in r[5])

    def percentile(q: float) -> float:
        if not latencies:
            return float("nan")
        return latencies[min(len(latencies) - 1, int(q * len(latencies)))]

    acked = sum(r[0] for r in results)
    acked_bytes = sum(r[1] for r in results)
    failed = sum(r[2] for r in results)
    elapsed = max(r[4] for r in results) - min(r[3] for r in results)

    # An ack means the records are durable in the source's log. They become readable once the
    # source's exports commit them, shortly after.
    acked_at = max(r[4] for r in results)
    while True:
        (count,) = sql(conn, f"SELECT count(*) FROM {cluster}_events")[0]
        if count >= acked or time.time() - acked_at > 120:
            break
        time.sleep(0.1)
    visible_after = time.time() - acked_at
    if not args.keep:
        sql(conn, f"DROP CLUSTER {cluster} CASCADE")
    return {
        "size": size,
        "producers": producers,
        "offered_per_s": args.rate * producers if args.rate else None,
        # Producers stop offering records while their queues are full, so less than all of
        # the offered records are acknowledged once the listener cannot keep up.
        "accepted_fraction": (
            acked / (args.rate * producers * args.seconds) if args.rate else None
        ),
        "latency_p50_s": percentile(0.5),
        "latency_p99_s": percentile(0.99),
        "replica_cores": sampler.mean("replica") if replica else None,
        "replica_peak_gib": sampler.peak_rss["replica"] / 2**30 if replica else None,
        "producer_cores": sampler.mean("producers"),
        "records_per_s": acked / elapsed,
        "mb_per_s": acked_bytes / elapsed / 1e6,
        "acked": acked,
        "failed": failed,
        "in_materialize": int(count),
        "seconds": elapsed,
        "visible_after_s": visible_after,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--sizes",
        default="scale=1,workers=1,scale=1,workers=2,scale=1,workers=4,scale=1,workers=8",
        help="replica sizes, as one comma-separated list of sizes, each starting with scale=",
    )
    parser.add_argument(
        "--producers",
        default="4",
        help="numbers of producers, as a comma-separated list, each run at every size",
    )
    parser.add_argument(
        "--rate",
        type=float,
        help="records a second that each producer offers; as fast as it can if unset",
    )
    parser.add_argument("--seconds", type=float, default=20)
    parser.add_argument("--keep", action="store_true", help="keep the clusters")
    parser.add_argument(
        "--timestamp-interval",
        help="the source's TIMESTAMP INTERVAL, which bounds acknowledgement latency, "
        "for example 100ms. Values below 1s need min_timestamp_interval lowered.",
    )
    parser.add_argument(
        "--idempotent", action="store_true", help="use idempotent producers"
    )
    parser.add_argument("--json", type=Path, help="write results to this file")
    args = parser.parse_args()

    sizes = re.split(r",(?=scale=)", args.sizes)

    with psycopg.connect(
        host="localhost", port=6877, user="mz_system", autocommit=True
    ) as system:
        sql(system, "ALTER SYSTEM SET enable_kafka_listener_source = true")
        sql(system, "ALTER SYSTEM SET enable_replica_kafka_listener = true")
    conn = psycopg.connect(
        host="localhost", port=6875, user="materialize", autocommit=True
    )

    results = []
    print(
        f"{'size':<30} {'prod':>4} {'offered/s':>11} {'accepted':>8} {'records/s':>11} {'MB/s':>7} "
        f"{'p50':>7} {'p99':>7} {'replica':>8} {'mem GiB':>8} {'clients':>8} {'acked':>11} {'in MZ':>11} "
        f"{'failed':>6} {'visible':>8}"
    )
    for size in sizes:
        for producers in [int(n) for n in args.producers.split(",")]:
            r = run(conn, size, producers, args)
            results.append(r)
            offered = f"{r['offered_per_s']:,.0f}" if r["offered_per_s"] else "max"
            accepted = (
                f"{r['accepted_fraction']:.0%}" if r["accepted_fraction"] else "-"
            )
            replica = (
                f"{r['replica_cores']:.1f}" if r["replica_cores"] is not None else "?"
            )
            mem = (
                f"{r['replica_peak_gib']:.1f}"
                if r["replica_peak_gib"] is not None
                else "?"
            )
            print(
                f"{r['size']:<30} {producers:>4} {offered:>11} {accepted:>8} "
                f"{r['records_per_s']:>11,.0f} "
                f"{r['mb_per_s']:>7.1f} {r['latency_p50_s'] * 1000:>6.0f}ms "
                f"{r['latency_p99_s'] * 1000:>6.0f}ms {replica:>8} {mem:>8} "
                f"{r['producer_cores']:>8.1f} {r['acked']:>11,} {r['in_materialize']:>11,} "
                f"{r['failed']:>6} {r['visible_after_s']:>7.1f}s",
                flush=True,
            )
            if r["in_materialize"] != r["acked"]:
                print(
                    f"  NOTE: {r['in_materialize'] - r['acked']:+} records beyond the acked ones"
                )
    if args.json:
        args.json.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
