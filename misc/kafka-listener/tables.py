# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Checks a Kafka listener source with a topic prefix and a table per topic.

Run against an environmentd started with misc/kafka-listener/run-environmentd.sh:

    bin/pyactivate misc/kafka-listener/tables.py

Idempotent producers write to the topics of two tables and of a single-topic
source on the same cluster. While they run, a table is added for a new topic,
and a second table for a topic that already has records, which must receive
them from the source's log. Both restart the source with exports at different
uppers. Then the replica's process is killed. Every acknowledged record must be
ingested exactly once into every table of its topic.
"""

import os
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path

import psycopg
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parent))
from demo import MZ_ROOT, kafka_endpoint, sql  # noqa: E402

CLUSTER = "listener_tables"


class Writer(threading.Thread):
    """Produces increasing integers to a topic and records the acknowledged ones."""

    def __init__(self, bootstrap: str, topic: str, stop: threading.Event):
        super().__init__(daemon=True)
        self.topic = topic
        self.stop = stop
        self.next = 0
        self.acked: set[int] = set()
        self.errors: list = []
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "linger.ms": 5,
                "message.timeout.ms": 300000,
            }
        )

    def on_delivery(self, err, msg) -> None:
        if err is None:
            self.acked.add(int(msg.value()))
        else:
            self.errors.append(err)

    def run(self) -> None:
        while not self.stop.is_set():
            for _ in range(200):
                self.producer.produce(
                    self.topic,
                    key=str(self.next % 97),
                    value=str(self.next),
                    on_delivery=self.on_delivery,
                )
                self.next += 1
            self.producer.poll(0)
            time.sleep(0.05)
        if self.producer.flush(300):
            self.errors.append("records left undelivered")


def wait_for(conn: psycopg.Connection, query: str, expected: list) -> None:
    for _ in range(240):
        if sql(conn, query) == expected:
            return
        time.sleep(0.5)
    raise SystemExit(f"timed out waiting for {query} to return {expected}")


def check(name: str, ok: bool, detail: str) -> None:
    print(f"  {'ok  ' if ok else 'FAIL'} {name}: {detail}")
    if not ok:
        raise SystemExit(1)


def check_append_only(conn: psycopg.Connection, relation: str, writer: Writer) -> None:
    check(f"{relation} delivery errors", not writer.errors, str(writer.errors[:3]))
    wait_for(
        conn,
        f"SELECT count(*) FROM {relation} WHERE text = '{max(writer.acked)}'",
        [(1,)],
    )
    total, distinct, lowest, highest = sql(
        conn,
        f"SELECT count(*), count(DISTINCT text), min(text::bigint), max(text::bigint) "
        f"FROM {relation}",
    )[0]
    # Every value below the greatest acknowledged one was acknowledged too.
    expected = max(writer.acked) + 1
    check(
        f"rows in {relation}",
        (total, distinct, lowest, highest) == (expected, expected, 0, expected - 1)
        and len(writer.acked) == expected,
        f"{total} rows, {distinct} distinct, {len(writer.acked)} acked",
    )


def kill_replica(cluster_id: str) -> None:
    """Kills the cluster's replica process.

    The process orchestrator restarts it behind the same proxy port, so producers
    reconnect to the same address, as they would to a Kubernetes service.
    """
    env = (MZ_ROOT / "mzdata" / "environment-id").read_text().strip()
    (pid_file,) = Path(tempfile.gettempdir()).glob(
        f"environmentd-{env}/cluster-{cluster_id}-replica-*/0.pid"
    )
    pid = int(pid_file.read_text().split()[0])
    os.kill(pid, signal.SIGKILL)


def main() -> None:
    with psycopg.connect(
        host="localhost", port=6877, user="mz_system", autocommit=True
    ) as system:
        sql(system, "ALTER SYSTEM SET enable_kafka_listener_source = true")
        sql(system, "ALTER SYSTEM SET enable_replica_kafka_listener = true")
        sql(system, "ALTER SYSTEM SET min_timestamp_interval = '100ms'")
    conn = psycopg.connect(
        host="localhost", port=6875, user="materialize", autocommit=True
    )
    sql(conn, f"DROP CLUSTER IF EXISTS {CLUSTER} CASCADE")
    sql(conn, "DROP SOURCE IF EXISTS cdc, single CASCADE")
    sql(conn, f"CREATE CLUSTER {CLUSTER} SIZE 'scale=1,workers=2'")
    sql(
        conn,
        f"CREATE SOURCE cdc IN CLUSTER {CLUSTER} "
        "FROM KAFKA LISTENER (TOPIC PREFIX 'cdc.') "
        "WITH (TIMESTAMP INTERVAL '100ms')",
    )
    sql(conn, 'CREATE TABLE a FROM SOURCE cdc (REFERENCE "cdc.a") FORMAT TEXT')
    sql(
        conn,
        'CREATE TABLE b FROM SOURCE cdc (REFERENCE "cdc.b") '
        "KEY FORMAT TEXT VALUE FORMAT TEXT ENVELOPE UPSERT",
    )
    sql(
        conn,
        f"CREATE SOURCE single IN CLUSTER {CLUSTER} "
        "FROM KAFKA LISTENER (TOPIC 'single') FORMAT TEXT",
    )
    (cluster_id,) = sql(conn, f"SELECT id FROM mz_clusters WHERE name = '{CLUSTER}'")[0]
    bootstrap = kafka_endpoint(cluster_id)

    print("Producing to topics without a table")
    for topic in ["other.x", "cdc.no_table"]:
        producer = Producer(
            {"bootstrap.servers": bootstrap, "message.timeout.ms": 5000}
        )
        errors = []
        producer.produce(topic, value=b"x", on_delivery=lambda e, _: errors.append(e))
        producer.flush(10)
        check(topic, bool(errors) and errors[0] is not None, str(errors))

    print("Producing to two tables and a single-topic source")
    stop = threading.Event()
    writers = {
        name: Writer(bootstrap, topic, stop)
        for name, topic in [("a", "cdc.a"), ("b", "cdc.b"), ("single", "single")]
    }
    for writer in writers.values():
        writer.start()
    time.sleep(5)

    print("Adding a table while producing")
    sql(conn, 'CREATE TABLE c FROM SOURCE cdc (REFERENCE "cdc.c") FORMAT TEXT')
    writers["c"] = Writer(bootstrap, "cdc.c", stop)
    writers["c"].start()
    time.sleep(4)

    print("Adding a second table for a topic with records")
    sql(conn, 'CREATE TABLE a2 FROM SOURCE cdc (REFERENCE "cdc.a") FORMAT TEXT')
    time.sleep(4)

    print("Killing the replica while producing")
    kill_replica(cluster_id)
    time.sleep(20)
    stop.set()
    for writer in writers.values():
        writer.join()

    sql(conn, f"SET cluster = {CLUSTER}")
    for relation, writer in [
        ("a", "a"),
        ("a2", "a"),
        ("c", "c"),
        ("single", "single"),
    ]:
        check_append_only(conn, relation, writers[writer])

    b = writers["b"]
    check("b delivery errors", not b.errors, str(b.errors[:3]))
    wait_for(conn, f"SELECT count(*) FROM b WHERE text = '{max(b.acked)}'", [(1,)])
    latest: dict[str, str] = {}
    for value in sorted(b.acked):
        latest[str(value % 97)] = str(value)
    check(
        "b's latest values",
        dict(sql(conn, "SELECT key, text FROM b")) == latest,
        f"{len(latest)} keys",
    )
    print("All checks passed")


if __name__ == "__main__":
    main()
