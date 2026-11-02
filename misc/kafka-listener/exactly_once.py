# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Checks that Kafka listener sources ingest idempotent producers' batches exactly once.

Run against an environmentd started with misc/kafka-listener/run-environmentd.sh:

    bin/pyactivate misc/kafka-listener/exactly_once.py

Kafka clients only resend a batch when they lose its response, which cannot be
triggered on demand. The first part therefore speaks the Kafka protocol
directly and sends the same batch several times, including after the replica
restarts. The second part runs librdkafka idempotent producers through a
replica restart and checks that every acknowledged record is ingested once.
"""

import json
import socket
import struct
import sys
import threading
import time
from pathlib import Path

import psycopg
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parent))
from demo import kafka_endpoint, sql  # noqa: E402

CLUSTER = "exactly_once"
TOPIC = "events"

ERRORS = {
    0: "NONE",
    6: "NOT_LEADER_OR_FOLLOWER",
    45: "OUT_OF_ORDER_SEQUENCE_NUMBER",
    47: "INVALID_PRODUCER_EPOCH",
}


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def varint(n: int) -> bytes:
    n = (n << 1) ^ (n >> 63)  # zigzag
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def record_batch(
    producer_id: int, epoch: int, base_sequence: int, values: list[bytes]
) -> bytes:
    """Encodes a v2 record batch, the format Kafka clients send."""
    now = int(time.time() * 1000)
    records = b""
    for i, value in enumerate(values):
        body = (
            b"\x00"  # attributes
            + varint(0)  # timestamp delta
            + varint(i)  # offset delta
            + varint(-1)  # null key
            + varint(len(value))
            + value
            + varint(0)  # no headers
        )
        records += varint(len(body)) + body
    after_crc = (
        struct.pack(
            ">hiqqqhii",
            0,
            len(values) - 1,
            now,
            now,
            producer_id,
            epoch,
            base_sequence,
            len(values),
        )
        + records
    )
    crc = crc32c(after_crc)
    after_length = struct.pack(">ibI", 0, 2, crc) + after_crc
    return struct.pack(">qi", 0, len(after_length)) + after_length


class RawProducer:
    """Sends Produce v3 requests over one connection."""

    def __init__(self, bootstrap: str):
        host, port = bootstrap.rsplit(":", 1)
        self.sock = socket.create_connection((host, int(port)))
        self.correlation_id = 0

    def produce(self, batch: bytes) -> tuple[str, int]:
        self.send(batch)
        return self.response()

    def send(self, batch: bytes) -> None:
        """Sends a Produce request without waiting for its response."""
        self.correlation_id += 1
        client_id = b"exactly-once-check"
        body = (
            struct.pack(">hhih", 0, 3, self.correlation_id, len(client_id))
            + client_id
            + struct.pack(">hhi", -1, -1, 30_000)  # null transactional id, acks=all
            + struct.pack(">ih", 1, len(TOPIC))
            + TOPIC.encode()
            + struct.pack(">iii", 1, 0, len(batch))
            + batch
        )
        self.sock.sendall(struct.pack(">i", len(body)) + body)

    def response(self) -> tuple[str, int]:
        (length,) = struct.unpack(">i", self._read(4))
        response = self._read(length)
        # correlation id, topics, name, partitions, index, error code, base offset
        name_len = struct.unpack(">h", response[8:10])[0]
        pos = 10 + name_len + 4
        _index, error, base_offset = struct.unpack(">ihq", response[pos : pos + 14])
        return ERRORS.get(error, str(error)), base_offset

    def _read(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed")
            buf += chunk
        return buf

    def close(self):
        self.sock.close()


def check(label: str, actual, expected) -> None:
    status = "ok  " if actual == expected else "FAIL"
    print(f"  {status} {label}: {actual}")
    if actual != expected:
        raise SystemExit(f"{label}: expected {expected}, got {actual}")


def restart_replica(conn: psycopg.Connection, cluster_id: str) -> str:
    """Replaces the cluster's replica and returns the new replica's Kafka address."""
    old = kafka_endpoint(cluster_id)
    sql(conn, f"ALTER CLUSTER {CLUSTER} SET (REPLICATION FACTOR 0)")
    sql(conn, f"ALTER CLUSTER {CLUSTER} SET (REPLICATION FACTOR 1)")
    # Locally, each replica gets a new proxy port.
    for _ in range(240):
        new = kafka_endpoint(cluster_id)
        if new != old:
            return new
        time.sleep(0.25)
    raise SystemExit("the new replica did not get a Kafka address")


def wait_running(conn: psycopg.Connection, source: str) -> None:
    for _ in range(240):
        rows = sql(
            conn,
            "SELECT st.status FROM mz_internal.mz_source_statuses st "
            f"JOIN mz_sources s ON s.id = st.id WHERE s.name = '{source}'",
        )
        if rows and rows[0][0] == "running":
            return
        time.sleep(0.25)
    raise SystemExit(f"source {source} is not running")


def count(conn: psycopg.Connection, source: str) -> tuple[int, int]:
    total, distinct = sql(
        conn, f"SELECT count(*), count(DISTINCT data->>'id') FROM {source}"
    )[0]
    return int(total), int(distinct)


def raw_duplicates(conn: psycopg.Connection, cluster_id: str) -> None:
    print("Resending batches over the raw protocol")
    sql(
        conn,
        f"CREATE SOURCE raw_events IN CLUSTER {CLUSTER} FROM KAFKA LISTENER (TOPIC '{TOPIC}') FORMAT JSON",
    )
    wait_running(conn, "raw_events")
    producer = RawProducer(kafka_endpoint(cluster_id))
    pid = 4242

    def batch(seq: int, epoch: int = 0, n: int = 3) -> bytes:
        return record_batch(
            pid, epoch, seq, [json.dumps({"id": seq + i}).encode() for i in range(n)]
        )

    check("first batch", producer.produce(batch(0)), ("NONE", 0))
    check("same batch again", producer.produce(batch(0)), ("NONE", 0))
    check("next batch", producer.produce(batch(3)), ("NONE", 3))
    check("first batch a third time", producer.produce(batch(0)), ("NONE", 0))
    check(
        "gap in sequence",
        producer.produce(batch(10))[0],
        "OUT_OF_ORDER_SEQUENCE_NUMBER",
    )
    check("new epoch", producer.produce(batch(0, epoch=1)), ("NONE", 6))
    check("old epoch", producer.produce(batch(6))[0], "INVALID_PRODUCER_EPOCH")
    producer.close()

    print("Restarting the replica")
    bootstrap = restart_replica(conn, cluster_id)
    wait_running(conn, "raw_events")
    producer = RawProducer(bootstrap)
    check(
        "epoch 1 batch after restart", producer.produce(batch(0, epoch=1)), ("NONE", 6)
    )
    check("next batch after restart", producer.produce(batch(3, epoch=1)), ("NONE", 9))
    producer.close()

    for _ in range(40):
        if count(conn, "raw_events")[0] >= 12:
            break
        time.sleep(0.25)
    # Ids 0..5 from epoch 0, then 0..5 again from epoch 1, which Kafka treats as new records.
    check("rows in Materialize", count(conn, "raw_events"), (12, 6))


def interrupted_batch(conn: psycopg.Connection, cluster_id: str) -> None:
    print("Resending a batch whose response a restart cut off")
    sql(
        conn,
        f"CREATE SOURCE cut_events IN CLUSTER {CLUSTER} FROM KAFKA LISTENER (TOPIC 'cut') FORMAT JSON",
    )
    wait_running(conn, "cut_events")
    pid = 4343

    def batch(seq: int, n: int = 3) -> bytes:
        return record_batch(
            pid, 0, seq, [json.dumps({"id": seq + i}).encode() for i in range(n)]
        )

    producer = RawProducer(kafka_endpoint(cluster_id))
    global TOPIC
    topic, TOPIC = TOPIC, "cut"
    try:
        producer.produce(batch(0))
        # The restart may come before or after the source commits this batch. Either way, the
        # resend must not ingest it twice or lose it.
        producer.send(batch(3))
        bootstrap = restart_replica(conn, cluster_id)
        producer.close()
        wait_running(conn, "cut_events")
        producer = RawProducer(bootstrap)
        check("resent batch", producer.produce(batch(3)), ("NONE", 3))
        check("next batch", producer.produce(batch(6)), ("NONE", 6))
        producer.close()
    finally:
        TOPIC = topic
    for _ in range(40):
        if count(conn, "cut_events")[0] >= 9:
            break
        time.sleep(0.25)
    check("rows in Materialize", count(conn, "cut_events"), (9, 9))


def librdkafka_through_restart(conn: psycopg.Connection, cluster_id: str) -> None:
    print("Producing with librdkafka idempotent producers through a replica restart")
    sql(
        conn,
        f"CREATE SOURCE rd_events IN CLUSTER {CLUSTER} FROM KAFKA LISTENER (TOPIC 'rd') FORMAT JSON",
    )
    wait_running(conn, "rd_events")
    bootstrap = kafka_endpoint(cluster_id)
    acked = 0
    errors: list[str] = []
    stop = threading.Event()

    def produce():
        nonlocal acked

        def on_delivery(err, msg):
            nonlocal acked
            if err is None:
                acked += 1
            else:
                errors.append(err.str())

        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "linger.ms": 5,
                "message.timeout.ms": 120_000,
            }
        )
        i = 0
        while not stop.is_set():
            try:
                producer.produce(
                    "rd", value=json.dumps({"id": i}).encode(), on_delivery=on_delivery
                )
                i += 1
            except BufferError:
                producer.poll(0.01)
            if i % 1000 == 0:
                producer.poll(0)
                time.sleep(0.01)
        producer.flush(120)
        return i

    # Locally a restarted replica listens on a new port, which a running client cannot
    # follow, so one producer runs before the restart and another after it.
    t = threading.Thread(target=produce)
    t.start()
    time.sleep(3)
    stop.set()
    t.join()
    first_acked = acked
    bootstrap = restart_replica(conn, cluster_id)
    wait_running(conn, "rd_events")
    stop.clear()
    t = threading.Thread(target=produce)
    t.start()
    time.sleep(3)
    stop.set()
    t.join()
    check("delivery errors", errors, [])
    for _ in range(40):
        total, distinct = count(conn, "rd_events")
        if total >= acked:
            break
        time.sleep(0.25)
    print(f"  acked {acked} records ({first_acked} before the restart)")
    check("rows in Materialize", count(conn, "rd_events")[0], acked)


def main() -> None:
    with psycopg.connect(
        host="localhost", port=6877, user="mz_system", autocommit=True
    ) as system:
        sql(system, "ALTER SYSTEM SET enable_kafka_listener_source = true")
        sql(system, "ALTER SYSTEM SET enable_replica_kafka_listener = true")
    conn = psycopg.connect(
        host="localhost", port=6875, user="materialize", autocommit=True
    )
    sql(conn, f"DROP CLUSTER IF EXISTS {CLUSTER} CASCADE")
    sql(conn, f"CREATE CLUSTER {CLUSTER} SIZE 'scale=1,workers=1'")
    (cluster_id,) = sql(conn, f"SELECT id FROM mz_clusters WHERE name = '{CLUSTER}'")[0]
    raw_duplicates(conn, cluster_id)
    interrupted_batch(conn, cluster_id)
    librdkafka_through_restart(conn, cluster_id)
    sql(conn, f"DROP CLUSTER {CLUSTER} CASCADE")
    print("All checks passed")


if __name__ == "__main__":
    main()
