# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Kafka source and Kafka sinks, checked against the topics themselves.

The topics are the ground truth. A background consumer tails each topic from
its first offset, so a check is one read of Materialize plus comparisons in
memory: a source's exports must equal the topic records below the offsets
its progress maps, and a sink's committed output must equal the view it
sinks at a committed progress frontier. Produces and writes whose outcome
the client never learned are judged by what the topics and Materialize
show, never by the client's guess.

The safety assertions keep the messages of the legacy Kafka source and sink
drivers. The coverage and post-fault source ones are new.

`KafkaSource` is the text-format source and the base of the other formats in
`kafka_formats`. The sinks come in JSON and, with a schema registry, in Avro.
"""

from __future__ import annotations

import io
import json
import struct
import threading
import time
import uuid
import zlib
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
    unreachable,
)

from materialize.antithesis import sink_oracle as oracle
from materialize.antithesis import sql
from materialize.antithesis_driver.client import (
    Op,
    Outcome,
    Status,
    Target,
    Worker,
    connect,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

CLUSTER = "sources"
VIEW_CLUSTER = "driver"
PARTITIONS = 2
ADMIN_TIMEOUT_S = 30.0
REGISTRY_TIMEOUT_S = 10.0
SETTINGS = {"transaction_isolation": "'strict serializable'", "cluster": "quickstart"}
PROGRESS_COLUMNS = [
    "lower(partition)::text",
    "upper(partition)::text",
    "lower_inc(partition)::text",
    "upper_inc(partition)::text",
    '"offset"::text',
]


def partition_for(key: str, partitions: int) -> int:
    return zlib.crc32(key.encode()) % partitions


def partition_frontier(
    rows: list[tuple[Any, ...]], partitions: int
) -> tuple[dict[int, int], list[int]]:
    """Offset per partition from a Kafka source's progress rows, and the
    partitions not covered exactly once.

    Each row is a `partition` numrange with the next offset to ingest: a single
    partition `[p, p]`, or an open range covering partitions that do not exist
    yet, with NULL bounds for infinities.
    """
    frontier: dict[int, int] = {}
    bad: list[int] = []
    for p in range(partitions):
        hits = []
        for lower, upper, lower_inc, upper_inc, offset in rows:
            lo = Decimal(lower) if lower is not None else None
            hi = Decimal(upper) if upper is not None else None
            above = lo is None or lo < p or (lo == p and lower_inc)
            below = hi is None or p < hi or (p == hi and upper_inc)
            if above and below:
                hits.append(int(offset))
        if len(hits) == 1:
            frontier[p] = hits[0]
        else:
            bad.append(p)
    return frontier, bad


def create_topics(broker: str, topics: list[str], partitions: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
    from confluent_kafka.cimpl import KafkaError, KafkaException

    admin = AdminClient({"bootstrap.servers": broker})
    futures = admin.create_topics(
        [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in topics],
        operation_timeout=ADMIN_TIMEOUT_S,
    )
    for future in futures.values():
        try:
            future.result(timeout=ADMIN_TIMEOUT_S)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


class Registry:
    """The schema registry's REST API and its wire format: a zero byte, the
    schema id as four big-endian bytes, then the Avro datum.

    Requests raise `OSError` (`requests.RequestException`) while the registry
    is unreachable. A schema, once seen, is kept: ids are immutable.
    """

    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self._lock = threading.Lock()
        self._schemas: dict[int, Any] = {}

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        import fastavro
        import requests

        response = requests.post(
            f"{self.url}/subjects/{subject}/versions",
            json={"schema": json.dumps(schema)},
            timeout=REGISTRY_TIMEOUT_S,
        )
        response.raise_for_status()
        schema_id = int(response.json()["id"])
        with self._lock:
            self._schemas[schema_id] = fastavro.parse_schema(schema)
        return schema_id

    def _schema(self, schema_id: int) -> Any:
        import fastavro
        import requests

        with self._lock:
            schema = self._schemas.get(schema_id)
        if schema is None:
            response = requests.get(
                f"{self.url}/schemas/ids/{schema_id}", timeout=REGISTRY_TIMEOUT_S
            )
            response.raise_for_status()
            try:
                text = response.json()["schema"]
            except (ValueError, KeyError, TypeError) as e:
                # Not a verdict on the payload being decoded.
                raise OSError(f"schema registry answer for id {schema_id}: {e}") from e
            schema = fastavro.parse_schema(json.loads(text))
            with self._lock:
                self._schemas[schema_id] = schema
        return schema

    def encode(self, schema_id: int, datum: Any) -> bytes:
        import fastavro

        buffer = io.BytesIO()
        buffer.write(b"\x00" + struct.pack(">I", schema_id))
        fastavro.schemaless_writer(buffer, self._schema(schema_id), datum)
        return buffer.getvalue()

    def decode(self, payload: bytes) -> tuple[int, Any]:
        """The schema id and the datum of `payload`. Raises `ValueError` for a
        payload that is not in the wire format or does not decode."""
        import fastavro

        if len(payload) < 5 or payload[0] != 0:
            raise ValueError("payload is not in the schema registry wire format")
        schema_id = struct.unpack(">I", payload[1:5])[0]
        schema = self._schema(schema_id)
        try:
            return schema_id, fastavro.schemaless_reader(
                io.BytesIO(payload[5:]), schema
            )
        except Exception as e:
            raise ValueError(f"Avro datum of schema {schema_id}: {e}") from e


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes | None], ...]


class Tailer:
    """Reads every record of some topics from their first offset, forever.

    `records[topic][partition]` grows in offset order. Offsets are not
    contiguous in a transactional topic, whose commit markers take offsets.
    """

    def __init__(self, broker: str, topics: list[str], committed: bool) -> None:
        self.broker = broker
        self.topics = topics
        self.committed = committed
        self._lock = threading.Lock()
        self.records: dict[str, dict[int, list[Record]]] = {t: {} for t in topics}
        self.positions: dict[tuple[str, int], int] = {}
        """The next offset the consumer will read, per topic and partition."""
        threading.Thread(target=self._loop, name="kafka-tail", daemon=True).start()

    def _consumer(self) -> Any:
        from confluent_kafka.cimpl import Consumer

        consumer = Consumer(
            {
                "bootstrap.servers": self.broker,
                "group.id": f"driver-tail-{uuid.uuid4().hex}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": (
                    "read_committed" if self.committed else "read_uncommitted"
                ),
                "topic.metadata.refresh.interval.ms": 5000,
            }
        )
        consumer.subscribe(self.topics)
        return consumer

    def _loop(self) -> None:
        consumer = None
        while True:
            try:
                if consumer is None:
                    consumer = self._consumer()
                for m in consumer.consume(num_messages=500, timeout=1.0):
                    if m.error() is not None:
                        continue
                    r = Record(
                        m.partition(),
                        m.offset(),
                        m.key(),
                        m.value(),
                        tuple(
                            (
                                k,
                                (
                                    v
                                    if isinstance(v, bytes) or v is None
                                    else str(v).encode()
                                ),
                            )
                            for k, v in (m.headers() or [])
                        ),
                    )
                    with self._lock:
                        records = self.records[m.topic()].setdefault(r.partition, [])
                        # A rebalance can replay records already read.
                        if not records or records[-1].offset < r.offset:
                            records.append(r)
                # The consumer's position counts the commit markers of a
                # transactional topic, which never arrive as messages.
                for tp in consumer.position(consumer.assignment()):
                    if tp.offset >= 0:
                        with self._lock:
                            key = (tp.topic, tp.partition)
                            self.positions[key] = max(
                                self.positions.get(key, 0), tp.offset
                            )
            except Exception as e:
                log(f"kafka tail of {self.topics}: {type(e).__name__}: {e}")
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                consumer = None
                time.sleep(1)

    def snapshot(self, topic: str) -> dict[int, list[Record]]:
        with self._lock:
            return {p: list(rs) for p, rs in self.records[topic].items()}

    def caught_up(self, topic: str, timeout: float) -> bool:
        """Whether the tail reaches each partition's end, as it is now, within
        `timeout`. A read-committed tail stops below an open transaction."""
        from confluent_kafka.cimpl import Consumer, TopicPartition

        probe = Consumer(
            {
                "bootstrap.servers": self.broker,
                "group.id": f"driver-probe-{uuid.uuid4().hex}",
                "enable.auto.commit": False,
                # The bound must be the high watermark. With the default
                # read_committed, the watermark is the last stable offset,
                # which moves per partition as a transaction's markers land:
                # a sink's data could then be read past its progress.
                "isolation.level": "read_uncommitted",
            }
        )
        try:
            metadata = probe.list_topics(topic, timeout=timeout)
            highs = {}
            for p in metadata.topics[topic].partitions:
                _, high = probe.get_watermark_offsets(
                    TopicPartition(topic, p), timeout=timeout, cached=False
                )
                highs[p] = high
        except Exception:
            return False
        finally:
            probe.close()
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                done = all(
                    high == 0 or self.positions.get((topic, p), 0) >= high
                    for p, high in highs.items()
                )
            if done or time.monotonic() >= deadline:
                return done
            time.sleep(0.2)


@dataclass
class Produced:
    key: bytes
    value: bytes | None
    partition: int
    acked: tuple[int, int] | None = None
    """(partition, offset) once the broker acknowledged it."""
    failed: bool = False


class KafkaSource:
    """A topic the driver produces to, its Kafka source, and exports of it.

    This class is the text-format source. A subclass replaces what depends on
    the format: the DDL, what a produce writes, which exports a check reads
    and how it judges them.
    """

    KIND = "src"
    """Names the topic and, in assertion details, the source."""
    SOURCE = "kafkasrc.src"
    """The Kafka source. Its relation is its progress collection."""

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        broker = ctx.config.kafka_broker
        assert broker is not None
        self.broker = broker
        self.topic = f"drv_{self.KIND}_{uuid.uuid4().hex[:8]}"
        self._lock = threading.Lock()
        self.produced: list[Produced] = []
        self.observed: list[tuple[int, dict[int, int]]] = []
        self.keyspace = 16
        self.tombstone_share = 0.05
        self._producer: Any = None
        self.tail: Tailer | None = None
        self.worker = Worker(f"kafka-{self.KIND}")

    def configure(self, d: Draw) -> None:
        self.keyspace = d.choice((1, 2, 16, 256, 4096))
        self.tombstone_share = d.choice((0.05, 0.3, 0.0))

    def setup_upstream(self) -> None:
        create_topics(self.broker, [self.topic], PARTITIONS)

    def _source_statements(self) -> list[str]:
        """The schema, the broker connection and the Kafka source of `SOURCE`."""
        schema = self.SOURCE.split(".")[0]
        mz_broker = self.ctx.config.kafka_mz_broker
        return [
            f"CREATE SCHEMA IF NOT EXISTS {schema}",
            f"CREATE CONNECTION IF NOT EXISTS {schema}.conn TO KAFKA (BROKER '{mz_broker}',"
            " SECURITY PROTOCOL PLAINTEXT)",
            f"CREATE SOURCE IF NOT EXISTS {self.SOURCE} IN CLUSTER {CLUSTER}"
            f" FROM KAFKA CONNECTION {schema}.conn (TOPIC '{self.topic}')",
        ]

    def _table(self, name: str, options: str) -> str:
        """An export of the topic named `name`, in the source's schema."""
        schema = self.SOURCE.split(".")[0]
        return (
            f"CREATE TABLE IF NOT EXISTS {schema}.{name} FROM SOURCE {self.SOURCE}"
            f' (REFERENCE "{self.topic}") {options}'
        )

    def setup_statements(self) -> list[str]:
        mz_broker = self.ctx.config.kafka_mz_broker
        return [
            "CREATE SCHEMA IF NOT EXISTS kafkasrc",
            f"CREATE CONNECTION IF NOT EXISTS kafkasrc.conn TO KAFKA (BROKER '{mz_broker}',"
            " SECURITY PROTOCOL PLAINTEXT)",
            f"CREATE SOURCE IF NOT EXISTS kafkasrc.src IN CLUSTER {CLUSTER}"
            f" FROM KAFKA CONNECTION kafkasrc.conn (TOPIC '{self.topic}')",
            f'CREATE TABLE IF NOT EXISTS kafkasrc.none FROM SOURCE kafkasrc.src (REFERENCE "{self.topic}")'
            " FORMAT TEXT INCLUDE PARTITION, OFFSET ENVELOPE NONE",
            f'CREATE TABLE IF NOT EXISTS kafkasrc.upsert FROM SOURCE kafkasrc.src (REFERENCE "{self.topic}")'
            " KEY FORMAT TEXT VALUE FORMAT TEXT ENVELOPE UPSERT",
        ]

    def start(self) -> None:
        """Start the producer and the tail, once the workload runs."""
        from confluent_kafka.cimpl import Producer

        self._producer = Producer(
            {
                "bootstrap.servers": self.broker,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
                "message.timeout.ms": 30_000,
            }
        )
        threading.Thread(target=self._poll, name="kafka-produce", daemon=True).start()
        self.tail = Tailer(self.broker, [self.topic], committed=False)

    def _poll(self) -> None:
        while True:
            try:
                self._producer.poll(0.2)
            except Exception as e:
                log(f"kafka producer poll: {e}")
                time.sleep(1)

    def produce(self, d: Draw) -> Status:
        if self._producer is None:
            return Status.NOT_SENT
        batch = d.choice((1, 10, 100))
        records = []
        for _ in range(batch):
            key = f"k{d.integer(0, self.keyspace - 1)}"
            value = (
                None if d.boolean(self.tombstone_share) else f"{d.integer(0, 2**48):x}"
            )
            records.append(self._record(key, None if value is None else value.encode()))
        return self._send(records)

    def _record(self, key: str, value: bytes | None) -> Produced:
        return Produced(key.encode(), value, partition_for(key, PARTITIONS))

    def _send(self, records: list[Produced]) -> Status:
        """Produce `records` in order from the worker, recording each
        acknowledgement when it arrives."""

        def action() -> None:
            for r in records:
                with self._lock:
                    self.produced.append(r)

                def delivered(err: Any, msg: Any, r: Produced = r) -> None:
                    with self._lock:
                        if err is None:
                            r.acked = (msg.partition(), msg.offset())
                        else:
                            r.failed = True

                while True:
                    try:
                        self._producer.produce(
                            self.topic,
                            key=r.key,
                            value=r.value,
                            partition=r.partition,
                            on_delivery=delivered,
                        )
                        break
                    except BufferError:
                        time.sleep(0.5)

        return self.worker.run(action, RULE_WAIT_SECONDS)

    def _exports(self) -> list[tuple[str, list[str], str]]:
        """What a check reads beside the progress: a tag, text-valued column
        expressions, and the relation."""
        return [
            ("n", ["text", "partition::text", '"offset"::text'], "kafkasrc.none"),
            ("u", ["key", "text"], "kafkasrc.upsert"),
        ]

    def _query(self) -> str:
        """One statement over the progress and every export, so all rows
        share a timestamp. Each row is the timestamp, a tag, then columns."""
        branches = [("p", PROGRESS_COLUMNS, self.SOURCE), *self._exports()]
        width = max(len(columns) for _, columns, _ in branches)
        selects = []
        for tag, columns, relation in branches:
            padded = [*columns, *["NULL::text"] * (width - len(columns))]
            named = ", ".join(f"{c} AS c{i}" for i, c in enumerate(padded))
            selects.append(f"SELECT '{tag}' AS tag, {named} FROM {relation}")
        return f"SELECT mz_now()::text, * FROM ({' UNION ALL '.join(selects)}) AS q"

    def _read(self, conn: psycopg.Connection) -> list[tuple[Any, ...]]:
        return conn.execute(self._query().encode()).fetchall()

    def _read_op(self, done: list[bool]) -> Op:
        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return self._read(conn)

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if outcome.ok:
                assert outcome.value is not None
                if self._judge(outcome.value):
                    done.append(True)

        return Op(f"kafka {self.KIND} check", execute, complete, SETTINGS)

    def _judge(self, rows: list[tuple[Any, ...]]) -> bool:
        """Returns whether the check compared, rather than waiting for the tail."""
        assert self.tail is not None
        ts = int(rows[0][0]) if rows else 0
        progress = [
            (r[2], r[3], r[4] == "true", r[5] == "true", r[6])
            for r in rows
            if r[1] == "p"
        ]
        frontier, bad = partition_frontier(progress, PARTITIONS)
        base = {"source": self.KIND, "ts": ts, "frontier": frontier}
        always(
            not bad,
            "kafka source: the progress collection maps each partition to exactly one offset",
            {**base, "bad_partitions": bad, "rows": progress[:10]},
        )
        with self._lock:
            regressions = [
                (t, f)
                for t, f in self.observed
                if any(
                    (t <= ts and f.get(p, 0) > frontier.get(p, 0))
                    or (t >= ts and f.get(p, 0) < frontier.get(p, 0))
                    for p in range(PARTITIONS)
                )
            ]
            self.observed = (self.observed + [(ts, frontier)])[-1000:]
            produced = list(self.produced)
        always(
            not regressions,
            "kafka source: the mapped offsets are monotone in mz time",
            {**base, "conflicting": regressions[:5]},
        )
        log_ = self.tail.snapshot(self.topic)
        if any(
            frontier.get(p, 0) > (log_[p][-1].offset + 1 if log_.get(p) else 0)
            for p in range(PARTITIONS)
        ):
            # The tail is behind what Materialize ingested. Compare later.
            return False
        below = [
            r
            for p in range(PARTITIONS)
            for r in log_.get(p, [])
            if r.offset < frontier.get(p, 0)
        ]
        by_offset = {(r.partition, r.offset): r for r in below}
        lost = [
            (p.key, p.acked)
            for p in produced
            if p.acked is not None
            and p.acked[1] < frontier.get(p.acked[0], 0)
            and (
                (rec := by_offset.get(p.acked)) is None
                or rec.key != p.key
                or rec.value != p.value
            )
        ]
        always(
            not lost,
            "kafka source: every acknowledged produce below the mapped offsets is in the topic as acknowledged",
            {**base, "lost": [(repr(k), a) for k, a in lost[:10]]},
        )
        self._judge_exports(base, below, rows)
        return True

    def _judge_exports(
        self, base: dict[str, Any], below: list[Record], rows: list[tuple[Any, ...]]
    ) -> None:
        """Compare the exports in `rows` with `below`, the topic's records
        below the mapped offsets."""
        expected_none = Counter(
            (r.value.decode(), str(r.partition), str(r.offset))
            for r in below
            if r.value is not None
        )
        observed_none = Counter((r[2], r[3], r[4]) for r in rows if r[1] == "n")
        always(
            expected_none == observed_none,
            "kafka source: the none-envelope export equals the topic records below the mapped offsets",
            {
                **base,
                **oracle.bag_diff(expected_none.elements(), observed_none.elements()),
            },
        )
        latest: dict[str, str | None] = {}
        for r in sorted(below, key=lambda r: (r.partition, r.offset)):
            assert r.key is not None
            latest[r.key.decode()] = None if r.value is None else r.value.decode()
        expected_upsert = {k: v for k, v in latest.items() if v is not None}
        upsert_rows = [(r[2], r[3]) for r in rows if r[1] == "u"]
        keys = Counter(k for k, _ in upsert_rows)
        always(
            all(n == 1 for n in keys.values()),
            "kafka source: the upsert export holds at most one row per key",
            {**base, "duplicates": sorted(k for k, n in keys.items() if n > 1)[:10]},
        )
        always(
            dict(upsert_rows) == expected_upsert,
            "kafka source: the upsert export equals the latest value per key below the mapped offsets",
            {
                **base,
                **oracle.bag_diff(expected_upsert.items(), dict(upsert_rows).items()),
            },
        )
        sometimes(
            len(expected_upsert) > 0 and dict(upsert_rows) == expected_upsert,
            "kafka source: a check matched a nonempty upsert export",
            {**base, "keys": len(expected_upsert)},
        )
        sometimes(
            any(r.value is None for r in below),
            "kafka source: a check covered a tombstone",
            base,
        )

    def check(self, d: Draw) -> Status:
        if self.tail is None:
            return Status.NOT_SENT
        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            self._read_op([]),
            RULE_WAIT_SECONDS,
        )

    def eventually(self, deadline: float) -> None:
        """After faults stop, the exports reach every acknowledged produce."""
        if self.tail is None:
            return
        try:
            self._producer.flush(30)
        except Exception as e:
            log(f"kafka source: flush: {e}")
        with self._lock:
            acked: dict[int, int] = {}
            for p in self.produced:
                if p.acked is not None:
                    acked[p.acked[0]] = max(acked.get(p.acked[0], -1), p.acked[1])
        reached = False
        while not reached and time.monotonic() < deadline:
            done: list[bool] = []
            rows: list[Any] = []

            def capture(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
                result = conn.execute(
                    "SELECT lower(partition)::text, upper(partition)::text,"
                    ' lower_inc(partition), upper_inc(partition), "offset"::text'
                    f" FROM {self.SOURCE}".encode()
                ).fetchall()
                rows.extend(result)
                return result

            op = Op(
                f"kafka {self.KIND} progress", capture, lambda outcome: None, SETTINGS
            )
            self.ctx.sessions.call(0, op, 60.0)
            frontier, _ = partition_frontier(rows, PARTITIONS)
            if all(frontier.get(p, 0) > off for p, off in acked.items()):
                self.ctx.sessions.call(0, self._read_op(done), 60.0)
                reached = bool(done)
            if not reached:
                time.sleep(2)
        always(
            reached,
            "kafka source: after faults stop the exports reach and match every acknowledged produce",
            {"source": self.KIND, "acked": acked},
        )


@dataclass
class SinkRow:
    grp: int
    val: int


@dataclass
class SinkModel:
    rows: dict[int, SinkRow] = field(default_factory=dict)
    uncertain: set[int] = field(default_factory=set)
    """Ids a write with an unknown outcome touched."""
    busy: set[int] = field(default_factory=set)


@dataclass(frozen=True)
class SinkSpec:
    name: str
    """The sink's name in the `snk` schema."""
    debezium: bool
    """Debezium envelope of the row-for-row view, else upsert of the grouped one."""
    avro: bool
    topic: str


class KafkaSink:
    """A Debezium sink of a row-for-row view, and an upsert sink of a grouped
    one, each in JSON and, with a schema registry, in Avro."""

    COLUMNS = ("id", "grp", "val")
    AGG_COLUMNS = ("grp", "n", "total")

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        broker = ctx.config.kafka_broker
        assert broker is not None
        self.broker = broker
        suffix = uuid.uuid4().hex[:8]
        registry = ctx.config.schema_registry_url
        self.registry = Registry(registry) if registry else None
        self.sinks = [
            SinkSpec("dbz", True, False, f"drv_sink_dbz_{suffix}"),
            SinkSpec("ups", False, False, f"drv_sink_upsert_{suffix}"),
        ]
        if self.registry is not None:
            self.sinks += [
                SinkSpec("dbz_avro", True, True, f"drv_sink_dbz_avro_{suffix}"),
                SinkSpec("ups_avro", False, True, f"drv_sink_upsert_avro_{suffix}"),
            ]
        self.progress_topic = f"drv_sink_progress_{suffix}"
        self._lock = threading.Lock()
        self.model = SinkModel()
        self._next_id = 1
        self.groups = 4
        self.tail: Tailer | None = None
        self.progress_keys: dict[str, str] = {}
        """Sink name to its progress record key."""
        self._decoded: dict[tuple[str, int, int], oracle.SinkMessage | str] = {}
        """Committed records never change, so each is decoded once."""
        self.worker = Worker("kafka-sink")

    def configure(self, d: Draw) -> None:
        self.groups = d.choice((1, 4, 32))

    def setup_statements(self) -> list[str]:
        mz_broker = self.ctx.config.kafka_mz_broker
        retain = "WITH (RETAIN HISTORY = FOR '10m')"
        topic = "TOPIC REPLICATION FACTOR 1, TOPIC PARTITION COUNT 2"
        statements = [
            "CREATE SCHEMA IF NOT EXISTS snk",
            f"CREATE CONNECTION IF NOT EXISTS snk.conn TO KAFKA (BROKER '{mz_broker}',"
            f" SECURITY PROTOCOL PLAINTEXT, PROGRESS TOPIC '{self.progress_topic}',"
            " PROGRESS TOPIC REPLICATION FACTOR 1)",
            "CREATE TABLE IF NOT EXISTS snk.items (id bigint NOT NULL, grp int NOT NULL, val bigint NOT NULL)",
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS snk.items_mv IN CLUSTER {VIEW_CLUSTER} {retain}"
            " AS SELECT id, grp, val FROM snk.items",
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS snk.agg_mv IN CLUSTER {VIEW_CLUSTER} {retain}"
            " AS SELECT grp, count(*)::bigint AS n, sum(val)::bigint AS total FROM snk.items GROUP BY grp",
        ]
        if self.registry is not None:
            statements.append(
                "CREATE CONNECTION IF NOT EXISTS snk.csr TO CONFLUENT SCHEMA REGISTRY"
                f" (URL '{self.ctx.config.schema_registry_mz_url}')"
            )
        for sink in self.sinks:
            view, key = ("items_mv", "id") if sink.debezium else ("agg_mv", "grp")
            format = (
                "AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION snk.csr"
                if sink.avro
                else "JSON"
            )
            statements.append(
                f"CREATE SINK IF NOT EXISTS snk.{sink.name} IN CLUSTER {CLUSTER}"
                f" FROM snk.{view} INTO KAFKA CONNECTION snk.conn"
                f" (TOPIC '{sink.topic}', {topic}) KEY ({key}) NOT ENFORCED"
                f" FORMAT {format}"
                f" ENVELOPE {'DEBEZIUM' if sink.debezium else 'UPSERT'}"
            )
        return statements

    def start(self) -> None:
        self.tail = Tailer(
            self.broker,
            [*(sink.topic for sink in self.sinks), self.progress_topic],
            committed=True,
        )

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def write(self, d: Draw) -> Status:
        kind = d.choice(("insert", "insert", "update", "update", "delete"))
        with self._lock:
            certain = sorted(
                i
                for i in self.model.rows
                if i not in self.model.uncertain and i not in self.model.busy
            )
            if kind == "insert" or not certain:
                kind = "insert"
                ids = list(range(self._next_id, self._next_id + d.choice((1, 2, 8))))
                self._next_id += len(ids)
            else:
                ids = sorted(
                    {
                        certain[d.integer(0, len(certain) - 1)]
                        for _ in range(d.choice((1, 2, 8)))
                    }
                )
            self.model.busy.update(ids)
        if kind == "insert":
            rows = {
                i: SinkRow(d.integer(0, self.groups - 1), d.integer(-1000, 1000))
                for i in ids
            }
            values = ", ".join(f"({i}, {r.grp}, {r.val})" for i, r in rows.items())
            statement = f"INSERT INTO snk.items VALUES {values}"
        elif kind == "update":
            delta = d.choice((-7, -1, 1, 3, 1000))
            statement = f"UPDATE snk.items SET val = val + {delta} WHERE id IN ({', '.join(map(str, ids))})"
        else:
            statement = (
                f"DELETE FROM snk.items WHERE id IN ({', '.join(map(str, ids))})"
            )

        def execute(conn: psycopg.Connection) -> int:
            return conn.execute(statement.encode()).rowcount

        def complete(outcome: Outcome[int]) -> None:
            with self._lock:
                self.model.busy.difference_update(ids)
                if outcome.unknown:
                    self.model.uncertain.update(ids)
                    if kind == "insert":
                        for i, r in rows.items():
                            self.model.rows[i] = r
                    return
                if not outcome.ok:
                    return
                if kind == "insert":
                    self.model.rows.update(rows)
                elif kind == "update":
                    for i in ids:
                        self.model.rows[i].val += delta
                else:
                    for i in ids:
                        self.model.rows.pop(i, None)
            always(
                outcome.value == len(ids),
                "kafka sink: an acknowledged write affects exactly the rows the model expects",
                {"kind": kind, "ids": ids[:8], "rowcount": outcome.value},
            )

        return self.ctx.sessions.call(
            self._session(d),
            Op(f"kafka sink {kind}", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _progress_keys(self) -> dict[str, str]:
        if len(self.progress_keys) == len(self.sinks):
            return self.progress_keys
        with connect(
            Target(self.ctx.config.host, internal=True), "driver-sink"
        ) as conn:
            for name in (sink.name for sink in self.sinks):
                row = conn.execute(
                    b"SELECT o.global_id FROM mz_internal.mz_object_global_ids o"
                    b" JOIN mz_catalog.mz_sinks s ON o.id = s.id"
                    b" JOIN mz_catalog.mz_schemas sc ON s.schema_id = sc.id"
                    b" WHERE s.name = %s AND sc.name = 'snk'",
                    (name,),
                ).fetchone()
                if row is not None:
                    self.progress_keys[name] = f"mz-sink-{row[0]}"
        return self.progress_keys

    def _stream(
        self,
    ) -> tuple[
        dict[str, list[oracle.SinkMessage]],
        dict[str, list[oracle.ProgressRecord]],
        list[str],
        list[str],
    ]:
        """Decoded committed messages per sink, progress per sink, and the data
        and progress decode errors.

        Raises `OSError` when an Avro sink's schema cannot be fetched from
        the registry.
        """
        assert self.tail is not None
        keys = self._progress_keys()
        errors: list[str] = []
        progress_errors: list[str] = []
        messages: dict[str, list[oracle.SinkMessage]] = {
            sink.name: [] for sink in self.sinks
        }
        for sink in self.sinks:
            loads: Callable[[bytes], Any] = (
                self._avro_loads if sink.avro else json.loads
            )
            for p, records in self.tail.snapshot(sink.topic).items():
                for r in records:
                    cached = (sink.name, p, r.offset)
                    m = self._decoded.get(cached)
                    if m is None:
                        m = oracle.decode_message(
                            p, r.offset, r.key, r.value, r.headers, loads
                        )
                        self._decoded[cached] = m
                    if isinstance(m, str):
                        errors.append(m)
                    else:
                        messages[sink.name].append(m)
        progress: dict[str, list[oracle.ProgressRecord]] = {
            sink.name: [] for sink in self.sinks
        }
        by_key = {v: k for k, v in keys.items()}
        for records in self.tail.snapshot(self.progress_topic).values():
            for r in records:
                name = by_key.get(r.key.decode() if r.key else "")
                if name is None:
                    continue
                pr = oracle.decode_progress(r.offset, r.value)
                if isinstance(pr, str):
                    progress_errors.append(pr)
                else:
                    progress[name].append(pr)
        return messages, progress, errors, progress_errors

    def _avro_loads(self, payload: bytes) -> Any:
        assert self.registry is not None
        return self.registry.decode(payload)[1]

    def _check_streams(
        self,
    ) -> tuple[dict[str, int | None], dict[str, list[oracle.SinkMessage]]] | None:
        """The stream invariants. Returns, for each sink, a committed frontier
        below which every data message has been read, or None when the tails
        could not catch up.

        Data and progress commit in one transaction. So data read after the
        progress holds everything that progress covers, and progress read
        after the data covers all of that data.
        """
        assert self.tail is not None
        if not self.tail.caught_up(self.progress_topic, 5.0):
            return None
        _, covered, _, _ = self._stream()
        if not all(self.tail.caught_up(sink.topic, 5.0) for sink in self.sinks):
            return None
        messages, _, errors, _ = self._stream()
        if not self.tail.caught_up(self.progress_topic, 5.0):
            return None
        _, progress, _, progress_errors = self._stream()

        def latest(records: list[oracle.ProgressRecord]) -> int | None:
            ordered = sorted(records, key=lambda r: r.offset)
            return ordered[-1].frontier if ordered else 0

        names = [sink.name for sink in self.sinks]
        frontiers = {name: latest(progress[name]) for name in names}
        complete = {name: latest(covered[name]) for name in names}
        details = {"frontiers": frontiers}
        always(
            not errors,
            "kafka sink: every committed data message decodes and carries one materialize-timestamp header",
            {**details, "errors": errors[:5]},
        )
        always(
            not progress_errors,
            "kafka sink: every progress record decodes",
            {**details, "errors": progress_errors[:5]},
        )
        for name in names:
            always(
                not oracle.duplicate_updates(messages[name]),
                "kafka sink: no update (key, timestamp) is committed to the data topic twice",
                {
                    **details,
                    "sink": name,
                    "duplicates": oracle.duplicate_updates(messages[name])[:5],
                },
            )
            always(
                not oracle.timestamp_regressions(messages[name]),
                "kafka sink: committed data timestamps never decrease within a partition",
                {
                    **details,
                    "sink": name,
                    "regressions": oracle.timestamp_regressions(messages[name])[:5],
                },
            )
            always(
                not oracle.progress_regressions(progress[name]),
                "kafka sink: committed progress frontiers and versions never regress",
                {
                    **details,
                    "sink": name,
                    "regressions": oracle.progress_regressions(progress[name])[:5],
                },
            )
            frontier = frontiers[name]
            above = [
                m for m in messages[name] if not oracle.frontier_le(m.ts + 1, frontier)
            ]
            always(
                not above,
                "kafka sink: every committed data message lies below the latest committed progress frontier",
                {
                    **details,
                    "sink": name,
                    "above": [(m.partition, m.offset, m.ts) for m in above[:5]],
                },
            )
        for name in (sink.name for sink in self.sinks if sink.debezium):
            mismatches = oracle.debezium_key_mismatches(messages[name], ("id",))
            always(
                not mismatches,
                "kafka sink: every Debezium message key equals the row id in its before and after images",
                {**details, "sink": name, "mismatches": mismatches[:5]},
            )
            breaks = oracle.debezium_chain_breaks(messages[name], self.COLUMNS)
            always(
                not breaks,
                "kafka sink: each Debezium before image equals the key's previous after image",
                {**details, "sink": name, "breaks": breaks[:5]},
            )
        return complete, messages

    def _compare_views(
        self,
        messages: dict[str, list[oracle.SinkMessage]],
        frontiers: dict[str, int | None],
        view_rows: dict[str, list[tuple[Any, ...]]],
    ) -> None:
        for sink in self.sinks:
            if sink.name not in view_rows:
                continue
            frontier = frontiers[sink.name]
            details = {"sink": sink.name, "frontier": frontier}
            if sink.debezium:
                rows, negative = oracle.consolidate_debezium(
                    messages[sink.name], frontier, self.COLUMNS
                )
                expected = Counter(
                    tuple(int(x) for x in r) for r in view_rows[sink.name]
                )
                always(
                    not negative and rows == expected,
                    "kafka sink: consolidated Debezium output at a committed progress frontier equals the sinked view at that time",
                    {
                        **details,
                        "negative": negative[:5],
                        **oracle.bag_diff(expected.elements(), rows.elements()),
                    },
                )
                matched = sum(rows.values()) > 0 and rows == expected
                sometimes(
                    matched,
                    "kafka sink: a check matched a nonempty sink against its view",
                    {**details, "rows": sum(rows.values())},
                )
                sometimes(
                    matched and sink.avro,
                    "kafka sink: a check matched a nonempty Avro Debezium sink against its view",
                    {**details, "rows": sum(rows.values())},
                )
            else:
                ups = oracle.consolidate_upsert(
                    messages[sink.name], frontier, self.AGG_COLUMNS
                )
                expected_ups = {
                    oracle.freeze({"grp": int(r[0])}): tuple(int(x) for x in r)
                    for r in view_rows[sink.name]
                }
                always(
                    ups == expected_ups,
                    "kafka sink: latest upsert value per key at a committed progress frontier equals the sinked view at that time",
                    {
                        **details,
                        **oracle.bag_diff(expected_ups.values(), ups.values()),
                    },
                )
                sometimes(
                    sink.avro and len(ups) > 0 and ups == expected_ups,
                    "kafka sink: a check matched a nonempty Avro upsert sink against its view",
                    {**details, "keys": len(ups)},
                )

    def _view_rows(
        self, frontiers: dict[str, int | None]
    ) -> dict[str, list[tuple[Any, ...]]]:
        """Each sink's view as of its frontier, for the sinks that have one
        and up to the first read that fails."""
        out: dict[str, list[tuple[Any, ...]]] = {}
        try:
            with connect(Target(self.ctx.config.host), "driver-sink") as conn:
                conn.execute(b"SET statement_timeout = '30s'")
                for sink in self.sinks:
                    frontier = frontiers[sink.name]
                    if not frontier:
                        continue
                    query = (
                        "SELECT id, grp, val FROM snk.items_mv"
                        if sink.debezium
                        else "SELECT grp, n, total FROM snk.agg_mv"
                    )
                    out[sink.name] = conn.execute(
                        f"{query} AS OF {frontier - 1}".encode()
                    ).fetchall()
        except (psycopg.Error, OSError) as e:
            c = sql.classify_as_of_read(e)
            if c.outcome == sql.Outcome.VIOLATION:
                unreachable(
                    "kafka sink: reads of the sinked views return only classified errors",
                    {"sqlstate": c.sqlstate, "error": c.template},
                )
        return out

    def check(self, d: Draw) -> Status:
        if self.tail is None:
            return Status.NOT_SENT

        def action() -> None:
            try:
                streams = self._check_streams()
            except (psycopg.Error, OSError) as e:
                # Looking up the sinks' progress keys needs Materialize, and
                # decoding an Avro sink needs the registry.
                log(f"kafka sink: check skipped: {e}")
                return
            if streams is None:
                return
            frontiers, messages = streams
            self._compare_views(messages, frontiers, self._view_rows(frontiers))

        return self.worker.run(action, RULE_WAIT_SECONDS)

    def eventually(self, deadline: float) -> None:
        """After faults stop, both sinks reach past every acknowledged write
        and equal the model there."""
        if self.tail is None:
            return
        t0 = None
        while t0 is None and time.monotonic() < deadline:
            try:
                with connect(Target(self.ctx.config.host), "driver-sink") as conn:
                    row = conn.execute(b"SELECT mz_now()::text").fetchone()
                    assert row is not None
                    t0 = int(row[0])
            except (psycopg.Error, OSError):
                time.sleep(2)
        compared = False
        running = False
        while t0 is not None and time.monotonic() < deadline:
            try:
                streams = self._check_streams()
            except (psycopg.Error, OSError):
                streams = None
            if streams is not None and all(
                f is None or f > t0 for f in streams[0].values()
            ):
                self._compare_model(*streams)
                compared = True
                break
            time.sleep(2)
        try:
            with connect(
                Target(self.ctx.config.host, internal=True), "driver-sink"
            ) as conn:
                statuses = conn.execute(
                    b"SELECT s.name, st.status FROM mz_catalog.mz_sinks s"
                    b" JOIN mz_internal.mz_sink_statuses st ON s.id = st.id"
                    b" JOIN mz_catalog.mz_schemas sc ON s.schema_id = sc.id WHERE sc.name = 'snk'"
                ).fetchall()
            running = bool(statuses) and all(
                status == "running" for _, status in statuses
            )
        except (psycopg.Error, OSError) as e:
            statuses = [("error", str(e)[:100])]
        always(
            running,
            "kafka sink: every sink reports running after faults stop",
            {"statuses": statuses},
        )
        always(
            compared,
            "kafka sink: after faults stop every sink passes every acknowledged write and is compared within the wedge bound",
            {"t0": t0},
        )

    def _compare_model(
        self,
        frontiers: dict[str, int | None],
        messages: dict[str, list[oracle.SinkMessage]],
    ) -> None:
        with self._lock:
            rows = dict(self.model.rows)
            # A write still running after the sessions went idle has an
            # outcome this comparison cannot know.
            uncertain = set(self.model.uncertain) | self.model.busy
        expected = Counter(
            {(i, r.grp, r.val): 1 for i, r in rows.items() if i not in uncertain}
        )
        uncertain_groups = {rows[i].grp for i in uncertain if i in rows}
        groups: dict[int, list[int]] = {}
        for r in rows.values():
            groups.setdefault(r.grp, []).append(r.val)
        expected_ups = {
            oracle.freeze({"grp": g}): (g, len(vals), sum(vals))
            for g, vals in groups.items()
            if g not in uncertain_groups
        }
        for sink in self.sinks:
            details = {"sink": sink.name, "frontier": frontiers[sink.name]}
            if sink.debezium:
                observed, negative = oracle.consolidate_debezium(
                    messages[sink.name], frontiers[sink.name], self.COLUMNS
                )
                observed_certain = Counter(
                    {r: n for r, n in observed.items() if r[0] not in uncertain}
                )
                always(
                    not negative and observed_certain == expected,
                    "kafka sink: the Debezium sink at a frontier past every acknowledged write equals the acknowledged-write model",
                    {
                        **details,
                        **oracle.bag_diff(
                            expected.elements(), observed_certain.elements()
                        ),
                    },
                )
            else:
                ups = oracle.consolidate_upsert(
                    messages[sink.name], frontiers[sink.name], self.AGG_COLUMNS
                )
                ups_certain = {
                    k: v for k, v in ups.items() if v[0] not in uncertain_groups
                }
                always(
                    ups_certain == expected_ups,
                    "kafka sink: the upsert sink at a frontier past every acknowledged write equals the model for groups without uncertain writes",
                    {
                        **details,
                        **oracle.bag_diff(expected_ups.values(), ups_certain.values()),
                    },
                )
