# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Kafka sources (upsert and none envelope) checked against the topic at the mapped offsets.

Properties: `source-matches-upstream-at-mapped-position` (A3, Kafka part),
`source-shards-respect-declared-key-uniqueness` (SQL `GROUP BY` fallback), and
anchor (3) of `source-restarts-reach-dangerous-mid-states` via
`source_status.observe`.

Each slot is one topic with one source and two tables reading it:
`KEY FORMAT TEXT VALUE FORMAT TEXT ENVELOPE UPSERT`, and
`FORMAT TEXT INCLUDE PARTITION, OFFSET ENVELOPE NONE` without the key. Without
`INCLUDE KEY`, the none envelope skips null-value records; with it, a tombstone
becomes a "Value not present" error row that poisons every read. Both tables
share the source's progress collection (the source item itself), so one
`AS OF T` read of the source gives the per-partition frontier F for both.

The expected state is read back from the topic, offsets `[0, F[p])` per
partition, rather than replayed from the producer's ledger alone: a produce
whose delivery report was lost may still be in the topic, and only the topic
knows. The ledger of acknowledged produces is checked against that read-back,
which validates the oracle itself.

The producer pins each key to one partition (crc32 of the key), so the order of
a key's records is their offset order and "last value per key" is well defined
for the upsert envelope.

A slot rotates to a fresh topic and source after `TOPIC_RECORD_CAP` attempted
produces, which bounds read-back and query cost; the previous generation's
source is dropped and its topic deleted.
"""

from __future__ import annotations

import sqlite3
import zlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
from confluent_kafka.cimpl import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

from materialize.antithesis import sql, state
from materialize.antithesis.drivers.source_status import Export, observe
from materialize.antithesis.drivers.sources_common import (
    RETAIN_HISTORY_CHOICES,
    SOURCES_CLUSTER,
    Deadline,
    Heartbeat,
    bag_diff,
    ensure_cluster,
    ensure_retain_history,
    frontiers,
    lookup_ids,
    mz_connect,
    mz_host,
    pick_as_of,
    record_progress_observation,
    source_error_text,
    timeline_choice,
)
from materialize.antithesis.drivers.sources_common import log as _log
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.rng import rng

STATE_DB = "kafka_sources"
KAFKA_CONNECTION = "antithesis_kafka"
SLOTS = 2

PARTITION_CHOICES = (1, 2, 4)
"""Up to the 4 workers of the larger replica sizes, so partitions spread over workers."""

# Calibration: bounds the per-check read-back and the `AS OF` scan on one
# core. 20k records of at most 16 KiB stay well under a second of decode.
TOPIC_RECORD_CAP = 20_000

BATCH_CHOICES = (1, 10, 100, 500)
KEYSPACE_CHOICES = (1, 2, 16, 256, 4096)
"""One key is pure churn on a single upsert key; 4096 mostly inserts."""
TOMBSTONE_P_CHOICES = (0.0, 0.05, 0.3, 0.7)
VALUE_SIZES = (0, 1, 7, 255, 1024, 16_384)
"""Empty, single byte, small, around common buffer boundaries, and large values."""
KEY_PREFIXES = ("k", "κλειδί-")
COMPRESSION_CHOICES = ("none", "gzip", "lz4", "zstd")
_FILLER = "abcdefghijklmnopqrstuvwxyzé€𝄞"

DRIVER_BUDGET_S = 90.0
CHECK_BUDGET_S = 120.0
DELIVERY_TIMEOUT_S = 30.0
ADMIN_TIMEOUT_S = 30.0


def log(message: str) -> None:
    _log("kafka sources", message)


def _topic(slot: int, gen: int) -> str:
    return f"antithesis_kafka_{slot}_g{gen}"


def _source(slot: int, gen: int) -> str:
    return f"kafka_src_{slot}_g{gen}"


def _upsert(slot: int, gen: int) -> str:
    return f"kafka_upsert_{slot}_g{gen}"


def _none(slot: int, gen: int) -> str:
    return f"kafka_none_{slot}_g{gen}"


def partition_for(key: str, partitions: int) -> int:
    return zlib.crc32(key.encode()) % partitions


@dataclass(frozen=True)
class Slot:
    slot: int
    gen: int
    partitions: int
    retain: str
    dropped_below: int

    @property
    def topic(self) -> str:
        return _topic(self.slot, self.gen)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS slots ("
            " slot INTEGER PRIMARY KEY, gen INTEGER NOT NULL,"
            " partitions INTEGER NOT NULL, retain TEXT NOT NULL,"
            " dropped_below INTEGER NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS ready (slot INTEGER, gen INTEGER,"
            " PRIMARY KEY (slot, gen))"
        )
        # Acknowledged produces only; a lost delivery report leaves no row.
        db.execute(
            "CREATE TABLE IF NOT EXISTS ledger ("
            " topic TEXT NOT NULL, partition INTEGER NOT NULL,"
            " offset INTEGER NOT NULL, key TEXT NOT NULL, value TEXT,"
            " PRIMARY KEY (topic, partition, offset))"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS attempts ("
            " topic TEXT PRIMARY KEY, attempted INTEGER NOT NULL,"
            " acked INTEGER NOT NULL)"
        )
    return db


def _read_slot(db: sqlite3.Connection, slot: int) -> Slot | None:
    row = db.execute(
        "SELECT slot, gen, partitions, retain, dropped_below FROM slots WHERE slot = ?",
        (slot,),
    ).fetchone()
    return Slot(*row) if row is not None else None


def _slot(db: sqlite3.Connection, slot: int) -> Slot:
    with db:
        db.execute(
            "INSERT OR IGNORE INTO slots VALUES (?, 0, ?, ?, 0)",
            (slot, rng.choice(PARTITION_CHOICES), rng.choice(RETAIN_HISTORY_CHOICES)),
        )
    s = _read_slot(db, slot)
    assert s is not None
    return s


def _is_ready(db: sqlite3.Connection, s: Slot) -> bool:
    row = db.execute(
        "SELECT 1 FROM ready WHERE slot = ? AND gen = ?", (s.slot, s.gen)
    ).fetchone()
    return row is not None


def _admin(broker: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": broker, "socket.timeout.ms": 10_000})


def _create_topic(admin: AdminClient, topic: str, partitions: int) -> None:
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)],
        operation_timeout=ADMIN_TIMEOUT_S,
    )
    for future in futures.values():
        try:
            future.result(timeout=ADMIN_TIMEOUT_S)
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def _ddl(conn: psycopg.Connection, statement: str) -> None:
    try:
        conn.execute(statement.encode())
    except psycopg.Error as e:
        if sql.classify(e).race is not sql.CatalogRace.EXISTS:
            raise


def ensure_slot(
    db: sqlite3.Connection, endpoints: Endpoints, host: str, slot: int
) -> Slot | None:
    """The slot's current generation with its topic, source, and tables in place.

    Returns `None` when setup failed for a reason the next invocation retries.
    """
    s = _slot(db, slot)
    if _is_ready(db, s):
        _drop_old_generations(db, endpoints, host, s)
        return s
    try:
        _create_topic(_admin(endpoints.kafka_broker), s.topic, s.partitions)
        ensure_retain_history(host, endpoints)
        with mz_connect(host) as conn:
            ensure_cluster(conn, db)
            _ddl(
                conn,
                f"CREATE CONNECTION IF NOT EXISTS {KAFKA_CONNECTION} TO KAFKA"
                f" (BROKER '{endpoints.kafka_broker}', SECURITY PROTOCOL PLAINTEXT)"
                " WITH (VALIDATE = false)",
            )
            retain = f"WITH (RETAIN HISTORY = FOR '{s.retain}')"
            _ddl(
                conn,
                f"CREATE SOURCE IF NOT EXISTS {_source(s.slot, s.gen)}"
                f" IN CLUSTER {SOURCES_CLUSTER}"
                f" FROM KAFKA CONNECTION {KAFKA_CONNECTION} (TOPIC '{s.topic}')"
                f" {retain}",
            )
            _ddl(
                conn,
                f"CREATE TABLE IF NOT EXISTS {_upsert(s.slot, s.gen)}"
                f' FROM SOURCE {_source(s.slot, s.gen)} (REFERENCE "{s.topic}")'
                " KEY FORMAT TEXT VALUE FORMAT TEXT ENVELOPE UPSERT"
                f" {retain}",
            )
            _ddl(
                conn,
                f"CREATE TABLE IF NOT EXISTS {_none(s.slot, s.gen)}"
                f' FROM SOURCE {_source(s.slot, s.gen)} (REFERENCE "{s.topic}")'
                " FORMAT TEXT INCLUDE PARTITION, OFFSET ENVELOPE NONE"
                f" {retain}",
            )
    except Exception as e:
        log(f"setup of slot {slot} gen {s.gen} failed: {e}")
        return None
    with db:
        db.execute("INSERT OR IGNORE INTO ready VALUES (?, ?)", (s.slot, s.gen))
    _drop_old_generations(db, endpoints, host, s)
    return s


def _drop_old_generations(
    db: sqlite3.Connection, endpoints: Endpoints, host: str, s: Slot
) -> None:
    if s.dropped_below >= s.gen:
        return
    try:
        with mz_connect(host) as conn:
            for gen in range(s.dropped_below, s.gen):
                conn.execute(
                    f"DROP SOURCE IF EXISTS {_source(s.slot, gen)} CASCADE".encode()
                )
        admin = _admin(endpoints.kafka_broker)
        old = [_topic(s.slot, gen) for gen in range(s.dropped_below, s.gen)]
        for future in admin.delete_topics(
            old, operation_timeout=ADMIN_TIMEOUT_S
        ).values():
            try:
                future.result(timeout=ADMIN_TIMEOUT_S)
            except KafkaException as e:
                if e.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise
    except Exception as e:
        log(f"dropping old generations of slot {s.slot} failed: {e}")
        return
    with db:
        for topic in old:
            db.execute("DELETE FROM ledger WHERE topic = ?", (topic,))
            db.execute("DELETE FROM attempts WHERE topic = ?", (topic,))
        db.execute(
            "UPDATE slots SET dropped_below = ? WHERE slot = ? AND dropped_below < ?",
            (s.gen, s.slot, s.gen),
        )


def _rotate_if_full(db: sqlite3.Connection, s: Slot) -> None:
    row = db.execute(
        "SELECT attempted FROM attempts WHERE topic = ?", (s.topic,)
    ).fetchone()
    if row is None or int(row[0]) < TOPIC_RECORD_CAP:
        return
    with db:
        cur = db.execute(
            "UPDATE slots SET gen = gen + 1, partitions = ?, retain = ?"
            " WHERE slot = ? AND gen = ?",
            (
                rng.choice(PARTITION_CHOICES),
                rng.choice(RETAIN_HISTORY_CHOICES),
                s.slot,
                s.gen,
            ),
        )
    if cur.rowcount:
        log(f"slot {s.slot} rotated past gen {s.gen}")


def _value(tag: str) -> str:
    """A value of a size from the menu, starting with a unique `tag` when it fits.

    The filler repeats one random chunk, so a large value costs a handful of
    random draws rather than one per character.
    """
    size = rng.choice(VALUE_SIZES)
    chunk = "".join(rng.choice(_FILLER) for _ in range(8))
    return (tag + chunk * (size // len(chunk) + 1))[:size]


def _producer(broker: str, compression: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": broker,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": compression,
            "message.timeout.ms": int(DELIVERY_TIMEOUT_S * 1000),
        }
    )


def _produce(
    db: sqlite3.Connection,
    producer: Producer,
    s: Slot,
    records: list[tuple[str, str | None]],
) -> list[tuple[int, int, str, str | None]]:
    """Produce `records` and record the acknowledged ones. Returns the acknowledged."""
    with db:
        db.execute(
            "INSERT INTO attempts VALUES (?, ?, 0) ON CONFLICT (topic)"
            " DO UPDATE SET attempted = attempted + excluded.attempted",
            (s.topic, len(records)),
        )
    acked: list[tuple[int, int, str, str | None]] = []

    def on_delivery(err: Any, msg: Any) -> None:
        if err is not None:
            return
        value = msg.value()
        acked.append(
            (
                msg.partition(),
                msg.offset(),
                msg.key().decode(),
                value.decode() if value is not None else None,
            )
        )

    for key, value in records:
        while True:
            try:
                producer.produce(
                    s.topic,
                    key=key.encode(),
                    value=value.encode() if value is not None else None,
                    partition=partition_for(key, s.partitions),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
    remaining = producer.flush(DELIVERY_TIMEOUT_S)
    if remaining:
        log(f"{remaining} produces to {s.topic} unconfirmed")
    with db:
        db.executemany(
            "INSERT OR REPLACE INTO ledger VALUES (?, ?, ?, ?, ?)",
            [(s.topic, p, o, k, v) for p, o, k, v in acked],
        )
        db.execute(
            "UPDATE attempts SET acked = acked + ? WHERE topic = ?",
            (len(acked), s.topic),
        )
    return acked


def produce_main() -> int:
    endpoints = Endpoints.from_env()
    db = open_state()
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    s = ensure_slot(db, endpoints, host, rng.randrange(SLOTS))
    if s is None:
        return 0
    keyspace = timeline_choice(db, "keyspace", KEYSPACE_CHOICES)
    tombstone_p = timeline_choice(db, "tombstone_p", TOMBSTONE_P_CHOICES)
    prefix = timeline_choice(db, "key_prefix", KEY_PREFIXES)
    compression = timeline_choice(db, "compression", COMPRESSION_CHOICES)
    deadline = Deadline(DRIVER_BUDGET_S)
    producer = _producer(endpoints.kafka_broker, compression)
    total = 0
    while not deadline.expired() and total < 2_000:
        n = rng.choice(BATCH_CHOICES)
        records: list[tuple[str, str | None]] = []
        for _ in range(n):
            key = f"{prefix}{rng.randrange(keyspace)}"
            if rng.random() < tombstone_p:
                records.append((key, None))
            else:
                records.append((key, _value(f"{rng.getrandbits(32):08x}")))
        _produce(db, producer, s, records)
        total += n
        _rotate_if_full(db, s)
        if rng.random() < 0.5:
            break
    log(f"produced {total} records to {s.topic}")
    return 0


def send_heartbeats(host: str) -> list[Heartbeat]:
    """Produce one fresh record per ready slot for the liveness check."""
    endpoints = Endpoints.from_env()
    db = open_state()
    out: list[Heartbeat] = []
    producer = _producer(endpoints.kafka_broker, "none")
    for slot in range(SLOTS):
        s = _read_slot(db, slot)
        if s is None or not _is_ready(db, s):
            continue
        tag = f"hb-{rng.getrandbits(48):012x}"
        acked = _produce(db, producer, s, [(tag, tag)])
        if not acked:
            continue
        out.append(
            Heartbeat(
                _upsert(s.slot, s.gen),
                f"SELECT count(*) FROM {_upsert(s.slot, s.gen)} WHERE key = %s",
                (tag,),
            )
        )
        out.append(
            Heartbeat(
                _none(s.slot, s.gen),
                f"SELECT count(*) FROM {_none(s.slot, s.gen)} WHERE text = %s",
                (tag,),
            )
        )
    return out


def _read_topic(
    broker: str, topic: str, upto: dict[int, int], deadline: Deadline
) -> tuple[dict[int, list[tuple[int, str, str | None]]], dict[int, int]] | None:
    """Records at offsets `[0, upto[p])` per partition, and the high watermarks.

    `None` if the topic lost its head (low watermark above zero), holds fewer
    records than `upto` claims, or could not be read in time.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "antithesis-source-readback",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        highs: dict[int, int] = {}
        for p, f in upto.items():
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=10
            )
            highs[p] = high
            if f > 0 and (low > 0 or high < f):
                log(f"{topic}[{p}] watermarks [{low}, {high}) cannot cover F={f}")
                return None
        out: dict[int, list[tuple[int, str, str | None]]] = {p: [] for p in upto}
        pending = {p for p, f in upto.items() if f > 0}
        if pending:
            consumer.assign([TopicPartition(topic, p, 0) for p in sorted(pending)])
        while pending and not deadline.expired():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            err = msg.error()
            if err:
                if err.code() == KafkaError._PARTITION_EOF:
                    eof_partition = msg.partition()
                    assert eof_partition is not None
                    pending.discard(eof_partition)
                    continue
                log(f"read-back of {topic} failed: {err}")
                return None
            p, offset = msg.partition(), msg.offset()
            if p not in pending:
                continue
            assert offset is not None
            if offset < upto[p]:
                key, value = msg.key(), msg.value()
                assert key is not None
                out[p].append(
                    (
                        offset,
                        key.decode(),
                        value.decode() if value is not None else None,
                    )
                )
            if offset >= upto[p] - 1:
                pending.discard(p)
        return (out, highs) if not pending else None
    finally:
        consumer.close()


def _report_query_error(e: BaseException, details: dict[str, Any]) -> None:
    """Assert on a failed check query unless the failure is expected under faults."""
    message = source_error_text(e)
    if message is not None:
        always_or_unreachable(
            False,
            "kafka source: exports and progress read without a source error",
            {**details, "error": message},
        )
        return
    c = sql.classify_as_of_read(e)
    if c.outcome == sql.Outcome.VIOLATION:
        always_or_unreachable(
            False,
            "kafka source: check queries return only classified errors",
            {**details, "sqlstate": c.sqlstate, "template": c.template},
        )


def _partition_frontier(
    rows: list[tuple[Any, ...]], partitions: int
) -> tuple[dict[int, int], list[int]]:
    """Offset per partition from the progress rows, and partitions not covered exactly once.

    Each row is a `partition` numrange with the next offset to ingest: either a
    single partition `[p, p]` or an open gap `(a, b)` covering partitions that
    do not exist yet, with NULL bounds for infinities.
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


def check_main() -> int:
    endpoints = Endpoints.from_env()
    db = open_state()
    s = _read_slot(db, rng.randrange(SLOTS))
    if s is None or not _is_ready(db, s):
        return 0
    deadline = Deadline(CHECK_BUDGET_S)
    try:
        host = mz_host()
        conn = mz_connect(host)
    except Exception as e:
        log(f"no connection: {e}")
        return 0
    try:
        _check(db, endpoints, conn, s, deadline)
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        log(f"check abandoned: {c.outcome.value} {c.sqlstate} {c.template}")
    finally:
        conn.close()
    return 0


def _check(
    db: sqlite3.Connection,
    endpoints: Endpoints,
    conn: psycopg.Connection,
    s: Slot,
    deadline: Deadline,
) -> None:
    src_name, up_name, none_name = (
        _source(s.slot, s.gen),
        _upsert(s.slot, s.gen),
        _none(s.slot, s.gen),
    )
    ids = lookup_ids(conn, [src_name, up_name, none_name])
    if len(ids) != 3:
        return
    src, up, none = ids[src_name], ids[up_name], ids[none_name]
    observe(conn, [Export(up, src, "upsert"), Export(none, src, "other")])

    fronts = frontiers(conn, [src, up, none])
    if len(fronts) != 3:
        return
    t = pick_as_of((f[0] for f in fronts.values()), (f[1] for f in fronts.values()))
    if t is None:
        return
    details: dict[str, Any] = {"topic": s.topic, "as_of": t}

    try:
        progress = conn.execute(
            "SELECT lower(partition)::text, upper(partition)::text,"
            ' lower_inc(partition), upper_inc(partition), "offset"::text'
            f" FROM {src_name} AS OF {t}".encode()
        ).fetchall()
        upsert_rows = conn.execute(
            f"SELECT key, text FROM {up_name} AS OF {t}".encode()
        ).fetchall()
        none_rows = conn.execute(
            f'SELECT text, partition, "offset"::text FROM {none_name} AS OF {t}'.encode()
        ).fetchall()
        dup_keys = conn.execute(
            f"SELECT key, count(*) FROM {up_name}"
            f" GROUP BY key HAVING count(*) > 1 LIMIT 10 AS OF {t}".encode()
        ).fetchall()
    except psycopg.Error as e:
        _report_query_error(e, details)
        return

    frontier, uncovered = _partition_frontier(progress, s.partitions)
    always(
        not uncovered,
        "kafka source: the progress collection maps each partition to exactly one offset",
        {**details, "uncovered": uncovered, "rows": [list(r) for r in progress[:10]]},
    )
    if uncovered:
        return
    details["frontier"] = frontier

    always(
        not dup_keys,
        "kafka source: the upsert export holds at most one row per key",
        {**details, "duplicates": [[k, int(c)] for k, c in dup_keys]},
    )

    regressions = record_progress_observation(db, s.topic, t, frontier)
    always(
        not regressions,
        "kafka source: the mapped offsets are monotone in mz time",
        {**details, "conflicting": regressions},
    )

    read = _read_topic(endpoints.kafka_broker, s.topic, frontier, deadline)
    if read is None:
        return
    records, highs = read
    by_offset = {
        (p, offset): (key, value)
        for p, recs in records.items()
        for offset, key, value in recs
    }

    ledger_mismatch = []
    for p, f in frontier.items():
        for offset, key, value in db.execute(
            "SELECT offset, key, value FROM ledger"
            " WHERE topic = ? AND partition = ? AND offset < ?",
            (s.topic, p, f),
        ):
            if by_offset.get((p, offset)) != (key, value):
                ledger_mismatch.append([p, offset, key, by_offset.get((p, offset))])
    always(
        not ledger_mismatch,
        "kafka source: every acknowledged produce below the mapped offsets is in the topic as acknowledged",
        {**details, "mismatches": ledger_mismatch[:10]},
    )

    latest: dict[str, str | None] = {}
    writes: dict[str, int] = {}
    home: dict[str, int] = {}
    split_keys = False
    for p, recs in records.items():
        for _, key, value in sorted(recs):
            if home.setdefault(key, p) != p:
                split_keys = True
            latest[key] = value
            writes[key] = writes.get(key, 0) + 1
    expected_none = [
        (value, p, offset)
        for p, recs in records.items()
        for offset, _, value in recs
        if value is not None
    ]
    observed_none = [(text, int(p), int(o)) for text, p, o in none_rows]
    none_diff = bag_diff(expected_none, observed_none)
    none_ok = none_diff["missing_count"] == 0 and none_diff["extra_count"] == 0
    always(
        none_ok,
        "kafka source: the none-envelope export equals the topic records below the mapped offsets",
        {**details, **none_diff},
    )

    upsert_ok = True
    if not split_keys:
        expected_upsert = [(k, v) for k, v in latest.items() if v is not None]
        upsert_diff = bag_diff(expected_upsert, [tuple(r) for r in upsert_rows])
        upsert_ok = (
            upsert_diff["missing_count"] == 0 and upsert_diff["extra_count"] == 0
        )
        always(
            upsert_ok,
            "kafka source: the upsert export equals the latest value per key below the mapped offsets",
            {**details, **upsert_diff},
        )
        sometimes(
            any(v is None for v in latest.values()),
            "kafka source: an upsert check covered a key whose latest record is a tombstone",
            details,
        )
        sometimes(
            any(n > 1 for n in writes.values()),
            "kafka source: an upsert check covered a key written more than once",
            details,
        )
    else:
        log(f"{s.topic}: a key appears in several partitions, upsert check skipped")

    sometimes(
        any(frontier[p] < highs.get(p, 0) for p in frontier),
        "kafka source: a check sampled a time whose mapped offsets trail the topic end",
        details,
    )
    sometimes(
        none_ok and upsert_ok and bool(expected_none),
        "kafka source: a non-empty export matched the topic at the mapped offsets",
        details,
    )
