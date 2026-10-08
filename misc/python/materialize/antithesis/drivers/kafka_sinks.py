# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Kafka sinks checked for exactly-once output against the sinked views and an
acknowledged-write model.

Property: `kafka-sink-output-is-exactly-once`.

Each generation is one table `snk_items_g<n> (id, grp, val, tok)`, a
row-for-row materialized view over it, an aggregate view
`SELECT grp, count(*), sum(val) GROUP BY grp` (so every row update retracts a
group row), and two JSON sinks: the item view with `KEY (id) ENVELOPE
DEBEZIUM` and the aggregate view with `KEY (grp) ENVELOPE UPSERT`. Both keys
are `NOT ENFORCED` but unique: ids are allocated once and `grp` is the
grouping key. The checks rely on that uniqueness, since it makes
`(key, timestamp)` identify one update.
The views run on the shared compute cluster and the sinks on the source
cluster, so sink restarts (source cluster replica kills, replication factor 2
moving the single active sink replica) happen independently of the views. All
sinks share one connection with its own progress topic.

The writer inserts, updates and deletes rows. Every row id is leased in the
state database while a statement on it is in flight, and an id with an
indeterminate outcome is never written again, so the model of each id is
exact or the id is marked uncertain. Rows never change group, so a group is
uncertain exactly when one of its ids is.

The check reads, with `read_committed` consumers that wait for every
transaction below the high watermark to resolve: the progress topic, then
each data topic, then the progress topic again. It asserts on the committed
output alone (no duplicate update, timestamps nondecreasing per partition, a
Debezium before image chain per key, progress never regressing, every data
message below the latest committed frontier), compares the output
consolidated below a committed frontier F with the view `AS OF F - 1`, and,
once a frontier passes every write acknowledged before the check started,
compares it with the model. The pure logic is in `sink_oracle`.

A generation rotates to fresh tables, views, sinks and topics after
`GEN_MAX_OPS` write statements; the previous generation's objects are dropped
and its topics deleted.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)
from confluent_kafka.admin import AdminClient  # type: ignore
from confluent_kafka.cimpl import (
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from kubernetes import client  # type: ignore

from materialize.antithesis import sink_oracle as oracle
from materialize.antithesis import sql, state
from materialize.antithesis.drivers import configure
from materialize.antithesis.drivers.sources_common import (
    LAZY_SETUP_BUDGET_S,
    RETAIN_HISTORY_CHOICES,
    SINCE_MARGIN_MS,
    SOURCES_CLUSTER,
    Deadline,
    ensure_cluster,
    ensure_retain_history,
    frontiers,
    id_list,
    lookup_ids,
    mz_connect,
    mz_host,
    mz_now_ms,
    timeline_choice,
    with_retry,
)
from materialize.antithesis.drivers.sources_common import log as _log
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.rng import rng

STATE_DB = "kafka_sinks"
CONNECTION = "antithesis_kafka_sink"
PROGRESS_TOPIC = "antithesis_sink_progress"
VIEW_CLUSTER = configure.SHARED_CLUSTER
SINK_CLUSTER = SOURCES_CLUSTER

ITEM_COLUMNS = ("id", "grp", "val", "tok")
GROUP_COLUMNS = ("grp", "n", "total")

PARTITION_CHOICES = (1, 2, 4)
GROUP_CHOICES = (1, 4, 32)
"""One group puts every write on one upsert key; 32 makes groups appear and vanish."""
BATCH_CHOICES = (1, 2, 8)
"""Ids per statement. Several ids at one timestamp exercise multi-key transactions."""
TARGET_ROWS = 200
"""Inserts are favored below this many present rows, deletes above it."""
DELTA_CHOICES = (-7, -1, 1, 3, 1000)

# Calibration: bounds the per-check topic read. A generation's data topics
# hold at most about GEN_MAX_OPS * max(BATCH_CHOICES) small JSON messages.
GEN_MAX_OPS = 1_000
SEED_ROWS = 8
DRIVER_BUDGET_S = 60.0
DRIVER_MAX_OPS = 200
CHECK_BUDGET_S = 150.0
# How long a check polls the progress topic for a frontier past the writes
# acknowledged before it started. Sinks commit about once per second.
MODEL_WAIT_S = 20.0
FIRST_SETUP_BUDGET_S = 120.0
CONSUMER_TIMEOUT_S = 10.0
ABORTED_READ_P = 0.25

# Calibration: after faults stop, the time for killed pods to return, the sink
# to restart, and its frontier to pass the latest acknowledged write.
EVENTUALLY_TIGHT_S = 120.0
EVENTUALLY_WEDGE_S = 600.0
EVENTUALLY_POLL_S = 5.0

FORBIDDEN_SINK_ERRORS = (
    "input compacted past resume upper",
    "upper regressed in topic",
    "Fenced off by newer version of the sink",
    "invalid progress record",
    "should contain a single partition",
)
"""Status errors no correct sink reports here: the workload never runs `ALTER
SINK` or touches the progress topic, so each of these means resume state was
lost or corrupted."""


def log(message: str) -> None:
    _log("kafka sinks", message)


@dataclass(frozen=True)
class Gen:
    gen: int
    parts_dbz: int
    parts_upsert: int
    retain: str
    dropped_below: int

    @property
    def items(self) -> str:
        return f"snk_items_g{self.gen}"

    @property
    def items_mv(self) -> str:
        return f"snk_items_mv_g{self.gen}"

    @property
    def agg_mv(self) -> str:
        return f"snk_agg_mv_g{self.gen}"

    @property
    def dbz_sink(self) -> str:
        return f"snk_dbz_g{self.gen}"

    @property
    def upsert_sink(self) -> str:
        return f"snk_upsert_g{self.gen}"

    @property
    def dbz_topic(self) -> str:
        return _dbz_topic(self.gen)

    @property
    def upsert_topic(self) -> str:
        return _upsert_topic(self.gen)


def _dbz_topic(gen: int) -> str:
    return f"antithesis_sink_dbz_g{gen}"


def _upsert_topic(gen: int) -> str:
    return f"antithesis_sink_upsert_g{gen}"


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS gens ("
            " slot INTEGER PRIMARY KEY CHECK (slot = 0), gen INTEGER NOT NULL,"
            " parts_dbz INTEGER NOT NULL, parts_upsert INTEGER NOT NULL,"
            " retain TEXT NOT NULL, dropped_below INTEGER NOT NULL)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS ready ("
            " gen INTEGER PRIMARY KEY, seeded INTEGER NOT NULL DEFAULT 0,"
            " complete INTEGER NOT NULL DEFAULT 0)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS ids (id INTEGER PRIMARY KEY AUTOINCREMENT)"
        )
        # The model. `busy` is set while a statement on the id is in flight
        # and stays set if the process died; `uncertain` is set after an
        # indeterminate outcome. Either excludes the id from comparisons.
        db.execute(
            "CREATE TABLE IF NOT EXISTS rows ("
            " gen INTEGER NOT NULL, id INTEGER NOT NULL, present INTEGER NOT NULL,"
            " grp INTEGER NOT NULL, val INTEGER NOT NULL, tok TEXT NOT NULL,"
            " uncertain INTEGER NOT NULL, busy INTEGER NOT NULL, seq INTEGER NOT NULL,"
            " PRIMARY KEY (gen, id))"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS attempts ("
            " gen INTEGER PRIMARY KEY, attempted INTEGER NOT NULL)"
        )
        # Values of restart signatures seen while a generation's sinks ran,
        # with the mz wall clock when each value was first seen.
        db.execute(
            "CREATE TABLE IF NOT EXISTS observations ("
            " gen INTEGER NOT NULL, kind TEXT NOT NULL, value TEXT NOT NULL,"
            " first_seen_ms INTEGER NOT NULL, PRIMARY KEY (gen, kind, value))"
        )
        db.execute(
            "INSERT OR IGNORE INTO gens VALUES (0, 0, ?, ?, ?, 0)",
            (
                rng.choice(PARTITION_CHOICES),
                rng.choice(PARTITION_CHOICES),
                rng.choice(RETAIN_HISTORY_CHOICES),
            ),
        )
    return db


def _current(db: sqlite3.Connection) -> Gen:
    row = db.execute(
        "SELECT gen, parts_dbz, parts_upsert, retain, dropped_below FROM gens"
    ).fetchone()
    assert row is not None
    return Gen(*row)


def _ready_flags(db: sqlite3.Connection, gen: int) -> tuple[bool, bool]:
    row = db.execute(
        "SELECT seeded, complete FROM ready WHERE gen = ?", (gen,)
    ).fetchone()
    return (bool(row[0]), bool(row[1])) if row is not None else (False, False)


def _groups(db: sqlite3.Connection) -> int:
    return int(timeline_choice(db, "groups", GROUP_CHOICES))


def _ddl(conn: psycopg.Connection, statement: str) -> None:
    try:
        conn.execute(statement.encode())
    except psycopg.Error as e:
        if sql.classify(e).race is not sql.CatalogRace.EXISTS:
            raise


def _setup_retryable(e: BaseException) -> bool:
    if isinstance(e, KafkaException):
        return not e.args[0].fatal()
    return sql.classify(e).outcome is not sql.Outcome.VIOLATION


def ensure_ready(
    db: sqlite3.Connection, endpoints: Endpoints, host: str, deadline: Deadline
) -> Gen | None:
    """The current generation with its table, views and sinks in place.

    Setup is retried until `deadline`. Returns None when it did not finish.
    """
    g = _current(db)
    if _ready_flags(db, g.gen)[1]:
        _drop_old_generations(db, endpoints, host, g)
        return g
    failure = with_retry(
        "kafka sinks",
        f"setup of gen {g.gen}",
        lambda: _setup(db, endpoints, host, g),
        deadline,
        _setup_retryable,
    )
    if failure is not None:
        always_or_unreachable(
            failure.retryable,
            "kafka sink: setup fails only with retryable errors",
            failure.details(),
        )
        return None
    with db:
        db.execute(
            "INSERT INTO ready (gen, complete) VALUES (?, 1)"
            " ON CONFLICT (gen) DO UPDATE SET complete = 1",
            (g.gen,),
        )
    _drop_old_generations(db, endpoints, host, g)
    return g


def _setup(db: sqlite3.Connection, endpoints: Endpoints, host: str, g: Gen) -> None:
    ensure_retain_history(host, endpoints)
    retain = f"WITH (RETAIN HISTORY = FOR '{g.retain}')"
    with mz_connect(host) as conn:
        ensure_cluster(conn, db)
        _ddl(
            conn,
            f"CREATE CONNECTION IF NOT EXISTS {CONNECTION} TO KAFKA"
            f" (BROKER '{endpoints.kafka_broker}', SECURITY PROTOCOL PLAINTEXT,"
            f" PROGRESS TOPIC '{PROGRESS_TOPIC}', PROGRESS TOPIC REPLICATION FACTOR 1)"
            " WITH (VALIDATE = false)",
        )
        _ddl(
            conn,
            f"CREATE TABLE IF NOT EXISTS {g.items}"
            " (id bigint NOT NULL, grp int NOT NULL, val bigint NOT NULL, tok text NOT NULL)",
        )
        # Seeding before the sinks exist gives them a non-empty snapshot. It is
        # attempted once per generation; its outcome lands in the model.
        if not _ready_flags(db, g.gen)[0]:
            with db:
                db.execute(
                    "INSERT INTO ready (gen, seeded) VALUES (?, 1)"
                    " ON CONFLICT (gen) DO UPDATE SET seeded = 1",
                    (g.gen,),
                )
            op = _claim_insert(db, g, SEED_ROWS, _groups(db))
            _run_op(db, conn, g, op)
        _ddl(
            conn,
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {g.items_mv}"
            f" IN CLUSTER {VIEW_CLUSTER} {retain}"
            f" AS SELECT id, grp, val, tok FROM {g.items}",
        )
        _ddl(
            conn,
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {g.agg_mv}"
            f" IN CLUSTER {VIEW_CLUSTER} {retain}"
            " AS SELECT grp, count(*)::bigint AS n, sum(val)::bigint AS total"
            f" FROM {g.items} GROUP BY grp",
        )
        _ddl(
            conn,
            f"CREATE SINK IF NOT EXISTS {g.dbz_sink} IN CLUSTER {SINK_CLUSTER}"
            f" FROM {g.items_mv} INTO KAFKA CONNECTION {CONNECTION}"
            f" (TOPIC '{g.dbz_topic}', TOPIC PARTITION COUNT {g.parts_dbz},"
            " TOPIC REPLICATION FACTOR 1)"
            " KEY (id) NOT ENFORCED FORMAT JSON ENVELOPE DEBEZIUM",
        )
        _ddl(
            conn,
            f"CREATE SINK IF NOT EXISTS {g.upsert_sink} IN CLUSTER {SINK_CLUSTER}"
            f" FROM {g.agg_mv} INTO KAFKA CONNECTION {CONNECTION}"
            f" (TOPIC '{g.upsert_topic}', TOPIC PARTITION COUNT {g.parts_upsert},"
            " TOPIC REPLICATION FACTOR 1)"
            " KEY (grp) NOT ENFORCED FORMAT JSON ENVELOPE UPSERT",
        )


def _admin(broker: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": broker, "socket.timeout.ms": 10_000})


def _drop_old_generations(
    db: sqlite3.Connection, endpoints: Endpoints, host: str, g: Gen
) -> None:
    if g.dropped_below >= g.gen:
        return
    old = list(range(g.dropped_below, g.gen))
    try:
        with mz_connect(host) as conn:
            for gen in old:
                conn.execute(f"DROP TABLE IF EXISTS snk_items_g{gen} CASCADE".encode())
        topics = [t for gen in old for t in (_dbz_topic(gen), _upsert_topic(gen))]
        for future in (
            _admin(endpoints.kafka_broker)
            .delete_topics(topics, operation_timeout=CONSUMER_TIMEOUT_S)
            .values()
        ):
            try:
                future.result(timeout=CONSUMER_TIMEOUT_S)
            except KafkaException as e:
                if e.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise
    except Exception as e:
        log(f"dropping generations {old} failed: {e}")
        return
    with db:
        for gen in old:
            db.execute("DELETE FROM rows WHERE gen = ?", (gen,))
            db.execute("DELETE FROM attempts WHERE gen = ?", (gen,))
            db.execute("DELETE FROM observations WHERE gen = ?", (gen,))
        db.execute(
            "UPDATE gens SET dropped_below = ? WHERE dropped_below < ?", (g.gen, g.gen)
        )


def _rotate_if_full(db: sqlite3.Connection, g: Gen) -> None:
    row = db.execute(
        "SELECT attempted FROM attempts WHERE gen = ?", (g.gen,)
    ).fetchone()
    if row is None or int(row[0]) < GEN_MAX_OPS:
        return
    with db:
        cur = db.execute(
            "UPDATE gens SET gen = gen + 1, parts_dbz = ?, parts_upsert = ?, retain = ?"
            " WHERE gen = ?",
            (
                rng.choice(PARTITION_CHOICES),
                rng.choice(PARTITION_CHOICES),
                rng.choice(RETAIN_HISTORY_CHOICES),
                g.gen,
            ),
        )
    if cur.rowcount:
        log(f"rotated past gen {g.gen}")


@dataclass
class Op:
    kind: str
    """`insert`, `update`, or `delete`."""
    ids: list[int]
    tok: str
    rows: list[tuple[int, int, int]] = field(default_factory=list)
    """`(id, grp, val)` per inserted row."""
    delta: int = 0


def _token() -> str:
    return f"{rng.getrandbits(64):016x}"


def _claim_insert(db: sqlite3.Connection, g: Gen, n: int, groups: int) -> Op:
    tok = _token()
    rows = []
    with db:
        for _ in range(n):
            new_id = db.execute("INSERT INTO ids DEFAULT VALUES").lastrowid
            assert new_id is not None
            grp, val = rng.randrange(groups), rng.randint(-1000, 1000)
            db.execute(
                "INSERT INTO rows VALUES (?, ?, 0, ?, ?, ?, 0, 1, 1)",
                (g.gen, new_id, grp, val, tok),
            )
            rows.append((new_id, grp, val))
    return Op("insert", [r[0] for r in rows], tok, rows=rows)


def _claim_existing(db: sqlite3.Connection, g: Gen, kind: str, n: int) -> Op | None:
    candidates = [
        int(r[0])
        for r in db.execute(
            "SELECT id FROM rows WHERE gen = ? AND present = 1 AND busy = 0"
            " AND uncertain = 0",
            (g.gen,),
        )
    ]
    if not candidates:
        return None
    claimed = []
    for i in rng.sample(candidates, min(n, len(candidates))):
        with db:
            cur = db.execute(
                "UPDATE rows SET busy = 1, seq = seq + 1 WHERE gen = ? AND id = ?"
                " AND present = 1 AND busy = 0 AND uncertain = 0",
                (g.gen, i),
            )
        if cur.rowcount == 1:
            claimed.append(i)
    if not claimed:
        return None
    return Op(kind, claimed, _token(), delta=rng.choice(DELTA_CHOICES))


def _statement(g: Gen, op: Op) -> tuple[str, tuple]:
    ids = ", ".join(str(int(i)) for i in op.ids)
    if op.kind == "insert":
        values = ", ".join("(%s, %s, %s, %s)" for _ in op.rows)
        params = tuple(x for i, grp, val in op.rows for x in (i, grp, val, op.tok))
        return f"INSERT INTO {g.items} (id, grp, val, tok) VALUES {values}", params
    if op.kind == "update":
        return (
            f"UPDATE {g.items} SET val = val + %s, tok = %s WHERE id IN ({ids})",
            (op.delta, op.tok),
        )
    return f"DELETE FROM {g.items} WHERE id IN ({ids})", ()


def _run_op(db: sqlite3.Connection, conn: psycopg.Connection, g: Gen, op: Op) -> None:
    """Execute `op` and settle its leased ids in the model."""
    with db:
        db.execute(
            "INSERT INTO attempts VALUES (?, 1) ON CONFLICT (gen)"
            " DO UPDATE SET attempted = attempted + 1",
            (g.gen,),
        )
    text, params = _statement(g, op)
    outcome: sql.Outcome | None = None
    rowcount = -1
    details: dict[str, Any] = {"op": op.kind, "ids": op.ids[:10], "gen": g.gen}
    try:
        cur = conn.execute(text.encode(), params)
        rowcount = cur.rowcount
    except psycopg.Error as e:
        c = sql.classify(e)
        outcome = c.outcome
        details.update({"sqlstate": c.sqlstate, "template": c.template})
        always(
            c.outcome is not sql.Outcome.VIOLATION,
            "kafka sink: source table writes fail only with classified errors",
            details,
        )
    with db:
        if outcome is None:
            always(
                rowcount == len(op.ids),
                "kafka sink: an acknowledged write affects exactly the rows the model expects",
                {**details, "rowcount": rowcount},
            )
            for i in op.ids:
                if op.kind == "insert":
                    db.execute(
                        "UPDATE rows SET present = 1, busy = 0 WHERE gen = ? AND id = ?",
                        (g.gen, i),
                    )
                elif op.kind == "update":
                    db.execute(
                        "UPDATE rows SET val = val + ?, tok = ?, busy = 0"
                        " WHERE gen = ? AND id = ?",
                        (op.delta, op.tok, g.gen, i),
                    )
                else:
                    db.execute(
                        "UPDATE rows SET present = 0, busy = 0 WHERE gen = ? AND id = ?",
                        (g.gen, i),
                    )
        elif outcome is sql.Outcome.REJECTED:
            db.executemany(
                "UPDATE rows SET busy = 0 WHERE gen = ? AND id = ?",
                [(g.gen, i) for i in op.ids],
            )
        else:
            db.executemany(
                "UPDATE rows SET uncertain = 1, busy = 0 WHERE gen = ? AND id = ?",
                [(g.gen, i) for i in op.ids],
            )


def _next_op(db: sqlite3.Connection, g: Gen) -> Op | None:
    row = db.execute(
        "SELECT count(*) FROM rows WHERE gen = ? AND present = 1", (g.gen,)
    ).fetchone()
    present = int(row[0]) if row is not None else 0
    n = rng.choice(BATCH_CHOICES)
    insert_p = 0.7 if present < TARGET_ROWS else 0.2
    if rng.random() < insert_p:
        return _claim_insert(db, g, n, _groups(db))
    kind = "update" if rng.random() < 0.6 else "delete"
    return _claim_existing(db, g, kind, n)


def setup_main() -> int:
    """Set up the first generation. Run from `first_configure`; the writer and
    the check repair setup lazily if this did not finish."""
    endpoints = Endpoints.from_env()
    db = open_state()
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    g = ensure_ready(db, endpoints, host, Deadline(FIRST_SETUP_BUDGET_S))
    sometimes(
        g is not None,
        "kafka sink: first_configure set up the sinks",
        {"gen": g.gen if g is not None else None},
    )
    return 0


def write_main() -> int:
    endpoints = Endpoints.from_env()
    db = open_state()
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    g = ensure_ready(db, endpoints, host, Deadline(LAZY_SETUP_BUDGET_S))
    if g is None:
        return 0
    deadline = Deadline(DRIVER_BUDGET_S)
    try:
        conn = mz_connect(host)
    except (psycopg.Error, OSError) as e:
        log(f"no connection: {e}")
        return 0
    ops = 0
    try:
        while not deadline.expired() and ops < DRIVER_MAX_OPS:
            op = _next_op(db, g)
            if op is not None:
                _run_op(db, conn, g, op)
                ops += 1
                if conn.closed:
                    break
            if rng.random() < 0.05:
                break
    finally:
        conn.close()
    _rotate_if_full(db, g)
    log(f"ran {ops} write statements on gen {g.gen}")
    return 0


@dataclass(frozen=True)
class TopicRead:
    messages: list[tuple[int, int, bytes | None, bytes | None, list]]
    """`(partition, offset, key, value, headers)` in consumption order."""
    highs: dict[int, int]


def read_topic(
    broker: str,
    topic: str,
    deadline: Deadline,
    committed: bool = True,
    after: TopicRead | None = None,
) -> TopicRead | None:
    """Every record below the high watermarks taken at the start of the read.

    With `committed`, the consumer is `read_committed` and the read finishes
    only once its position passes each high watermark, so every transaction
    with records below it has resolved and its committed records are
    included. With `after`, only records at or above its high watermarks are
    fetched and appended to its records. Returns None if the topic is
    missing, lost its head, shrank, or could not be read in time.
    """
    start = after.highs if after is not None else {}
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "antithesis-sink-check",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed" if committed else "read_uncommitted",
        }
    )
    try:
        md = consumer.list_topics(topic, timeout=CONSUMER_TIMEOUT_S)
        meta = md.topics.get(topic)
        if meta is None or meta.error is not None or not meta.partitions:
            log(f"{topic}: no metadata ({meta.error if meta else 'missing'})")
            return None
        highs: dict[int, int] = {}
        for p in sorted(meta.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=CONSUMER_TIMEOUT_S
            )
            if low > 0 or high < start.get(p, 0):
                log(f"{topic}[{p}] watermarks [{low}, {high}): head lost, skipping")
                return None
            highs[p] = high
        pending = {p for p, h in highs.items() if h > start.get(p, 0)}
        if pending:
            consumer.assign(
                [TopicPartition(topic, p, start.get(p, 0)) for p in sorted(pending)]
            )
        out: list[tuple[int, int, bytes | None, bytes | None, list]] = (
            list(after.messages) if after is not None else []
        )
        while pending:
            if deadline.expired():
                log(f"{topic}: read timed out with {sorted(pending)} pending")
                return None
            msg = consumer.poll(1.0)
            if msg is not None:
                err = msg.error()
                if err is not None:
                    if err.code() != KafkaError._PARTITION_EOF:
                        log(f"{topic}: read failed: {err}")
                        return None
                else:
                    p, offset = msg.partition(), msg.offset()
                    assert p is not None and offset is not None
                    if start.get(p, 0) <= offset < highs[p]:
                        raw = msg.headers() or []
                        headers = list(raw.items() if isinstance(raw, dict) else raw)
                        out.append((p, offset, msg.key(), msg.value(), headers))
            for tp in consumer.position(
                [TopicPartition(topic, p) for p in sorted(pending)]
            ):
                if tp.offset >= highs[tp.partition]:
                    pending.discard(tp.partition)
        return TopicRead(out, highs)
    except KafkaException as e:
        log(f"{topic}: read failed: {e}")
        return None
    finally:
        consumer.close()


def _progress(
    read: TopicRead, sink_id: str
) -> tuple[list[oracle.ProgressRecord], list[str]]:
    key = f"mz-sink-{sink_id}".encode()
    records: list[oracle.ProgressRecord] = []
    errors: list[str] = []
    for _, offset, k, v, _ in read.messages:
        if k != key:
            continue
        r = oracle.decode_progress(offset, v)
        if isinstance(r, str):
            errors.append(r)
        else:
            records.append(r)
    return records, errors


def _decode(read: TopicRead) -> tuple[list[oracle.SinkMessage], list[str]]:
    messages: list[oracle.SinkMessage] = []
    errors: list[str] = []
    for p, offset, k, v, headers in read.messages:
        m = oracle.decode_message(p, offset, k, v, headers)
        if isinstance(m, str):
            errors.append(m)
        else:
            messages.append(m)
    return messages, errors


def _latest(records: list[oracle.ProgressRecord]) -> oracle.ProgressRecord | None:
    return max(records, key=lambda r: r.offset) if records else None


def _model(db: sqlite3.Connection, gen: int) -> dict[int, oracle.ModelRow]:
    return {
        int(i): oracle.ModelRow(
            int(i), bool(p), int(grp), int(val), str(tok), bool(u or b), int(seq)
        )
        for i, p, grp, val, tok, u, b, seq in db.execute(
            "SELECT id, present, grp, val, tok, uncertain, busy, seq FROM rows"
            " WHERE gen = ?",
            (gen,),
        )
    }


def _restart_signature(env: Environment, cluster_id: str) -> str:
    """Uid and restart count of every clusterd pod of the sink cluster."""
    pods = client.CoreV1Api().list_namespaced_pod(env.endpoints.namespace).items
    sig = []
    for pod in pods:
        assert pod.metadata is not None
        labels = pod.metadata.labels or {}
        if not any(
            k.endswith("/cluster-id") and v == cluster_id for k, v in labels.items()
        ):
            continue
        assert pod.status is not None
        restarts = sum(s.restart_count for s in pod.status.container_statuses or [])
        sig.append((pod.metadata.name, pod.metadata.uid, restarts))
    return json.dumps(sorted(sig))


@dataclass(frozen=True)
class Deployment:
    sink_pods: str | None
    active_generation: str | None
    generations: int | None
    """environmentd generations deployed; two during a rollout."""


def _deployment(conn: psycopg.Connection) -> Deployment:
    row = conn.execute(
        "SELECT id FROM mz_clusters WHERE name = %s", (SINK_CLUSTER,)
    ).fetchone()
    try:
        env = Environment()
        sig = _restart_signature(env, str(row[0])) if row is not None else None
        mz = env.materialize
        active = mz.status().get("activeGeneration")
        gens = len(mz.environmentd_generations())
    except Exception as e:
        # Observation only: it feeds coverage anchors, never a safety check.
        log(f"cannot read the deployment: {e}")
        return Deployment(None, None, None)
    return Deployment(sig, str(active) if active is not None else None, gens)


def _observe(db: sqlite3.Connection, gen: int, now_ms: int, d: Deployment) -> None:
    with db:
        for kind, value in (
            ("sink_pods", d.sink_pods),
            ("active_generation", d.active_generation),
        ):
            if value is not None:
                db.execute(
                    "INSERT OR IGNORE INTO observations VALUES (?, ?, ?, ?)",
                    (gen, kind, value, now_ms),
                )


def _changed_before(db: sqlite3.Connection, gen: int, kind: str, frontier: int) -> bool:
    """A value of `kind` changed while the generation's sinks ran, before `frontier`.

    The second value ever seen was first seen after the change, so a
    frontier above that time was committed after the change, by a producer
    started after it.
    """
    rows = db.execute(
        "SELECT first_seen_ms FROM observations WHERE gen = ? AND kind = ?"
        " ORDER BY first_seen_ms LIMIT 2",
        (gen, kind),
    ).fetchall()
    return len(rows) == 2 and int(rows[1][0]) < frontier


def _check_statuses(conn: psycopg.Connection, ids: dict[str, str]) -> dict[str, str]:
    """Assert on the sinks' current and historical statuses. Returns the current ones."""
    by_id = {v: k for k, v in ids.items()}
    current = {
        str(i): (str(s), e)
        for i, s, e in conn.execute(
            "SELECT id, status, error FROM mz_internal.mz_sink_statuses"
            f" WHERE id IN ({id_list(by_id)})".encode()
        ).fetchall()
    }
    for i, (status, error) in current.items():
        if status == "stalled":
            always_or_unreachable(
                bool(error),
                "kafka sink: a stalled sink status carries an error",
                {"sink": by_id.get(i), "status": status},
            )
    history = conn.execute(
        "SELECT sink_id, status, error, replica_id FROM mz_internal.mz_sink_status_history"
        f" WHERE sink_id IN ({id_list(by_id)}) AND error IS NOT NULL".encode()
    ).fetchall()
    for i, status, error, replica in history:
        hits = [m for m in FORBIDDEN_SINK_ERRORS if m in str(error)]
        always_or_unreachable(
            not hits,
            "kafka sink: a sink never reports lost or corrupted resume state",
            {
                "sink": by_id.get(str(i)),
                "status": status,
                "replica": replica,
                "error": str(error)[:500],
            },
        )
    return {by_id[i]: s for i, (s, _) in current.items() if i in by_id}


@dataclass
class CheckResult:
    model_compared: bool = False
    statuses: dict[str, str] = field(default_factory=dict)


@dataclass
class SinkOutput:
    """One sink's committed output and progress as read by a check."""

    name: str
    envelope: str
    messages: list[oracle.SinkMessage]
    progress: list[oracle.ProgressRecord]
    progress_after: list[oracle.ProgressRecord]


def _read_sink(
    endpoints: Endpoints,
    name: str,
    topic: str,
    deadline: Deadline,
    details: dict[str, Any],
) -> tuple[list[oracle.SinkMessage], int] | None:
    """The sink's committed data messages, after asserting they decode.

    Also returns the number of aborted records seen when a `read_uncommitted`
    pass was drawn, else -1.
    """
    data = read_topic(endpoints.kafka_broker, topic, deadline)
    if data is None:
        return None
    messages, errors = _decode(data)
    always(
        not errors,
        "kafka sink: every committed data message decodes and carries one materialize-timestamp header",
        {**details, "sink": name, "errors": errors[:10], "error_count": len(errors)},
    )
    aborted = -1
    if rng.random() < ABORTED_READ_P:
        raw = read_topic(endpoints.kafka_broker, topic, deadline, committed=False)
        if raw is not None:
            committed = {(p, o) for p, o, *_ in data.messages}
            aborted = sum(
                1
                for p, o, *_ in raw.messages
                if (p, o) not in committed and o < data.highs.get(p, 0)
            )
    return messages, aborted


def _check_stream(out: SinkOutput, details: dict[str, Any]) -> None:
    """Assertions over a sink's committed output alone."""
    d = {**details, "sink": out.name}
    dups = oracle.duplicate_updates(out.messages)
    always(
        not dups,
        "kafka sink: no update (key, timestamp) is committed to the data topic twice",
        {**d, "duplicates": dups[:10], "duplicate_count": len(dups)},
    )
    regressions = oracle.timestamp_regressions(out.messages)
    always(
        not regressions,
        "kafka sink: committed data timestamps never decrease within a partition",
        {**d, "regressions": regressions[:10]},
    )
    progress_regressions = oracle.progress_regressions(out.progress_after)
    always(
        not progress_regressions,
        "kafka sink: committed progress frontiers and versions never regress",
        {**d, "regressions": progress_regressions[:10]},
    )
    latest = _latest(out.progress_after)
    max_ts = max((m.ts for m in out.messages), default=None)
    if max_ts is not None:
        always(
            latest is not None and oracle.frontier_le(max_ts + 1, latest.frontier),
            "kafka sink: every committed data message lies below the latest committed progress frontier",
            {
                **d,
                "max_ts": max_ts,
                "latest_frontier": latest.frontier if latest else None,
            },
        )
    if out.envelope == "debezium":
        breaks = oracle.debezium_chain_breaks(out.messages, ITEM_COLUMNS)
        always(
            not breaks,
            "kafka sink: each Debezium before image equals the key's previous after image",
            {**d, "breaks": breaks[:10], "break_count": len(breaks)},
        )


def _consolidated(out: SinkOutput, below: int | None) -> tuple[dict[int, tuple], list]:
    """The output below `below` (None: all of it) by key, and keys or rows
    that are inconsistent."""
    if out.envelope == "debezium":
        rows, negative = oracle.consolidate_debezium(out.messages, below, ITEM_COLUMNS)
        keyed, conflicts = oracle.keyed_rows(rows, 0)
        return keyed, [["negative", list(r)] for r in negative] + [
            ["conflict", k] for k in conflicts
        ]
    latest = oracle.consolidate_upsert(out.messages, below, GROUP_COLUMNS)
    mismatched = [repr(k) for k, r in latest.items() if k != (("grp", r[0]),)]
    return {r[0]: r for r in latest.values()}, [["key", k] for k in mismatched]


def _compare_view(
    conn: psycopg.Connection,
    out: SinkOutput,
    view: str,
    view_id: str,
    details: dict[str, Any],
) -> bool:
    """Compare the output below a committed frontier F with `view AS OF F - 1`.

    Returns whether a non-empty comparison matched.
    """
    since = frontiers(conn, [view_id]).get(view_id, (None, None))[0]
    if since is None:
        return False
    candidates = [
        r.frontier
        for r in out.progress
        if r.frontier is not None and r.frontier - 1 >= since + SINCE_MARGIN_MS
    ]
    if not candidates:
        log(f"{out.name}: no committed frontier above the since of {view}")
        return False
    f = rng.choice([max(candidates), rng.choice(candidates)])
    columns = ITEM_COLUMNS if out.envelope == "debezium" else GROUP_COLUMNS
    try:
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM {view} AS OF {f - 1}".encode()
        ).fetchall()
    except psycopg.Error as e:
        c = sql.classify_as_of_read(e)
        always(
            c.outcome is not sql.Outcome.VIOLATION,
            "kafka sink: reads of the sinked views return only classified errors",
            {**details, "view": view, "sqlstate": c.sqlstate, "template": c.template},
        )
        return False
    observed, bad = _consolidated(out, f)
    diff = oracle.bag_diff([tuple(r) for r in rows], observed.values())
    ok = not bad and diff["missing_count"] == 0 and diff["extra_count"] == 0
    d = {**details, "sink": out.name, "frontier": f, "since": since, **diff}
    if out.envelope == "debezium":
        always(
            ok,
            "kafka sink: consolidated Debezium output at a committed progress frontier equals the sinked view at that time",
            {**d, "inconsistent": bad[:10]},
        )
    else:
        always(
            ok,
            "kafka sink: latest upsert value per key at a committed progress frontier equals the sinked view at that time",
            {**d, "inconsistent": bad[:10]},
        )
    return ok and bool(rows)


def _compare_model(
    out: SinkOutput,
    frontier: int | None,
    expected: oracle.ModelExpectation,
    details: dict[str, Any],
) -> bool:
    observed, bad = _consolidated(out, frontier)
    d = {**details, "sink": out.name, "frontier": frontier}
    if out.envelope == "debezium":
        phantoms = sorted(k for k in observed if k not in expected.known_ids)
        mismatches = oracle.compare_keyed(expected.items, observed)
        ok = not bad and not phantoms and not mismatches
        always(
            ok,
            "kafka sink: the Debezium sink at a frontier past every acknowledged write equals the acknowledged-write model",
            {
                **d,
                "mismatches": mismatches,
                "phantom_ids": phantoms[:10],
                "inconsistent": bad[:10],
                "compared_ids": len(expected.items),
            },
        )
        return ok and any(v is not None for v in expected.items.values())
    mismatches = oracle.compare_keyed(expected.groups, observed)
    ok = not bad and not mismatches
    always(
        ok,
        "kafka sink: the upsert sink at a frontier past every acknowledged write equals the model for groups without uncertain writes",
        {
            **d,
            "mismatches": mismatches,
            "inconsistent": bad[:10],
            "compared_groups": len(expected.groups),
        },
    )
    return ok and any(v is not None for v in expected.groups.values())


def _wait_for_frontier(
    endpoints: Endpoints,
    sink_ids: dict[str, str],
    t0: int,
    wait: Deadline,
    deadline: Deadline,
) -> tuple[dict[str, list[oracle.ProgressRecord]], TopicRead] | None:
    """Progress records per sink, polled until every sink's latest frontier
    passes `t0` or `wait` expires, and the progress topic read they came from."""
    read: TopicRead | None = None
    while True:
        read = read_topic(endpoints.kafka_broker, PROGRESS_TOPIC, deadline, after=read)
        if read is None:
            return None
        out: dict[str, list[oracle.ProgressRecord]] = {}
        for name, sid in sink_ids.items():
            records, errors = _progress(read, sid)
            always(
                not errors,
                "kafka sink: every progress record decodes",
                {"sink": name, "errors": errors[:10]},
            )
            out[name] = records
        passed = all(
            (r := _latest(out[n])) is not None
            and oracle.frontier_le(t0 + 1, r.frontier)
            for n in sink_ids
        )
        if passed or wait.expired() or deadline.expired():
            return out, read
        time.sleep(2.0)


def _check(
    db: sqlite3.Connection,
    endpoints: Endpoints,
    conn: psycopg.Connection,
    g: Gen,
    deadline: Deadline,
    model_wait_s: float,
) -> CheckResult:
    result = CheckResult()
    names = [g.items, g.items_mv, g.agg_mv, g.dbz_sink, g.upsert_sink]
    ids = lookup_ids(conn, names)
    if len(ids) != len(names):
        log(f"gen {g.gen}: check skipped, catalog has only {sorted(ids)}")
        return result
    sinks = {g.dbz_sink: ids[g.dbz_sink], g.upsert_sink: ids[g.upsert_sink]}
    result.statuses = _check_statuses(conn, sinks)
    # The progress key names the sink's global id, not its catalog item id.
    global_ids = {
        str(i): str(gid)
        for i, gid in conn.execute(
            "SELECT id, global_id FROM mz_internal.mz_object_global_ids"
            f" WHERE id IN ({id_list(sinks.values())})".encode()
        ).fetchall()
    }
    if set(global_ids) != set(sinks.values()):
        log(f"gen {g.gen}: check skipped, global ids only for {sorted(global_ids)}")
        return result
    progress_keys = {name: global_ids[i] for name, i in sinks.items()}
    deployment = _deployment(conn)
    _observe(db, g.gen, mz_now_ms(conn), deployment)

    # The model snapshot precedes `t0`, and `t0` is a strict serializable read
    # of the table, so every write acknowledged before the snapshot has a
    # timestamp at or below `t0`.
    model_before = _model(db, g.gen)
    row = conn.execute(
        f"SELECT mz_now()::text, count(*) FROM {g.items}".encode()
    ).fetchone()
    assert row is not None
    t0 = int(row[0])
    details: dict[str, Any] = {"gen": g.gen, "t0": t0}

    waited = _wait_for_frontier(
        endpoints, progress_keys, t0, Deadline(model_wait_s), deadline
    )
    if waited is None:
        return result
    progress, progress_read = waited
    outputs: list[SinkOutput] = []
    aborted_seen = False
    for name, envelope, topic in (
        (g.dbz_sink, "debezium", g.dbz_topic),
        (g.upsert_sink, "upsert", g.upsert_topic),
    ):
        read = _read_sink(endpoints, name, topic, deadline, details)
        if read is None:
            return result
        messages, aborted = read
        aborted_seen |= aborted > 0
        outputs.append(SinkOutput(name, envelope, messages, progress[name], []))
    after = read_topic(
        endpoints.kafka_broker, PROGRESS_TOPIC, deadline, after=progress_read
    )
    if after is None:
        return result
    model_after = _model(db, g.gen)
    for out in outputs:
        out.progress_after = _progress(after, progress_keys[out.name])[0]
        _check_stream(out, details)

    sometimes(
        aborted_seen,
        "kafka sink: a check saw records of an aborted sink transaction",
        details,
    )

    matched = [
        _compare_view(conn, out, view, ids[view], details)
        for out, view in zip(outputs, (g.items_mv, g.agg_mv))
    ]
    any_matched = any(matched)
    sometimes(
        any_matched,
        "kafka sink: a non-empty sink output matched the sinked view at a progress frontier",
        details,
    )
    if any_matched:
        frontier = min(
            (
                r.frontier
                for out in outputs
                if (r := _latest(out.progress)) and r.frontier
            ),
            default=0,
        )
        sometimes(
            _changed_before(db, g.gen, "sink_pods", frontier),
            "kafka sink: a matching check covered output written across a sink cluster restart",
            details,
        )
        sometimes(
            _changed_before(db, g.gen, "active_generation", frontier),
            "kafka sink: a matching check covered output written across an environmentd generation change",
            details,
        )
        sometimes(
            (deployment.generations or 0) > 1,
            "kafka sink: a check matched while a second environmentd generation was deployed",
            details,
        )
        sometimes(
            any((m.value or {}).get("before") is not None for m in outputs[0].messages)
            and any(m.value is None for m in outputs[1].messages),
            "kafka sink: a matching check covered Debezium retractions and upsert tombstones",
            details,
        )

    expected = oracle.model_expectation(model_before, model_after, _groups(db))
    compared = []
    for out in outputs:
        latest = _latest(out.progress)
        if latest is None or not oracle.frontier_le(t0 + 1, latest.frontier):
            log(f"{out.name}: no committed frontier past t0 {t0} yet")
            continue
        compared.append(_compare_model(out, latest.frontier, expected, details))
    result.model_compared = len(compared) == len(outputs)
    sometimes(
        result.model_compared and any(compared),
        "kafka sink: the acknowledged-write model was compared non-empty at a frontier past every acknowledged write",
        details,
    )
    return result


def check_main() -> int:
    endpoints = Endpoints.from_env()
    db = open_state()
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    g = ensure_ready(db, endpoints, host, Deadline(LAZY_SETUP_BUDGET_S))
    if g is None:
        return 0
    try:
        conn = mz_connect(host)
    except (psycopg.Error, OSError) as e:
        log(f"no connection: {e}")
        return 0
    try:
        _check(db, endpoints, conn, g, Deadline(CHECK_BUDGET_S), MODEL_WAIT_S)
    except (psycopg.Error, OSError) as e:
        c = sql.classify(e)
        log(f"check abandoned: {c.outcome.value} {c.sqlstate} {c.template}")
    finally:
        conn.close()
    return 0


def eventually_main() -> int:
    """After faults stop, every sink runs and catches up with every
    acknowledged write, and its output equals the model."""
    endpoints = Endpoints.from_env()
    db = open_state()
    start = time.monotonic()
    wedge = Deadline(EVENTUALLY_WEDGE_S)
    try:
        host = mz_host()
    except Exception as e:
        log(f"no environmentd host: {e}")
        return 0
    g = _current(db)
    if not _ready_flags(db, g.gen)[1]:
        log(f"gen {g.gen} never finished setup; nothing to check")
        return 0
    result = CheckResult()
    while not wedge.expired():
        try:
            with mz_connect(host) as conn:
                result = _check(
                    db,
                    endpoints,
                    conn,
                    g,
                    Deadline(min(CHECK_BUDGET_S, max(wedge.remaining(), 1.0))),
                    min(60.0, max(wedge.remaining(), 1.0)),
                )
        except (psycopg.Error, OSError) as e:
            c = sql.classify(e)
            log(f"eventual check attempt failed: {c.outcome.value} {c.template}")
        running = bool(result.statuses) and all(
            s == "running" for s in result.statuses.values()
        )
        if result.model_compared and running:
            break
        time.sleep(EVENTUALLY_POLL_S)
    elapsed = time.monotonic() - start
    details = {
        "gen": g.gen,
        "elapsed_s": round(elapsed, 1),
        "statuses": result.statuses,
        "model_compared": result.model_compared,
    }
    always(
        result.model_compared,
        "kafka sink: after faults stop every sink passes every acknowledged write and is compared within the wedge bound",
        details,
    )
    always(
        bool(result.statuses) and all(s == "running" for s in result.statuses.values()),
        "kafka sink: every sink reports running after faults stop",
        details,
    )
    sometimes(
        result.model_compared and elapsed <= EVENTUALLY_TIGHT_S,
        "kafka sink: after faults stop every sink caught up within the tight bound",
        details,
    )
    return 0
