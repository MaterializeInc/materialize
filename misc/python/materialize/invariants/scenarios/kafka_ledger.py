# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Uniquely-numbered events produced to Kafka and ingested by a source.

Kafka sources make no transaction-atomicity promise, so instead of a
conservation invariant this scenario uses set semantics: every acknowledged
event must eventually be ingested exactly once, no event may appear that was
never attempted, and per-worker counts and sequence numbers must never move
backwards. Producers are idempotent so that librdkafka's internal retries
cannot legitimately duplicate an event, which makes "exactly once" checkable.
"""

import json
import random

from materialize.invariants.checkers import PeekChecker, SubscribeChecker
from materialize.invariants.framework import (
    CONVERGE_TIMEOUT,
    SEED_RANGE,
    Action,
    Checker,
    InvariantViolation,
    OpLog,
    Outcome,
    Scenario,
    ScenarioContext,
    TransientError,
    Watermark,
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient

TOPIC = "invariants-ledger"
PARTITIONS = 3
# Bounds how long an unresolved produce can straddle the converge phase.
MESSAGE_TIMEOUT_MS = 30_000
SENTINEL_WORKER = -1


def make_producer(bootstrap: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "message.timeout.ms": MESSAGE_TIMEOUT_MS,
        }
    )


class ProduceEvent(Action):
    """Produce one uniquely-numbered event, resolving its outcome async.

    The op is registered as UNKNOWN before the send. The delivery callback
    upgrades it to COMMITTED on a broker ack. A delivery error leaves it
    UNKNOWN: an earlier attempt may still have been appended.
    """

    name = "produce"

    def __init__(self, rng: random.Random, worker: int, oplog: OpLog, producer) -> None:
        super().__init__(rng)
        self.worker = worker
        self.oplog = oplog
        self.producer = producer
        self.seq = 0

    def run(self) -> Outcome | None:
        self.seq += 1
        seq = self.seq
        worker = self.worker
        payload = json.dumps(
            {"worker": worker, "seq": seq, "amount": self.rng.randint(1, 100)}
        )
        self.oplog.record(worker, seq, Outcome.UNKNOWN)

        def on_delivery(err, msg, worker=worker, seq=seq) -> None:
            if err is None:
                self.oplog.record(worker, seq, Outcome.COMMITTED)

        try:
            self.producer.produce(
                TOPIC,
                value=payload.encode(),
                key=str(worker).encode(),
                on_delivery=on_delivery,
            )
        except BufferError as e:
            # The local queue is full and nothing was sent.
            self.oplog.record(worker, seq, Outcome.FAILED)
            self.producer.poll(1)
            raise TransientError(f"producer queue full: {e}") from e
        # Serve delivery callbacks for previously sent events.
        self.producer.poll(0)
        return None

    def close(self) -> None:
        self.producer.flush(60)


class EventCountPeek(PeekChecker):
    """Event count: monotonic and never above what was attempted."""

    def __init__(self, rng, ctx, scenario: "KafkaLedger") -> None:
        super().__init__(rng, ctx, "events-peek", ["quickstart", "compute"])
        self.scenario = scenario

    def check_once(self) -> None:
        watermark = self.scenario.event_rows.get()
        rows = self.peek(
            "SELECT count(*) FROM events"
            f" WHERE (data->>'worker')::int <> {SENTINEL_WORKER}"
        )
        high = self.scenario.oplog.attempted_count()
        count = int(rows[0][0])
        if count < watermark:
            raise InvariantViolation(
                f"event count went backwards: {count} < watermark {watermark}"
            )
        if count > high:
            raise InvariantViolation(
                f"event count {count} exceeds attempted produces {high}:"
                " events were duplicated"
            )
        self.scenario.event_rows.advance(count)
        self.validations += 1


class WorkerStatsSubscribe(SubscribeChecker):
    """Per-worker count and max seq must never move backwards in-session."""

    def __init__(self, rng, ctx, scenario: "KafkaLedger") -> None:
        super().__init__(
            rng,
            ctx,
            "stats-subscribe",
            "SELECT worker, cnt, max_seq FROM worker_stats",
        )
        self.scenario = scenario
        self.last: dict[int, tuple[int, int]] = {}

    def check_once(self) -> None:
        if not self._active:
            # Fresh session, no cross-session continuity is guaranteed.
            self.last = {}
        super().check_once()

    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        seen: dict[int, tuple[int, int]] = {}
        for (worker, cnt, max_seq), multiplicity in state.items():
            if multiplicity != 1 or int(worker) in seen:
                raise InvariantViolation(
                    f"worker_stats at {ts}: duplicate group for worker {worker}:"
                    f" {state}"
                )
            seen[int(worker)] = (int(cnt), int(max_seq))
        for worker, (cnt, max_seq) in seen.items():
            if worker in self.last:
                last_cnt, last_max = self.last[worker]
                if cnt < last_cnt or max_seq < last_max:
                    raise InvariantViolation(
                        f"worker {worker} stats moved backwards at {ts}:"
                        f" ({cnt}, {max_seq}) < ({last_cnt}, {last_max})"
                    )
            self.last[worker] = (cnt, max_seq)


class KafkaLedger(Scenario):
    name = "kafka-ledger"
    services = ["kafka"]
    legs = ["metadata", "blob", "clusterd-storage", "clusterd-compute", "kafka-source"]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.oplog = OpLog()
        self.event_rows = Watermark()
        assert ctx.endpoints.kafka_bootstrap is not None
        self.bootstrap = ctx.endpoints.kafka_bootstrap

    def setup(self) -> None:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka.cimpl import NewTopic

        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        futures = admin.create_topics(
            [NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)]
        )
        for future in futures.values():
            future.result(timeout=60)
        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE CONNECTION kafka_src_conn TO KAFKA"
            " (BROKER 'toxiproxy:9092', SECURITY PROTOCOL PLAINTEXT)",
            "CREATE SOURCE ledger_source IN CLUSTER storage"
            f" FROM KAFKA CONNECTION kafka_src_conn (TOPIC '{TOPIC}')",
            f'CREATE TABLE events FROM SOURCE ledger_source (REFERENCE "{TOPIC}")'
            " FORMAT JSON ENVELOPE NONE",
            "CREATE MATERIALIZED VIEW worker_stats IN CLUSTER compute AS"
            " SELECT (data->>'worker')::int AS worker, count(*) AS cnt,"
            " max((data->>'seq')::bigint) AS max_seq FROM events"
            f" WHERE (data->>'worker')::int <> {SENTINEL_WORKER} GROUP BY 1",
            "CREATE INDEX worker_stats_idx IN CLUSTER compute"
            " ON worker_stats (worker)",
        ]:
            client.query(sql, timeout=120)
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        producer = make_producer(self.bootstrap)
        return WorkerBundle(
            actions=[ProduceEvent(rng, index, self.oplog, producer)], weights=[1]
        )

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(2)]
        return [
            EventCountPeek(rngs[0], self.ctx, self),
            WorkerStatsSubscribe(rngs[1], self.ctx, self),
        ]

    def converge(self) -> None:
        # Workers already flushed their producers on close, so the topic's
        # regular contents are frozen. One sentinel per partition proves MZ
        # ingested everything before them.
        producer = make_producer(self.bootstrap)
        for partition in range(PARTITIONS):
            producer.produce(
                TOPIC,
                value=json.dumps(
                    {"worker": SENTINEL_WORKER, "seq": partition}
                ).encode(),
                partition=partition,
            )
        if producer.flush(60) != 0:
            raise InvariantViolation("liveness: sentinel produce did not flush")
        client = MzClient(self.ctx, "converge")

        def sentinels_visible() -> bool:
            client.query("SET cluster = quickstart")
            rows = client.query(
                "SELECT count(*) FROM events"
                f" WHERE (data->>'worker')::int = {SENTINEL_WORKER}"
            )
            return int(rows[0][0]) == PARTITIONS

        wait_until(sentinels_visible, CONVERGE_TIMEOUT, "kafka source catching up")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        client.query("SET cluster = quickstart")
        for worker in range(self.ctx.complexity.workers):
            rows = client.query(
                "SELECT (data->>'seq')::bigint, count(*) FROM events"
                f" WHERE (data->>'worker')::int = {worker} GROUP BY 1"
            )
            duplicated = [(int(r[0]), int(r[1])) for r in rows if int(r[1]) != 1]
            if duplicated:
                raise InvariantViolation(
                    f"worker {worker}: events ingested more than once:"
                    f" {duplicated[:20]}"
                )
            present = {int(r[0]) for r in rows}
            committed = self.oplog.seqs(worker, Outcome.COMMITTED)
            unknown = self.oplog.seqs(worker, Outcome.UNKNOWN)
            missing = committed - present
            if missing:
                raise InvariantViolation(
                    f"worker {worker}: acknowledged events lost: {sorted(missing)[:20]}"
                )
            phantom = present - committed - unknown
            if phantom:
                raise InvariantViolation(
                    f"worker {worker}: events present that were never attempted:"
                    f" {sorted(phantom)[:20]}"
                )
        client.reset()

    def diagnostics(self) -> None:
        client = MzClient(self.ctx, "post-heal")
        try:
            client.query("SET cluster = quickstart")
            rows = client.query("SELECT count(*) FROM events", timeout=60)
            counts = self.oplog.counts()
            self.ctx.log.log("diag", f"post-heal: events={rows[0][0]} oplog={counts}")
        except Exception as e:
            self.ctx.log.log("diag", f"post-heal event count failed: {e}")
        client.reset()
