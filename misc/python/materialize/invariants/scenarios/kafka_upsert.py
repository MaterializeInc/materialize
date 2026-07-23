# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Keyed events with per-key versions through an ENVELOPE UPSERT source.

Each key is owned by exactly one producer worker, which writes strictly
increasing versions and occasional tombstones. That makes the upsert state
machine checkable without any transaction-atomicity assumption: at every
timestamp the ingested state must hold at most one row per key, per-key
versions must never move backwards, and after quiescing each key must show
the effect of the last message that landed. With an idempotent producer the
per-partition order is preserved, so "the last message that landed" is the
last acknowledged write or one of the unacknowledged writes queued after it.
"""

import json
import random
from typing import Any

from materialize.invariants.checkers import PeekChecker, SubscribeChecker
from materialize.invariants.framework import (
    CONVERGE_TIMEOUT,
    SEED_RANGE,
    Action,
    Checker,
    InvariantViolation,
    Outcome,
    Scenario,
    ScenarioContext,
    TransientError,
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient
from materialize.invariants.scenarios.kafka_ledger import make_producer

TOPIC = "invariants-upsert"
PARTITIONS = 3
TOMBSTONE_PROBABILITY = 0.15
SENTINEL_KEYS = ["s-0", "s-1", "s-2"]


class UpsertEvent(Action):
    """Write the next version (or a tombstone) for one owned key.

    Every write is logged as {effect, outcome} in the scenario's per-key
    log. The delivery callback upgrades the outcome to COMMITTED, so the
    final check can compute the admissible final values per key.
    """

    name = "upsert"

    def __init__(
        self, rng: random.Random, worker: int, scenario: "KafkaUpsert", producer
    ) -> None:
        super().__init__(rng)
        self.worker = worker
        self.scenario = scenario
        self.producer = producer
        self.versions = [0] * scenario.keys_per_worker

    def run(self) -> Outcome | None:
        k = self.rng.randrange(self.scenario.keys_per_worker)
        key = f"{self.worker}-{k}"
        self.versions[k] += 1
        version = self.versions[k]
        tombstone = self.rng.random() < TOMBSTONE_PROBABILITY
        entry: dict[str, Any] = {
            "effect": None if tombstone else version,
            "outcome": Outcome.UNKNOWN,
        }
        self.scenario.key_log[(self.worker, k)].append(entry)

        def on_delivery(err, msg, entry=entry) -> None:
            if err is None:
                entry["outcome"] = Outcome.COMMITTED

        value = None if tombstone else json.dumps({"ver": version}).encode()
        try:
            self.producer.produce(
                TOPIC, value=value, key=key.encode(), on_delivery=on_delivery
            )
        except BufferError as e:
            entry["outcome"] = Outcome.FAILED
            self.producer.poll(1)
            raise TransientError(f"producer queue full: {e}") from e
        self.producer.poll(0)
        return None

    def close(self) -> None:
        self.producer.flush(60)


class UpsertUniquePeek(PeekChecker):
    """The core upsert invariant: never more than one row per key."""

    def __init__(self, rng, ctx, scenario: "KafkaUpsert") -> None:
        super().__init__(rng, ctx, "unique-peek", ["quickstart", "compute"])
        self.scenario = scenario

    def check_once(self) -> None:
        duplicated = self.peek("SELECT key FROM state GROUP BY key HAVING count(*) > 1")
        if duplicated:
            raise InvariantViolation(
                f"upsert produced multiple rows per key: {duplicated[:20]}"
            )
        rows = self.peek("SELECT count(*) FROM state")
        limit = self.ctx.complexity.workers * self.scenario.keys_per_worker + len(
            SENTINEL_KEYS
        )
        if int(rows[0][0]) > limit:
            raise InvariantViolation(
                f"upsert state holds {rows[0][0]} keys, only {limit} exist"
            )
        self.validations += 1


class KeyStateSubscribe(SubscribeChecker):
    """Per-key versions must never move backwards within a session.

    Uses WITHIN TIMESTAMP ORDER BY, so the base checker additionally
    asserts the documented in-timestamp arrival order by key.
    """

    def __init__(self, rng, ctx, scenario: "KafkaUpsert") -> None:
        super().__init__(rng, ctx, "state-subscribe", "SELECT key, ver FROM key_state")
        self.order_clause = "WITHIN TIMESTAMP ORDER BY key"
        self.order_index = 0
        self.scenario = scenario
        self.last: dict[str, int] = {}

    def check_once(self) -> None:
        if not self._active:
            # Fresh session, no cross-session continuity is guaranteed.
            self.last = {}
        super().check_once()

    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        seen: dict[str, int] = {}
        for (key, version), multiplicity in state.items():
            if multiplicity != 1 or key in seen:
                raise InvariantViolation(
                    f"upsert state at {ts}: multiple rows for key {key}: {state}"
                )
            seen[str(key)] = int(version)
        for key, version in seen.items():
            if key in self.last and version < self.last[key]:
                raise InvariantViolation(
                    f"key {key} version moved backwards at {ts}:"
                    f" {version} < {self.last[key]}"
                )
            self.last[key] = version


class UpsertEnvelopeSubscribe(Checker):
    """SUBSCRIBE .. ENVELOPE UPSERT emits a sound per-key state machine.

    The documented envelope output: `mz_state` is `upsert` with the new
    value, `delete` with nulled value columns, or `key_violation` if the
    query held more than one live row per key (which key_state never
    legitimately does). Per session: timestamps never decrease, a delete
    only follows a live key, and versions never move backwards (the single
    owner only writes increasing versions).
    """

    name = "envelope-subscribe"
    pause = (0.0, 0.2)

    def __init__(self, rng: random.Random, ctx: ScenarioContext) -> None:
        super().__init__(rng)
        self.ctx = ctx
        self.client = MzClient(ctx, self.name)
        self._cursor = "c_envelope"
        self._active = False
        self.last_ts: int | None = None
        self.live: set[str] = set()
        self.high_water: dict[str, int] = {}

    def check_once(self) -> None:
        try:
            if not self._active:
                # Fresh session, no cross-session continuity is guaranteed.
                self.last_ts = None
                self.live = set()
                self.high_water = {}
                self.client.query("BEGIN")
                self.client.query(
                    f"DECLARE {self._cursor} CURSOR FOR"
                    " SUBSCRIBE (SELECT key, ver FROM key_state)"
                    " ENVELOPE UPSERT (KEY (key)) WITH (PROGRESS)"
                )
                self._active = True
            rows = self.client.query(
                f"FETCH ALL {self._cursor} WITH (timeout = '1s')",
                timeout=max(30.0, self.ctx.complexity.query_timeout),
            )
        except TransientError:
            self._active = False
            raise
        validated = False
        for row in rows:
            ts, progressed, state = int(row[0]), bool(row[1]), row[2]
            if self.last_ts is not None and ts < self.last_ts:
                raise InvariantViolation(
                    f"{self.name}: timestamp went backwards: {ts} < {self.last_ts}"
                )
            self.last_ts = ts
            if progressed:
                continue
            key, ver = row[3], row[4]
            if state == "upsert":
                if ver is None:
                    raise InvariantViolation(
                        f"{self.name}: upsert for {key} at {ts} without a value"
                    )
                version = int(ver)
                if version < self.high_water.get(str(key), -1):
                    raise InvariantViolation(
                        f"{self.name}: key {key} version moved backwards at"
                        f" {ts}: {version} < {self.high_water[str(key)]}"
                    )
                self.high_water[str(key)] = version
                self.live.add(str(key))
                validated = True
            elif state == "delete":
                if str(key) not in self.live:
                    raise InvariantViolation(
                        f"{self.name}: delete for key {key} at {ts} that was"
                        " never live in this session"
                    )
                self.live.discard(str(key))
                validated = True
            else:
                # key_violation means the subscribed query held more than
                # one live row per key, which key_state never does.
                raise InvariantViolation(
                    f"{self.name}: mz_state {state!r} for key {key} at {ts}"
                )
        if validated:
            self.validations += 1

    def close(self) -> None:
        self.client.reset()


class KafkaUpsert(Scenario):
    name = "kafka-upsert"
    services = ["kafka"]
    legs = [
        "metadata",
        "blob",
        "clusterd-storage",
        "clusterd-compute",
        "clusterd-compute2",
        "kafka-source",
    ]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.keys_per_worker = ctx.complexity.accounts
        # (worker, key index) -> ordered write log, appended by the owning
        # worker thread only and read after all workers stopped.
        self.key_log: dict[tuple[int, int], list[dict[str, Any]]] = {
            (worker, k): []
            for worker in range(ctx.complexity.workers)
            for k in range(self.keys_per_worker)
        }
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
            "CREATE SOURCE upsert_source IN CLUSTER storage"
            f" FROM KAFKA CONNECTION kafka_src_conn (TOPIC '{TOPIC}')",
            f'CREATE TABLE state FROM SOURCE upsert_source (REFERENCE "{TOPIC}")'
            " KEY FORMAT TEXT VALUE FORMAT JSON ENVELOPE UPSERT",
            "CREATE MATERIALIZED VIEW key_state IN CLUSTER compute AS"
            " SELECT key, (data->>'ver')::bigint AS ver FROM state",
            "CREATE INDEX key_state_idx IN CLUSTER compute ON key_state (key)",
        ]:
            client.query(sql, timeout=120)
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        producer = make_producer(self.bootstrap)
        return WorkerBundle(
            actions=[UpsertEvent(rng, index, self, producer)], weights=[1]
        )

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(3)]
        return [
            UpsertUniquePeek(rngs[0], self.ctx, self),
            KeyStateSubscribe(rngs[1], self.ctx, self),
            UpsertEnvelopeSubscribe(rngs[2], self.ctx),
        ]

    def converge(self) -> None:
        # Workers flushed their producers on close, so the topic's regular
        # contents are frozen. One committed sentinel per partition proves
        # everything before them was ingested.
        producer = make_producer(self.bootstrap)
        for partition, key in enumerate(SENTINEL_KEYS):
            producer.produce(
                TOPIC,
                value=json.dumps({"ver": 0}).encode(),
                key=key.encode(),
                partition=partition,
            )
        if producer.flush(60) != 0:
            raise InvariantViolation("liveness: sentinel produce did not flush")
        client = MzClient(self.ctx, "converge")
        sentinels = ", ".join(f"'{key}'" for key in SENTINEL_KEYS)

        def sentinels_visible() -> bool:
            client.query("SET cluster = quickstart")
            rows = client.query(
                f"SELECT count(*) FROM state WHERE key IN ({sentinels})"
            )
            return int(rows[0][0]) == len(SENTINEL_KEYS)

        wait_until(sentinels_visible, CONVERGE_TIMEOUT, "kafka source catching up")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        client.query("SET cluster = quickstart")
        actual = {
            str(row[0]): int(row[1]) if row[1] is not None else None
            for row in client.query(
                "SELECT key, (data->>'ver')::bigint FROM state"
                " WHERE key NOT LIKE 's-%'"
            )
        }
        for (worker, k), log in self.key_log.items():
            key = f"{worker}-{k}"
            # The final value is the effect of the last message that landed:
            # the last acknowledged write, or any unacknowledged write
            # queued after it (order is preserved per partition). FAILED
            # writes were never sent.
            admissible: set[int | None] = set()
            last_committed = None
            for index, entry in enumerate(log):
                if entry["outcome"] == Outcome.COMMITTED:
                    last_committed = index
            if last_committed is None:
                admissible.add(None)
                tail = log
            else:
                admissible.add(log[last_committed]["effect"])
                tail = log[last_committed + 1 :]
            for entry in tail:
                if entry["outcome"] == Outcome.UNKNOWN:
                    admissible.add(entry["effect"])
            got = actual.get(key)
            if got not in admissible:
                raise InvariantViolation(
                    f"key {key}: final state {got} not admissible"
                    f" ({sorted(str(a) for a in admissible)})"
                )
        phantom = set(actual) - {f"{worker}-{k}" for worker, k in self.key_log.keys()}
        if phantom:
            raise InvariantViolation(f"keys that were never written: {phantom}")
        client.reset()

    def diagnostics(self) -> None:
        client = MzClient(self.ctx, "post-heal")
        try:
            client.query("SET cluster = quickstart")
            rows = client.query("SELECT count(*) FROM state", timeout=60)
            self.ctx.log.log("diag", f"post-heal: upsert state keys={rows[0][0]}")
        except Exception as e:
            self.ctx.log.log("diag", f"post-heal state count failed: {e}")
        client.reset()
