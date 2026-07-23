# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bank balances exported through Kafka sinks, verified by consumers.

Ledger transfers (as in table-bank) feed a per-account balances MV (only
nonzero balances, so account rows also get deleted and re-created) that two
Kafka sinks export: ENVELOPE DEBEZIUM into a single-partition topic, and
ENVELOPE UPSERT into a two-partition topic routed by PARTITION BY.
read_committed consumers reconstruct the balances. Soundness of the checks
relies on documented sink behavior: every message carries a
materialize-timestamp header, updates for one timestamp never straddle two
Kafka transactions, and within one partition timestamps are non-decreasing,
so a header change proves the previous timestamp complete.

Invariants: at every timestamp boundary the reconstructed DEBEZIUM balances
sum to zero (transfers pass through the sink atomically), each message's
`before` matches the reconstructed state (the exactly-once check: a
duplicated or lost message breaks the chain), the upsert sink routes every
key to its stable PARTITION BY partition, and after quiescing both
reconstructed states equal the balances MV. A churn action additionally
flips the DEBEZIUM sink between two identical relations via ALTER SINK ..
SET FROM: the documented cutover has no double emission and no gap, so the
before/after chain must survive it unbroken.
"""

import json
import random
import time

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
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient
from materialize.invariants.scenarios.table_bank import LedgerTransfer

TOPIC = "invariants-balances"
UPSERT_TOPIC = "invariants-balances-upsert"
UPSERT_PARTITIONS = 2


class SinkConsumerChecker(Checker):
    """Tails the sink topic and replays it against a reconstructed state."""

    name = "sink-consumer"
    pause = (0.0, 0.2)

    def __init__(
        self, rng: random.Random, ctx: ScenarioContext, bootstrap: str
    ) -> None:
        super().__init__(rng)
        self.ctx = ctx
        self.bootstrap = bootstrap
        self.consumer = None
        self.state: dict[int, int] = {}
        self.current_ts: int | None = None

    def open(self) -> None:
        from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

        self.close()
        # Offsets are never committed and assignment is manual, so every
        # (re)open deterministically replays the topic from the beginning.
        self.state = {}
        self.current_ts = None
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"invariants-{self.ctx.seed}",
                "enable.auto.commit": False,
                # The sink writes transactionally. Aborted transactions from
                # sink restarts must stay invisible.
                "isolation.level": "read_committed",
            }
        )
        self.consumer.assign([TopicPartition(TOPIC, 0, OFFSET_BEGINNING)])

    def check_once(self) -> None:
        self.drain(max_messages=1000)

    def drain(self, max_messages: int = 100_000) -> None:
        assert self.consumer is not None
        for _ in range(max_messages):
            message = self.consumer.poll(0.1)
            if message is None:
                return
            if message.error() is not None:
                raise TransientError(f"consumer error: {message.error()}")
            self._handle(message)

    def _handle(self, message) -> None:
        headers = dict(message.headers() or [])
        ts = int(headers["materialize-timestamp"].decode())
        if self.current_ts is None:
            self.current_ts = ts
        elif ts < self.current_ts:
            raise InvariantViolation(
                f"sink timestamps went backwards: {ts} < {self.current_ts}"
            )
        elif ts > self.current_ts:
            # The previous timestamp is complete: single partition, and one
            # timestamp never straddles two sink transactions.
            self.validate_boundary()
            self.current_ts = ts
        raw_value = message.value()
        if raw_value is None:
            # Compaction tombstone, carries no state.
            return
        account = int(json.loads(message.key())["account"])
        value = json.loads(raw_value)
        before, after = value["before"], value["after"]
        expected_before = self.state.get(account)
        actual_before = int(before["balance"]) if before is not None else None
        if actual_before != expected_before:
            raise InvariantViolation(
                f"sink message chain broken for account {account} at"
                f" {self.current_ts}: before={actual_before},"
                f" reconstructed={expected_before} (lost or duplicated message)"
            )
        if after is None:
            self.state.pop(account, None)
        else:
            self.state[account] = int(after["balance"])

    def validate_boundary(self) -> None:
        self.validations += 1
        balance_sum = sum(self.state.values())
        if balance_sum != 0:
            raise InvariantViolation(
                f"balances through the sink sum to {balance_sum} != 0 at"
                f" timestamp {self.current_ts}: {self.state}"
            )

    def close(self) -> None:
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.consumer = None


class UpsertSinkChecker(Checker):
    """Tails the ENVELOPE UPSERT sink topic across its two partitions.

    Per documented upsert-sink semantics: each message carries the latest
    value for its key (or a null-value tombstone on delete), so
    last-write-wins reconstruction per key yields the sinked relation. Per
    partition the materialize-timestamp headers never decrease, and with
    PARTITION BY every key maps to one stable partition, verified both as
    constancy while tailing and against Materialize's own hash in the final
    check.
    """

    name = "upsert-sink-consumer"
    pause = (0.0, 0.2)

    def __init__(
        self, rng: random.Random, ctx: ScenarioContext, bootstrap: str
    ) -> None:
        super().__init__(rng)
        self.ctx = ctx
        self.bootstrap = bootstrap
        self.consumer = None
        self.state: dict[int, int] = {}
        self.partition_of: dict[int, int] = {}
        self.partition_ts: dict[int, int] = {}

    def open(self) -> None:
        from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

        self.close()
        self.state = {}
        self.partition_of = {}
        self.partition_ts = {}
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"invariants-upsert-{self.ctx.seed}",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )
        self.consumer.assign(
            [
                TopicPartition(UPSERT_TOPIC, partition, OFFSET_BEGINNING)
                for partition in range(UPSERT_PARTITIONS)
            ]
        )

    def check_once(self) -> None:
        self.drain(max_messages=1000)

    def drain(self, max_messages: int = 100_000) -> None:
        assert self.consumer is not None
        for _ in range(max_messages):
            message = self.consumer.poll(0.1)
            if message is None:
                return
            if message.error() is not None:
                raise TransientError(f"consumer error: {message.error()}")
            self._handle(message)
            self.validations += 1

    def _handle(self, message) -> None:
        headers = dict(message.headers() or [])
        ts = int(headers["materialize-timestamp"].decode())
        partition = message.partition()
        if ts < self.partition_ts.get(partition, 0):
            raise InvariantViolation(
                f"upsert sink timestamps went backwards on partition"
                f" {partition}: {ts} < {self.partition_ts[partition]}"
            )
        self.partition_ts[partition] = ts
        account = int(json.loads(message.key())["account"])
        known = self.partition_of.setdefault(account, partition)
        if known != partition:
            raise InvariantViolation(
                f"PARTITION BY routed account {account} to partition"
                f" {partition} after {known}"
            )
        raw_value = message.value()
        if raw_value is None:
            self.state.pop(account, None)
        else:
            self.state[account] = int(json.loads(raw_value)["balance"])

    def close(self) -> None:
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception:
                pass
            self.consumer = None


class AlterSinkFlip(Action):
    """Flip the DEBEZIUM sink between two identical relations.

    ALTER SINK .. SET FROM cuts over at a timestamp with everything before
    it from the old relation and everything after from the new one. The
    relations are identical, so the consumer's before/after chain must
    survive every cutover unbroken: a re-emission or gap breaks it. The
    statement legitimately times out while the sink is unhealthy (UNKNOWN).
    """

    name = "alter-sink"

    def __init__(self, rng: random.Random, client: MzClient) -> None:
        super().__init__(rng)
        self.client = client
        self.next_at = time.monotonic() + 15.0
        self.flips = 0

    def run(self) -> Outcome | None:
        now = time.monotonic()
        if now < self.next_at:
            return None
        self.next_at = now + self.rng.uniform(20.0, 40.0)
        self.flips += 1
        target = "balances_alt" if self.flips % 2 else "balances"
        return self.client.write(f"ALTER SINK bank_sink SET FROM {target}")

    def close(self) -> None:
        self.client.reset()


class SinkRoundtrip(Scenario):
    name = "sink-roundtrip"
    services = ["kafka"]
    legs = [
        "metadata",
        "blob",
        "clusterd-compute",
        "clusterd-compute2",
        "clusterd-storage",
        "kafka-sink",
    ]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.accounts = ctx.complexity.accounts
        self.oplog = OpLog()
        self.sink_checker: SinkConsumerChecker | None = None
        self.upsert_checker: UpsertSinkChecker | None = None
        assert ctx.endpoints.kafka_bootstrap is not None
        self.bootstrap = ctx.endpoints.kafka_bootstrap

    # HAVING drops zero balances, so accounts also leave the relation and
    # both sinks exercise their delete paths (Debezium after: null, upsert
    # tombstones). Dropping zero terms does not change the conserved sum.
    BALANCES_DEF = (
        "SELECT account, sum(amount) AS balance FROM ledger"
        " GROUP BY account HAVING sum(amount) <> 0"
    )

    def setup(self) -> None:
        from confluent_kafka.admin import AdminClient

        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE ledger (worker int, seq bigint, account int,"
            " amount bigint, at timestamptz)",
            f"CREATE MATERIALIZED VIEW balances IN CLUSTER compute AS"
            f" {self.BALANCES_DEF}",
            # The identical relation the ALTER SINK churn cuts over to.
            f"CREATE MATERIALIZED VIEW balances_alt IN CLUSTER compute AS"
            f" {self.BALANCES_DEF}",
            "CREATE CONNECTION kafka_sink_conn TO KAFKA"
            " (BROKER 'toxiproxy:9192', SECURITY PROTOCOL PLAINTEXT)",
            "CREATE SINK bank_sink IN CLUSTER storage FROM balances"
            " INTO KAFKA CONNECTION kafka_sink_conn"
            f" (TOPIC '{TOPIC}', TOPIC PARTITION COUNT 1, TOPIC REPLICATION FACTOR 1)"
            " KEY (account) NOT ENFORCED FORMAT JSON ENVELOPE DEBEZIUM",
            # GROUP BY makes (account) a provable unique key, so no NOT
            # ENFORCED here: this also covers the documented key check.
            "CREATE SINK bank_sink_upsert IN CLUSTER storage FROM balances"
            " INTO KAFKA CONNECTION kafka_sink_conn"
            f" (TOPIC '{UPSERT_TOPIC}',"
            f" TOPIC PARTITION COUNT {UPSERT_PARTITIONS},"
            " TOPIC REPLICATION FACTOR 1,"
            " PARTITION BY seahash(account::text))"
            " KEY (account) FORMAT JSON ENVELOPE UPSERT",
        ]:
            client.query(sql, timeout=120)
        client.reset()
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        wait_until(
            lambda: {TOPIC, UPSERT_TOPIC} <= set(admin.list_topics(timeout=10).topics),
            CONVERGE_TIMEOUT,
            "sink topic creation",
        )

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        client = MzClient(self.ctx, f"worker-{index}")
        actions: list[Action] = [
            LedgerTransfer(rng, index, self.accounts, client, self.oplog)
        ]
        weights = [10]
        if index == 0:
            actions.append(AlterSinkFlip(rng, client))
            weights.append(1)
        return WorkerBundle(actions=actions, weights=weights)

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(2)]
        self.sink_checker = SinkConsumerChecker(rngs[0], self.ctx, self.bootstrap)
        self.upsert_checker = UpsertSinkChecker(rngs[1], self.ctx, self.bootstrap)
        return [self.sink_checker, self.upsert_checker]

    def _mz_balances(self, client: MzClient) -> dict[int, int]:
        client.query("SET cluster = quickstart")
        return {
            int(row[0]): int(row[1])
            for row in client.query("SELECT account, balance FROM balances")
        }

    def converge(self) -> None:
        checker = self.sink_checker
        upsert = self.upsert_checker
        assert checker is not None and upsert is not None
        if checker.consumer is None:
            checker.open()
        if upsert.consumer is None:
            upsert.open()
        client = MzClient(self.ctx, "converge")

        def caught_up() -> bool:
            checker.drain()
            upsert.drain()
            balances = self._mz_balances(client)
            return checker.state == balances and upsert.state == balances

        wait_until(caught_up, CONVERGE_TIMEOUT, "sink output catching up")
        client.reset()

    def final_check(self) -> None:
        checker = self.sink_checker
        upsert = self.upsert_checker
        assert checker is not None and upsert is not None
        client = MzClient(self.ctx, "final-check")
        last: dict[str, dict[int, int]] = {}

        def settled() -> bool:
            checker.drain()
            upsert.drain()
            last["sink"] = dict(checker.state)
            last["upsert"] = dict(upsert.state)
            last["mz"] = self._mz_balances(client)
            return last["sink"] == last["mz"] and last["upsert"] == last["mz"]

        try:
            # Retried, not one-shot: an UNKNOWN write whose group commit was
            # still in flight during the last disruption can land after
            # converge, and the sink legitimately lags the MV by a moment. A
            # genuine divergence persists and still fails at the timeout.
            wait_until(settled, 60, "final sink/MV agreement")
        except InvariantViolation:
            raise InvariantViolation(
                f"sink state diverged from balances MV:"
                f" sink={last.get('sink')} upsert={last.get('upsert')}"
                f" mz={last.get('mz')}"
            ) from None
        if sum(checker.state.values()) != 0:
            raise InvariantViolation(
                f"final sink balances sum to {sum(checker.state.values())} != 0"
            )
        # PARTITION BY: every key's observed partition must equal
        # Materialize's own hash-mod assignment.
        expected_partition = {
            int(row[0]): int(row[1])
            for row in client.query(
                "SELECT account,"
                f" (seahash(account::text) % {UPSERT_PARTITIONS})::int"
                " FROM balances"
            )
        }
        for account, partition in upsert.partition_of.items():
            expected = expected_partition.get(account)
            if expected is not None and expected != partition:
                raise InvariantViolation(
                    f"PARTITION BY put account {account} on partition"
                    f" {partition}, seahash says {expected}"
                )
        client.query("SET cluster = quickstart")
        broken = client.query(
            "SELECT worker, seq, count(*), sum(amount) FROM ledger"
            " GROUP BY worker, seq HAVING count(*) <> 2 OR sum(amount) <> 0"
        )
        if broken:
            raise InvariantViolation(f"non-atomic ledger transfers: {broken[:20]}")
        for worker in range(self.ctx.complexity.workers):
            present = {
                int(row[0])
                for row in client.query(
                    f"SELECT seq FROM ledger WHERE worker = {worker} GROUP BY seq"
                )
            }
            committed = self.oplog.seqs(worker, Outcome.COMMITTED)
            unknown = self.oplog.seqs(worker, Outcome.UNKNOWN)
            missing = committed - present
            if missing:
                raise InvariantViolation(
                    f"worker {worker}: committed transfers lost: {sorted(missing)[:20]}"
                )
            phantom = present - committed - unknown
            if phantom:
                raise InvariantViolation(
                    f"worker {worker}: transfers present that never committed:"
                    f" {sorted(phantom)[:20]}"
                )
        client.reset()

    def diagnostics(self) -> None:
        checker = self.sink_checker
        client = MzClient(self.ctx, "post-heal")
        try:
            mz_balances = self._mz_balances(client)
            state = checker.state if checker else None
            verdict = "converged" if state == mz_balances else "still diverged"
            self.ctx.log.log(
                "diag",
                f"post-heal: sink boundaries={checker.validations if checker else 0},"
                f" state {verdict}",
            )
        except Exception as e:
            self.ctx.log.log("diag", f"post-heal comparison failed: {e}")
        client.reset()
