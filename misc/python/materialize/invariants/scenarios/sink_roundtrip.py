# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bank balances exported through a Kafka sink, verified by a consumer.

Ledger transfers (as in table-bank) feed a per-account balances MV that a
Kafka sink exports with ENVELOPE DEBEZIUM into a single-partition topic. A
read_committed consumer reconstructs the balances from the before/after
pairs. Soundness of the checks relies on documented sink behavior: every
message carries a materialize-timestamp header, updates for one timestamp
never straddle two Kafka transactions, and within one partition timestamps
are non-decreasing, so a header change proves the previous timestamp
complete.

Invariants: at every timestamp boundary the reconstructed balances sum to
zero (transfers pass through the sink atomically), each message's `before`
matches the reconstructed state (the exactly-once check: a duplicated or
lost message breaks the chain), and after quiescing the reconstructed state
equals the balances MV.
"""

import json
import random

from materialize.invariants.framework import (
    CONVERGE_TIMEOUT,
    SEED_RANGE,
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


class SinkRoundtrip(Scenario):
    name = "sink-roundtrip"
    services = ["kafka"]
    legs = ["metadata", "blob", "clusterd-compute", "clusterd-storage", "kafka-sink"]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.accounts = ctx.complexity.accounts
        self.oplog = OpLog()
        self.sink_checker: SinkConsumerChecker | None = None
        assert ctx.endpoints.kafka_bootstrap is not None
        self.bootstrap = ctx.endpoints.kafka_bootstrap

    def setup(self) -> None:
        from confluent_kafka.admin import AdminClient

        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE ledger (worker int, seq bigint, account int, amount bigint)",
            "CREATE MATERIALIZED VIEW balances IN CLUSTER compute AS"
            " SELECT account, sum(amount) AS balance FROM ledger GROUP BY account",
            "CREATE CONNECTION kafka_sink_conn TO KAFKA"
            " (BROKER 'toxiproxy:9192', SECURITY PROTOCOL PLAINTEXT)",
            "CREATE SINK bank_sink IN CLUSTER storage FROM balances"
            " INTO KAFKA CONNECTION kafka_sink_conn"
            f" (TOPIC '{TOPIC}', TOPIC PARTITION COUNT 1, TOPIC REPLICATION FACTOR 1)"
            " KEY (account) NOT ENFORCED FORMAT JSON ENVELOPE DEBEZIUM",
        ]:
            client.query(sql, timeout=120)
        client.reset()
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        wait_until(
            lambda: TOPIC in admin.list_topics(timeout=10).topics,
            CONVERGE_TIMEOUT,
            "sink topic creation",
        )

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        client = MzClient(self.ctx, f"worker-{index}")
        return WorkerBundle(
            actions=[LedgerTransfer(rng, index, self.accounts, client, self.oplog)],
            weights=[1],
        )

    def checkers(self) -> list[Checker]:
        rng = random.Random(self.ctx.rng.randrange(SEED_RANGE))
        self.sink_checker = SinkConsumerChecker(rng, self.ctx, self.bootstrap)
        return [self.sink_checker]

    def _mz_balances(self, client: MzClient) -> dict[int, int]:
        client.query("SET cluster = quickstart")
        return {
            int(row[0]): int(row[1])
            for row in client.query("SELECT account, balance FROM balances")
        }

    def converge(self) -> None:
        checker = self.sink_checker
        assert checker is not None
        if checker.consumer is None:
            checker.open()
        client = MzClient(self.ctx, "converge")

        def caught_up() -> bool:
            checker.drain()
            return checker.state == self._mz_balances(client)

        wait_until(caught_up, CONVERGE_TIMEOUT, "sink output catching up")
        client.reset()

    def final_check(self) -> None:
        checker = self.sink_checker
        assert checker is not None
        client = MzClient(self.ctx, "final-check")
        last: dict[str, dict[int, int]] = {}

        def settled() -> bool:
            checker.drain()
            last["sink"] = dict(checker.state)
            last["mz"] = self._mz_balances(client)
            return last["sink"] == last["mz"]

        try:
            # Retried, not one-shot: an UNKNOWN write whose group commit was
            # still in flight during the last disruption can land after
            # converge, and the sink legitimately lags the MV by a moment. A
            # genuine divergence persists and still fails at the timeout.
            wait_until(settled, 60, "final sink/MV agreement")
        except InvariantViolation:
            raise InvariantViolation(
                f"sink state diverged from balances MV:"
                f" sink={last.get('sink')} mz={last.get('mz')}"
            ) from None
        if sum(checker.state.values()) != 0:
            raise InvariantViolation(
                f"final sink balances sum to {sum(checker.state.values())} != 0"
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
