# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Balances looped through Kafka as Avro Debezium, and re-ingested.

Ledger transfers feed a balances MV that a Kafka sink exports with FORMAT
AVRO USING CONFLUENT SCHEMA REGISTRY and ENVELOPE DEBEZIUM, and a Kafka
source re-ingests the same topic with the same format and envelope into a
table. This covers Avro encoding and decoding, CSR schema publishing and
fetching, and DEBEZIUM-envelope ingestion, all across their own disrupted
legs (source, sink, and schema registry).

Kafka sources promise no cross-message transaction atomicity, so the
continuous invariant is the DEBEZIUM state machine itself: applied
before/after pairs must never produce duplicate (or negative) per-account
multiplicities in the re-ingested table. A lost or duplicated message
surfaces there immediately. After quiescing, the re-ingested state must
exactly equal the balances MV (the sink's exactly-once guarantee through
the whole loop), and the ledger is reconciled against the op log.
"""

import random

from materialize.invariants.checkers import (
    PeekChecker,
    ProgressPeek,
    SubscribeChecker,
)
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
from materialize.invariants.mz import MzClient, UnexpectedQueryError
from materialize.invariants.scenarios.table_bank import LedgerTransfer

TOPIC = "invariants-loopback"


class LoopStatePeek(PeekChecker):
    """The re-ingested DEBEZIUM state holds at most one row per account."""

    def __init__(self, rng, ctx, scenario: "AvroLoopback") -> None:
        super().__init__(rng, ctx, "loop-peek", ["quickstart", "compute"])
        self.scenario = scenario

    def check_once(self) -> None:
        duplicated = self.peek(
            "SELECT account FROM balances_rt GROUP BY account" " HAVING count(*) > 1"
        )
        if duplicated:
            raise InvariantViolation(
                f"re-ingested state holds duplicate accounts (a message was"
                f" duplicated or lost): {duplicated[:20]}"
            )
        rows = self.peek("SELECT count(*) FROM balances_rt")
        if int(rows[0][0]) > self.scenario.accounts:
            raise InvariantViolation(
                f"re-ingested state holds {rows[0][0]} accounts, only"
                f" {self.scenario.accounts} exist"
            )
        self.validations += 1


class LoopStateSubscribe(SubscribeChecker):
    """Every consistent snapshot of the re-ingested state is per-key sane."""

    def __init__(self, rng, ctx) -> None:
        super().__init__(
            rng,
            ctx,
            "loop-subscribe",
            "SELECT account, balance FROM balances_rt",
        )

    def validate_state(self, state: dict[tuple, int], ts: int) -> None:
        seen: set = set()
        for (account, _balance), multiplicity in state.items():
            if multiplicity != 1 or account in seen:
                raise InvariantViolation(
                    f"re-ingested state at {ts}: duplicate rows for account"
                    f" {account}"
                )
            seen.add(account)


class AvroLoopback(Scenario):
    name = "avro-loopback"
    services = ["kafka", "schema-registry"]
    legs = [
        "metadata",
        "blob",
        "clusterd-storage",
        "clusterd-compute",
        "clusterd-compute2",
        "kafka-source",
        "kafka-sink",
        "csr",
    ]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.accounts = ctx.complexity.accounts
        self.oplog = OpLog()
        assert ctx.endpoints.kafka_bootstrap is not None
        self.bootstrap = ctx.endpoints.kafka_bootstrap

    def setup(self) -> None:
        from confluent_kafka.admin import AdminClient

        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE ledger (worker int, seq bigint, account int,"
            " amount bigint, at timestamptz)",
            # HAVING drops zero balances so accounts also get deleted and
            # re-created, exercising Debezium deletes through the loop.
            "CREATE MATERIALIZED VIEW balances IN CLUSTER compute AS"
            " SELECT account, sum(amount) AS balance FROM ledger"
            " GROUP BY account HAVING sum(amount) <> 0",
            "CREATE CONNECTION kafka_sink_conn TO KAFKA"
            " (BROKER 'toxiproxy:9192', SECURITY PROTOCOL PLAINTEXT)",
            "CREATE CONNECTION kafka_src_conn TO KAFKA"
            " (BROKER 'toxiproxy:9092', SECURITY PROTOCOL PLAINTEXT)",
            "CREATE CONNECTION csr_conn TO CONFLUENT SCHEMA REGISTRY"
            " (URL 'http://toxiproxy:8081')",
            "CREATE SINK loop_sink IN CLUSTER storage FROM balances"
            " INTO KAFKA CONNECTION kafka_sink_conn"
            f" (TOPIC '{TOPIC}', TOPIC PARTITION COUNT 1,"
            " TOPIC REPLICATION FACTOR 1)"
            " KEY (account)"
            " FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn"
            " ENVELOPE DEBEZIUM",
        ]:
            client.query(sql, timeout=120)
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        wait_until(
            lambda: TOPIC in admin.list_topics(timeout=10).topics,
            CONVERGE_TIMEOUT,
            "sink topic creation",
        )
        client.query(
            "CREATE SOURCE loop_source IN CLUSTER storage"
            f" FROM KAFKA CONNECTION kafka_src_conn (TOPIC '{TOPIC}')",
            timeout=120,
        )

        # The source table resolves the value schema from the registry at
        # creation time, so it can only be created once the sink has
        # published it.
        def reingest_table_created() -> bool:
            try:
                client.query(
                    "CREATE TABLE balances_rt FROM SOURCE loop_source"
                    f' (REFERENCE "{TOPIC}")'
                    " FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY"
                    " CONNECTION csr_conn ENVELOPE DEBEZIUM",
                    timeout=60,
                )
                return True
            except UnexpectedQueryError as e:
                if "not found" in str(e) or "No subject" in str(e):
                    raise TransientError(str(e)) from e
                raise

        wait_until(reingest_table_created, CONVERGE_TIMEOUT, "sink schema publication")
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        client = MzClient(self.ctx, f"worker-{index}")
        return WorkerBundle(
            actions=[LedgerTransfer(rng, index, self.accounts, client, self.oplog)],
            weights=[1],
        )

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(3)]
        return [
            LoopStatePeek(rngs[0], self.ctx, self),
            LoopStateSubscribe(rngs[1], self.ctx),
            ProgressPeek(rngs[2], self.ctx, "loop_source"),
        ]

    def _balances(self, client: MzClient, relation: str) -> dict[int, int]:
        client.query("SET cluster = quickstart")
        return {
            int(row[0]): int(row[1])
            for row in client.query(f"SELECT account, balance FROM {relation}")
        }

    def converge(self) -> None:
        client = MzClient(self.ctx, "converge")

        def caught_up() -> bool:
            return self._balances(client, "balances_rt") == self._balances(
                client, "balances"
            )

        wait_until(caught_up, CONVERGE_TIMEOUT, "loopback catching up")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        upstream = self._balances(client, "balances")
        reingested = self._balances(client, "balances_rt")
        if reingested != upstream:
            diff = {
                account: (upstream.get(account), reingested.get(account))
                for account in set(upstream) | set(reingested)
                if upstream.get(account) != reingested.get(account)
            }
            raise InvariantViolation(
                f"re-ingested balances diverged: {dict(list(diff.items())[:20])}"
            )
        if sum(upstream.values()) != 0:
            raise InvariantViolation(f"balances sum to {sum(upstream.values())} != 0")
        client.query("SET cluster = quickstart")
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
                    f"worker {worker}: committed transfers lost:"
                    f" {sorted(missing)[:20]}"
                )
            phantom = present - committed - unknown
            if phantom:
                raise InvariantViolation(
                    f"worker {worker}: transfers present that never committed:"
                    f" {sorted(phantom)[:20]}"
                )
        client.reset()

    def diagnostics(self) -> None:
        client = MzClient(self.ctx, "post-heal")
        try:
            upstream = self._balances(client, "balances")
            reingested = self._balances(client, "balances_rt")
            verdict = "converged" if upstream == reingested else "still diverged"
            self.ctx.log.log(
                "diag",
                f"post-heal: balances={len(upstream)}"
                f" reingested={len(reingested)} ({verdict})",
            )
        except Exception as e:
            self.ctx.log.log("diag", f"post-heal comparison failed: {e}")
        client.reset()
