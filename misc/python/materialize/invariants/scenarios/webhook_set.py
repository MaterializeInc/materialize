# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Uniquely-numbered events POSTed to a webhook source.

The documented webhook contract: a 2xx response is returned only after the
event was durably recorded, CHECK-rejected requests are definitely not
recorded, and a JSON ARRAY body appends one row per element. That maps
directly onto the op-outcome model: 2xx means COMMITTED (visible exactly
once, since the harness never retries a request), a validation rejection
means FAILED (must never appear), and timeouts or 5xx are UNKNOWN. The
chaos phase cuts the metadata and blob legs and kills envd while POSTs are
in flight, targeting exactly the durable-write-before-ack promise.

A COUNTER load generator source runs alongside: its count only grows, at
the configured tick cadence.
"""

import json
import random
import secrets

from materialize.invariants.checkers import PeekChecker, ProgressPeek
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
from materialize.invariants.scenarios.kafka_ledger import WorkerStatsSubscribe

# Statuses that prove the server did not record the request. Validation
# failures (400/401/403) happen before append, as do 404 (the source is not
# resolvable, e.g. envd still booting), 413 (oversized body), and 429 (the
# documented concurrent-request limit). Only 400/401/403 for a well-formed
# authorized request are a bug, the others are legitimate under chaos.
REJECTED_STATUSES = {400, 401, 403, 404, 413, 429}
VALIDATION_STATUSES = {400, 401, 403}

SENTINEL_WORKER = -1


class PostEvents(Action):
    """POST one event or a JSON ARRAY batch, sometimes with bad credentials.

    Every element is registered as its own op: a 2xx commits them all, a
    validation rejection fails them all, anything else leaves them UNKNOWN.
    """

    name = "post"

    def __init__(self, rng: random.Random, worker: int, scenario: "WebhookSet") -> None:
        super().__init__(rng)
        self.worker = worker
        self.scenario = scenario
        self.session = None
        self.seq = 0

    def run(self) -> Outcome | None:
        import requests

        if self.session is None:
            self.session = requests.Session()
        batch = self.rng.randint(1, 5) if self.rng.random() < 0.3 else 1
        seqs = []
        events = []
        for _ in range(batch):
            self.seq += 1
            seqs.append(self.seq)
            events.append({"worker": self.worker, "seq": self.seq})
        bad_auth = self.rng.random() < 0.05
        token = "wrong-token" if bad_auth else self.scenario.token
        payload = events[0] if batch == 1 else events
        for seq in seqs:
            self.scenario.oplog.record(self.worker, seq, Outcome.UNKNOWN)
        try:
            response = self.session.post(
                self.scenario.url,
                data=json.dumps(payload),
                headers={
                    "content-type": "application/json",
                    "authorization": token,
                },
                timeout=(5, self.scenario.ctx.complexity.query_timeout),
            )
        except Exception as e:
            self.session = None
            raise TransientError(f"webhook POST failed: {e}") from e
        if 200 <= response.status_code < 300:
            if bad_auth:
                raise InvariantViolation(
                    "webhook accepted a request that must fail validation"
                )
            outcome = Outcome.COMMITTED
        elif response.status_code in REJECTED_STATUSES:
            outcome = Outcome.FAILED
        else:
            outcome = Outcome.UNKNOWN
        for seq in seqs:
            self.scenario.oplog.record(self.worker, seq, outcome)
        if not bad_auth and response.status_code in VALIDATION_STATUSES:
            raise InvariantViolation(
                f"webhook rejected a well-formed authorized request:"
                f" {response.status_code} {response.text[:200]}"
            )
        return outcome

    def close(self) -> None:
        if self.session is not None:
            self.session.close()


class EventCountPeek(PeekChecker):
    """Event count: monotonic and bounded by the op ledger."""

    def __init__(self, rng, ctx, scenario: "WebhookSet") -> None:
        super().__init__(rng, ctx, "events-peek", ["quickstart", "compute"])
        self.scenario = scenario

    def check_once(self) -> None:
        oplog = self.scenario.oplog
        watermark = self.scenario.event_rows.get()
        rows = self.peek("SELECT count(*) FROM wh WHERE (body->>'worker')::int >= 0")
        high = oplog.attempted_count()
        count = int(rows[0][0])
        if count < watermark:
            raise InvariantViolation(
                f"event count went backwards: {count} < watermark {watermark}"
            )
        if count > high:
            raise InvariantViolation(
                f"event count {count} exceeds attempted events {high}"
            )
        # NOTE: no live lower bound against acknowledged events: webhook data
        # is source data, which linearizability does not cover, so an acked
        # append may become readable only when the collection's frontier
        # catches up. Loss of acknowledged events is enforced by the final
        # reconciliation instead.
        self.scenario.event_rows.advance(count)
        self.validations += 1


class CounterPeek(PeekChecker):
    """The COUNTER load generator's count only grows."""

    pause = (1.0, 3.0)

    def __init__(self, rng, ctx) -> None:
        super().__init__(rng, ctx, "counter-peek", ["quickstart"])
        self.last = 0

    def check_once(self) -> None:
        count = int(self.peek("SELECT count(*) FROM counter_tbl")[0][0])
        if count < self.last:
            raise InvariantViolation(
                f"counter count went backwards: {count} < {self.last}"
            )
        self.last = count
        self.validations += 1


class WebhookSet(Scenario):
    name = "webhook-set"
    services: list[str] = []
    legs = [
        "metadata",
        "blob",
        "clusterd-storage",
        "clusterd-compute",
        "clusterd-compute2",
    ]

    def __init__(self, ctx: ScenarioContext) -> None:
        super().__init__(ctx)
        self.oplog = OpLog()
        self.event_rows = Watermark()
        self.token = secrets.token_hex(16)
        self.url = (
            f"http://{ctx.endpoints.mz_host}:{ctx.endpoints.mz_http_port}"
            "/api/webhook/materialize/public/wh"
        )

    def setup(self) -> None:
        client = MzClient(self.ctx, "setup")
        for sql in [
            f"CREATE SECRET wh_secret AS '{self.token}'",
            "CREATE SOURCE wh IN CLUSTER storage FROM WEBHOOK"
            " BODY FORMAT JSON ARRAY"
            " CHECK (WITH (HEADERS, SECRET wh_secret)"
            " constant_time_eq(headers->'authorization', wh_secret))",
            "CREATE SOURCE counter IN CLUSTER storage"
            " FROM LOAD GENERATOR COUNTER (TICK INTERVAL '100ms')",
            # A source ingests through its table exports: without one, the
            # source never leaves the 'created' status.
            "CREATE TABLE counter_tbl FROM SOURCE counter",
            "CREATE MATERIALIZED VIEW worker_stats IN CLUSTER compute AS"
            " SELECT (body->>'worker')::int AS worker, count(*) AS cnt,"
            " max((body->>'seq')::bigint) AS max_seq FROM wh"
            " WHERE (body->>'worker')::int >= 0 GROUP BY 1",
            "CREATE INDEX worker_stats_idx IN CLUSTER compute"
            " ON worker_stats (worker)",
        ]:
            client.query(sql, timeout=120)
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        return WorkerBundle(actions=[PostEvents(rng, index, self)], weights=[1])

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(4)]
        return [
            EventCountPeek(rngs[0], self.ctx, self),
            WorkerStatsSubscribe(rngs[1], self.ctx),
            CounterPeek(rngs[2], self.ctx),
            # Webhook sources report no frontier progress observable here,
            # the load generator source stands in for the progress contract.
            ProgressPeek(rngs[3], self.ctx, "counter"),
        ]

    def converge(self) -> None:
        import requests

        # Webhook appends are synchronous, so there is no ingestion lag to
        # wait out. Liveness: a fresh POST must succeed again, and the
        # counter must keep ticking. Each attempt uses a fresh seq so the
        # duplicate check stays meaningful for sentinels too.
        sentinel_seq = [0]

        def webhook_live() -> bool:
            sentinel_seq[0] += 1
            try:
                response = requests.post(
                    self.url,
                    data=json.dumps(
                        {"worker": SENTINEL_WORKER, "seq": sentinel_seq[0]}
                    ),
                    headers={
                        "content-type": "application/json",
                        "authorization": self.token,
                    },
                    timeout=(5, 30),
                )
            except Exception as e:
                raise TransientError(f"webhook POST failed: {e}") from e
            return 200 <= response.status_code < 300

        wait_until(webhook_live, CONVERGE_TIMEOUT, "webhook accepting requests")
        client = MzClient(self.ctx, "converge")
        # The baseline read runs inside the retried condition too: right
        # after healing it can block long enough for the watchdog to cancel
        # it, which must be a retry, not a scenario failure.
        base: list[int | None] = [None]

        def counter_ticking() -> bool:
            count = int(client.query("SELECT count(*) FROM counter_tbl")[0][0])
            if base[0] is None:
                base[0] = count
                return False
            return count > base[0]

        wait_until(counter_ticking, CONVERGE_TIMEOUT, "counter source ticking")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        client.query("SET cluster = quickstart")
        duplicated = client.query(
            "SELECT body->>'worker', body->>'seq', count(*) FROM wh"
            " GROUP BY 1, 2 HAVING count(*) > 1"
        )
        if duplicated:
            raise InvariantViolation(
                f"webhook events recorded more than once: {duplicated[:20]}"
            )
        for worker in range(self.ctx.complexity.workers):
            present = {
                int(row[0])
                for row in client.query(
                    "SELECT (body->>'seq')::bigint FROM wh"
                    f" WHERE (body->>'worker')::int = {worker}"
                )
            }
            committed = self.oplog.seqs(worker, Outcome.COMMITTED)
            unknown = self.oplog.seqs(worker, Outcome.UNKNOWN)
            missing = committed - present
            if missing:
                raise InvariantViolation(
                    f"worker {worker}: acknowledged events lost:"
                    f" {sorted(missing)[:20]}"
                )
            phantom = present - committed - unknown
            if phantom:
                raise InvariantViolation(
                    f"worker {worker}: events present that were rejected or"
                    f" never sent: {sorted(phantom)[:20]}"
                )
        client.reset()

    def diagnostics(self) -> None:
        client = MzClient(self.ctx, "post-heal")
        try:
            client.query("SET cluster = quickstart")
            rows = client.query("SELECT count(*) FROM wh", timeout=60)
            counts = self.oplog.counts()
            self.ctx.log.log("diag", f"post-heal: events={rows[0][0]} oplog={counts}")
        except Exception as e:
            self.ctx.log.log("diag", f"post-heal event count failed: {e}")
        client.reset()
