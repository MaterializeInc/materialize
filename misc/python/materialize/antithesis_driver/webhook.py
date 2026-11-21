# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Webhook sources: every acknowledged request is appended once, in order.

The driver posts JSON rows with unique ids, concurrently, to a webhook source
and to a webhook table. Without a 200 response a row's outcome is unknown,
and the driver never retries an id.

An append is stamped with the storage controller's wall clock, not the
timestamp oracle, so a strict serializable read right after a 200 may miss
the row. Appends to one webhook are serial and their timestamps increase, so
a read that shows a row must show every row acknowledged before that row was
sent.
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import wait as wait_futures
from dataclasses import dataclass
from typing import Any

import psycopg
import requests
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis.endpoints import HTTP_PORT
from materialize.antithesis_driver.client import Op, Outcome, Status
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

SCHEMA = "whk"
TARGETS = ("src", "tbl")
"""A webhook source and a webhook table."""
SENDERS = 4
MAX_QUEUED = 32
MALFORMED_SHARE = 0.05
SETTINGS = {"transaction_isolation": "'strict serializable'", "cluster": "quickstart"}
VALUES = ("", "'", '"', "\\", "κλειδί", "x" * 3000)


@dataclass
class Request:
    id: int
    target: str
    value: str | None
    malformed: bool = False
    """Its body is not JSON, so the webhook must reject it."""
    sent_at: float | None = None
    acked_at: float | None = None
    """When the 200 arrived. Without one, the outcome is unknown."""


@dataclass
class Verdict:
    phantoms: list[Any]
    duplicates: list[Any]
    altered: list[Any]
    out_of_order: list[int]
    ordered_pairs: int
    """Rows acknowledged before a visible row was sent, which the read had
    to show."""
    unknown_visible: int


def judge(
    requests_: dict[int, Request], rows: list[tuple[str, str | None, str | None]]
) -> Verdict:
    """`rows` are (target, id, value) of one read."""
    counts = Counter((target, ident) for target, ident, _ in rows)
    phantoms: list[Any] = []
    altered: list[Any] = []
    visible: dict[str, set[int]] = {t: set() for t in TARGETS}
    for target, ident, value in rows:
        r = requests_.get(int(ident)) if ident and ident.isdigit() else None
        if r is None or r.target != target:
            phantoms.append((target, ident))
            continue
        visible[target].add(r.id)
        if value != r.value:
            altered.append((target, r.id))
    out_of_order: list[int] = []
    ordered_pairs = 0
    for target in TARGETS:
        mine = [r for r in requests_.values() if r.target == target]
        sent = [r.sent_at for r in mine if r.id in visible[target] and r.sent_at]
        if not sent:
            continue
        latest = max(sent)
        before = [r for r in mine if r.acked_at is not None and r.acked_at < latest]
        ordered_pairs += len(before)
        out_of_order += [r.id for r in before if r.id not in visible[target]]
    return Verdict(
        phantoms=phantoms,
        duplicates=sorted(k for k, n in counts.items() if n > 1),
        altered=altered,
        out_of_order=sorted(out_of_order),
        ordered_pairs=ordered_pairs,
        unknown_visible=sum(
            1
            for target in TARGETS
            for i in visible[target]
            if requests_[i].acked_at is None
        ),
    )


class Webhooks:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.requests: dict[int, Request] = {}
        self._next_id = 1
        self._senders = ThreadPoolExecutor(SENDERS, thread_name_prefix="webhook")
        self._queued: list[Future[None]] = []
        self.batch = 4

    def configure(self, d: Draw) -> None:
        self.batch = d.choice((1, 4, 16))

    def setup_statements(self) -> list[str]:
        return [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE SOURCE IF NOT EXISTS {SCHEMA}.src IN CLUSTER sources"
            " FROM WEBHOOK BODY FORMAT JSON",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.tbl FROM WEBHOOK BODY FORMAT JSON",
        ]

    def _send(self, r: Request) -> None:
        url = f"http://{self.ctx.config.host}:{HTTP_PORT}/api/webhook/materialize/{SCHEMA}/{r.target}"
        body = json.dumps({"id": r.id, "v": r.value})
        if r.malformed:
            body = body[:-1]
        with self._lock:
            r.sent_at = time.monotonic()
        try:
            response = requests.post(
                url,
                data=body.encode(),
                headers={"Content-Type": "application/json"},
                timeout=(5, 30),
            )
            status: int | None = response.status_code
        except requests.RequestException as e:
            log(f"webhook: {type(e).__name__}")
            status = None
        if r.malformed:
            always(
                status != 200,
                "webhook: a request whose body is not JSON is never accepted",
                {"id": r.id, "target": r.target},
            )
        elif status == 200:
            with self._lock:
                r.acked_at = time.monotonic()
        sometimes(
            status is not None and status != 200,
            "webhook: a request was answered with an error status",
            {"status": status},
        )

    def post(self, d: Draw) -> Status:
        with self._lock:
            self._queued = [f for f in self._queued if not f.done()]
            if len(self._queued) >= MAX_QUEUED:
                return Status.NOT_SENT
            batch = []
            for _ in range(self.batch):
                r = Request(
                    self._next_id,
                    d.choice(TARGETS),
                    self._value(d),
                    d.boolean(MALFORMED_SHARE),
                )
                self._next_id += 1
                self.requests[r.id] = r
                batch.append(r)
            futures = [self._senders.submit(self._send, r) for r in batch]
            self._queued += futures
        _, pending = wait_futures(futures, timeout=RULE_WAIT_SECONDS)
        return Status.PENDING if pending else Status.DONE

    def _value(self, d: Draw) -> str | None:
        if d.boolean(0.1):
            return None
        if d.boolean(0.2):
            return d.choice(VALUES)
        return f"{d.integer(0, 2**32):x}"

    def _read_op(self, done: list[Verdict]) -> Op:
        query = " UNION ALL ".join(
            f"SELECT '{t}', body->>'id', body->>'v' FROM {SCHEMA}.{t}" for t in TARGETS
        )

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if outcome.ok:
                assert outcome.value is not None
                done.append(self._judge(outcome.value))

        return Op("webhook check", execute, complete, SETTINGS)

    def _judge(self, rows: list[tuple[Any, ...]]) -> Verdict:
        with self._lock:
            v = judge(self.requests, rows)
        details = {"rows": len(rows)}
        always(
            not v.phantoms,
            "webhook: every visible row was sent by the driver to that webhook",
            {**details, "phantoms": v.phantoms[:10]},
        )
        always(
            not v.duplicates,
            "webhook: every request appears at most once",
            {**details, "duplicates": v.duplicates[:10]},
        )
        always(
            not v.altered,
            "webhook: every visible row carries the body that was sent",
            {**details, "altered": v.altered[:10]},
        )
        always(
            not v.out_of_order,
            "webhook: a read that shows a row shows every row acknowledged before that row was sent",
            {**details, "missing": v.out_of_order[:10], "ordered": v.ordered_pairs},
        )
        sometimes(
            v.ordered_pairs > 0 and not v.out_of_order,
            "webhook: a check confirmed the append order of acknowledged requests",
            details,
        )
        sometimes(
            v.unknown_visible > 0,
            "webhook: a request without a 200 response turned out appended",
            {**details, "unknown_visible": v.unknown_visible},
        )
        return v

    def check(self, d: Draw) -> Status:
        return self.ctx.sessions.call(
            d.integer(0, len(self.ctx.sessions.sessions) - 1),
            self._read_op([]),
            RULE_WAIT_SECONDS,
        )

    def eventually(self, deadline: float) -> None:
        """After faults stop, every acknowledged request is visible.

        Like `health.poll`, this reads once even past `deadline`.
        """
        wait_futures(list(self._queued), timeout=max(0.0, deadline - time.monotonic()))
        with self._lock:
            acked = {(r.target, r.id) for r in self.requests.values() if r.acked_at}
        query = " UNION ALL ".join(
            f"SELECT '{t}', body->>'id' FROM {SCHEMA}.{t}" for t in TARGETS
        ).encode()
        missing: list[tuple[str, int]] | None = None
        while True:
            reads: list[list[tuple[Any, ...]]] = []

            def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
                if outcome.ok and outcome.value is not None:
                    reads.append(outcome.value)

            for i in range(len(self.ctx.sessions.sessions)):
                op = Op(
                    "webhook final read",
                    lambda conn: conn.execute(query).fetchall(),
                    complete,
                    SETTINGS,
                )
                if self.ctx.sessions.call(i, op, 60.0) != Status.NOT_SENT:
                    break
            if reads:
                seen = {(t, int(i)) for t, i in reads[0] if i and i.isdigit()}
                missing = sorted(acked - seen)
            if missing == [] or time.monotonic() >= deadline:
                break
            time.sleep(2)
        if missing is None:
            log("webhook: no final read completed")
            return
        always(
            not missing,
            "webhook: after faults stop every acknowledged request is visible",
            {"acknowledged": len(acked), "missing": missing[:10]},
        )
