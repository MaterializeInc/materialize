# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import json
import random
from unittest.mock import Mock

import pytest

from materialize.invariants.framework import InvariantViolation, Outcome, TransientError
from materialize.invariants.scenarios import kafka_ledger, kafka_upsert
from materialize.invariants.scenarios.avro_loopback import AvroLoopback
from materialize.invariants.scenarios.cdc_bank import UpstreamSchemaChurn


@pytest.mark.parametrize("module", [kafka_ledger, kafka_upsert])
def test_worker_counts_only_acknowledged_deliveries(monkeypatch, module) -> None:
    ctx = Mock()
    ctx.complexity.workers = ctx.complexity.accounts = 1
    scenario = (
        kafka_ledger.KafkaLedger(ctx)
        if module is kafka_ledger
        else kafka_upsert.KafkaUpsert(ctx)
    )
    producer = Mock()
    monkeypatch.setattr(module, "make_producer", lambda _: producer)
    action = scenario.make_worker(0, random.Random(1)).actions[0]
    action.run()
    report_first = producer.produce.call_args.kwargs["on_delivery"]
    action.run()
    report_second = producer.produce.call_args.kwargs["on_delivery"]
    assert action.async_commits == 0
    report_first("timed out", None)
    assert action.async_commits == 0
    report_second(None, None)
    assert action.async_commits == 1


def test_ledger_reconciles_each_produced_amount(monkeypatch) -> None:
    ctx = Mock()
    ctx.complexity.workers = 1
    scenario = kafka_ledger.KafkaLedger(ctx)
    producer = Mock()
    client = Mock()
    monkeypatch.setattr(kafka_ledger, "make_producer", lambda _: producer)
    monkeypatch.setattr(kafka_ledger, "MzClient", lambda *_: client)
    action = scenario.make_worker(0, random.Random(1)).actions[0]
    rows = []
    for _ in range(2):
        action.run()
        args = producer.produce.call_args.kwargs
        args["on_delivery"](None, None)
        value = json.loads(args["value"])
        rows.append((value["seq"], value["amount"]))
    client.query.return_value = rows
    scenario.final_check()
    client.query.return_value = [(rows[0][0], rows[1][1]), (rows[1][0], rows[0][1])]
    with pytest.raises(InvariantViolation, match="amount"):
        scenario.final_check()


def test_recency_checks_the_exact_acknowledged_marker(monkeypatch) -> None:
    producer = Mock()
    client = Mock()
    monkeypatch.setattr(kafka_ledger, "make_producer", lambda _: producer)
    monkeypatch.setattr(kafka_ledger, "MzClient", lambda *_: client)
    checker = kafka_ledger.KafkaRtrPeek(random.Random(1), Mock(), "")
    reports = iter(["timed out", None])

    def flush(timeout):
        producer.produce.call_args.kwargs["on_delivery"](next(reports), None)
        return 0

    producer.flush.side_effect = flush
    with pytest.raises(TransientError, match="not acknowledged"):
        checker.check_once()
    client.query.assert_not_called()
    client.query.return_value = [(0,)]
    with pytest.raises(InvariantViolation, match="marker 2"):
        checker.check_once()
    query, params = client.query.call_args.args
    assert "data->>'seq'" in query
    assert params == (kafka_ledger.RTR_MARKER_WORKER, 2)


@pytest.mark.parametrize("first_add_applied", [False, True])
def test_schema_churn_recovers_after_ambiguous_add(first_add_applied: bool) -> None:
    scenario = Mock()
    cur = scenario.connect_upstream.return_value.cursor.return_value
    columns = {"worker"}

    def execute(sql):
        words = sql.split()
        if words[0] == "SELECT":
            cur.description = [(name,) for name in sorted(columns)]
        elif "DROP" in words:
            columns.remove(words[-1])
        elif words[-2] == "extra_1":
            if first_add_applied:
                columns.add("extra_1")
            raise ConnectionError("lost response")
        else:
            columns.add(words[-2])

    cur.execute.side_effect = execute
    action = UpstreamSchemaChurn(random.Random(1), scenario)
    action.next_at = 0
    with pytest.raises(TransientError):
        action.run()
    action.next_at = 0
    assert action.run() == Outcome.COMMITTED
    assert columns == {"worker", "extra_2"}


def test_loopback_rejects_identical_duplicate_rows() -> None:
    client = Mock()
    client.query.return_value = [(1, 5), (1, 5)]
    scenario = AvroLoopback(Mock())
    with pytest.raises(InvariantViolation, match="duplicate account 1"):
        scenario._balances(client, "balances_rt")
