# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import sqlite3
import threading
import time
import weakref
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from random import Random
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest

from materialize.antithesis import upsert_recovery_io as io
from materialize.antithesis import upsert_recovery_model as model
from materialize.antithesis.drivers import upsert_recovery as driver


def test_concurrent_state_migrations_preserve_the_original_journal(
    monkeypatch, tmp_path
):
    path = tmp_path / "upsert.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE rounds (id INTEGER PRIMARY KEY)")
        db.execute("INSERT INTO rounds VALUES (7)")
    inspected = threading.Barrier(2)

    class Connection(sqlite3.Connection):
        first_inspection = True

        def execute(self, statement, *args):
            cursor = super().execute(statement, *args)
            if statement == "PRAGMA table_info(rounds)" and self.first_inspection:
                self.first_inspection = False
                rows = cursor.fetchall()
                try:
                    inspected.wait(timeout=0.1)
                except threading.BrokenBarrierError:
                    pass
                return iter(rows)
            return cursor

    monkeypatch.setattr(
        io.state, "open_db", lambda name: sqlite3.connect(path, factory=Connection)
    )

    def migrate():
        with io.open_state() as db:
            assert db.execute("SELECT id FROM rounds").fetchone() == (7,)
            assert {"cleanup_session_id", "topic_id"} <= {
                row[1] for row in db.execute("PRAGMA table_info(rounds)")
            }

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: migrate(), range(2)))


def test_generated_transitions_have_independent_error_and_repair_states(monkeypatch):
    for seed in range(40):
        monkeypatch.setattr(driver, "rng", Random(seed))
        plan = driver.generate()
        records = []
        offsets: Counter[int] = Counter()
        for phase in (
            plan.seed,
            plan.poison,
            plan.replace_error,
            plan.repair,
            plan.churn,
        ):
            for partition, key, value in phase:
                records.append(model.Record(partition, offsets[partition], key, value))
                offsets[partition] += 1
            expected, errors = model.expected(records)
            assert bool(errors) == (phase is plan.poison or phase is plan.replace_error)
            if not errors:
                assert not model.validate(records, list(expected.elements()), False)


def test_json_null_is_a_value_and_null_payload_is_a_tombstone():
    records = [model.Record(0, 0, "key", "null")]
    assert not model.validate(records, [("key", "null")], False)
    assert model.validate(records, [], False)
    assert model.validate(records, [("key", None)], False)
    records.append(model.Record(0, 1, "key", None))
    assert not model.validate(records, [], False)
    assert model.validate(records, [("key", "null")], False)
    assert model.validate(records, None, False)


def test_errors_replace_prior_values_and_repair_retracts_every_error():
    records = [model.Record(0, 0, "a", "1"), model.Record(0, 1, "b", "2")]
    records.extend((model.Record(0, 2, "a", "{"), model.Record(0, 3, "a", "[,")))
    assert not model.validate(records, None, True)
    assert model.validate(records, [("a", "1"), ("b", "2")], False)
    records.append(model.Record(0, 4, "a", ' {"n":3} '))
    assert not model.validate(records, [("a", '{"n":3}'), ("b", "2")], False)
    assert model.validate(records, None, True)
    assert model.validate(records, [("a", "1"), ("b", "2")], False)
    assert model.validate(records, [("a", '{"n":3}')] * 2 + [("b", "2")], False)


@pytest.mark.parametrize("failed_phase", [None, "replace_error", "rehydrated"])
def test_restart_checks_persisted_error_before_repair(monkeypatch, failed_phase):
    monkeypatch.setattr(driver, "rng", Random(17))
    plan = driver.generate()
    events = []
    offsets: Counter[int] = Counter()
    fixture = io.Fixture(1, "test")
    monkeypatch.setattr(io, "allocate", lambda db: fixture)
    monkeypatch.setattr(io, "create_topic", Mock())
    cleanup = Mock()
    monkeypatch.setattr(io, "cleanup", cleanup)
    monkeypatch.setattr(driver, "Producer", Mock())
    monkeypatch.setattr(driver, "_context", lambda host: {})
    monkeypatch.setattr(driver.sql, "connect", Mock())
    monkeypatch.setattr(driver, "_setup", Mock(return_value=("u1", "u2")))
    monkeypatch.setattr(driver, "sometimes", Mock())

    def produce(db, fixture, producer, messages, deadline):
        phase = next(
            name
            for name in ("seed", "poison", "replace_error", "repair", "churn")
            if messages is getattr(plan, name)
        )
        events.append(f"deliver {phase}")
        acknowledged = []
        for partition, key, value in messages:
            acknowledged.append(model.Record(partition, offsets[partition], key, value))
            offsets[partition] += 1
        return acknowledged

    def check(conn, fixture, ids, records, partitions, deadline, phase, context):
        events.append(f"check {phase}")
        _, errors = model.expected(records)
        assert bool(errors) == (phase in ("poison", "replace_error", "rehydrated"))
        return phase != failed_phase

    def restart(*args):
        events.append("restart")
        return {"prior_uid": "before", "new_uid": "after"}

    monkeypatch.setattr(io, "produce", produce)
    monkeypatch.setattr(driver, "check", check)
    monkeypatch.setattr(driver, "_restart", restart)
    assert driver.run_round(Mock(), "host", Mock(), plan) == (failed_phase is None)
    expected = [
        "deliver seed",
        "check seed",
        "deliver poison",
        "check poison",
        "deliver replace_error",
        "check replace_error",
        "restart",
        "check rehydrated",
        "deliver repair",
        "check repair",
        "deliver churn",
        "check churn",
    ]
    if failed_phase is not None:
        expected = expected[: expected.index(f"check {failed_phase}") + 1]
        driver.sometimes.assert_not_called()
    assert events == expected
    cleanup.assert_called_once()


def test_model_refuses_ambiguous_key_order_and_conflicting_acknowledgements():
    with pytest.raises(ValueError, match="partitions"):
        model.expected([model.Record(0, 0, "a", "1"), model.Record(1, 0, "a", "2")])
    with pytest.raises(ValueError, match="conflicting"):
        model.expected([model.Record(0, 0, "a", "1"), model.Record(0, 0, "a", "2")])


def test_unacknowledged_delivery_cannot_enter_the_input_model():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE records (run,partition,offset,key,value)")
    producer = Mock()
    with pytest.raises(io.Incomplete, match="not all acknowledged"):
        io.produce(
            db, io.Fixture(1, "test"), producer, [(0, "a", "1")], time.monotonic()
        )
    assert db.execute("SELECT count(*) FROM records").fetchone()[0] == 0


def test_topic_creation_keeps_its_broker_client_alive_until_ack(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE rounds (id,broker_state,topic_id)")
    db.execute("INSERT INTO rounds VALUES (1,'uncreated',NULL)")

    class Client:
        def create_topics(self, topics, **kwargs):
            owner = weakref.ref(self)

            def acknowledged(**kwargs):
                assert owner() is not None

            return {topics[0].topic: SimpleNamespace(result=acknowledged)}

        def describe_topics(self, topics, **kwargs):
            return {
                "antithesis-upsert-recovery-r1-test": SimpleNamespace(
                    result=lambda **kwargs: SimpleNamespace(topic_id="topic-uuid")
                )
            }

    monkeypatch.setattr(io, "admin", lambda broker: Client())
    io.create_topic(db, io.Fixture(1, "test"), "broker", 1)
    assert db.execute("SELECT broker_state FROM rounds").fetchone()[0] == "live"


def test_source_offset_fence_precedes_every_oracle_verdict(monkeypatch):
    fixture = io.Fixture(1, "test")
    monkeypatch.setattr(
        driver, "frontiers", lambda *args: {"u1": (0, 10), "u2": (0, 10)}
    )
    monkeypatch.setattr(
        driver,
        "_execute",
        lambda *args: SimpleNamespace(fetchall=lambda: [("0", "0", True, True, "0")]),
    )
    compare = Mock()
    monkeypatch.setattr(driver, "_compare", compare)
    with pytest.raises(io.Incomplete, match="not fenced"):
        driver.check(
            Mock(),
            fixture,
            ("u1", "u2"),
            [model.Record(0, 3, "a", "1")],
            1,
            time.monotonic() + 0.02,
            "repair",
            {},
        )
    assert not compare.called
    monkeypatch.setattr(
        driver,
        "_execute",
        lambda *args: SimpleNamespace(fetchall=lambda: [("0", "0", True, True, "4")]),
    )
    assert driver.check(
        Mock(),
        fixture,
        ("u1", "u2"),
        [model.Record(0, 3, "a", "1")],
        1,
        time.monotonic() + 1,
        "repair",
        {},
    )
    assert compare.call_count == 1


@pytest.mark.parametrize("wrong", ["rows", "stale_error"])
def test_wrong_repair_results_cannot_earn_positive_coverage(monkeypatch, wrong):
    verdicts = []
    observations = []
    monkeypatch.setattr(driver, "always", lambda passed, *args: verdicts.append(passed))
    monkeypatch.setattr(
        driver, "sometimes", lambda observed, *args: observations.append(observed)
    )
    monkeypatch.setattr(driver.telemetry, "comparison", lambda *args: None)

    def execute(*args):
        if wrong == "stale_error":
            raise psycopg.errors.InternalError("Failed to decode JSON: old error")
        return SimpleNamespace(fetchall=lambda: [("a", "2")])

    monkeypatch.setattr(driver, "_execute", execute)
    assert not driver._compare(
        Mock(),
        io.Fixture(1, "test"),
        [model.Record(0, 0, "a", "1")],
        time.monotonic() + 1,
        9,
        "repair",
        {},
        {0: 1},
    )
    assert verdicts == [False] and observations == [False]


@pytest.fixture
def cleanup_state(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE rounds (id,token,owner_pid,owner_token,session_id,schema_id,source_id,broker_state,ddl_ambiguous,cleanup_session_id,topic_id)"
    )
    db.execute("CREATE TABLE records (run)")
    db.execute(
        "INSERT INTO rounds VALUES (1,'test',999999,'old','original','schema1','u1','live',1,NULL,'original-topic')"
    )
    monkeypatch.setattr(io.ddl_hold, "active", lambda: False)
    monkeypatch.setattr(io, "process_token", lambda pid: None)
    conn = Mock()
    monkeypatch.setattr(io.sql, "connection", lambda *args, **kwargs: nullcontext(conn))
    endpoints = SimpleNamespace(kafka_broker="broker")
    return db, conn, endpoints


def test_dead_owner_does_not_override_a_still_live_original_sql_session(cleanup_state):
    db, conn, endpoints = cleanup_state
    conn.execute.return_value.fetchone.return_value = (1,)
    done, details = io.cleanup("host", endpoints, db)
    assert (
        not done and details["pending"][0]["reason"] == "original SQL session is active"
    )
    assert details["sql_fixtures_absent"] is False
    assert not any("DROP" in str(call) for call in conn.execute.call_args_list)


def test_dead_owner_does_not_override_a_still_live_cleanup_sql_session(cleanup_state):
    db, conn, endpoints = cleanup_state
    db.execute("UPDATE rounds SET cleanup_session_id='cleanup'")
    conn.execute.side_effect = [
        Mock(),
        SimpleNamespace(fetchone=lambda: None),
        SimpleNamespace(fetchone=lambda: (1,)),
    ]
    done, details = io.cleanup("host", endpoints, db)
    assert (
        not done
        and details["pending"][0]["reason"] == "prior cleanup SQL session is active"
    )
    assert details["sql_fixtures_absent"] is False
    assert not any("DROP" in str(call) for call in conn.execute.call_args_list)


def test_cleanup_cannot_drop_a_replaced_schema_incarnation(cleanup_state):
    db, conn, endpoints = cleanup_state
    conn.execute.side_effect = [
        Mock(),
        SimpleNamespace(fetchone=lambda: None),
        SimpleNamespace(fetchone=lambda: ("other-schema",)),
    ]
    done, details = io.cleanup("host", endpoints, db)
    assert not done and details["pending"][0]["reason"] == "schema incarnation changed"
    assert not any("DROP" in str(call) for call in conn.execute.call_args_list)


def test_unknown_kafka_create_is_retained_until_metadata_resolves_it(
    monkeypatch, cleanup_state
):
    db, conn, endpoints = cleanup_state
    db.execute("UPDATE rounds SET broker_state='creating'")
    conn.execute.return_value.fetchone.return_value = None
    monkeypatch.setattr(
        io,
        "admin",
        lambda broker: SimpleNamespace(
            list_topics=lambda **kwargs: SimpleNamespace(topics={})
        ),
    )
    done, details = io.cleanup("host", endpoints, db)
    assert (
        not done and details["pending"][0]["reason"] == "Kafka creation is unresolved"
    )
    assert details["sql_fixtures_absent"] is True
    assert details["broker_cleanup_complete"] is False
    assert db.execute("SELECT count(*) FROM rounds").fetchone()[0] == 1


def test_cleanup_refuses_a_different_broker_topic_identity(monkeypatch, cleanup_state):
    db, conn, endpoints = cleanup_state
    conn.execute.return_value.fetchone.return_value = None
    broker = Mock()
    broker.list_topics.return_value.topics = {
        io.Fixture(1, "test").topic: SimpleNamespace(error=None)
    }
    monkeypatch.setattr(io, "admin", lambda broker_name: broker)
    monkeypatch.setattr(io, "topic_identity", lambda *args: "replacement-topic")
    done, details = io.cleanup("host", endpoints, db)
    assert not done and details["sql_fixtures_absent"] is True
    assert details["pending"][0]["reason"] == "Kafka topic incarnation changed"
    assert not broker.delete_topics.called


def test_cleanup_retains_original_writer_separately_from_cleanup_session(
    monkeypatch, cleanup_state
):
    db, conn, _ = cleanup_state
    monkeypatch.setattr(io, "session_id", lambda conn: "cleanup-session")
    io.ddl(db, io.Fixture(1, "test"), conn, "DROP SCHEMA example CASCADE", cleanup=True)
    assert db.execute(
        "SELECT session_id,cleanup_session_id FROM rounds"
    ).fetchone() == ("original", "cleanup-session")


@pytest.mark.parametrize(
    "mutation", ["same_uid", "missing_uid", "other_cluster", "not_ready", "valid"]
)
def test_restart_witness_requires_concrete_scoped_ready_replacement(
    monkeypatch, mutation
):
    monkeypatch.setattr(driver.quiet, "active_quiet_period", lambda: False)
    monkeypatch.setattr(driver.telemetry, "resolve_cluster_id", lambda *args: "u3")
    victim = driver.pod_restarts.Victim("clusterd", "namespace", "pod", "old-uid", "u3")
    monkeypatch.setattr(
        driver.pod_restarts, "pick_victim", lambda *args, **kwargs: victim
    )
    deletes = []
    monkeypatch.setattr(
        driver.pod_restarts,
        "delete_recorded",
        lambda kube, **kwargs: deletes.append(kwargs) or True,
    )
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name="pod", uid="new-uid", labels={"cluster/cluster-id": "u3"}
        ),
        status=SimpleNamespace(
            phase="Running", container_statuses=[SimpleNamespace(ready=True)]
        ),
    )
    if mutation == "same_uid":
        pod.metadata.uid = "old-uid"
    elif mutation == "missing_uid":
        pod.metadata.uid = None
    elif mutation == "other_cluster":
        pod.metadata.labels = {"cluster/cluster-id": "u4"}
    elif mutation == "not_ready":
        pod.status.container_statuses[0].ready = False
    kube = SimpleNamespace(
        core=SimpleNamespace(
            list_namespaced_pod=lambda *args, **kwargs: SimpleNamespace(items=[pod])
        )
    )
    monkeypatch.setattr(driver.rollouts, "Kube", lambda env: kube)
    proof = driver._restart(Mock(), "host", time.monotonic() + 0.02)
    assert (proof is not None) == (mutation == "valid")
    assert deletes[0]["uid"] == "old-uid" and deletes[0]["cluster_id"] == "u3"
