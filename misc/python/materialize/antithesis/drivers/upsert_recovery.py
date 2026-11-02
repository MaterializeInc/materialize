# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Drive keyed JSON decode errors through persisted upsert state and repair."""

from __future__ import annotations

import json
import sqlite3
import time
import zlib
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)
from confluent_kafka.cimpl import KafkaException, Producer

from materialize.antithesis import ddl_hold, quiet, sql, telemetry
from materialize.antithesis import upsert_recovery_io as io
from materialize.antithesis import upsert_recovery_model as model
from materialize.antithesis.drivers import configure, history, pod_restarts, rollouts
from materialize.antithesis.drivers.access_control import exclusive
from materialize.antithesis.drivers.kafka_sources import _partition_frontier
from materialize.antithesis.drivers.sources_common import (
    SOURCES_CLUSTER,
    ensure_cluster,
    ensure_retain_history,
    frontiers,
)
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.environment import Environment
from materialize.antithesis.observations import sometimes
from materialize.antithesis.rng import rng

BUDGET_S = 150.0
FENCE_S = 25.0
Message = tuple[int, str, str | None]


@dataclass(frozen=True)
class Plan:
    partitions: int
    seed: list[Message]
    poison: list[Message]
    replace_error: list[Message]
    repair: list[Message]
    churn: list[Message]


def generate() -> Plan:
    partitions = rng.choice((1, 2))
    keys = [f"k{i}" for i in range(rng.choice((3, 5, 9)))]

    def message(key: str, value: Any) -> Message:
        return (zlib.crc32(key.encode()) % partitions, key, value)

    def value() -> str:
        return json.dumps(
            rng.choice(
                (
                    None,
                    rng.randint(-1000, 1000),
                    [rng.randint(-9, 9), "λ"],
                    {"v": rng.randint(-100, 100), "s": "κ"},
                )
            ),
            ensure_ascii=False,
        )

    seed = [
        message(key, "null" if index == 0 else value())
        for index, key in enumerate(keys)
    ]
    bad = rng.sample(keys[1:], rng.randint(1, min(2, len(keys) - 1)))
    poison = [message(key, '{"broken":[' + str(rng.randint(0, 99))) for key in bad]
    replace_error = [message(bad[0], '"unterminated-' + str(rng.randint(0, 99)))]
    repair = [message(key, rng.choice((None, "null", value()))) for key in bad]
    churn = []
    for key in rng.sample(keys, rng.randint(1, len(keys))):
        churn.extend(
            (
                message(key, "[,"),
                message(key, value()),
                message(key, None),
                message(key, value()),
            )
        )
    return Plan(partitions, seed, poison, replace_error, repair, churn)


def _execute(conn: psycopg.Connection, deadline: float, text: str, params=None):
    remaining = int(1000 * (deadline - time.monotonic()))
    if remaining <= 0:
        raise io.Incomplete("upsert round deadline elapsed")
    conn.execute(f"SET statement_timeout='{min(5000, remaining)}ms'".encode())
    return conn.execute(text.encode(), params)


def _context(host: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "scope": "reported system parameters, not proof of the rendered operator arm"
    }
    with sql.connection(
        host, internal=True, connect_timeout=3, statement_timeout_ms=3000
    ) as conn:
        for name in (
            "enable_upsert_v2",
            "enable_upsert_chunked_stash",
            "enable_upsert_paged_spill",
        ):
            try:
                row = conn.execute(f"SHOW {name}".encode()).fetchone()
                result[name] = None if row is None else row[0]
            except psycopg.Error as error:
                result[name] = {"unavailable": error.sqlstate}
    return result


def _setup(
    host: str,
    endpoints: Endpoints,
    db: sqlite3.Connection,
    fixture: io.Fixture,
    plan: Plan,
    conn: psycopg.Connection,
) -> tuple[str, str]:
    if not ensure_retain_history(host, endpoints):
        raise io.Incomplete("retained-history precondition is unavailable")
    ensure_cluster(conn, db)
    io.ddl(db, fixture, conn, f"CREATE SCHEMA {fixture.schema}")
    schema_id = conn.execute(
        "SELECT id FROM mz_schemas WHERE name=%s", (fixture.schema,)
    ).fetchone()
    if schema_id is None:
        raise io.Incomplete("new schema identity is unavailable")
    with db:
        db.execute(
            "UPDATE rounds SET schema_id=? WHERE id=?",
            (str(schema_id[0]), fixture.number),
        )
    io.ddl(
        db,
        fixture,
        conn,
        f"CREATE CONNECTION {fixture.schema}.broker TO KAFKA (BROKER '{endpoints.kafka_broker}', SECURITY PROTOCOL PLAINTEXT) WITH (VALIDATE=false)",
    )
    io.ddl(
        db,
        fixture,
        conn,
        f"CREATE SOURCE {fixture.schema}.src IN CLUSTER {SOURCES_CLUSTER} FROM KAFKA CONNECTION {fixture.schema}.broker (TOPIC '{fixture.topic}') WITH (RETAIN HISTORY=FOR '2m')",
    )
    _, source_id = io.bind_ids(db, fixture, conn)
    io.ddl(
        db,
        fixture,
        conn,
        f"CREATE TABLE {fixture.schema}.data FROM SOURCE {fixture.schema}.src (REFERENCE \"{fixture.topic}\") KEY FORMAT TEXT VALUE FORMAT JSON ENVELOPE UPSERT WITH (RETAIN HISTORY=FOR '2m')",
    )
    table = conn.execute(
        "SELECT id FROM mz_tables WHERE name='data' AND schema_id=%s",
        (str(schema_id[0]),),
    ).fetchone()
    if table is None:
        raise io.Incomplete("new table identity is unavailable")
    return source_id, str(table[0])


def check(
    conn: psycopg.Connection,
    fixture: io.Fixture,
    ids: tuple[str, str],
    records: list[model.Record],
    partitions: int,
    deadline: float,
    phase: str,
    context: dict[str, Any],
) -> bool:
    target = model.offsets(records)
    stop = min(deadline, time.monotonic() + FENCE_S)
    while time.monotonic() < stop:
        bounds = frontiers(conn, ids)
        if len(bounds) == 2 and all(
            since is not None and upper is not None for since, upper in bounds.values()
        ):
            since = max(int(row[0]) for row in bounds.values() if row[0] is not None)
            stamp = (
                min(int(row[1]) for row in bounds.values() if row[1] is not None) - 1
            )
            if stamp >= since:
                try:
                    progress = _execute(
                        conn,
                        stop,
                        f'SELECT lower(partition)::text,upper(partition)::text,lower_inc(partition),upper_inc(partition),"offset"::text FROM {fixture.schema}.src AS OF {stamp}',
                    ).fetchall()
                    mapped, uncovered = _partition_frontier(progress, partitions)
                    if not uncovered and all(
                        mapped.get(partition, 0) >= offset
                        for partition, offset in target.items()
                    ):
                        return _compare(
                            conn, fixture, records, stop, stamp, phase, context, mapped
                        )
                except psycopg.Error as error:
                    classified = sql.classify_as_of_read(error)
                    if classified.outcome is sql.Outcome.VIOLATION:
                        raise
        time.sleep(min(0.2, max(0.0, stop - time.monotonic())))
    raise io.Incomplete(f"{phase}: ingestion has not fenced every delivery offset")


def _compare(conn, fixture, records, deadline, stamp, phase, context, mapped) -> bool:
    rows = None
    decode_error = False
    try:
        rows = _execute(
            conn,
            deadline,
            f"SELECT key,data::text FROM {fixture.schema}.data AS OF {stamp}",
        ).fetchall()
    except psycopg.Error as error:
        if "Failed to decode JSON" in str(error):
            decode_error = True
        else:
            raise
    errors = model.validate(records, rows, decode_error)
    details = {
        "run": fixture.number,
        "phase": phase,
        "as_of": stamp,
        "mapped_offsets": mapped,
        "parameter_context": context,
        "errors": errors[:5],
    }
    passed = not errors
    always(
        passed,
        "upsert recovery: fenced acknowledged input determines JSON rows and decode errors",
        details,
    )
    telemetry.comparison("upsert_recovery", "point", passed, max(1, len(records)))
    if phase == "seed":
        sometimes(
            passed,
            "upsert recovery: valid input including JSON null matched its acknowledged offsets",
            details,
        )
    elif phase == "poison":
        sometimes(
            passed and decode_error,
            "upsert recovery: malformed keyed JSON produced its expected decode error",
            details,
        )
    elif phase == "replace_error":
        sometimes(
            passed and decode_error,
            "upsert recovery: a separately fenced decode error replaced an earlier error",
            details,
        )
    elif phase == "rehydrated":
        sometimes(
            passed and decode_error,
            "upsert recovery: persisted decode errors matched after source-cluster replacement",
            details,
        )
    elif phase == "repair":
        sometimes(
            passed and not decode_error,
            "upsert recovery: acknowledged repairs retracted decode errors",
            details,
        )
    elif phase == "churn":
        sometimes(
            passed and not decode_error,
            "upsert recovery: last offsets survived repeated valid error and tombstone transitions",
            details,
        )
    return passed


def _restart(env: Environment, host: str, deadline: float) -> dict[str, Any] | None:
    if quiet.active_quiet_period() or time.monotonic() >= deadline:
        return None
    cluster_id = telemetry.resolve_cluster_id(host, SOURCES_CLUSTER)
    if cluster_id is None:
        return None
    kube = rollouts.Kube(env)
    victim = pod_restarts.pick_victim(kube, "clusterd", cluster_id=cluster_id)
    if victim is None or victim.uid is None:
        return None
    if not pod_restarts.delete_recorded(
        kube,
        origin="upsert_recovery",
        role="clusterd",
        namespace=victim.namespace,
        pod=victim.pod,
        uid=victim.uid,
        cluster_id=cluster_id,
        grace=1,
        reason="persisted upsert decode-error state",
        deadline=deadline,
    ):
        return None
    stop = min(deadline, time.monotonic() + 20)
    while time.monotonic() < stop:
        pods = kube.core.list_namespaced_pod(
            victim.namespace,
            _request_timeout=3,  # pyright: ignore[reportCallIssue]
        ).items
        replacement = next(
            (
                pod
                for pod in pods
                if pod.metadata is not None
                and pod.metadata.name == victim.pod
                and isinstance(pod.metadata.uid, str)
                and pod.metadata.uid
                and pod.metadata.uid != victim.uid
                and {
                    str(value)
                    for key, value in (pod.metadata.labels or {}).items()
                    if key.endswith("/cluster-id")
                }
                == {cluster_id}
                and pod.status is not None
                and pod.status.phase == "Running"
                and pod.status.container_statuses
                and all(status.ready for status in pod.status.container_statuses)
            ),
            None,
        )
        if replacement is not None:
            return {
                "pod": victim.pod,
                "prior_uid": victim.uid,
                "new_uid": replacement.metadata.uid,
                "cluster_id": cluster_id,
            }
        time.sleep(min(0.2, max(0.0, stop - time.monotonic())))
    return None


def run_round(
    env: Environment,
    host: str,
    db: sqlite3.Connection,
    plan: Plan,
    *,
    restart: bool = True,
) -> bool:
    deadline = time.monotonic() + BUDGET_S
    fixture = io.allocate(db)
    conn = None
    try:
        io.create_topic(db, fixture, env.endpoints.kafka_broker, plan.partitions)
        producer = Producer(
            {
                "bootstrap.servers": env.endpoints.kafka_broker,
                "enable.idempotence": True,
                "acks": "all",
                "message.timeout.ms": 10000,
            }
        )
        records = io.produce(
            db, fixture, producer, plan.seed, min(deadline, time.monotonic() + 12)
        )
        context = _context(host)
        conn = sql.connect(
            host,
            connect_timeout=3,
            statement_timeout_ms=5000,
            options={"cluster": configure.SHARED_CLUSTER},
        )
        conn.execute("SET transaction_isolation='strict serializable'")
        ids = _setup(host, env.endpoints, db, fixture, plan, conn)
        replaced = None
        for phase, messages in (
            ("seed", []),
            ("poison", plan.poison),
            ("replace_error", plan.replace_error),
            ("repair", plan.repair),
            ("churn", plan.churn),
        ):
            if messages:
                records.extend(
                    io.produce(
                        db,
                        fixture,
                        producer,
                        messages,
                        min(deadline, time.monotonic() + 12),
                    )
                )
            if not check(
                conn, fixture, ids, records, plan.partitions, deadline, phase, context
            ):
                return False
            if phase == "replace_error" and restart:
                try:
                    replaced = _restart(env, host, deadline)
                except pod_restarts.TRANSIENT_ERRORS:
                    replaced = None
                if replaced is not None and not check(
                    conn,
                    fixture,
                    ids,
                    records,
                    plan.partitions,
                    deadline,
                    "rehydrated",
                    context,
                ):
                    return False
            if phase == "repair":
                sometimes(
                    replaced is not None,
                    "upsert recovery: repaired input matched after a scoped source-cluster pod replacement",
                    {
                        "run": fixture.number,
                        "replacement": replaced,
                        "parameter_context": context,
                    },
                )
        return True
    finally:
        if conn is not None:
            conn.close()
        io.cleanup(host, env.endpoints, db, current=fixture.number)


def cleanup_abandoned(host: str) -> tuple[bool, dict[str, Any]]:
    db = io.open_state()
    try:
        return io.cleanup(host, Endpoints.from_env(), db)
    finally:
        db.close()


def main() -> int:
    history.start_watchdog(BUDGET_S + 45)
    env = Environment()
    if quiet.active_quiet_period() or ddl_hold.active():
        return 0
    with exclusive(str(env.endpoints.state_dir / "upsert_recovery.lock")) as owned:
        if not owned:
            return 0
        db = io.open_state()
        try:
            host = env.sql_host()
            io.cleanup(host, env.endpoints, db)
            if (
                db.execute("SELECT count(*) FROM rounds").fetchone()[0]
                >= io.MAX_PENDING
            ):
                return 0
            run_round(env, host, db, generate(), restart=rng.random() < 0.5)
        except (
            psycopg.Error,
            OSError,
            KafkaException,
            io.Incomplete,
            TimeoutError,
        ) as error:
            telemetry.comparison("upsert_recovery", "point", None)
            if isinstance(error, psycopg.Error):
                classified = sql.classify_as_of_read(error)
                always(
                    classified.outcome is not sql.Outcome.VIOLATION,
                    "upsert recovery: lifecycle and reads return only classified SQL errors",
                    {"sqlstate": classified.sqlstate, "template": classified.template},
                )
            print(f"upsert recovery: incomplete: {error}", flush=True)
        finally:
            db.close()
    return 0
