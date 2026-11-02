# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Journal immutable upsert fixtures and reconcile their original SQL sessions."""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from confluent_kafka import (
    TopicCollection,  # pyright: ignore[reportAttributeAccessIssue]
)
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
from confluent_kafka.cimpl import KafkaError, KafkaException, Producer, Uuid

from materialize.antithesis import ddl_hold, sql, state
from materialize.antithesis.endpoints import Endpoints
from materialize.antithesis.rng import rng
from materialize.antithesis.upsert_recovery_model import Record

STATE = "upsert_recovery"
MAX_PENDING = 2


class Incomplete(Exception):
    pass


@dataclass(frozen=True)
class Fixture:
    number: int
    token: str

    @property
    def schema(self) -> str:
        return f"upsert_recovery_r{self.number}_{self.token}"

    @property
    def topic(self) -> str:
        return f"antithesis-upsert-recovery-r{self.number}-{self.token}"


def process_token(pid: int) -> str | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return None if fields[0] == "Z" else fields[19]


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE)
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "CREATE TABLE IF NOT EXISTS rounds (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " token TEXT NOT NULL, owner_pid INTEGER NOT NULL, owner_token TEXT NOT NULL,"
            " session_id TEXT, schema_id TEXT, source_id TEXT,"
            " broker_state TEXT NOT NULL DEFAULT 'uncreated', ddl_ambiguous INTEGER NOT NULL DEFAULT 0)"
        )
        if "cleanup_session_id" not in {
            row[1] for row in db.execute("PRAGMA table_info(rounds)")
        }:
            db.execute("ALTER TABLE rounds ADD COLUMN cleanup_session_id TEXT")
        if "topic_id" not in {
            row[1] for row in db.execute("PRAGMA table_info(rounds)")
        }:
            db.execute("ALTER TABLE rounds ADD COLUMN topic_id TEXT")
        db.execute(
            "CREATE TABLE IF NOT EXISTS records (run INTEGER, partition INTEGER,"
            " offset INTEGER, key TEXT, value TEXT, PRIMARY KEY (run,partition,offset))"
        )
    return db


def allocate(db: sqlite3.Connection) -> Fixture:
    if db.execute("SELECT count(*) FROM rounds").fetchone()[0] >= MAX_PENDING:
        raise Incomplete("unresolved fixture cleanup reached its bound")
    marker = process_token(os.getpid())
    if marker is None:
        raise Incomplete("cannot identify the fixture owner")
    token = f"{rng.getrandbits(80):020x}"
    with db:
        number = db.execute(
            "INSERT INTO rounds (token,owner_pid,owner_token) VALUES (?,?,?)",
            (token, os.getpid(), marker),
        ).lastrowid
    assert number is not None
    return Fixture(number, token)


def session_id(conn: psycopg.Connection) -> str:
    row = conn.execute(
        "SELECT id::text FROM mz_internal.mz_sessions WHERE connection_id=pg_backend_pid()"
    ).fetchone()
    if row is None:
        raise Incomplete("original SQL session identity is unavailable")
    return str(row[0])


def ddl(
    db: sqlite3.Connection,
    fixture: Fixture,
    conn: psycopg.Connection,
    text: str,
    *,
    cleanup: bool = False,
) -> None:
    identity = session_id(conn)
    # A dead client can leave purification running. Retain its SQL identity
    # before sending DDL, so absence can later be checked with a strict read.
    column = "cleanup_session_id" if cleanup else "session_id"
    with db:
        db.execute(
            f"UPDATE rounds SET {column}=?,ddl_ambiguous=1 WHERE id=?",
            (identity, fixture.number),
        )
    conn.execute(text.encode())
    with db:
        db.execute("UPDATE rounds SET ddl_ambiguous=0 WHERE id=?", (fixture.number,))


def bind_ids(
    db: sqlite3.Connection, fixture: Fixture, conn: psycopg.Connection
) -> tuple[str, str]:
    row = conn.execute(
        "SELECT s.id,o.id FROM mz_schemas s LEFT JOIN mz_sources o ON o.schema_id=s.id"
        " AND o.name='src' WHERE s.name=%s AND s.database_id=(SELECT id FROM mz_databases WHERE name='materialize')",
        (fixture.schema,),
    ).fetchone()
    if row is None or row[1] is None:
        raise Incomplete("new source identity is unavailable")
    with db:
        db.execute(
            "UPDATE rounds SET schema_id=?,source_id=? WHERE id=?",
            (str(row[0]), str(row[1]), fixture.number),
        )
    return str(row[0]), str(row[1])


def admin(broker: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": broker, "socket.timeout.ms": 5000})


def topic_identity(client: AdminClient, topic: str) -> str:
    description = client.describe_topics(TopicCollection([topic]), request_timeout=5)[
        topic
    ].result(timeout=6)
    identity = str(description.topic_id)
    if not identity or identity == str(Uuid(0, 0)):
        raise Incomplete("broker did not provide a topic identity")
    return identity


def create_topic(
    db: sqlite3.Connection, fixture: Fixture, broker: str, partitions: int
) -> None:
    with db:
        db.execute(
            "UPDATE rounds SET broker_state='creating' WHERE id=?", (fixture.number,)
        )
    client = admin(broker)
    future = client.create_topics(
        [NewTopic(fixture.topic, num_partitions=partitions, replication_factor=1)],
        operation_timeout=5,
    )[fixture.topic]
    future.result(timeout=6)
    with db:
        db.execute(
            "UPDATE rounds SET broker_state='live' WHERE id=?", (fixture.number,)
        )
    identity = topic_identity(client, fixture.topic)
    with db:
        db.execute(
            "UPDATE rounds SET topic_id=? WHERE id=?", (identity, fixture.number)
        )


def produce(
    db: sqlite3.Connection,
    fixture: Fixture,
    producer: Producer,
    messages: list[tuple[int, str, str | None]],
    deadline: float,
) -> list[Record]:
    acknowledged: list[Record] = []
    failed = []

    def delivered(error: Any, message: Any) -> None:
        if error is not None:
            failed.append(str(error))
            return
        value = message.value()
        record = Record(
            message.partition(),
            message.offset(),
            message.key().decode(),
            value.decode() if value is not None else None,
        )
        with db:
            db.execute(
                "INSERT INTO records VALUES (?,?,?,?,?)",
                (
                    fixture.number,
                    record.partition,
                    record.offset,
                    record.key,
                    record.value,
                ),
            )
        acknowledged.append(record)

    for partition, key, value in messages:
        producer.produce(
            fixture.topic,
            partition=partition,
            key=key.encode(),
            value=value.encode() if value is not None else None,
            on_delivery=delivered,
        )
    while (
        len(acknowledged) + len(failed) < len(messages) and time.monotonic() < deadline
    ):
        producer.poll(min(0.1, max(0.0, deadline - time.monotonic())))
    if failed or len(acknowledged) != len(messages):
        raise Incomplete("Kafka delivery outcomes are not all acknowledged")
    return acknowledged


def cleanup(
    host: str,
    endpoints: Endpoints,
    db: sqlite3.Connection,
    *,
    current: int | None = None,
) -> tuple[bool, dict[str, Any]]:
    pending = []
    sql_absent: set[int] = set()

    def result(error: Exception | None = None) -> tuple[bool, dict[str, Any]]:
        retained = [row[0] for row in db.execute("SELECT id FROM rounds")]
        details: dict[str, Any] = {
            "pending": pending,
            "sql_fixtures_absent": all(number in sql_absent for number in retained),
            "broker_cleanup_complete": not retained,
        }
        if error is not None:
            details["error"] = f"{type(error).__name__}: {error}"
        return not retained, details

    if ddl_hold.active():
        return False, {
            "held": True,
            "sql_fixtures_absent": False,
            "broker_cleanup_complete": False,
        }
    rows = db.execute(
        "SELECT id,token,owner_pid,owner_token,session_id,schema_id,source_id,broker_state,cleanup_session_id,topic_id FROM rounds ORDER BY id LIMIT ?",
        (MAX_PENDING,),
    ).fetchall()
    if not rows:
        return result()
    try:
        with sql.connection(
            host, internal=True, connect_timeout=3, statement_timeout_ms=5000
        ) as conn:
            conn.execute("SET transaction_isolation='strict serializable'")
            for (
                number,
                token,
                pid,
                marker,
                session,
                schema_id,
                source_id,
                broker_state,
                cleanup_session,
                topic_id,
            ) in rows:
                fixture = Fixture(number, token)
                if number != current and process_token(pid) == marker:
                    pending.append({"run": number, "reason": "owner is active"})
                    continue
                if (
                    session
                    and conn.execute(
                        "SELECT 1 FROM mz_internal.mz_sessions WHERE id=%s::uuid",
                        (session,),
                    ).fetchone()
                ):
                    pending.append(
                        {"run": number, "reason": "original SQL session is active"}
                    )
                    continue
                if (
                    cleanup_session
                    and conn.execute(
                        "SELECT 1 FROM mz_internal.mz_sessions WHERE id=%s::uuid",
                        (cleanup_session,),
                    ).fetchone()
                ):
                    pending.append(
                        {"run": number, "reason": "prior cleanup SQL session is active"}
                    )
                    continue
                present = conn.execute(
                    "SELECT id FROM mz_schemas WHERE name=%s AND database_id=(SELECT id FROM mz_databases WHERE name='materialize')",
                    (fixture.schema,),
                ).fetchone()
                if present is not None:
                    if schema_id is not None and str(present[0]) != schema_id:
                        pending.append(
                            {"run": number, "reason": "schema incarnation changed"}
                        )
                        continue
                    if (
                        source_id is not None
                        and not conn.execute(
                            "SELECT 1 FROM mz_sources WHERE id=%s AND schema_id=%s",
                            (source_id, str(present[0])),
                        ).fetchone()
                    ):
                        pending.append(
                            {"run": number, "reason": "source incarnation changed"}
                        )
                        continue
                    ddl(
                        db,
                        fixture,
                        conn,
                        f"DROP SCHEMA {fixture.schema} CASCADE",
                        cleanup=True,
                    )
                if conn.execute(
                    "SELECT 1 FROM mz_schemas WHERE name=%s AND database_id=(SELECT id FROM mz_databases WHERE name='materialize')",
                    (fixture.schema,),
                ).fetchone():
                    pending.append(
                        {"run": number, "reason": "SQL namespace is still present"}
                    )
                    continue
                if process_token(pid) != marker:
                    sql_absent.add(number)
                client = admin(endpoints.kafka_broker)
                metadata = client.list_topics(timeout=5).topics.get(fixture.topic)
                if (
                    metadata is not None
                    and metadata.error is not None
                    and metadata.error.code() != KafkaError.UNKNOWN_TOPIC_OR_PART
                ):
                    raise Incomplete("Kafka metadata cannot establish topic presence")
                exists = metadata is not None and metadata.error is None
                if broker_state == "creating" and not exists:
                    pending.append(
                        {"run": number, "reason": "Kafka creation is unresolved"}
                    )
                    continue
                if exists:
                    observed_topic = topic_identity(client, fixture.topic)
                    if topic_id is not None and observed_topic != topic_id:
                        pending.append(
                            {"run": number, "reason": "Kafka topic incarnation changed"}
                        )
                        continue
                    with db:
                        db.execute(
                            "UPDATE rounds SET broker_state='deleting',topic_id=? WHERE id=?",
                            (observed_topic, number),
                        )
                    try:
                        client.delete_topics([fixture.topic], operation_timeout=5)[
                            fixture.topic
                        ].result(timeout=6)
                    except KafkaException as error:
                        if error.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                            raise
                with db:
                    db.execute("DELETE FROM records WHERE run=?", (number,))
                    db.execute("DELETE FROM rounds WHERE id=?", (number,))
    except Exception as error:
        return result(error)
    return result()
