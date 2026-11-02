# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check COPY aborts, transaction visibility, and reuse of the original session."""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)
from psycopg.abc import Buffer, PQGen
from psycopg.copy import LibpqWriter
from psycopg.generators import copy_end, copy_to
from psycopg.pq import TransactionStatus

from materialize.antithesis import telemetry
from materialize.antithesis.observations import sometimes

FAMILY = "copy_recovery"
DEFAULT_ABORT_BYTES = 64 * 1024
# copy_from_inner dispatches complete input chunks to workers at this size.
STREAMING_BATCH_BYTES = 32 * 1024 * 1024
LARGE_ABORT_BYTES = STREAMING_BATCH_BYTES + 1024 * 1024
MAX_ABORT_BYTES = LARGE_ABORT_BYTES
MAX_CHUNK_BYTES = 128 * 1024
MAX_PLAN_ROWS = 32
Row = tuple[int, int | None, str | None]
Execute = Callable[[psycopg.Connection, float, str, Any], psycopg.Cursor]
Ddl = Callable[[psycopg.Connection, float, str], None]
T = TypeVar("T")


class Incomplete(RuntimeError):
    """A COPY protocol boundary or correctness check was not reached."""


class _AbortCopy(Exception):
    pass


class _Writer(LibpqWriter):
    def __init__(self, cursor: psycopg.Cursor, deadline: float, token: str) -> None:
        super().__init__(cursor)
        self.deadline = deadline
        self.token = token
        self.confirmed_abort = False
        self.sent_bytes = 0
        self.sent_chunks = 0
        self.poisoned = False

    def _wait(self, operation: PQGen[T]) -> T:
        def bounded() -> PQGen[T]:
            ready: Any = None
            while True:
                if time.monotonic() >= self.deadline:
                    self.poisoned = True
                    self.connection.close()
                    raise Incomplete("COPY protocol deadline elapsed")
                try:
                    wait = operation.send(ready)
                except StopIteration as done:
                    return done.value
                ready = yield wait

        return self.connection.wait(bounded(), interval=0.1)

    def write(self, data: Buffer) -> None:
        for offset in range(0, len(data), MAX_CHUNK_BYTES):
            chunk = data[offset : offset + MAX_CHUNK_BYTES]
            self._wait(copy_to(self._pgconn, chunk, flush=True))
            self.sent_bytes += len(chunk)
            self.sent_chunks += 1

    def finish(self, exc: BaseException | None = None) -> None:
        if self.poisoned:
            raise Incomplete("COPY connection was closed after its deadline")
        if not isinstance(exc, _AbortCopy):
            # The default implementation suppresses QueryCanceled after a
            # CopyFail. Only our own token establishes the no-write boundary.
            if exc is not None:
                self.connection.close()
                return
            result = self._wait(copy_end(self._pgconn, None))
            self.cursor._results = [result]
            return
        try:
            self._wait(copy_end(self._pgconn, self.token.encode()))
        except psycopg.errors.QueryCanceled as error:
            if error.diag.message_primary != f"COPY from stdin failed: {self.token}":
                raise
            self.confirmed_abort = True
        else:
            _check(False, True, "table", "CopyFail response", {})


def encode_rows(rows: Sequence[Row]) -> bytes:
    def cell(value: int | str | None) -> str:
        if value is None:
            return r"\N"
        return (
            str(value)
            .replace("\\", "\\\\")
            .replace("\t", r"\t")
            .replace("\n", r"\n")
            .replace("\r", r"\r")
        )

    return "".join("\t".join(map(cell, row)) + "\n" for row in rows).encode()


def _check(
    actual: Any, expected: Any, path: str, stage: str, details: dict[str, Any]
) -> None:
    passed = actual == expected
    telemetry.comparison(FAMILY, path, passed)
    always(
        passed,
        "copy recovery: COPY visibility and session state match acknowledged inputs",
        {**details, "stage": stage, "expected": expected, "actual": actual},
    )
    if not passed:
        raise Incomplete("COPY protocol violated its acknowledged contract")


def _rows(
    conn: psycopg.Connection,
    table: str,
    expected: Counter[Row],
    deadline: float,
    execute: Execute,
    stage: str,
    details: dict[str, Any],
) -> None:
    rows = execute(conn, deadline, f"SELECT id,v,tag FROM {table}", None).fetchall()
    valid = all(
        isinstance(row, (tuple, list))
        and len(row) == 3
        and type(row[0]) is int
        and (row[1] is None or type(row[1]) is int)
        and (row[2] is None or type(row[2]) is str)
        for row in rows
    )
    _check(valid, True, "table", stage + " row types", details)

    def encode(bag: Counter) -> list:
        return [[list(row), count] for row, count in sorted(bag.items(), key=repr)]

    _check(
        encode(Counter(tuple(row) for row in rows)),
        encode(expected),
        "table",
        stage,
        details,
    )


def run_round(
    admin: psycopg.Connection,
    worker: psycopg.Connection,
    peer: psycopg.Connection,
    *,
    schema: str,
    rows: Sequence[Row],
    deadline: float,
    execute: Execute,
    ddl: Ddl,
    abort_bytes: int = DEFAULT_ABORT_BYTES,
    chunk_bytes: int = 4093,
    details: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Check COPY recovery on distinct, idle autocommit sessions in an owned schema."""
    connections = (admin, worker, peer)
    if len({id(conn) for conn in connections}) != 3 or any(
        not conn.autocommit or conn.info.transaction_status != TransactionStatus.IDLE
        for conn in connections
    ):
        raise ValueError("COPY requires three distinct idle autocommit sessions")
    if (
        not 1 <= len(rows) <= MAX_PLAN_ROWS
        or not 1 <= abort_bytes <= MAX_ABORT_BYTES
        or not 1 <= chunk_bytes <= MAX_CHUNK_BYTES
    ):
        raise ValueError("COPY plan must have bounded rows, bytes, and chunks")
    details = {} if details is None else details
    table = f"{schema}.copy_t"
    ddl(admin, deadline, f"CREATE TABLE {table}(id bigint,v bigint,tag text)")
    marks = ",".join("(%s,%s,%s)" for _ in rows)
    execute(
        admin,
        deadline,
        f"INSERT INTO {table} VALUES {marks}",
        [cell for row in rows for cell in row],
    )
    expected = Counter(rows)
    first = rows[0]
    batch = (
        *rows,
        first,
        (first[0], None, None),
        (first[0], None, r"\N"),
        (first[0], -7, "tab\tline\nslash\\return\r雪"),
    )
    payload = encode_rows(batch)
    execute(
        worker,
        deadline,
        f'PREPARE "Copy Recovery" AS SELECT count(*) FROM {table}',
        None,
    )

    def check(stage: str) -> None:
        _rows(peer, table, expected, deadline, execute, stage, details)
        _check(
            execute(worker, deadline, 'EXECUTE "Copy Recovery"', None).fetchall(),
            [(sum(expected.values()),)],
            "prepared",
            stage + " prepared reuse",
            details,
        )

    copy_sequence = 0

    def send(cursor: psycopg.Cursor, abort: int = 0) -> _Writer:
        nonlocal copy_sequence
        copy_sequence += 1
        writer = _Writer(cursor, deadline, f"copy-abort-{schema}-{copy_sequence}")
        try:
            with cursor.copy(
                f"COPY {table} FROM STDIN".encode(), writer=writer
            ) as copy:
                if abort:
                    padded = encode_rows(
                        [
                            (key, value, (tag or "") + "x" * 1024)
                            for key, value, tag in batch
                        ]
                    )
                    block = padded * max(1, MAX_CHUNK_BYTES // len(padded))
                    while writer.sent_bytes < abort:
                        copy.write(block[: min(len(block), abort - writer.sent_bytes)])
                else:
                    offset = 0
                    for _ in range(32):
                        if offset >= len(payload):
                            break
                        copy.write(payload[offset : offset + chunk_bytes])
                        offset += chunk_bytes
                    if offset < len(payload):
                        copy.write(payload[offset:])
                _rows(
                    peer,
                    table,
                    expected,
                    deadline,
                    execute,
                    "COPY input before CopyDone",
                    details,
                )
                if abort:
                    raise _AbortCopy()
        except _AbortCopy:
            _check(
                writer.confirmed_abort, True, "table", "confirmed own CopyFail", details
            )
        if not abort:
            _check(cursor.rowcount, len(batch), "table", "CopyDone row count", details)
        return writer

    sent = chunks = 0
    for explicit in (False, True):
        if explicit:
            execute(worker, deadline, "BEGIN", None)
        with worker.cursor() as cursor:
            writer = send(cursor, DEFAULT_ABORT_BYTES if explicit else abort_bytes)
            sent += writer.sent_bytes
            chunks += writer.sent_chunks
        _check(
            worker.info.transaction_status,
            TransactionStatus.INERROR if explicit else TransactionStatus.IDLE,
            "prepared",
            "transaction after CopyFail",
            details,
        )
        if explicit:
            execute(worker, deadline, "ROLLBACK", None)
        check("after CopyFail")

    with worker.cursor() as cursor:
        send(cursor)
    expected.update(batch)
    check("autocommit CopyDone")

    for ending in ("ROLLBACK", "COMMIT"):
        execute(worker, deadline, "BEGIN", None)
        with worker.cursor() as cursor:
            send(cursor)
        _rows(
            peer,
            table,
            expected,
            deadline,
            execute,
            "CopyDone before " + ending,
            details,
        )
        execute(worker, deadline, ending, None)
        if ending == "COMMIT":
            expected.update(batch)
        check("after " + ending)

    execute(worker, deadline, "BEGIN READ ONLY", None)
    execute(worker, deadline, "SELECT 1", None)
    rejected = False
    try:
        with worker.cursor() as cursor:
            send(cursor)
    except psycopg.Error as error:
        if (
            error.sqlstate != "25006"
            or error.diag.message_primary != "transaction in read-only mode"
        ):
            raise
        rejected = True
    _check(rejected, True, "table", "READ ONLY COPY rejection", details)
    execute(worker, deadline, "ROLLBACK", None)
    check("after READ ONLY rejection")
    result = {
        "abort_bytes": sent,
        "abort_chunks": chunks,
        "rows_per_copy": len(batch),
        "committed_copy_rows": 2 * len(batch),
    }
    sometimes(
        True,
        "copy recovery: confirmed CopyFail discards flushed input and preserves the original session",
        {**details, **result},
    )
    sometimes(
        True,
        "copy recovery: COPY commits and rollbacks match acknowledged input bags",
        {**details, **result},
    )
    sometimes(
        True,
        "copy recovery: READ ONLY rejects COPY without row effects",
        {**details, **result},
    )
    sometimes(
        True,
        "copy recovery: COPY preserves escaped text NULL tokens and duplicate rows",
        {**details, **result},
    )
    sometimes(
        abort_bytes > STREAMING_BATCH_BYTES,
        "copy recovery: confirmed CopyFail follows more than 32 MiB of flushed input",
        {**details, **result},
    )
    return result
