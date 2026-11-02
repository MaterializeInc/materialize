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

from materialize.antithesis import telemetry

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
