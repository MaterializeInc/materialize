# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""COPY: batches arrive whole or not at all, and leave as they were stored.

`COPY ... FROM STDIN` loads a batch of rows in text or csv format, split into
chunks at arbitrary bytes. Every row carries its batch, the batch's size and
its position in it, so one read can judge a batch: it shows every row of the
batch exactly once or none of them. An acknowledged batch is visible to every
later read. A batch never shows a row if the driver ended the COPY with an
error or by closing the connection before it completed, or if the system
rejected it. After any other error a batch may or may not arrive, but whole.

`COPY (SELECT ...) TO STDOUT` in text, csv or binary format must return
exactly the rows the same `SELECT` returns in the same read transaction.

The rows hold a column per data type, with the values of
`datatypes.row_values` for a seed the row also carries. So both directions
carry every type, and every value is compared with the one its seed
determines, whichever COPY wrote the row.
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis_driver.client import Op, Outcome, Status, commit
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.datatypes import (
    COLUMNS,
    SEEDS,
    column_ddl,
    row_values,
)
from materialize.antithesis_driver.draw import Draw

SCHEMA = "copies"
SIZES = (1, 2, 5, 12, 30)
PRUNE_ABOVE = 10
"""Settled batches kept before their rows are deleted."""
INPUT_FORMATS = ("text", "csv")
OUTPUT_FORMATS = ("text", "csv", "binary")
ENDINGS = ("done", "done", "done", "done", "fail", "drop")
"""How the driver ends a COPY: completes it, fails it with an error, or
closes the connection in the middle of it."""
SETTINGS = {"transaction_isolation": "'strict serializable'", "cluster": "quickstart"}
Fields = Sequence[str | None]
Rows = list[tuple[Any, ...]]


def setup_statements() -> list[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t"
        f" (batch bigint, size int4, n int4, seed bigint, {column_ddl()})",
    ]


def input_fields(batch: int, size: int, n: int, seed: int) -> list[str | None]:
    """A row as COPY FROM takes it: each column's input text."""
    return [str(batch), str(size), str(n), str(seed)] + [
        None if v is None else v.text if c.rounds else v.input
        for c, v in zip(COLUMNS, row_values(seed))
    ]


def wire_fields(seed: int) -> list[str | None]:
    """A row's data columns as COPY TO renders them in text and csv format."""
    return [
        None if v is None else c.wire(v.text) for c, v in zip(COLUMNS, row_values(seed))
    ]


_TEXT_ESCAPES = str.maketrans({"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r"})


def text_line(fields: Fields) -> str:
    return (
        "\t".join("\\N" if f is None else f.translate(_TEXT_ESCAPES) for f in fields)
        + "\n"
    )


def csv_line(fields: Fields) -> str:
    def field(f: str | None) -> str:
        if f is None:
            return ""
        # An unquoted empty field is NULL, so the empty string is quoted.
        if f and not any(c in f for c in ',"\n\r'):
            return f
        return '"' + f.replace('"', '""') + '"'

    return ",".join(field(f) for f in fields) + "\n"


def parse_csv(text: str) -> list[list[str | None]]:
    """The rows of COPY's csv output, with an unquoted empty field as NULL."""
    rows: list[list[str | None]] = []
    row: list[str | None] = []
    field: list[str] = []
    quoted = in_quotes = False
    i = 0
    while i < len(text):
        c = text[i]
        if in_quotes:
            if c != '"':
                field.append(c)
            elif text[i + 1 : i + 2] == '"':
                field.append('"')
                i += 1
            else:
                in_quotes = False
        elif c == '"':
            quoted = in_quotes = True
        elif c in ",\n":
            row.append("".join(field) if field or quoted else None)
            field, quoted = [], False
            if c == "\n":
                rows.append(row)
                row = []
        else:
            field.append(c)
        i += 1
    return rows


def wrong_rows(copied: Sequence[Sequence[Any]]) -> list[dict[str, Any]]:
    """The rows of a text or csv COPY TO whose data columns are not what
    their seed determines."""
    return [
        {"batch": batch, "n": n, "seed": seed}
        for batch, _, n, seed, *fields in copied
        if fields != wire_fields(int(seed))
    ]


def chunks(data: bytes, cuts: Sequence[int]) -> list[bytes]:
    """`data` cut at the given offsets, which may fall inside a row."""
    bounds = [0, *sorted({c for c in cuts if 0 < c < len(data)}), len(data)]
    return [data[a:b] for a, b in zip(bounds, bounds[1:])]


@dataclass(frozen=True)
class Plan:
    """What a read may see of a batch."""

    seeds: tuple[int, ...]
    state: str
    """`visible`: acknowledged before the read began. `absent`: never
    completed, or rejected. `maybe`: anything else."""


def judge(
    rows: Sequence[Sequence[Any]], plans: dict[int, Plan]
) -> dict[str, list[Any]]:
    """Compare one read of the table with the batches the driver sent.

    A row is `batch, size, n, seed` and each data column as text.
    """
    errors: dict[str, list[Any]] = {
        "torn": [],
        "unsent": [],
        "missing": [],
        "values": [],
    }
    seen: dict[int, Counter[tuple[int, int, int]]] = {}
    for batch, size, n, seed, *texts in rows:
        seen.setdefault(batch, Counter())[(size, n, seed)] += 1
        expected = [None if v is None else v.text for v in row_values(seed)]
        for column, want, got in zip(COLUMNS, expected, texts):
            if want != got:
                errors["values"].append(
                    {
                        "batch": batch,
                        "n": n,
                        "seed": seed,
                        "column": column.name,
                        "expected": None if want is None else want[:200],
                        "observed": None if got is None else got[:200],
                    }
                )
    for batch, found in seen.items():
        plan = plans.get(batch)
        if plan is None or plan.state == "absent":
            errors["unsent"].append({"batch": batch, "rows": sum(found.values())})
            continue
        whole = Counter((len(plan.seeds), n, seed) for n, seed in enumerate(plan.seeds))
        if found != whole:
            errors["torn"].append(
                {
                    "batch": batch,
                    "size": len(plan.seeds),
                    "rows": sum(found.values()),
                    "missing": list(whole - found)[:3],
                    "extra": list(found - whole)[:3],
                }
            )
    errors["missing"] = [
        batch
        for batch, plan in plans.items()
        if plan.state == "visible" and batch not in seen
    ]
    return errors


@dataclass
class Batch:
    seeds: tuple[int, ...]
    format: str
    ending: str
    outcome: str = "pending"
    """`pending`, `ok`, `rejected` or `indeterminate`."""
    acked_at: float | None = None
    retired: bool = False
    """Its rows are being deleted, so a read may see all or none of them."""
    pruned_at: float | None = None
    """When the deletion was acknowledged. A read that began later sees none
    of its rows."""


class _Stop(Exception):
    """Ends a COPY that the driver does not complete."""


class Copies:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next = 1
        self.batches: dict[int, Batch] = {}
        self._pruning = False
        self._reads: dict[int, float] = {}
        """Reads in flight, each with a time no later than its start."""
        self._next_read = 0
        self.loaded: set[str] = set()
        """Input formats of which a check saw a batch of several rows."""
        self.unloaded: set[str] = set()
        """Output formats in which a COPY TO returned rows."""

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def copy_in(self, d: Draw) -> Status:
        """Load a batch, or delete old ones once enough have settled."""
        pruning = self._prune()
        if pruning is not None:
            return self.ctx.sessions.call(self._session(d), pruning, RULE_WAIT_SECONDS)
        size = d.choice(SIZES)
        batch = Batch(
            tuple(d.integer(0, SEEDS - 1) for _ in range(size)),
            d.choice(INPUT_FORMATS),
            d.choice(ENDINGS),
        )
        with self._lock:
            number = self._next
            self._next += 1
            self.batches[number] = batch
        line = csv_line if batch.format == "csv" else text_line
        data = "".join(
            line(input_fields(number, size, n, seed))
            for n, seed in enumerate(batch.seeds)
        ).encode()
        pieces = chunks(data, [d.integer(0, len(data)) for _ in range(d.integer(0, 3))])
        # An unfinished COPY sends some of its chunks, possibly all of them.
        sent = pieces if batch.ending == "done" else pieces[: d.integer(0, len(pieces))]
        statement = f"COPY {SCHEMA}.t FROM STDIN" + (
            " WITH (FORMAT CSV)" if batch.format == "csv" else ""
        )

        def execute(conn: psycopg.Connection) -> None:
            try:
                with conn.cursor().copy(statement.encode()) as copy:
                    for piece in sent:
                        copy.write(piece)
                    if batch.ending == "fail":
                        raise _Stop()
                    if batch.ending == "drop":
                        conn.close()
            except _Stop:
                pass
            except psycopg.Error:
                # Leaving the COPY block of a connection closed on purpose.
                if batch.ending != "drop":
                    raise

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                if batch.ending != "done" or outcome.rejected:
                    # The COPY was never completed, whatever else happened.
                    batch.outcome = "rejected"
                elif outcome.ok:
                    batch.outcome, batch.acked_at = "ok", outcome.end
                else:
                    batch.outcome = "indeterminate"
            if batch.ending != "done" and sent and not outcome.rejected:
                reachable(
                    "copy: the driver ended a COPY FROM STDIN after sending rows, without completing it",
                    {"ending": batch.ending, "format": batch.format},
                )

        return self.ctx.sessions.call(
            self._session(d),
            Op(f"copy from stdin ({batch.format})", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _plans(self, start: float) -> dict[int, Plan]:
        plans = {}
        for number, b in self.batches.items():
            if b.pruned_at is not None and b.pruned_at < start:
                continue
            if b.outcome == "rejected":
                state = "absent"
            elif (
                b.outcome == "ok"
                and not b.retired
                and b.acked_at is not None
                and b.acked_at < start
            ):
                state = "visible"
            else:
                state = "maybe"
            plans[number] = Plan(b.seeds, state)
        return plans

    def _read_op(self, done: list[bool]) -> Op:
        query = (
            "SELECT batch, size, n, seed, "
            + ", ".join(f"{c.name}::text" for c in COLUMNS)
            + f" FROM {SCHEMA}.t"
        )

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query.encode()).fetchall()

        with self._lock:
            token = self._next_read
            self._next_read += 1
            self._reads[token] = time.monotonic()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            with self._lock:
                plans = self._plans(outcome.start)
                # A read that began before a deletion was acknowledged may
                # still see its rows, so pruned batches stay in the model
                # until every such read is done.
                del self._reads[token]
                oldest = min(self._reads.values(), default=float("inf"))
                for number in [
                    n
                    for n, b in self.batches.items()
                    if b.pruned_at is not None and b.pruned_at < oldest
                ]:
                    del self.batches[number]
            if not outcome.ok:
                return
            assert outcome.value is not None
            self.compare(outcome.value, plans)
            done.append(True)

        return Op("copy check", execute, complete, SETTINGS)

    def compare(self, rows: list[tuple[Any, ...]], plans: dict[int, Plan]) -> None:
        errors = judge(rows, plans)
        details = {"rows": len(rows), "batches": len(plans)}
        always(
            not errors["torn"],
            "copy: a COPY FROM STDIN batch is visible whole and once, or not at all",
            {**details, "torn": errors["torn"][:5]},
        )
        always(
            not errors["unsent"],
            "copy: a COPY FROM STDIN that was rejected or never completed leaves no row",
            {**details, "unsent": errors["unsent"][:5]},
        )
        always(
            not errors["missing"],
            "copy: every row of an acknowledged COPY FROM STDIN is visible",
            {**details, "missing": errors["missing"][:10]},
        )
        always(
            not errors["values"],
            "copy: every copied column holds the value its row's seed determines",
            {**details, "values": errors["values"][:10]},
        )
        visible = {row[0] for row in rows}
        with self._lock:
            for number in visible:
                b = self.batches.get(number)
                if b is not None and len(b.seeds) > 1:
                    self.loaded.add(b.format)
            loaded = len(self.loaded)
            unknown = any(
                number in visible and b.outcome == "indeterminate"
                for number, b in self.batches.items()
            )
        sometimes(
            loaded == len(INPUT_FORMATS),
            "copy: checks saw batches of several rows loaded in text and in csv format",
            details,
        )
        sometimes(
            unknown,
            "copy: a check saw a batch whose COPY FROM STDIN outcome was indeterminate",
            details,
        )

    def check(self, d: Draw) -> Status:
        return self.ctx.sessions.call(
            self._session(d), self._read_op([]), RULE_WAIT_SECONDS
        )

    def copy_out(self, d: Draw) -> Status:
        format = d.choice(OUTPUT_FORMATS)
        columns = [c for c in COLUMNS if c.binary or format != "binary"]
        select = (
            "SELECT batch, size, n, seed, "
            + ", ".join(c.name for c in columns)
            + f" FROM {SCHEMA}.t"
        )
        statement = f"COPY ({select}) TO STDOUT WITH (FORMAT {format.upper()})"

        def execute(conn: psycopg.Connection) -> tuple[Rows, Rows]:
            conn.execute(b"BEGIN")
            result = conn.cursor(binary=format == "binary").execute(select.encode())
            raw = result.pgresult
            assert raw is not None
            queried = [
                tuple(raw.get_value(r, c) for c in range(raw.nfields))
                for r in range(raw.ntuples)
            ]
            with conn.cursor().copy(statement.encode()) as copy:
                if format == "csv":
                    text = b"".join(bytes(block) for block in copy).decode()
                    copied = [tuple(row) for row in parse_csv(text)]
                else:
                    copied = list(copy.rows())
            commit(conn)
            return queried, copied

        def complete(outcome: Outcome[tuple[Rows, Rows]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            queried, copied = outcome.value
            if format != "binary":
                queried = [
                    tuple(None if f is None else f.decode() for f in row)
                    for row in queried
                ]
            else:
                copied = [
                    tuple(None if f is None else bytes(f) for f in row)
                    for row in copied
                ]
            details = {"format": format, "queried": len(queried), "copied": len(copied)}
            difference = Counter(queried) - Counter(copied)
            surplus = Counter(copied) - Counter(queried)
            always(
                not difference and not surplus,
                "copy: COPY TO STDOUT returns the rows of the same query in the same transaction",
                {
                    **details,
                    "only_queried": [str(r)[:300] for r in list(difference)[:2]],
                    "only_copied": [str(r)[:300] for r in list(surplus)[:2]],
                },
            )
            if format != "binary":
                wrong = wrong_rows(copied)
                always(
                    not wrong,
                    "copy: COPY TO STDOUT carries every column as its row's seed determines",
                    {**details, "wrong": wrong[:5]},
                )
            with self._lock:
                if copied:
                    self.unloaded.add(format)
                unloaded = len(self.unloaded)
            sometimes(
                unloaded == len(OUTPUT_FORMATS),
                "copy: COPY TO STDOUT returned rows in text, csv and binary format",
                details,
            )

        return self.ctx.sessions.call(
            self._session(d),
            Op(f"copy to stdout ({format})", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _prune(self) -> Op | None:
        """Deletes the rows of the acknowledged batches, so the table stays small."""
        with self._lock:
            # A rejected batch has no rows, and `judge` reports a batch it
            # does not know as one that must not be visible.
            for number in [
                n for n, b in self.batches.items() if b.outcome == "rejected"
            ]:
                del self.batches[number]
            settled = [
                n
                for n, b in self.batches.items()
                if b.outcome == "ok" and not b.retired
            ]
            if self._pruning or len(settled) <= PRUNE_ABOVE:
                return None
            self._pruning = True
            for number in settled:
                self.batches[number].retired = True
        numbers = ", ".join(str(n) for n in settled)

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(f"DELETE FROM {SCHEMA}.t WHERE batch IN ({numbers})".encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self._pruning = False
                for number in settled:
                    if outcome.ok:
                        self.batches[number].pruned_at = outcome.end
                    elif outcome.rejected:
                        self.batches[number].retired = False

        return Op("copy prune", execute, complete, SETTINGS)

    def eventually(self, deadline: float) -> None:
        done: list[bool] = []
        while not done and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                if (
                    self.ctx.sessions.call(i, self._read_op(done), 60.0)
                    != Status.NOT_SENT
                ):
                    break
            if not done:
                time.sleep(1)
        always(
            bool(done),
            "copy: a read of every copied batch succeeds after faults stop",
            {},
        )
