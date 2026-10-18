# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Data model for the schema evolution driver, free of the Antithesis SDK.

Every table starts as `(k int8 NOT NULL, w text DEFAULT 'w-default')`, and
`ALTER TABLE ADD COLUMN` appends nullable columns named `c<n>_<code>`, where
the code names the type (`COLUMN_TYPES`). A column's name alone determines the
value any row stores in it: an INSERT that names a column writes
`written_value(k, column)`, and one that omits it gets the column's default,
which is NULL for every added column. So the expected contents of a table
follow from the insert ledger (key and the set of columns each INSERT
supplied) and the table's current column list, whatever version a reader
resolved and whenever the column was added relative to the write.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

KEY = "k"
DEFAULTED = "w"
DEFAULT_VALUE = "w-default"
BASE_COLUMNS = (KEY, DEFAULTED)
BASE_DDL = f"({KEY} int8 NOT NULL, {DEFAULTED} text DEFAULT '{DEFAULT_VALUE}')"

COLUMN_TYPES = {"i4": "int4", "i8": "int8", "tx": "text", "nm": "numeric"}
_ADDED = re.compile(r"^c(\d+)_(" + "|".join(COLUMN_TYPES) + r")$")
# An added column in a table's `create_sql` with the version that added it:
# `"c3_tx" [s46 AS "pg_catalog"."text"] VERSION ADDED 2` in the stored form
# that `mz_tables.create_sql` shows, `c3_tx pg_catalog.text VERSION ADDED 2`
# in `SHOW CREATE TABLE`.
_VERSION_ADDED = re.compile(r'\b(c\d+_[a-z0-9]+)"? [^,()]*?VERSION ADDED (\d+)')


def added_column(n: int, code: str) -> str:
    assert code in COLUMN_TYPES, code
    return f"c{n}_{code}"


def is_model_column(name: str) -> bool:
    return name in BASE_COLUMNS or _ADDED.match(name) is not None


def written_value(k: int, column: str) -> str:
    """The value, in text form, that an INSERT naming `column` stores for key `k`."""
    if column == KEY:
        return str(k)
    if column == DEFAULTED:
        return f"w{k}"
    m = _ADDED.match(column)
    if m is None:
        raise ValueError(f"not a model column: {column}")
    n, code = int(m[1]), m[2]
    if code == "tx":
        return f"{k}:{n}"
    # Fits int4 for every key the driver allocates (well below 2 million).
    return str(k * 1000 + n % 1000)


def sql_literal(k: int, column: str) -> str:
    v = written_value(k, column)
    m = _ADDED.match(column)
    if column == DEFAULTED or (m is not None and m[2] == "tx"):
        return "'" + v.replace("'", "''") + "'"
    return v


def default_value(column: str) -> str | None:
    return DEFAULT_VALUE if column == DEFAULTED else None


def expected_row(k: int, written: Iterable[str], columns: Sequence[str]) -> tuple:
    """The row key `k` reads as under `columns`, given the columns its INSERT named."""
    named = set(written)
    return tuple(
        written_value(k, c) if c in named else default_value(c) for c in columns
    )


def normalize(row: Iterable[object]) -> tuple:
    """Text form of a row read through psycopg, matching `written_value`."""
    return tuple(None if v is None else str(v) for v in row)


def version_columns(create_sql: str, version: int) -> list[str]:
    """Columns of table version `version`, from the table's `create_sql`.

    Version 0 has the base columns. Each `ADD COLUMN` creates the next version
    and records `VERSION ADDED <n>` on its column.
    """
    added = sorted(
        ((int(v), name) for name, v in _VERSION_ADDED.findall(create_sql)),
        key=lambda x: x[0],
    )
    return [*BASE_COLUMNS, *(name for v, name in added if v <= version)]


def latest_version(create_sql: str) -> int:
    return max((int(v) for _, v in _VERSION_ADDED.findall(create_sql)), default=0)


@dataclass(frozen=True)
class Insert:
    """One INSERT statement in the ledger."""

    keys: tuple[int, ...]
    written: tuple[str, ...]
    """Columns the statement supplied values for, `KEY` always among them."""
    outcome: str
    """`ok`, `rejected`, `indeterminate`, or `pending` (no response recorded)."""
    invoke: float
    complete: float | None


@dataclass
class TableVerdict:
    missing: list[int] = field(default_factory=list)
    """Keys acknowledged before the read began that the read lacks."""
    unexpected: list[int] = field(default_factory=list)
    """Keys present that no INSERT could have written by the time the read
    completed: never attempted, attempted after, or definitely rejected."""
    duplicated: list[int] = field(default_factory=list)
    wrong: list[dict] = field(default_factory=list)
    """Rows whose values differ from `expected_row`."""
    unmodeled_columns: list[str] = field(default_factory=list)
    rows: int = 0
    with_added_column: int = 0
    """Rows that hold a non-NULL value in an added column."""

    @property
    def complete(self) -> bool:
        return not self.missing

    @property
    def sound(self) -> bool:
        return not (self.unexpected or self.duplicated)

    @property
    def values_ok(self) -> bool:
        return not (self.wrong or self.unmodeled_columns)


def check_table(
    columns: Sequence[str],
    rows: Iterable[tuple],
    inserts: Iterable[Insert],
    read_start: float,
    read_end: float,
    limit: int = 10,
) -> TableVerdict:
    """Compare a full read of a table against the insert ledger.

    `read_start` and `read_end` bracket the read in the clock the ledger
    uses. Under strict serializable reads, a row whose INSERT was acknowledged
    before `read_start` must be present, and a row whose INSERT was first
    invoked after `read_end` must be absent. Rows from INSERTs without a
    definite outcome may go either way.
    """
    v = TableVerdict()
    v.unmodeled_columns = [c for c in columns if not is_model_column(c)]
    if KEY not in columns:
        v.unmodeled_columns.append(f"<no {KEY} column>")
        return v
    key_at = columns.index(KEY)
    added_at = [i for i, c in enumerate(columns) if c not in BASE_COLUMNS]

    by_key: dict[int, list[Insert]] = {}
    for ins in inserts:
        for k in ins.keys:
            by_key.setdefault(k, []).append(ins)

    seen: Counter[int] = Counter()
    for raw in rows:
        row = normalize(raw)
        v.rows += 1
        k = int(row[key_at])
        seen[k] += 1
        if any(row[i] is not None for i in added_at):
            v.with_added_column += 1
        candidates = [
            ins
            for ins in by_key.get(k, [])
            if ins.outcome != "rejected" and ins.invoke <= read_end
        ]
        if not candidates:
            if len(v.unexpected) < limit:
                v.unexpected.append(k)
            continue
        if v.unmodeled_columns:
            continue
        if not any(row == expected_row(k, ins.written, columns) for ins in candidates):
            if len(v.wrong) < limit:
                v.wrong.append(
                    {
                        "k": k,
                        "row": list(row),
                        "expected": [
                            list(expected_row(k, ins.written, columns))
                            for ins in candidates
                        ],
                    }
                )
    v.duplicated = [k for k, n in seen.items() if n > 1][:limit]
    for k, ins_list in by_key.items():
        acked = any(
            i.outcome == "ok" and i.complete is not None and i.complete < read_start
            for i in ins_list
        )
        if acked and k not in seen and len(v.missing) < limit:
            v.missing.append(k)
    return v


def project(
    columns: Sequence[str],
    rows: Iterable[tuple],
    keep: Sequence[str],
    not_null: str | None = None,
) -> Counter[tuple]:
    """`SELECT keep FROM rows [WHERE not_null IS NOT NULL]` as a multiset."""
    idx = [columns.index(c) for c in keep]
    filt = None if not_null is None else columns.index(not_null)
    out: Counter[tuple] = Counter()
    for raw in rows:
        row = normalize(raw)
        if filt is not None and row[filt] is None:
            continue
        out[tuple(row[i] for i in idx)] += 1
    return out
