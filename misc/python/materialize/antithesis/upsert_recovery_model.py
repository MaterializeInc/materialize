# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""An input-derived model of keyed JSON values, decode errors, and tombstones."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    key: str
    value: str | None


def canonical(value: str) -> str:
    """One text per JSON value, or `ValueError` for text that is not JSON.

    Numbers compare by value, as in `jsonb`, which stores them as `numeric`:
    `1e2`, `100` and `100.0` are one value, and so are `0` and `-0.0`. A
    repeated key keeps its last value.
    """

    def invalid(token: str) -> None:
        raise ValueError(f"invalid JSON constant: {token}")

    def text(node: Any) -> str:
        if isinstance(node, dict):
            members = (
                f"{json.dumps(k, ensure_ascii=False)}:{text(node[k])}"
                for k in sorted(node)
            )
            return "{" + ",".join(members) + "}"
        if isinstance(node, list):
            return "[" + ",".join(text(item) for item in node) + "]"
        if isinstance(node, Decimal):
            return format(node.normalize(), "f") if node else "0"
        return json.dumps(node, ensure_ascii=False)

    return text(
        json.loads(
            value, parse_constant=invalid, parse_float=Decimal, parse_int=Decimal
        )
    )


def expected(records: Sequence[Record]) -> tuple[Counter[tuple[str, str]], set[str]]:
    latest: dict[str, Record] = {}
    offsets: dict[tuple[int, int], Record] = {}
    for record in sorted(records, key=lambda row: (row.partition, row.offset)):
        identity = (record.partition, record.offset)
        if identity in offsets and offsets[identity] != record:
            raise ValueError("conflicting acknowledged Kafka offset")
        offsets[identity] = record
        if record.key in latest and latest[record.key].partition != record.partition:
            raise ValueError("a model key changed Kafka partitions")
        latest[record.key] = record
    rows: Counter[tuple[str, str]] = Counter()
    errors = set()
    for key, record in latest.items():
        if record.value is None:
            continue
        try:
            rows[(key, canonical(record.value))] += 1
        except ValueError:
            errors.add(key)
    return rows, errors


def validate(
    records: Sequence[Record], rows: Sequence[Sequence[Any]] | None, decode_error: bool
) -> list[str]:
    wanted, errors = expected(records)
    if bool(errors) != decode_error:
        return [f"decode-error state differs: expected keys={sorted(errors)}"]
    if errors:
        return []
    if rows is None:
        return ["no successful JSON source read"]
    actual: Counter[tuple[str, str]] = Counter()
    for row in rows:
        if (
            not isinstance(row, (tuple, list))
            or len(row) != 2
            or not isinstance(row[0], str)
            or not isinstance(row[1], str)
        ):
            return ["malformed upsert row or SQL NULL JSON value"]
        try:
            actual[(row[0], canonical(row[1]))] += 1
        except ValueError:
            return ["successful JSON source returned invalid JSON"]
    if actual != wanted:
        return [
            f"latest acknowledged values differ: expected={wanted}, actual={actual}"
        ]
    return []


def validate_inline(
    records: Sequence[Record], rows: Sequence[Sequence[Any]]
) -> list[str]:
    """Judge a read of an export that reports value decode errors inline.

    Each row is the key, the value's text or None, and the error or None. A
    key whose latest value does not decode must show an error and no value,
    every other key its latest value and no error.
    """
    wanted, errors = expected(records)
    seen: dict[str, tuple[Any, Any]] = {}
    for row in rows:
        if len(row) != 3 or not isinstance(row[0], str):
            return ["malformed inline-error row"]
        if row[0] in seen:
            return [f"key {row[0]!r} has more than one row"]
        seen[row[0]] = (row[1], row[2])
    problems = []
    for key in sorted(errors):
        value, error = seen.pop(key, (None, None))
        if error is None or value is not None:
            problems.append(f"key {key!r} shows value {value!r} and error {error!r}")
    for key, document in sorted(wanted):
        value, error = seen.pop(key, (None, None))
        try:
            same = error is None and value is not None and canonical(value) == document
        except ValueError:
            same = False
        if not same:
            problems.append(
                f"key {key!r} shows value {value!r} and error {error!r},"
                f" expected {document!r}"
            )
    problems.extend(f"unexpected key {key!r}" for key in sorted(seen))
    return problems


def offsets(records: Sequence[Record]) -> dict[int, int]:
    result: dict[int, int] = {}
    for record in records:
        result[record.partition] = max(
            result.get(record.partition, 0), record.offset + 1
        )
    return result
