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
from typing import Any


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    key: str
    value: str | None


def canonical(value: str) -> str:
    def invalid(token: str) -> None:
        raise ValueError(f"invalid JSON constant: {token}")

    return json.dumps(
        json.loads(value, parse_constant=invalid),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
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


def offsets(records: Sequence[Record]) -> dict[int, int]:
    result: dict[int, int] = {}
    for record in records:
        result[record.partition] = max(
            result.get(record.partition, 0), record.offset + 1
        )
    return result
