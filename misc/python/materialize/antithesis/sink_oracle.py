# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Pure checks over the committed output of a Materialize Kafka sink.

Kept free of the Antithesis SDK and of any client so it can be unit tested
with synthetic message streams.

Wire contract, from `src/storage/src/sink/kafka.rs`:

- Every data message carries a `materialize-timestamp` header with the decimal
  mz timestamp of the update. The encoder emits one message per `DiffPair` at
  each `(key, timestamp)`, so with a unique sink key a `(key, timestamp)` pair
  identifies an update and must be committed at most once.
- Data and the progress record are produced in one Kafka transaction. The
  progress record has key `mz-sink-<id>` and a JSON payload
  `{"frontier": [t] or [], "version": v}`; `frontier` is the upper of
  everything the transaction (and every earlier one) committed, and `[]`
  means the sink is complete.
- Within a transaction updates are sent in timestamp order, transactions
  commit in frontier order, and a restarted sink resumes at the last
  committed frontier, dropping updates below it. So the committed timestamps
  in each partition are nondecreasing in offset order.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

TIMESTAMP_HEADER = "materialize-timestamp"


def freeze(value: Any) -> Any:
    """A hashable form of a decoded JSON value."""
    if isinstance(value, dict):
        return tuple(sorted((k, freeze(v)) for k, v in value.items()))
    if isinstance(value, list):
        return tuple(freeze(v) for v in value)
    return value


@dataclass(frozen=True)
class SinkMessage:
    partition: int
    offset: int
    key: Any
    """The frozen decoded key, or None for a keyless message."""
    value: Any
    """The decoded value (a dict), or None for a tombstone."""
    ts: int


@dataclass(frozen=True)
class ProgressRecord:
    offset: int
    frontier: int | None
    """The single element of the frontier, or None for the empty frontier."""
    version: int


def decode_message(
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    headers: Sequence[tuple[str, bytes | None]],
) -> SinkMessage | str:
    """Decode a JSON-format sink message, or describe why it does not decode."""
    ts_raw = [v for k, v in headers if k == TIMESTAMP_HEADER]
    if len(ts_raw) != 1 or ts_raw[0] is None:
        return f"{partition}@{offset}: {len(ts_raw)} timestamp headers"
    try:
        ts = int(ts_raw[0].decode())
        k = freeze(json.loads(key)) if key is not None else None
        v = json.loads(value) if value is not None else None
    except (ValueError, UnicodeDecodeError) as e:
        return f"{partition}@{offset}: {e}"
    if v is not None and not isinstance(v, dict):
        return f"{partition}@{offset}: value is not an object"
    return SinkMessage(partition, offset, k, v, ts)


def decode_progress(offset: int, payload: bytes | None) -> ProgressRecord | str:
    if payload is None:
        return f"{offset}: empty progress payload"
    try:
        doc = json.loads(payload)
        elems = doc["frontier"]
        version = int(doc.get("version", 0))
    except (ValueError, KeyError, TypeError, UnicodeDecodeError) as e:
        return f"{offset}: {e}"
    if not isinstance(elems, list) or len(elems) > 1:
        return f"{offset}: frontier {elems!r} is not a total-order antichain"
    return ProgressRecord(offset, int(elems[0]) if elems else None, version)


def frontier_le(a: int | None, b: int | None) -> bool:
    """`a <= b` for frontiers where None is the empty frontier (above everything)."""
    if b is None:
        return True
    return a is not None and a <= b


def progress_regressions(records: Iterable[ProgressRecord]) -> list[dict[str, Any]]:
    """Consecutive records, in offset order, whose frontier or version decreased."""
    out: list[dict[str, Any]] = []
    prev: ProgressRecord | None = None
    for r in sorted(records, key=lambda r: r.offset):
        if prev is not None and (
            not frontier_le(prev.frontier, r.frontier) or r.version < prev.version
        ):
            out.append(
                {
                    "offset": r.offset,
                    "frontier": r.frontier,
                    "version": r.version,
                    "prev_offset": prev.offset,
                    "prev_frontier": prev.frontier,
                    "prev_version": prev.version,
                }
            )
        prev = r
    return out


def duplicate_updates(messages: Iterable[SinkMessage]) -> list[dict[str, Any]]:
    """`(key, timestamp)` pairs committed more than once."""
    counts = Counter((m.key, m.ts) for m in messages)
    return [
        {"key": repr(k), "ts": ts, "count": n} for (k, ts), n in counts.items() if n > 1
    ]


def timestamp_regressions(messages: Iterable[SinkMessage]) -> list[dict[str, Any]]:
    """Messages whose timestamp is below an earlier offset's in the same partition."""
    by_partition: dict[int, list[SinkMessage]] = {}
    for m in messages:
        by_partition.setdefault(m.partition, []).append(m)
    out: list[dict[str, Any]] = []
    for p, ms in by_partition.items():
        high: SinkMessage | None = None
        for m in sorted(ms, key=lambda m: m.offset):
            if high is not None and m.ts < high.ts:
                out.append(
                    {
                        "partition": p,
                        "offset": m.offset,
                        "ts": m.ts,
                        "earlier_offset": high.offset,
                        "earlier_ts": high.ts,
                    }
                )
            if high is None or m.ts >= high.ts:
                high = m
    return out


def row(obj: dict[str, Any] | None, columns: Sequence[str]) -> tuple | None:
    if obj is None:
        return None
    return tuple(obj.get(c) for c in columns)


def _key_order(messages: Iterable[SinkMessage]) -> dict[Any, list[SinkMessage]]:
    by_key: dict[Any, list[SinkMessage]] = {}
    for m in messages:
        by_key.setdefault(m.key, []).append(m)
    for ms in by_key.values():
        ms.sort(key=lambda m: (m.ts, m.partition, m.offset))
    return by_key


def debezium_chain_breaks(
    messages: Iterable[SinkMessage], columns: Sequence[str]
) -> list[dict[str, Any]]:
    """Messages whose `before` differs from the key's previous `after`.

    Walks each key's messages in timestamp order from an empty state, which
    is what a consumer applying the topic sees. A duplicate, a lost update,
    or a resume at the wrong frontier all break the chain.
    """
    out: list[dict[str, Any]] = []
    for key, ms in _key_order(messages).items():
        current: tuple | None = None
        for m in ms:
            value = m.value or {}
            before = row(value.get("before"), columns)
            if m.value is None or before != current:
                out.append(
                    {
                        "key": repr(key),
                        "ts": m.ts,
                        "partition": m.partition,
                        "offset": m.offset,
                        "before": before,
                        "expected_before": current,
                    }
                )
            current = row(value.get("after"), columns)
    return out


def consolidate_debezium(
    messages: Iterable[SinkMessage], below: int | None, columns: Sequence[str]
) -> tuple[Counter[tuple], list[tuple]]:
    """Rows with their multiplicity after every update with `ts < below`.

    Each message retracts its `before` and inserts its `after`; a message
    without a value (which `debezium_chain_breaks` reports) contributes
    nothing. Returns the positive rows and the rows whose multiplicity went
    negative.
    """
    acc: Counter[tuple] = Counter()
    for m in messages:
        if m.value is None or (below is not None and m.ts >= below):
            continue
        before = row(m.value.get("before"), columns)
        after = row(m.value.get("after"), columns)
        if before is not None:
            acc[before] -= 1
        if after is not None:
            acc[after] += 1
    negative = [r for r, n in acc.items() if n < 0]
    return Counter({r: n for r, n in acc.items() if n > 0}), negative


def keyed_rows(rows: Counter[tuple], key_index: int) -> tuple[dict[Any, tuple], list]:
    """Rows by the column at `key_index`, and the keys held by more than one row."""
    out: dict[Any, tuple] = {}
    conflicts = []
    for r, n in rows.items():
        k = r[key_index]
        if n > 1 or k in out:
            conflicts.append(k)
        out[k] = r
    return out, sorted(set(conflicts), key=repr)


def consolidate_upsert(
    messages: Iterable[SinkMessage], below: int | None, columns: Sequence[str]
) -> dict[Any, tuple]:
    """The latest value per key among updates with `ts < below`; tombstones delete."""
    out: dict[Any, tuple] = {}
    for key, ms in _key_order(messages).items():
        latest: SinkMessage | None = None
        for m in ms:
            if below is None or m.ts < below:
                latest = m
        if latest is not None and latest.value is not None:
            r = row(latest.value, columns)
            assert r is not None
            out[key] = r
    return out


def bag_diff(
    expected: Iterable[tuple], observed: Iterable[tuple], limit: int = 10
) -> dict[str, Any]:
    """Multiset difference, truncated for assertion details."""
    e = Counter(expected)
    o = Counter(observed)
    missing = list((e - o).elements())
    extra = list((o - e).elements())
    return {
        "missing_count": len(missing),
        "extra_count": len(extra),
        "missing": [list(x) for x in sorted(missing, key=repr)[:limit]],
        "extra": [list(x) for x in sorted(extra, key=repr)[:limit]],
    }


@dataclass(frozen=True)
class ModelRow:
    """One row id of the acknowledged-write model."""

    id: int
    present: bool
    grp: int
    val: int
    tok: str
    uncertain: bool
    """An operation on the id had an indeterminate outcome, or is in flight."""
    seq: int
    """Bumped whenever an operation on the id is claimed."""


@dataclass(frozen=True)
class ModelExpectation:
    items: dict[int, tuple | None]
    """Expected `(id, grp, val, tok)` per comparable id; None means absent."""
    groups: dict[int, tuple | None]
    """Expected `(grp, n, total)` per comparable group; None means absent."""
    known_ids: set[int]
    """Every id the model allocated; any other id in the sink is a phantom."""


def model_expectation(
    before: dict[int, ModelRow], after: dict[int, ModelRow], groups: int
) -> ModelExpectation:
    """What the sinks must hold at a frontier past every write acknowledged
    before the snapshot `before` was taken.

    `after` is a second snapshot taken once the sink output was read. Ids that
    were uncertain in `before`, or that changed or appeared between the two
    snapshots, are excluded, since a concurrent writer may have moved them
    before the frontier. A group is comparable only if none of its ids is
    excluded.
    """
    excluded = {i for i, r in before.items() if r.uncertain}
    excluded |= {
        i for i, r in after.items() if i not in before or before[i].seq != r.seq
    }
    poisoned = {before[i].grp for i in excluded if i in before}
    poisoned |= {after[i].grp for i in excluded if i in after}
    items: dict[int, tuple | None] = {}
    counts: dict[int, list[int]] = {g: [0, 0] for g in range(groups)}
    for i, r in before.items():
        if i in excluded:
            continue
        items[i] = (i, r.grp, r.val, r.tok) if r.present else None
        if r.present:
            acc = counts.setdefault(r.grp, [0, 0])
            acc[0] += 1
            acc[1] += r.val
    group_rows: dict[int, tuple | None] = {
        g: ((g, n, total) if n > 0 else None)
        for g, (n, total) in counts.items()
        if g not in poisoned
    }
    return ModelExpectation(items, group_rows, set(before) | set(after))


def compare_keyed(
    expected: dict[int, tuple | None], observed: dict[int, tuple], limit: int = 10
) -> list[dict[str, Any]]:
    """Keys of `expected` whose observed row differs (absent counts as None)."""
    out = []
    for k in sorted(expected):
        if expected[k] != observed.get(k):
            out.append(
                {
                    "key": k,
                    "expected": expected[k],
                    "observed": observed.get(k),
                }
            )
            if len(out) >= limit:
                break
    return out
