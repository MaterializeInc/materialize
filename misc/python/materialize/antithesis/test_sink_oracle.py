# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from materialize.antithesis import sink_oracle as o

ITEM = ("id", "grp", "val", "tok")
GROUP = ("grp", "n", "total")


def item(i: int, grp: int, val: int, tok: str = "t") -> dict[str, Any]:
    return {"id": i, "grp": grp, "val": val, "tok": tok}


def dbz(
    ts: int,
    key: int,
    before: dict | None,
    after: dict | None,
    partition: int = 0,
    offset: int | None = None,
) -> o.SinkMessage:
    return o.SinkMessage(
        partition,
        ts if offset is None else offset,
        o.freeze({"id": key}),
        {"before": before, "after": after},
        ts,
    )


def ups(
    ts: int, grp: int, n: int | None, total: int = 0, offset: int | None = None
) -> o.SinkMessage:
    value = None if n is None else {"grp": grp, "n": n, "total": total}
    return o.SinkMessage(
        0, ts if offset is None else offset, o.freeze({"grp": grp}), value, ts
    )


def test_decode_message() -> None:
    m = o.decode_message(
        1,
        7,
        b'{"id": 3}',
        json.dumps({"before": None, "after": item(3, 0, 5)}).encode(),
        [("materialize-timestamp", b"1000"), ("other", b"x")],
    )
    assert isinstance(m, o.SinkMessage)
    assert (m.partition, m.offset, m.ts) == (1, 7, 1000)
    assert m.key == (("id", 3),)
    tombstone = o.decode_message(
        0, 0, b'{"grp": 1}', None, [(o.TIMESTAMP_HEADER, b"5")]
    )
    assert isinstance(tombstone, o.SinkMessage) and tombstone.value is None


def test_decode_message_rejects_missing_or_repeated_timestamp() -> None:
    assert isinstance(o.decode_message(0, 0, b"{}", b"{}", []), str)
    assert isinstance(
        o.decode_message(
            0, 0, b"{}", b"{}", [(o.TIMESTAMP_HEADER, b"1"), (o.TIMESTAMP_HEADER, b"2")]
        ),
        str,
    )
    assert isinstance(
        o.decode_message(0, 0, b"{}", b"not json", [(o.TIMESTAMP_HEADER, b"1")]), str
    )


def test_decode_progress() -> None:
    assert o.decode_progress(3, b'{"frontier": [17], "version": 2}') == (
        o.ProgressRecord(3, 17, 2)
    )
    assert o.decode_progress(4, b'{"frontier": []}') == o.ProgressRecord(4, None, 0)
    assert isinstance(o.decode_progress(5, b'{"timestamp": 1}'), str)
    assert isinstance(o.decode_progress(5, b'{"frontier": [1, 2]}'), str)
    assert isinstance(o.decode_progress(5, None), str)


def test_progress_regressions() -> None:
    ok = [
        o.ProgressRecord(0, 5, 0),
        o.ProgressRecord(1, 5, 0),
        o.ProgressRecord(2, 9, 1),
    ]
    assert o.progress_regressions(ok) == []
    assert o.progress_regressions(ok + [o.ProgressRecord(3, None, 1)]) == []
    back = ok + [o.ProgressRecord(3, 8, 1)]
    assert [r["offset"] for r in o.progress_regressions(back)] == [3]
    version = ok + [o.ProgressRecord(3, 10, 0)]
    assert [r["offset"] for r in o.progress_regressions(version)] == [3]
    after_empty = [o.ProgressRecord(0, None, 0), o.ProgressRecord(1, 3, 0)]
    assert len(o.progress_regressions(after_empty)) == 1


def test_duplicates_across_a_restart() -> None:
    first = [
        dbz(10, 1, None, item(1, 0, 5)),
        dbz(12, 1, item(1, 0, 5), item(1, 0, 6)),
    ]
    # A restarted sink that resumed below the committed frontier replays ts 12.
    replay = [dbz(12, 1, item(1, 0, 5), item(1, 0, 6), offset=20)]
    assert o.duplicate_updates(first) == []
    dups = o.duplicate_updates(first + replay)
    assert [(d["ts"], d["count"]) for d in dups] == [(12, 2)]
    # The same timestamp on different keys is not a duplicate.
    assert (
        o.duplicate_updates(
            [dbz(10, 1, None, item(1, 0, 1)), dbz(10, 2, None, item(2, 0, 1))]
        )
        == []
    )


def test_timestamp_regressions_per_partition() -> None:
    ms = [
        dbz(10, 1, None, item(1, 0, 1), partition=0, offset=0),
        dbz(10, 2, None, item(2, 0, 1), partition=0, offset=1),
        dbz(15, 3, None, item(3, 0, 1), partition=0, offset=2),
        # Partitions are ordered independently.
        dbz(11, 4, None, item(4, 0, 1), partition=1, offset=0),
    ]
    assert o.timestamp_regressions(ms) == []
    late = ms + [dbz(12, 5, None, item(5, 0, 1), partition=0, offset=3)]
    regressions = o.timestamp_regressions(late)
    assert [(r["offset"], r["earlier_ts"]) for r in regressions] == [(3, 15)]


def test_debezium_consolidation_with_retractions_and_snapshot() -> None:
    ms = [
        # Snapshot at the sink's as_of.
        dbz(10, 1, None, item(1, 0, 5)),
        dbz(10, 2, None, item(2, 1, 7)),
        # Update, then delete.
        dbz(12, 1, item(1, 0, 5), item(1, 0, 6)),
        dbz(13, 2, item(2, 1, 7), None),
        dbz(14, 3, None, item(3, 1, 1)),
    ]
    rows, negative = o.consolidate_debezium(ms, 13, ITEM)
    assert negative == []
    assert dict(rows) == {(1, 0, 6, "t"): 1, (2, 1, 7, "t"): 1}
    rows, _ = o.consolidate_debezium(ms, None, ITEM)
    assert set(rows) == {(1, 0, 6, "t"), (3, 1, 1, "t")}
    assert o.debezium_chain_breaks(ms, ITEM) == []


def test_debezium_detects_lost_and_duplicated_updates() -> None:
    lost = [
        dbz(10, 1, None, item(1, 0, 5)),
        dbz(14, 1, item(1, 0, 6), item(1, 0, 7)),
    ]
    breaks = o.debezium_chain_breaks(lost, ITEM)
    assert [b["ts"] for b in breaks] == [14]
    dup_delete = [
        dbz(10, 1, None, item(1, 0, 5)),
        dbz(11, 1, item(1, 0, 5), None),
        dbz(12, 1, item(1, 0, 5), None),
    ]
    rows, negative = o.consolidate_debezium(dup_delete, None, ITEM)
    assert negative == [(1, 0, 5, "t")] and not rows
    assert [b["ts"] for b in o.debezium_chain_breaks(dup_delete, ITEM)] == [12]
    no_value = [o.SinkMessage(0, 0, o.freeze({"id": 1}), None, 10)]
    assert len(o.debezium_chain_breaks(no_value, ITEM)) == 1
    assert o.consolidate_debezium(no_value, None, ITEM) == (Counter(), [])


def test_keyed_rows_flags_conflicts() -> None:
    rows = Counter({(1, 0, 5, "a"): 1, (1, 0, 6, "b"): 1, (2, 0, 1, "c"): 2})
    keyed, conflicts = o.keyed_rows(rows, 0)
    assert set(keyed) == {1, 2}
    assert conflicts == [1, 2]


def test_upsert_latest_value_and_tombstones() -> None:
    ms = [
        ups(10, 0, 2, 12),
        ups(10, 1, 1, 7),
        ups(12, 0, 3, 13),
        ups(13, 1, None),
        ups(15, 0, 1, 1),
    ]
    assert o.consolidate_upsert(ms, 13, GROUP) == {
        (("grp", 0),): (0, 3, 13),
        (("grp", 1),): (1, 1, 7),
    }
    assert o.consolidate_upsert(ms, 14, GROUP) == {(("grp", 0),): (0, 3, 13)}
    assert o.consolidate_upsert(ms, None, GROUP) == {(("grp", 0),): (0, 1, 1)}
    # Latest is chosen by timestamp, not by offset.
    shuffled = [ups(12, 0, 3, 13, offset=0), ups(10, 0, 2, 12, offset=1)]
    assert o.consolidate_upsert(shuffled, None, GROUP) == {(("grp", 0),): (0, 3, 13)}


def row(
    i: int,
    present: bool,
    grp: int,
    val: int,
    uncertain: bool = False,
    seq: int = 1,
    tok: str = "t",
) -> o.ModelRow:
    return o.ModelRow(i, present, grp, val, tok, uncertain, seq)


def test_model_expectation() -> None:
    before = {
        1: row(1, True, 0, 5),
        2: row(2, True, 0, 7),
        3: row(3, False, 1, 9),  # deleted, or a rejected insert
        4: row(4, True, 1, 2, uncertain=True),
        5: row(5, True, 2, 4),
        6: row(6, True, 3, 1),
    }
    after = dict(before)
    after[5] = row(5, True, 2, 5, seq=2)  # written concurrently with the check
    after[7] = row(7, False, 4, 1, uncertain=True)  # inserted after the snapshot
    e = o.model_expectation(before, after, groups=6)
    assert e.items == {
        1: (1, 0, 5, "t"),
        2: (2, 0, 7, "t"),
        3: None,
        6: (6, 3, 1, "t"),
    }
    # Groups 1, 2 and 4 hold an excluded id; group 5 is empty.
    assert e.groups == {0: (0, 2, 12), 3: (3, 1, 1), 5: None}
    assert e.known_ids == {1, 2, 3, 4, 5, 6, 7}


def test_compare_keyed() -> None:
    expected: dict[int, tuple | None] = {1: (1, 0, 5, "t"), 2: None, 3: (3, 0, 1, "t")}
    observed = {1: (1, 0, 5, "t"), 2: (2, 0, 1, "t"), 9: (9, 0, 0, "t")}
    diffs = o.compare_keyed(expected, observed)
    assert [d["key"] for d in diffs] == [2, 3]


def test_bag_diff() -> None:
    d = o.bag_diff([(1,), (1,), (2,)], [(1,), (3,)])
    assert d["missing_count"] == 2 and d["extra_count"] == 1
    assert d["missing"] == [[1], [2]] and d["extra"] == [[3]]
