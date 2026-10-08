# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from materialize.antithesis.isolation import (
    AclModel,
    CrPoint,
    Key,
    OracleMark,
    PodRef,
    Walsender,
    advance_marks,
    data_version_exceeds,
    foreign_handles,
    foreign_walsenders,
    hostname_generation,
    oracle_regressions,
    pod_generation,
    promotable_ceiling,
    version_tuple,
)


def test_pod_generation() -> None:
    assert pod_generation("mz5x7k-environmentd-3-0") == 3
    assert pod_generation("mz5x7k-cluster-u1-replica-u2-gen-12-0") == 12
    assert pod_generation("mz5x7k-cluster-s2-replica-s1-gen-1-0") == 1
    assert pod_generation("orchestratord-6d9f-abcde") is None
    assert pod_generation("postgres-0") is None


def test_hostname_generation() -> None:
    assert hostname_generation("mz5x7k-environmentd-4-0 26.46.0-dev.0") == 4
    assert hostname_generation("mz5x7k-cluster-u1-replica-u1-gen-2-0 26.45.0") == 2
    assert hostname_generation("") is None
    assert hostname_generation("tests") is None


def test_version_tuple() -> None:
    assert version_tuple("v26.46.0-dev (8b2eecd06)") == (26, 46, 0)
    assert version_tuple("26.45.3") == (26, 45, 3)
    assert version_tuple("0.147.20-dev.0") == (0, 147, 20)
    assert version_tuple("") is None
    assert version_tuple("garbage") is None


def test_promotable_ceiling() -> None:
    assert promotable_ceiling(CrPoint(2, "Applying"), CrPoint(2, "Applying")) == 2
    assert (
        promotable_ceiling(CrPoint(2, "ReadyToPromote"), CrPoint(2, "Promoting")) == 3
    )
    assert promotable_ceiling(CrPoint(2, "Promoting"), CrPoint(2, "Promoting")) == 3
    assert promotable_ceiling(CrPoint(2, "Promoting"), CrPoint(3, "Applied")) is None
    assert promotable_ceiling(CrPoint(None, None), CrPoint(None, None)) is None


def state(writers: dict[str, str], critical: dict[str, str]) -> dict[str, object]:
    return {
        "applier_version": "26.46.0-dev.0",
        "writers": {
            w: {
                "debug": {"hostname": h, "purpose": "p"},
                "most_recent_write_upper": [5],
            }
            for w, h in writers.items()
        },
        "critical_readers": {
            c: {"debug": {"hostname": h, "purpose": "c"}, "since": [1]}
            for c, h in critical.items()
        },
        "leased_readers": {
            "r1": {"debug": {"hostname": "mz-environmentd-9-0 26.46.0", "purpose": "x"}}
        },
    }


def test_foreign_handles() -> None:
    s = state(
        {
            "w1": "mz-environmentd-2-0 26.46.0",
            "w2": "mz-cluster-u1-replica-u1-gen-3-0 26.46.0",
            "w3": "unknown 26.46.0",
        },
        {"c1": "mz-environmentd-3-0 26.46.0", "c2": "mz-environmentd-1-0 26.46.0"},
    )
    found = foreign_handles(s, 2)
    assert sorted((f["kind"], f["id"]) for f in found) == [
        ("critical_readers", "c1"),
        ("writers", "w2"),
    ]
    assert foreign_handles(s, 3) == []
    assert foreign_handles({}, 0) == []


def test_data_version_exceeds() -> None:
    s = {"applier_version": "26.46.0-dev.0"}
    assert data_version_exceeds(s, (26, 45, 0))
    assert not data_version_exceeds(s, (26, 46, 0))
    assert not data_version_exceeds({}, (26, 45, 0))


def test_oracle_regressions() -> None:
    marks = advance_marks({}, [("EpochMilliseconds", 100, 105)], finished=10.0)
    # Started before the mark's sample finished: concurrent, not bound.
    assert oracle_regressions(marks, [("EpochMilliseconds", 90, 90)], 9.0) == []
    found = oracle_regressions(marks, [("EpochMilliseconds", 100, 104)], 11.0)
    assert [(f["column"], f["observed"], f["previous"]) for f in found] == [
        ("write_ts", 104, 105)
    ]
    assert oracle_regressions(marks, [("EpochMilliseconds", 101, 106)], 11.0) == []
    assert oracle_regressions(marks, [("other", 1, 1)], 11.0) == []


def test_advance_marks_keeps_earliest_finish() -> None:
    marks = advance_marks({}, [("t", 5, 6)], finished=1.0)
    marks = advance_marks(marks, [("t", 5, 7)], finished=2.0)
    assert marks[("t", "read_ts")] == OracleMark(5, 1.0)
    assert marks[("t", "write_ts")] == OracleMark(7, 2.0)
    marks = advance_marks(marks, [("t", 4, 6)], finished=3.0)
    assert marks[("t", "read_ts")] == OracleMark(5, 1.0)


def test_foreign_walsenders() -> None:
    w = Walsender(11, "2026-01-01 00:00:00", "10.0.0.7", "materialize_abc")
    candidate = PodRef("mz-cluster-u1-replica-u1-gen-3-0", "uid-a", 3)
    pods = {"10.0.0.7": candidate}
    found = foreign_walsenders([w], [w], pods, pods, ceiling=2)
    assert [f["pod"] for f in found] == [candidate.name]
    assert foreign_walsenders([w], [w], pods, pods, ceiling=3) == []
    # A different backend in the second sample is not the same connection.
    other = Walsender(12, "2026-01-01 00:00:05", "10.0.0.7", "materialize_abc")
    assert foreign_walsenders([w], [other], pods, pods, ceiling=2) == []
    # The IP changed owners between the listings.
    replaced = {"10.0.0.7": PodRef(candidate.name, "uid-b", 3)}
    assert foreign_walsenders([w], [w], pods, replaced, ceiling=2) == []
    # Not a pod the workload knows about.
    assert foreign_walsenders([w], [w], {}, {}, ceiling=2) == []


def test_acl_model_adopts_then_checks() -> None:
    model = AclModel({}, settle_s=100.0)
    assert model.observe({"owner:t1": "r1", "grant:t1:g1:SELECT": False}, 0.0) == []
    model.sent("owner:t1", "r2", 1.0)
    model.acknowledged("owner:t1", "r2", 1.0)
    assert model.observe({"owner:t1": "r2", "grant:t1:g1:SELECT": False}, 2.0) == []
    bad = model.observe({"owner:t1": "r1", "grant:t1:g1:SELECT": False}, 3.0)
    assert [m["key"] for m in bad] == ["owner:t1"]


def test_acl_model_rejected_is_not_allowed() -> None:
    model = AclModel({"owner:t1": Key("r1")}, settle_s=100.0)
    model.sent("owner:t1", "r2", 1.0)
    model.rejected("owner:t1", "r2", 1.0)
    assert [m["key"] for m in model.observe({"owner:t1": "r2"}, 2.0)] == ["owner:t1"]


def test_acl_model_indeterminate_stays_allowed_until_settled() -> None:
    model = AclModel({"owner:t1": Key("r1")}, settle_s=100.0)
    model.sent("owner:t1", "r2", 1.0)
    # Outcome unknown: both values are allowed, and an observation of the
    # old value does not rule out a late apply.
    assert model.observe({"owner:t1": "r1"}, 2.0) == []
    model.sent("owner:t1", "r3", 3.0)
    model.acknowledged("owner:t1", "r3", 3.0)
    assert model.observe({"owner:t1": "r2"}, 50.0) == []
    assert model.observe({"owner:t1": "r2"}, 200.0) == []
    assert [m["key"] for m in model.observe({"owner:t1": "r1"}, 201.0)] == ["owner:t1"]


def test_key_json_roundtrip() -> None:
    k = Key("r1", [("r2", 1.5)])
    assert Key.from_json(k.to_json()) == k
