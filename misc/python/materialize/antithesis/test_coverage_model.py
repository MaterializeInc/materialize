# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from dataclasses import dataclass, replace

import pytest

from materialize.antithesis import coverage_model as m
from materialize.antithesis import serializability as sz
from materialize.antithesis.ivm_model import WriteOp


@dataclass
class Event:
    kind: str
    started: float
    completed: float
    phase: str = ""
    role: str = ""
    cluster_id: str = ""


def history() -> list[sz.Op]:
    return [
        sz.Op(1, sz.Kind.RMW, sz.Outcome.OK, 1, 4, write_key=0, read_key=1),
        sz.Op(
            2,
            sz.Kind.READ,
            sz.Outcome.OK,
            5,
            6,
            observations=(sz.Observation(0, "mv", (1,), ((1, 0),)),),
        ),
    ]


def test_witness_requires_unresolved_write_then_read_of_its_id() -> None:
    events = [
        Event("phase", 2, 2, phase="Promoting"),
        Event("fault", 2.5, 3, role="environmentd_active"),
    ]
    assert m.history_cases(history(), events, {2}) == {
        ("Promoting", "environmentd_active", "rmw", "mv", "acked")
    }
    assert not m.history_cases(history(), events, set())
    absent = replace(history()[1], observations=(sz.Observation(0, "mv", ()),))
    assert not m.history_cases([history()[0], absent], events, {2})


def test_delete_must_be_acknowledged_entirely_while_write_is_unresolved() -> None:
    assert not m.history_cases(
        history(), [Event("fault", 3, 4.1, role="environmentd_active")], {2}
    )
    assert not m.history_cases(
        history(), [Event("fault", 0, 3, role="environmentd_active")], {2}
    )
    assert m.history_cases(
        history(), [Event("fault", 1, 4, role="environmentd_active")], {2}
    )


def test_a_delayed_phase_get_must_fit_entirely_inside_the_unresolved_write() -> None:
    assert not m.history_cases(
        history(), [Event("phase", 0.5, 2, phase="Promoting")], {2}
    )
    assert not m.history_cases(
        history(), [Event("phase", 2, 4.5, phase="Promoting")], {2}
    )
    assert m.history_cases(history(), [Event("phase", 1, 4, phase="Promoting")], {2})


def test_read_must_start_after_observed_exposure() -> None:
    early = replace(history()[1], invoke=2.5)
    assert not m.history_cases(
        [history()[0], early],
        [Event("fault", 2, 3, role="environmentd_active")],
        {2},
    )


def test_indeterminate_write_resolution_uses_first_read_response() -> None:
    write = replace(history()[0], outcome=sz.Outcome.INFO, complete=2)
    events = [Event("phase", 3, 3, phase="Applying")]
    assert m.history_cases([write, history()[1]], events, {2}) == {
        ("Applying", "", "rmw", "mv", "indeterminate")
    }
    later = replace(history()[1], op_id=3, invoke=9, complete=10)
    assert not m.history_cases(
        [write, history()[1], later],
        [Event("phase", 7, 7, phase="Promoting")],
        {3},
    )


def test_rejected_writes_and_wrong_keys_never_receive_credit() -> None:
    event = Event("phase", 2, 2, phase="Applying")
    for write in (
        replace(history()[0], outcome=sz.Outcome.FAIL),
        replace(history()[0], write_key=7),
    ):
        assert not m.history_cases([write, history()[1]], [event], {2})


def test_history_report_success_includes_every_anomaly_field() -> None:
    assert m.report_passed(sz.Report())
    assert not m.report_passed(sz.Report(invalid_rmw_counts=[{"count": -1}]))
    report = sz.Report()
    report.anomaly_counts["G2"] = 1
    assert not m.report_passed(report)


def test_subscribe_witness_requires_a_closed_read_requested_after_the_exposure() -> (
    None
):
    writes = [WriteOp(1, "ok", 1, 4, 10)]
    events = [Event("phase", 2, 3, phase="Promoting")]
    assert m.stream_cases(writes, {1: (5, 6)}, events) == {
        ("Promoting", "", "ivm", "subscribe", "acked")
    }
    assert not m.stream_cases(writes, {}, events)
    assert not m.stream_cases(writes, {1: (2, 6)}, events)
    assert not m.stream_cases(
        [replace(writes[0], complete_rt=1.5)], {1: (5, 6)}, events
    )
    assert not m.stream_cases(
        [replace(writes[0], outcome="rejected")], {1: (5, 6)}, events
    )


def test_subscribe_can_resolve_a_lost_write_ack_after_a_replica_delete() -> None:
    write = WriteOp(1, "indeterminate", 1, 2, None)
    event = Event("fault", 3, 4, role="clusterd", cluster_id="u1")
    assert m.stream_cases([write], {1: (5, 6)}, [event], cluster_id="u1") == {
        ("", "clusterd", "ivm", "subscribe", "indeterminate")
    }


def test_replica_delete_without_oracle_cluster_never_receives_credit() -> None:
    event = Event("fault", 2, 3, role="clusterd", cluster_id="u1")
    assert not m.history_cases(history(), [event], {2})
    assert not m.stream_cases([WriteOp(1, "ok", 1, 4, 10)], {1: (5, 6)}, [event])


@pytest.mark.parametrize(
    "oracle_cluster,event_cluster,expected",
    [("u1", "u1", True), ("u1", "u2", False), ("u1", "", False), ("", "", False)],
)
def test_replica_exposure_is_bound_to_the_verified_cluster(
    oracle_cluster: str, event_cluster: str, expected: bool
) -> None:
    event = Event("fault", 2, 3, role="clusterd", cluster_id=event_cluster)
    assert (
        bool(m.history_cases(history(), [event], {2}, cluster_id=oracle_cluster))
        is expected
    )
    assert (
        bool(
            m.stream_cases(
                [WriteOp(1, "ok", 1, 4, 10)],
                {1: (5, 6)},
                [event],
                cluster_id=oracle_cluster,
            )
        )
        is expected
    )


def test_unrelated_replica_does_not_erase_a_real_promotion_witness() -> None:
    events = [
        Event("fault", 2, 3, role="clusterd", cluster_id="u2"),
        Event("phase", 2, 3, phase="Promoting"),
    ]
    assert m.history_cases(history(), events, {2}, cluster_id="u1") == {
        ("Promoting", "", "rmw", "mv", "acked")
    }
