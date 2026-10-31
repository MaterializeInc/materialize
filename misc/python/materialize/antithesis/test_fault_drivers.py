# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import copy
import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest
import urllib3

pytest.importorskip("antithesis")

from materialize.antithesis import observations, quiet
from materialize.antithesis.drivers import canaries, recovery, resource_kills, rollouts


@pytest.fixture
def halt_capture(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    finished = "2026-10-30T12:00:00.900000000+00:00"
    term = SimpleNamespace(
        exit_code=166,
        reason="Error",
        signal=0,
        finished_at=finished,
        started_at="2026-10-30T11:59:59+00:00",
    )
    status = SimpleNamespace(
        name="environmentd",
        restart_count=0,
        state=SimpleNamespace(terminated=term, running=None, waiting=None),
        last_state=SimpleNamespace(terminated=None),
        container_id="container-id",
    )
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name="envd",
            uid="pod-uid",
            deletion_timestamp=None,
            labels={"materialize.cloud/app": "environmentd"},
        ),
        status=SimpleNamespace(container_statuses=[status]),
    )
    observation = recovery.ContainerObservation(
        role="environmentd",
        namespace="mz",
        pod="envd",
        pod_uid="pod-uid",
        container="environmentd",
        restart_count=0,
        exit_code=166,
        reason="Error",
        signal=0,
        finished_at=finished,
        waiting_reason=None,
        current_terminated=True,
    )
    message = "fenced out old deployment; rebooting as leader"
    text = (
        "2026-10-30T12:00:00.800000000Z "
        + json.dumps({"fields": {"message": f"halting process: {message}"}})
        + "\n"
    )
    core = Mock()
    core.read_namespaced_pod.return_value = pod
    core.read_namespaced_pod_log.return_value = text
    monkeypatch.setattr(recovery.client, "CoreV1Api", lambda: core)
    db = sqlite3.connect(":memory:")
    monkeypatch.setattr(recovery.state, "open_db", lambda _: db)
    recovery.open_state()
    emitted = Mock()
    monkeypatch.setattr(recovery, "send_event", emitted)
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(recovery.time, "monotonic", lambda: clock.now)
    return SimpleNamespace(
        observation=observation,
        pod=pod,
        core=core,
        db=db,
        emitted=emitted,
        clock=clock,
        text=text,
        message=message,
    )


def _termination_frame(message: str, sequence: int = 0) -> str:
    location = b"src/proof.rs:1"
    encoded = message.encode()
    stamp = (
        int(datetime(2026, 10, 30, 12, tzinfo=UTC).timestamp()) * 10**9 + 800_000_000
    )
    return (
        f"MZHALT1 {sequence} 166 {stamp} {len(location)} {len(encoded)} "
        f"{location.hex()} {encoded.hex()} END\n"
    )


@pytest.mark.parametrize("current", [False, True])
def test_halt_capture_uses_incarnation_bound_file_when_runtime_logs_are_unavailable(
    halt_capture: SimpleNamespace, current: bool
) -> None:
    o = halt_capture.observation
    cs = halt_capture.pod.status.container_statuses[0]
    term = cs.state.terminated
    term.container_id = "cri-o://terminated"
    term.message = _termination_frame(halt_capture.message)
    if not current:
        o = replace(o, current_terminated=False, restart_count=1)
        cs.restart_count = 1
        cs.last_state.terminated = term
        cs.state.terminated = None
        cs.state.running = SimpleNamespace(started_at="2026-10-30T12:00:00+00:00")
    halt_capture.core.read_namespaced_pod_log.side_effect = RuntimeError("unavailable")

    result = recovery.capture_halt_evidence(o, 10)

    assert result is not None
    assert result["halt"]["message"] == halt_capture.message
    assert result["termination"]["container_id"] == "cri-o://terminated"
    assert result["halt"]["termination"] == result["termination"]
    assert result["termination_file"]["contents"] == term.message
    assert "log_capture" not in result
    halt_capture.core.read_namespaced_pod_log.assert_not_called()
    assert halt_capture.core.read_namespaced_pod.call_count == 2


@pytest.mark.parametrize("change", ["truncated", "conflicting", "container_id", "time"])
def test_halt_file_evidence_cannot_fall_back_to_a_conflicting_log(
    halt_capture: SimpleNamespace, change: str
) -> None:
    term = halt_capture.pod.status.container_statuses[0].state.terminated
    term.container_id = "cri-o://terminated"
    term.message = _termination_frame(halt_capture.message)
    if change == "truncated":
        term.message = term.message.rstrip("\n")
    elif change == "conflicting":
        term.message += _termination_frame("an unexpected halt", 1)
    elif change == "container_id":
        term.container_id = None
    else:
        term.started_at = "2026-10-30T12:00:02Z"

    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    halt_capture.core.read_namespaced_pod_log.assert_not_called()


def test_halt_file_evidence_survives_current_to_previous_routing_change(
    halt_capture: SimpleNamespace,
) -> None:
    cs = halt_capture.pod.status.container_statuses[0]
    cs.state.terminated.container_id = "cri-o://terminated"
    cs.state.terminated.message = _termination_frame(halt_capture.message)
    after = copy.deepcopy(halt_capture.pod)
    after_cs = after.status.container_statuses[0]
    after_cs.restart_count = 1
    after_cs.last_state.terminated = after_cs.state.terminated
    after_cs.state.terminated = None
    after_cs.container_id = "cri-o://replacement"
    after_cs.state.running = SimpleNamespace(started_at="2026-10-30T12:00:01Z")
    halt_capture.core.read_namespaced_pod.side_effect = [halt_capture.pod, after]

    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is not None
    halt_capture.core.read_namespaced_pod_log.assert_not_called()


@pytest.mark.parametrize(
    "change", ["uid", "ordinal", "container_id", "contents", "finished"]
)
def test_halt_file_rejects_changed_termination_identity(
    halt_capture: SimpleNamespace, change: str
) -> None:
    cs = halt_capture.pod.status.container_statuses[0]
    cs.state.terminated.container_id = "cri-o://terminated"
    cs.state.terminated.message = _termination_frame(halt_capture.message)
    after = copy.deepcopy(halt_capture.pod)
    after_cs = after.status.container_statuses[0]
    if change == "uid":
        after.metadata.uid = "replacement-uid"
    elif change == "ordinal":
        after_cs.restart_count += 1
    elif change == "container_id":
        after_cs.state.terminated.container_id = "cri-o://replacement"
    elif change == "contents":
        after_cs.state.terminated.message = _termination_frame("an unexpected halt")
    else:
        after_cs.state.terminated.finished_at = "2026-10-30T12:00:01Z"
    halt_capture.core.read_namespaced_pod.side_effect = [halt_capture.pod, after]

    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    halt_capture.core.read_namespaced_pod_log.assert_not_called()


@pytest.mark.parametrize("current", [True, False])
def test_halt_capture_uses_the_stable_current_or_previous_log(
    halt_capture: SimpleNamespace, current: bool
) -> None:
    o = halt_capture.observation
    if not current:
        o = replace(o, current_terminated=False, restart_count=1)
        cs = halt_capture.pod.status.container_statuses[0]
        cs.restart_count = 1
        cs.last_state.terminated = cs.state.terminated
        cs.state.terminated = None
        cs.state.running = SimpleNamespace(started_at="2026-10-30T12:00:01+00:00")
    result = recovery.capture_halt_evidence(o, 10)
    assert result is not None
    assert result["halt"]["message"] == halt_capture.message
    assert result["halt"]["complete"] is True
    assert result["termination"] == result["halt"]["termination"]
    assert result["termination"]["termination_count"] == 1
    call = halt_capture.core.read_namespaced_pod_log.call_args
    assert call.kwargs["previous"] is (not current)
    assert call.kwargs["timestamps"] is True
    assert call.kwargs["limit_bytes"] == recovery.HALT_EVIDENCE_MAX_BYTES + 1
    assert call.kwargs["tail_lines"] == recovery.HALT_EVIDENCE_TAILS[0]
    assert 0 < call.kwargs["_request_timeout"] <= recovery.HALT_EVIDENCE_REQUEST_S


@pytest.mark.parametrize(
    "change", ["uid", "restart", "selector", "finished", "container_id"]
)
def test_halt_capture_rejects_log_routing_changes(
    halt_capture: SimpleNamespace, change: str
) -> None:
    after = copy.deepcopy(halt_capture.pod)
    cs = after.status.container_statuses[0]
    if change == "uid":
        after.metadata.uid = "replacement-uid"
    elif change == "restart":
        cs.restart_count += 1
    elif change == "selector":
        cs.restart_count += 1
        cs.last_state.terminated = cs.state.terminated
        cs.state.terminated = None
    elif change == "finished":
        cs.state.terminated.finished_at = "2026-10-30T12:00:10+00:00"
    else:
        cs.container_id = "replacement-container"
    halt_capture.core.read_namespaced_pod.side_effect = [halt_capture.pod, after]
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None


def test_halt_capture_rejects_an_already_stale_observation(
    halt_capture: SimpleNamespace,
) -> None:
    halt_capture.pod.metadata.uid = "replacement-uid"
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    halt_capture.core.read_namespaced_pod_log.assert_not_called()


def test_halt_capture_cannot_explain_an_exit_by_signal(
    halt_capture: SimpleNamespace,
) -> None:
    o = replace(halt_capture.observation, signal=38)
    assert recovery.capture_halt_evidence(o, 10) is None
    halt_capture.core.read_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "next_start,accepted",
    [
        ("2026-10-30T12:00:01+00:00", True),
        ("2026-10-30T12:00:00.950000000+00:00", True),
        ("2026-10-30T12:00:00+00:00", False),
        ("2026-10-30T12:00:00.800000000+00:00", False),
    ],
)
def test_halt_capture_models_whole_second_exit_precision(
    halt_capture: SimpleNamespace, next_start: str, accepted: bool
) -> None:
    finished = "2026-10-30T12:00:00+00:00"
    o = replace(
        halt_capture.observation,
        finished_at=finished,
        current_terminated=False,
        restart_count=1,
    )
    cs = halt_capture.pod.status.container_statuses[0]
    cs.state.terminated.finished_at = finished
    cs.last_state.terminated = cs.state.terminated
    cs.state.terminated = None
    cs.state.running = SimpleNamespace(started_at=next_start)
    cs.restart_count = 1
    result = recovery.capture_halt_evidence(o, 10)
    assert (result is not None) is accepted


def test_halt_capture_defers_an_ambiguous_current_exit_until_a_later_sample(
    halt_capture: SimpleNamespace,
) -> None:
    finished = "2026-10-30T12:00:00+00:00"
    halt_capture.pod.status.container_statuses[0].state.terminated.finished_at = (
        finished
    )
    o = replace(halt_capture.observation, finished_at=finished)
    assert recovery.capture_halt_evidence(o, 10) is None


@pytest.mark.parametrize(
    "prior_finished,accepted",
    [(None, False), ("2026-10-30T11:59:59Z", True), ("2026-10-30T12:00:00Z", False)],
)
def test_halt_capture_rejects_overlapping_start_precision(
    halt_capture: SimpleNamespace, prior_finished: str | None, accepted: bool
) -> None:
    cs = halt_capture.pod.status.container_statuses[0]
    cs.restart_count = 1
    cs.state.terminated.started_at = "2026-10-30T12:00:00+00:00"
    if prior_finished is not None:
        cs.last_state.terminated = SimpleNamespace(finished_at=prior_finished)
    o = replace(halt_capture.observation, restart_count=1)
    assert (recovery.capture_halt_evidence(o, 10) is not None) is accepted


@pytest.mark.parametrize(
    "stamp,precision_ns",
    [
        ("2026-10-30T12:00:00Z", 1_000_000_000),
        ("2026-10-30T12:00:00.123Z", 1_000_000),
        ("2026-10-30T12:00:00.123456Z", 1_000),
        ("2026-10-30T12:00:00.123456789Z", 1),
    ],
)
def test_kubernetes_timestamp_precision_is_an_interval(
    stamp: str, precision_ns: int
) -> None:
    interval = recovery._timestamp_interval(stamp)
    assert interval is not None
    assert interval[1] - interval[0] == precision_ns


def test_kubernetes_timestamp_intervals_normalize_offsets() -> None:
    assert recovery._timestamp_interval("2026-10-30T13:00:00.123+01:00") == (
        recovery._timestamp_interval("2026-10-30T12:00:00.123Z")
    )


@pytest.mark.parametrize("stage", ["before", "log", "after"])
@pytest.mark.parametrize(
    "error",
    [
        recovery.ApiException(status=503),
        urllib3.exceptions.ReadTimeoutError(
            urllib3.HTTPConnectionPool("localhost"), "/log", "read timed out"
        ),
        urllib3.exceptions.MaxRetryError(
            urllib3.HTTPConnectionPool("localhost"),
            "/log",
            TimeoutError("read timed out"),
        ),
    ],
    ids=["api", "read_timeout", "max_retry"],
)
def test_halt_capture_api_faults_remain_missing_evidence(
    halt_capture: SimpleNamespace, stage: str, error: Exception
) -> None:
    if stage == "before":
        halt_capture.core.read_namespaced_pod.side_effect = error
    elif stage == "after":
        halt_capture.core.read_namespaced_pod.side_effect = [halt_capture.pod, error]
    else:
        halt_capture.core.read_namespaced_pod_log.side_effect = error
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    halt_capture.emitted.assert_not_called()


@pytest.mark.parametrize("stage", ["before", "log", "after"])
def test_halt_capture_reports_forbidden_reads_as_invalid_harness_health(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace, stage: str
) -> None:
    health_error = Mock()
    monkeypatch.setattr(observations, "harness_error", health_error)
    forbidden = recovery.ApiException(status=403)
    if stage == "before":
        halt_capture.core.read_namespaced_pod.side_effect = forbidden
    elif stage == "after":
        halt_capture.core.read_namespaced_pod.side_effect = [
            halt_capture.pod,
            forbidden,
        ]
    else:
        halt_capture.core.read_namespaced_pod_log.side_effect = forbidden
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    health_error.assert_called_once_with(
        "halt evidence",
        "workload service account cannot read termination evidence",
        {"namespace": "mz", "pod": "envd", "container": "environmentd", "status": 403},
    )
    halt_capture.emitted.assert_not_called()


def test_halt_capture_keeps_api_outages_out_of_invalid_harness_health(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    health_error = Mock()
    monkeypatch.setattr(observations, "harness_error", health_error)
    halt_capture.core.read_namespaced_pod_log.side_effect = recovery.ApiException(
        status=503
    )
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    health_error.assert_not_called()


@pytest.mark.parametrize("kind", ["unavailable", "unavailable_newline", "truncated"])
def test_halt_capture_distinguishes_unavailable_runtime_logs_from_partial_records(
    halt_capture: SimpleNamespace, kind: str
) -> None:
    from collections import Counter

    counts: Counter[str] = Counter()
    unavailable = "unable to retrieve container logs for cri-o://" + "a" * 64
    assert len(unavailable.encode()) == 110
    text = {
        "unavailable": unavailable,
        "unavailable_newline": unavailable + "\n",
        "truncated": halt_capture.text.rstrip("\n"),
    }[kind]
    halt_capture.core.read_namespaced_pod_log.return_value = text

    assert (
        recovery.capture_halt_evidence(halt_capture.observation, 10, stats=counts)
        is None
    )

    assert counts["runtime_log_unavailable"] == int(kind != "truncated")
    assert counts["truncated_log"] == int(kind == "truncated")
    assert counts["uncorrelated_or_incomplete_log"] == 1
    assert counts["log_reads"] == 1
    assert counts["pod_reads"] == 2
    halt_capture.emitted.assert_not_called()


@pytest.mark.parametrize(
    "kind", ["empty", "truncated", "oversized", "multiple", "nonutf8", "deep"]
)
def test_halt_capture_requires_one_complete_message(
    halt_capture: SimpleNamespace, kind: str
) -> None:
    text = halt_capture.text
    if kind == "empty":
        text = ""
    elif kind == "truncated":
        text = text.rstrip("\n")
    elif kind == "oversized":
        text = "x" * recovery.HALT_EVIDENCE_MAX_BYTES + "\n" + text
    elif kind == "multiple":
        text += text
    elif kind == "nonutf8":
        text = "\ud800\n"
    else:
        text = "2026-10-30T12:00:00Z " + "[" * 1100 + "]" * 1100 + "\n"
    halt_capture.core.read_namespaced_pod_log.return_value = text
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None


def test_halt_capture_retries_a_byte_capped_tail(
    halt_capture: SimpleNamespace,
) -> None:
    routine = (
        "2026-10-30T12:00:00.700000000Z "
        + json.dumps({"fields": {"message": "r" * 512}})
        + "\n"
    )
    lines = [routine] * 199 + [halt_capture.text]

    def read_log(*args, tail_lines: int, limit_bytes: int, **kwargs):
        return "".join(lines[-tail_lines:]).encode()[:limit_bytes].decode()

    halt_capture.core.read_namespaced_pod_log.side_effect = read_log
    result = recovery.capture_halt_evidence(halt_capture.observation, 10)
    assert result is not None
    assert result["halt"]["message"] == halt_capture.message
    assert [
        call.kwargs["tail_lines"]
        for call in halt_capture.core.read_namespaced_pod_log.call_args_list
    ] == [200, 20]
    assert halt_capture.core.read_namespaced_pod.call_count == 3


def test_halt_capture_checks_identity_again_after_shrinking(
    halt_capture: SimpleNamespace,
) -> None:
    after = copy.deepcopy(halt_capture.pod)
    after.metadata.uid = "replacement-uid"
    halt_capture.core.read_namespaced_pod.side_effect = [
        halt_capture.pod,
        halt_capture.pod,
        after,
    ]
    halt_capture.core.read_namespaced_pod_log.side_effect = [
        "x" * (recovery.HALT_EVIDENCE_MAX_BYTES + 1),
        halt_capture.text,
    ]
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    assert halt_capture.core.read_namespaced_pod_log.call_count == 2


def test_halt_capture_bounds_retries_for_a_truncated_single_line(
    halt_capture: SimpleNamespace,
) -> None:
    halt_capture.core.read_namespaced_pod_log.return_value = (
        halt_capture.text.rstrip("\n") + "x" * recovery.HALT_EVIDENCE_MAX_BYTES
    )[: recovery.HALT_EVIDENCE_MAX_BYTES + 1]
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    assert [
        call.kwargs["tail_lines"]
        for call in halt_capture.core.read_namespaced_pod_log.call_args_list
    ] == [200, 20, 1]
    assert halt_capture.core.read_namespaced_pod.call_count == 4
    assert all(
        0 < call.kwargs["_request_timeout"] <= recovery.HALT_EVIDENCE_REQUEST_S
        for call in halt_capture.core.read_namespaced_pod_log.call_args_list
        + halt_capture.core.read_namespaced_pod.call_args_list
    )


def test_halt_capture_stops_shrinking_when_its_deadline_elapses(
    halt_capture: SimpleNamespace,
) -> None:
    def read_log(*args, **kwargs):
        halt_capture.clock.now = 9
        return "x" * (recovery.HALT_EVIDENCE_MAX_BYTES + 1)

    def read_pod(*args, **kwargs):
        if halt_capture.clock.now:
            halt_capture.clock.now = 10
        return halt_capture.pod

    halt_capture.core.read_namespaced_pod_log.side_effect = read_log
    halt_capture.core.read_namespaced_pod.side_effect = read_pod
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    assert halt_capture.core.read_namespaced_pod_log.call_count == 1
    assert halt_capture.core.read_namespaced_pod.call_count == 2
    assert (
        halt_capture.core.read_namespaced_pod.call_args.kwargs["_request_timeout"] == 1
    )


def test_halt_capture_does_not_shrink_a_complete_ambiguous_tail(
    halt_capture: SimpleNamespace,
) -> None:
    halt_capture.core.read_namespaced_pod_log.side_effect = [
        halt_capture.text * 2,
        halt_capture.text,
    ]
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None
    assert halt_capture.core.read_namespaced_pod_log.call_count == 1


def test_halt_capture_makes_no_requests_after_its_deadline(
    halt_capture: SimpleNamespace,
) -> None:
    assert recovery.capture_halt_evidence(halt_capture.observation, 0) is None
    halt_capture.core.read_namespaced_pod.assert_not_called()


def test_halt_evidence_is_cached_and_emitted_once(
    halt_capture: SimpleNamespace,
) -> None:
    budget = recovery.HaltEvidenceBudget()
    for _ in range(2):
        recovery.record_halt_evidence(
            halt_capture.db, [halt_capture.observation], budget
        )
    assert halt_capture.core.read_namespaced_pod_log.call_count == 1
    halt_capture.emitted.assert_called_once()
    assert halt_capture.emitted.call_args.args[0] == "antithesis_exit_evidence"
    assert halt_capture.db.execute("SELECT emitted FROM halt_evidence").fetchall() == [
        (1,)
    ]


def test_halt_evidence_retries_missing_logs_without_polling_flood(
    halt_capture: SimpleNamespace,
) -> None:
    budget = recovery.HaltEvidenceBudget()
    halt_capture.core.read_namespaced_pod_log.side_effect = ["", halt_capture.text]
    for now in (0, 1, recovery.HALT_EVIDENCE_RETRY_S):
        halt_capture.clock.now = now
        recovery.record_halt_evidence(
            halt_capture.db, [halt_capture.observation], budget
        )
    assert halt_capture.core.read_namespaced_pod_log.call_count == 2
    assert [call.args[0] for call in halt_capture.emitted.call_args_list] == [
        "antithesis_exit_evidence_missing",
        "antithesis_exit_evidence",
    ]


def test_missing_halt_diagnostics_are_deduplicated_and_allow_later_capture(
    halt_capture: SimpleNamespace,
) -> None:
    from materialize.antithesis import findings

    budget = recovery.HaltEvidenceBudget()
    halt_capture.core.read_namespaced_pod_log.side_effect = ["", "", halt_capture.text]
    for now in (0, 30, 60):
        halt_capture.clock.now = now
        recovery.record_halt_evidence(
            halt_capture.db, [halt_capture.observation], budget
        )
    events = [
        {call.args[0]: call.args[1]} for call in halt_capture.emitted.call_args_list
    ]
    missing = [
        event["antithesis_exit_evidence_missing"]
        for event in events
        if "antithesis_exit_evidence_missing" in event
    ]
    captured = [
        event["antithesis_exit_evidence"]
        for event in events
        if "antithesis_exit_evidence" in event
    ]
    assert len(missing) == len(captured) == 1
    assert missing[0]["termination"] == captured[0]["termination"]
    assert missing[0]["counts"]["pod_reads"] == 2
    assert missing[0]["counts"]["log_reads"] == 1
    assert "uncorrelated_or_incomplete_log" in missing[0]["reasons"]
    report = findings.summarize(
        [
            {
                "id": "check",
                "name": "probe",
                "group": "Always: Always",
                "status": "Passing",
                "example_count": 1,
                "counterexample_count": 0,
            },
            {
                "id": "exit",
                "name": "No unexpected container exits: container: environmentd, namespace: mz, exit code: 166",
                "group": "No unexpected container exits",
                "status": "Failing",
                "example_count": 0,
                "counterexample_count": 1,
            },
        ],
        events=events,
    )
    assert report["verdict"] == "no_product_findings"


def test_halt_evidence_has_a_shared_attempt_limit(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    capture = Mock(return_value=None)
    monkeypatch.setattr(recovery, "capture_halt_evidence", capture)
    observations = [
        replace(halt_capture.observation, pod_uid=f"pod-{i}") for i in range(20)
    ]
    budget = recovery.HaltEvidenceBudget()
    for _ in range(2):
        recovery.record_halt_evidence(halt_capture.db, observations, budget)
    assert capture.call_count == recovery.HALT_EVIDENCE_ATTEMPTS
    assert all(
        call.args[0] == "antithesis_exit_evidence_missing"
        for call in halt_capture.emitted.call_args_list
    )


def test_halt_evidence_has_a_shared_time_limit(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    def capture(*args, **kwargs):
        halt_capture.clock.now += 5
        return None

    calls = Mock(side_effect=capture)
    monkeypatch.setattr(recovery, "capture_halt_evidence", calls)
    observations = [
        replace(halt_capture.observation, pod_uid=f"pod-{i}") for i in range(20)
    ]
    recovery.record_halt_evidence(
        halt_capture.db, observations, recovery.HaltEvidenceBudget(attempts=20)
    )
    assert calls.call_count == 2


def test_halt_evidence_can_emit_a_cached_record_after_interruption(
    halt_capture: SimpleNamespace,
) -> None:
    budget = recovery.HaltEvidenceBudget()
    halt_capture.emitted.side_effect = [KeyboardInterrupt, None]
    with pytest.raises(KeyboardInterrupt):
        recovery.record_halt_evidence(
            halt_capture.db, [halt_capture.observation], budget
        )
    recovery.record_halt_evidence(halt_capture.db, [halt_capture.observation], budget)
    assert halt_capture.core.read_namespaced_pod_log.call_count == 1
    assert halt_capture.emitted.call_count == 2
    assert halt_capture.db.execute("SELECT emitted FROM halt_evidence").fetchall() == [
        (1,)
    ]


def test_halt_budget_renews_without_retrying_the_same_exit_early(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    capture = Mock(return_value=None)
    monkeypatch.setattr(recovery, "capture_halt_evidence", capture)
    observed = [replace(halt_capture.observation, pod_uid=f"pod-{i}") for i in range(6)]
    budget = recovery.HaltEvidenceBudget()
    recovery.record_halt_evidence(halt_capture.db, observed, budget)
    assert capture.call_count == recovery.HALT_EVIDENCE_ATTEMPTS
    halt_capture.clock.now = recovery.HALT_EVIDENCE_WINDOW_S
    recovery.record_halt_evidence(halt_capture.db, observed, budget)
    assert capture.call_count == 2 * recovery.HALT_EVIDENCE_ATTEMPTS
    assert [call.args[0].pod_uid for call in capture.call_args_list] == [
        f"pod-{i}" for i in range(6)
    ]


def test_passive_observer_samples_and_emits_a_halt_without_a_test_command(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    stop = threading.Event()
    samples = 0

    def observe(env, **kwargs):
        nonlocal samples
        samples += 1
        return [halt_capture.observation]

    def wait(timeout):
        halt_capture.clock.now += timeout
        if samples == 4:
            stop.set()
        return stop.is_set()

    monkeypatch.setattr(stop, "wait", wait)
    monkeypatch.setenv("MZ_ANTITHESIS_TEMPLATE_PROFILE", "observer")
    monkeypatch.setattr(recovery, "Environment", lambda: SimpleNamespace())
    monkeypatch.setattr(recovery, "open_state", lambda: halt_capture.db)
    monkeypatch.setattr(recovery, "observe_pods", observe)
    monkeypatch.setattr(recovery, "always_or_unreachable", Mock())
    monkeypatch.setattr(recovery, "sometimes", Mock())
    monkeypatch.setattr(recovery.observations, "durable_health", lambda: None)

    assert recovery.observer_main(stop=stop) == 0

    assert samples == 4
    assert halt_capture.core.read_namespaced_pod_log.call_count == 1
    emitted = [
        call.args
        for call in halt_capture.emitted.call_args_list
        if call.args[0] == "antithesis_exit_evidence"
    ]
    assert len(emitted) == 1
    status = [
        call.args[1]
        for call in halt_capture.emitted.call_args_list
        if call.args[0] == "antithesis_exit_observer"
    ]
    assert status[-1]["samples"] == 4
    health = [
        call.args[1]
        for call in halt_capture.emitted.call_args_list
        if call.args[0] == "harness_health"
    ]
    assert health[-1]["valid"] is True


def test_anytime_exit_audit_reuses_a_recent_passive_sample(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    with halt_capture.db:
        halt_capture.db.execute(
            "INSERT INTO exit_observer VALUES (1, ?, ?)",
            (0.0, json.dumps({"samples": 7})),
        )
    observe = Mock()
    monkeypatch.setattr(recovery, "open_state", lambda: halt_capture.db)
    monkeypatch.setattr(recovery, "observe_pods", observe)
    monkeypatch.setattr(recovery, "Environment", Mock())

    assert recovery.anytime_main() == 0

    observe.assert_not_called()


def test_capture_records_a_missing_evidence_reason(
    halt_capture: SimpleNamespace,
) -> None:
    from collections import Counter

    counts: Counter[str] = Counter()
    halt_capture.core.read_namespaced_pod_log.return_value = ""
    assert (
        recovery.capture_halt_evidence(halt_capture.observation, 10, stats=counts)
        is None
    )
    assert counts["uncorrelated_or_incomplete_log"] == 1
    assert counts["pod_reads"] == 2
    assert counts["log_reads"] == 1


def test_predelete_capture_is_targeted_and_keeps_the_uid_precondition(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    monkeypatch.setattr(recovery, "open_state", lambda: halt_capture.db)
    env = SimpleNamespace(endpoints=SimpleNamespace(operator_namespace="operator"))
    recovery.preserve_pod_exit(
        cast(recovery.Environment, env),
        namespace="mz",
        pod="envd",
        uid="pod-uid",
        deadline=2,
    )
    halt_capture.core.list_namespaced_pod.assert_not_called()
    assert halt_capture.core.read_namespaced_pod.call_count == 3
    assert halt_capture.core.read_namespaced_pod_log.call_count == 1
    assert halt_capture.emitted.call_args.args[0] == "antithesis_exit_evidence"


@pytest.mark.parametrize("stale", [True, False])
def test_predelete_capture_does_no_log_io_for_stale_or_live_pods(
    halt_capture: SimpleNamespace, stale: bool
) -> None:
    if stale:
        halt_capture.pod.metadata.uid = "replacement"
    else:
        halt_capture.pod.status.container_statuses[0].state.terminated = None
    env = SimpleNamespace(endpoints=SimpleNamespace(operator_namespace="operator"))
    recovery.preserve_pod_exit(
        cast(recovery.Environment, env),
        namespace="mz",
        pod="envd",
        uid="pod-uid",
        deadline=2,
    )
    halt_capture.core.read_namespaced_pod_log.assert_not_called()
    assert halt_capture.core.read_namespaced_pod.call_count == 1


def test_predelete_capture_does_not_spend_an_expired_delete_budget(
    halt_capture: SimpleNamespace,
) -> None:
    recovery.preserve_pod_exit(
        cast(recovery.Environment, SimpleNamespace()),
        namespace="mz",
        pod="envd",
        uid="pod-uid",
        deadline=0,
    )
    halt_capture.core.read_namespaced_pod.assert_not_called()


def test_observer_detects_a_changed_previous_container_id(
    halt_capture: SimpleNamespace,
) -> None:
    cs = halt_capture.pod.status.container_statuses[0]
    cs.state.terminated.container_id = "terminated-id"
    after = copy.deepcopy(halt_capture.pod)
    after.status.container_statuses[0].state.terminated.container_id = "different-id"
    halt_capture.core.read_namespaced_pod.side_effect = [halt_capture.pod, after]
    assert recovery.capture_halt_evidence(halt_capture.observation, 10) is None


def test_passive_observer_survives_a_transient_api_outage(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    stop = threading.Event()
    waits = 0

    def wait(timeout):
        nonlocal waits
        waits += 1
        halt_capture.clock.now += timeout
        if waits == 2:
            stop.set()
        return stop.is_set()

    observe = Mock(side_effect=[recovery.ApiException(status=503), []])
    health_error = Mock()
    monkeypatch.setattr(stop, "wait", wait)
    monkeypatch.setenv("MZ_ANTITHESIS_TEMPLATE_PROFILE", "observer")
    monkeypatch.setattr(recovery, "Environment", Mock())
    monkeypatch.setattr(recovery, "open_state", lambda: halt_capture.db)
    monkeypatch.setattr(recovery, "observe_pods", observe)
    monkeypatch.setattr(observations, "harness_error", health_error)
    monkeypatch.setattr(observations, "durable_health", lambda: None)

    assert recovery.observer_main(stop=stop) == 0

    assert observe.call_count == 2
    health_error.assert_not_called()
    stopped = halt_capture.emitted.call_args.args[1]
    assert stopped["samples"] == 1
    assert stopped["counts"]["api_or_log_unavailable"] == 1
    health = [
        call.args[1]
        for call in halt_capture.emitted.call_args_list
        if call.args[0] == "harness_health"
    ]
    assert health[-1]["valid"] is True


def test_passive_observer_reports_permanent_denial_and_stops(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    observe = Mock(side_effect=recovery.ApiException(status=403))
    health_error = Mock()
    monkeypatch.setenv("MZ_ANTITHESIS_TEMPLATE_PROFILE", "observer")
    monkeypatch.setattr(recovery, "Environment", Mock())
    monkeypatch.setattr(recovery, "open_state", lambda: halt_capture.db)
    monkeypatch.setattr(recovery, "observe_pods", observe)
    monkeypatch.setattr(observations, "harness_error", health_error)
    monkeypatch.setattr(observations, "durable_health", lambda: None)

    assert recovery.observer_main() == 1

    assert observe.call_count == 1
    health_error.assert_called_once()
    failed = halt_capture.emitted.call_args.args[1]
    assert failed["mode"] == "failed"
    health = [
        call.args[1]
        for call in halt_capture.emitted.call_args_list
        if call.args[0] == "harness_health"
    ]
    assert health[-1]["valid"] is False


def test_passive_observer_bounds_steady_state_api_calls(
    monkeypatch: pytest.MonkeyPatch, halt_capture: SimpleNamespace
) -> None:
    stop = threading.Event()
    waits = 0

    def wait(timeout):
        nonlocal waits
        waits += 1
        halt_capture.clock.now += timeout
        if waits == 5:
            stop.set()
        return stop.is_set()

    halt_capture.core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    env = SimpleNamespace(
        endpoints=SimpleNamespace(namespace="mz", operator_namespace="op")
    )
    monkeypatch.setattr(stop, "wait", wait)
    monkeypatch.setenv("MZ_ANTITHESIS_TEMPLATE_PROFILE", "observer")
    monkeypatch.setattr(recovery, "Environment", lambda: env)
    monkeypatch.setattr(recovery, "open_state", lambda: halt_capture.db)
    monkeypatch.setattr(observations, "durable_health", lambda: None)

    assert recovery.observer_main(stop=stop) == 0

    assert halt_capture.clock.now == 5 * recovery.OBSERVER_POLL_S
    assert halt_capture.core.list_namespaced_pod.call_count == 10
    halt_capture.core.read_namespaced_pod_log.assert_not_called()
    assert halt_capture.emitted.call_args.args[1]["counts"]["pod_lists"] == 10


@pytest.mark.parametrize("kind", ["node", "pressure"])
def test_resource_sizing_findings_are_harness_errors(
    monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE seen (key TEXT PRIMARY KEY, kind TEXT, details TEXT, at REAL)"
    )
    finding = resource_kills.Finding(kind, "finding", {"reason": "memory pressure"})
    product = Mock()
    health = Mock()
    monkeypatch.setattr(resource_kills, "always", product)
    monkeypatch.setattr(resource_kills, "harness_error", health, raising=False)
    monkeypatch.setattr(resource_kills, "node_memory", lambda: {})
    resource_kills.check_once(db, Mock(sample=lambda: ([finding], True)))
    assert not any(call.args[0] is False for call in product.call_args_list)
    assert health.call_count == 1
    assert health.call_args.args[1] == resource_kills.MESSAGES[kind]


def test_own_limit_oom_remains_a_product_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE seen (key TEXT PRIMARY KEY, kind TEXT, details TEXT, at REAL)"
    )
    finding = resource_kills.Finding("limit", "finding", {"memory_limit": "1Gi"})
    product = Mock()
    monkeypatch.setattr(resource_kills, "always", product)
    monkeypatch.setattr(resource_kills, "node_memory", lambda: {})
    resource_kills.check_once(db, Mock(sample=lambda: ([finding], True)))
    assert any(
        call.args[:2] == (False, resource_kills.MSG_LIMIT)
        for call in product.call_args_list
    )


def test_incomplete_resource_attribution_is_retried_without_a_product_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE seen (key TEXT PRIMARY KEY, kind TEXT, details TEXT, at REAL)"
    )
    finding = resource_kills.Finding("limit", "finding", {"memory_limit": "1Gi"})
    product = Mock()
    evidence = Mock()
    monkeypatch.setattr(resource_kills, "always", product)
    monkeypatch.setattr(resource_kills, "send_event", evidence, raising=False)
    monkeypatch.setattr(resource_kills, "node_memory", lambda: {})
    resource_kills.check_once(db, Mock(sample=lambda: ([finding], False)))
    product.assert_not_called()
    assert evidence.call_args.args[0] == "workload_harness_unresolved"
    assert evidence.call_args.args[1]["details"]["finding"] == "finding"
    assert db.execute("SELECT count(*) FROM seen").fetchone() == (0,)
    resource_kills.check_once(db, Mock(sample=lambda: ([finding], True)))
    assert any(call.args[0] is False for call in product.call_args_list)


def snapshot(reason: str, request: str = "r0") -> rollouts.Snapshot:
    return rollouts.Snapshot(
        {
            "spec": {"requestRollout": request},
            "status": {
                "conditions": [
                    {"type": "UpToDate", "status": "False", "reason": reason}
                ]
            },
        }
    )


def test_recovery_retries_unacknowledged_normalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kube = Mock()
    kube.snapshot.return_value = snapshot("Applying")
    normalize = Mock(side_effect=[False, True])
    monkeypatch.setattr(rollouts, "normalize", normalize)
    unstick = rollouts.OperatorUnstick(Mock(kube=kube), "test")
    unstick.step()
    assert not unstick.normalized
    unstick.step()
    assert unstick.normalized
    assert normalize.call_count == 2


def test_recovery_retries_unacknowledged_fresh_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kube = Mock()
    kube.snapshot.return_value = snapshot("WaitingForApproval")
    submit = Mock(side_effect=[("r1", None), ("r1", snapshot("Applying", "r1"))])
    monkeypatch.setattr(rollouts, "submit_patch", submit)
    monkeypatch.setattr(
        rollouts, "new_request", lambda: {"requestRollout": "r1", "forceRollout": "r1"}
    )
    unstick = rollouts.OperatorUnstick(Mock(kube=kube), "test", normalized=True)
    assert unstick.step()[1] is False
    assert not unstick.fresh_requested
    assert unstick.step()[1] is True
    assert unstick.fresh_requested
    assert submit.call_count == 2
    assert submit.call_args_list[0] == submit.call_args_list[1]


def test_recovery_reconciles_indeterminate_fresh_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kube = Mock()
    kube.snapshot.side_effect = [
        snapshot("WaitingForApproval"),
        snapshot("Applying", "r1"),
    ]
    submit = Mock(return_value=("r1", None))
    monkeypatch.setattr(rollouts, "submit_patch", submit)
    monkeypatch.setattr(
        rollouts, "new_request", lambda: {"requestRollout": "r1", "forceRollout": "r1"}
    )
    unstick = rollouts.OperatorUnstick(Mock(kube=kube), "test", normalized=True)
    unstick.step()
    unstick.step()
    assert unstick.fresh_requested
    assert submit.call_count == 1


def test_canary_paths_start_without_waiting_for_a_stalled_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Barrier(len(canaries.PROBES))

    def probe(host: str, name: str) -> canaries.ProbeResult:
        started.wait(timeout=2)
        return canaries.ProbeResult(name, "ok", True, 0.0)

    monkeypatch.setattr(canaries, "run_probe", probe)
    assert [r.name for r in canaries._run_probes("host")] == list(canaries.PROBES)


def test_current_termination_is_observed_and_not_counted_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    term = SimpleNamespace(exit_code=42, reason="Error", signal=0, finished_at="now")
    previous = SimpleNamespace(
        exit_code=166, reason="Error", signal=0, finished_at="before"
    )
    status = SimpleNamespace(
        name="environmentd",
        restart_count=1,
        last_state=SimpleNamespace(terminated=previous),
        state=SimpleNamespace(terminated=term, waiting=None),
    )
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            name="envd",
            uid="uid",
            labels={"materialize.cloud/app": "environmentd"},
            deletion_timestamp=None,
        ),
        status=SimpleNamespace(container_statuses=[status]),
    )
    core = Mock()
    core.list_namespaced_pod.side_effect = [
        SimpleNamespace(items=[pod]),
        SimpleNamespace(items=[]),
    ] * 2
    monkeypatch.setattr(recovery.client, "CoreV1Api", lambda: core)
    env = SimpleNamespace(
        endpoints=SimpleNamespace(namespace="mz", operator_namespace="operator")
    )
    observed = recovery.observe_pods(env)  # type: ignore[arg-type]
    assert observed[0].exit_code == 42
    assert core.list_namespaced_pod.call_args.kwargs["_request_timeout"] > 0
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE terminations (pod_uid TEXT, container TEXT, restart_count INTEGER, role TEXT, pod TEXT, exit_code INTEGER, reason TEXT, signal INTEGER, finished_at TEXT, classification TEXT, observed_at REAL, PRIMARY KEY (pod_uid, container, restart_count))"
    )
    verdict = Mock()
    monkeypatch.setattr(recovery, "always_or_unreachable", verdict)
    assert recovery.record_terminations(db, observed) == 1
    assert verdict.call_args.args[0] is False
    status.restart_count = 2
    status.last_state.terminated = term
    status.state.terminated = None
    observed = recovery.observe_pods(env)  # type: ignore[arg-type]
    assert recovery.record_terminations(db, observed) == 0


def test_recovery_comparison_records_and_guards_its_pod_deletes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from materialize.antithesis.drivers import pod_restarts

    envd = SimpleNamespace(
        name="envd",
        namespace="mz",
        uid="envd-uid",
        labels={"materialize.cloud/app": "environmentd"},
        deletion_timestamp=None,
    )
    replica = SimpleNamespace(
        name="replica",
        namespace="mz",
        uid="replica-uid",
        labels={"materialize.cloud/cluster-id": "u1"},
        deletion_timestamp=None,
    )
    core = Mock()
    core.list_namespaced_pod.return_value.items = [
        SimpleNamespace(metadata=m) for m in (envd, replica)
    ]
    monkeypatch.setattr(recovery.client, "CoreV1Api", lambda: core)
    env = SimpleNamespace(
        endpoints=SimpleNamespace(namespace="mz", operator_namespace="operator")
    )
    kube = Mock(core=core)
    monkeypatch.setattr(rollouts, "Kube", lambda _: kube)
    deleted = Mock(return_value=True)
    monkeypatch.setattr(pod_restarts, "delete_recorded", deleted)
    monkeypatch.setattr(recovery.rng, "choice", Mock(side_effect=["both", replica]))
    monkeypatch.setattr(recovery.lifecycle, "Session", Mock())
    monkeypatch.setattr(recovery.lifecycle, "open_state", Mock())
    monkeypatch.setattr(
        recovery.lifecycle,
        "quiet_projection",
        Mock(side_effect=[(1, "before"), (1, "after")]),
    )
    monkeypatch.setattr(recovery.lifecycle, "compare_projections", Mock())
    monkeypatch.setattr(recovery.lifecycle, "check_readable", Mock())
    monkeypatch.setattr(recovery, "poll", Mock(return_value=(True, {}, 0.0)))
    check = recovery.Recovery.__new__(recovery.Recovery)
    check.env = env  # type: ignore[assignment]
    check.mode = "test"
    check.timings = {}
    check.restart_and_compare()
    assert deleted.call_count == 2
    assert [c.kwargs["uid"] for c in deleted.call_args_list] == [
        "envd-uid",
        "replica-uid",
    ]
    assert all(c.kwargs["origin"] == "recovery" for c in deleted.call_args_list)
    core.delete_namespaced_pod.assert_not_called()
    assert core.list_namespaced_pod.call_args.kwargs["_request_timeout"] > 0


def test_rollout_pod_kills_respect_quiet_periods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from materialize.antithesis.drivers import pod_restarts

    monkeypatch.setattr(pod_restarts, "quiet_period_active", lambda _: True)
    deleted = Mock(return_value=True)
    monkeypatch.setattr(pod_restarts, "delete_recorded", deleted)
    ctx = Mock()
    ctx.kube.lease_holder.return_value = "operator"
    assert not rollouts.execute_kill(
        ctx, rollouts.KillPlan("Applying", "orchestratord", 1), snapshot("Applying")
    )
    deleted.assert_not_called()


@pytest.mark.parametrize(
    "before,after,checked",
    [
        (
            {"pods": [["u1", 0]], "active_generation": 1, "phase": "Applying"},
            {"pods": [["u1", 1]], "active_generation": 1, "phase": "Applying"},
            True,
        ),
        (
            {"active_generation": 1, "phase": "Applying"},
            {"active_generation": 1, "phase": "ReadyToPromote"},
            True,
        ),
        (
            {"active_generation": 1, "phase": "Applying"},
            {"active_generation": 1, "phase": "Promoting"},
            False,
        ),
        (
            {"active_generation": 1, "phase": "Applying"},
            {"active_generation": 2, "phase": "Applied"},
            False,
        ),
    ],
)
def test_only_promotion_exempts_under_load_canary_deadlines(
    monkeypatch: pytest.MonkeyPatch, before: dict, after: dict, checked: bool
) -> None:
    monkeypatch.setattr(canaries, "_wait_for_sql", lambda *args: 0.0)
    monkeypatch.setattr(canaries.time, "sleep", lambda _: None)
    monkeypatch.setattr(canaries, "_ensure_objects", lambda _: None)
    monkeypatch.setattr(
        canaries, "_deployment_signature", Mock(side_effect=[before, after])
    )
    monkeypatch.setattr(canaries.history, "recent_activity", lambda: 1)
    monkeypatch.setattr(canaries.counters, "recent_activity", lambda: 1)
    result = canaries.ProbeResult("P1", "unanswered", True, 181)
    monkeypatch.setattr(canaries, "_run_probes", lambda _: [result])
    assertion = Mock()
    monkeypatch.setattr(canaries, "_assert_under_load", assertion)
    canaries._probe_under_load(Mock(), "host", True)
    assert assertion.called is checked


def test_temporary_probe_stall_keeps_the_latency_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def attempt(
        host: str, name: str, start: float, end: float, result: canaries.ProbeResult
    ) -> None:
        result.status = "ok"
        result.accepted = True

    monkeypatch.setattr(canaries, "_attempt", attempt)
    monkeypatch.setattr(
        canaries.time,
        "monotonic",
        Mock(side_effect=[0.0, canaries.PROBE_DEADLINE_S + 1]),
    )
    result = canaries.run_probe("host", "P1")
    assert result.answered
    assert not result.answered_in_time
    verdict = Mock()
    latency = Mock()
    monkeypatch.setattr(canaries, "always", verdict)
    monkeypatch.setattr(canaries, "sometimes", latency)
    canaries._assert_under_load(result)
    assert verdict.call_args.args[0] is True
    assert latency.call_args.args[0] is False


def test_probe_that_never_answers_fails_the_wedge_property(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()

    def attempt(
        host: str, name: str, start: float, end: float, result: canaries.ProbeResult
    ) -> None:
        result.accepted = True
        release.wait(5)
        result.status = "ok"

    monkeypatch.setattr(canaries, "_attempt", attempt)
    monkeypatch.setattr(canaries, "_wedge_bound", lambda _: 0.01)
    try:
        result = canaries.run_probe("host", "P1")
        assert result.status == "unanswered"
        assert result.accepted
        verdict = Mock()
        monkeypatch.setattr(canaries, "always", verdict)
        canaries._assert_under_load(result)
        assert verdict.call_args.args[0] is False
    finally:
        release.set()


def test_interrupted_paced_ddl_releases_renewable_quiet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = []

    @contextmanager
    def held(requester: str, chunk_s: int, exclusive: bool):
        assert chunk_s <= 60
        assert exclusive
        events.append("held")
        try:
            yield True
        finally:
            events.append("released")

    monkeypatch.setattr(quiet, "held", held)
    monkeypatch.setattr(
        rollouts,
        "ddl_check_interval_seconds",
        Mock(side_effect=RuntimeError("interrupted")),
    )
    with pytest.raises(RuntimeError, match="interrupted"):
        rollouts.action_paced_ddl(Mock())
    assert events == ["held", "released"]
