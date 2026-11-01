# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from materialize.antithesis import local_runner as runner
from materialize.antithesis import local_scheduler as scheduling


def command(
    tmp_path,
    name="parallel_driver_history",
    family="history",
    mode="parallel",
    body="import time; time.sleep(0.15)",
):
    path = tmp_path / f"{name}.py"
    path.write_text(body)
    return scheduling.Command(path, family, mode)


def worker(path, seed, output):
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "materialize.antithesis.local_runner",
            "--worker",
            str(path),
            "--seed",
            str(seed),
        ],
        env={**os.environ, "ANTITHESIS_SDK_LOCAL_OUTPUT": str(output)},
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_child_seed_controls_real_workload_random_draws(tmp_path):
    source = command(
        tmp_path,
        body="import json; from materialize.antithesis.rng import rng; print(json.dumps([rng.randrange(1000000) for _ in range(10)]))",
    )
    results = [
        worker(source.path, seed, tmp_path / f"sdk-{i}.jsonl")
        for i, seed in enumerate((17, 17, 18))
    ]
    assert all(result.returncode == 0 for result in results)
    values = [result.stdout.splitlines()[-1] for result in results]
    assert values[0] == values[1] != values[2]
    assert scheduling.child_seed(1, source.path.name, 1) != scheduling.child_seed(
        1, source.path.name, 2
    )


@pytest.mark.parametrize("family,limit", list(scheduling.EXCLUSIVE.items()))
def test_only_known_exclusive_families_are_capped(tmp_path, family, limit):
    source = command(tmp_path, family=family)
    assert scheduling.compatible(source, [source] * (limit - 1), 8)
    assert not scheduling.compatible(source, [source] * limit, 8)
    ordinary = command(tmp_path, name="parallel_driver_history_other")
    assert scheduling.compatible(ordinary, [ordinary] * 7, 8)


def test_serial_can_share_with_anytime_but_not_other_drivers(tmp_path):
    ordinary = command(tmp_path)
    serial = command(tmp_path, "serial_driver_test", "serial_test", "serial")
    check = command(tmp_path, "anytime_check", "check", "anytime")
    assert scheduling.compatible(serial, [check], 8)
    assert scheduling.compatible(check, [serial], 8)
    assert not scheduling.compatible(serial, [ordinary], 8)
    assert not scheduling.compatible(ordinary, [serial], 8)


def test_higher_concurrency_runs_overlapping_ordinary_writers(tmp_path):
    source = command(tmp_path)
    scheduler = scheduling.Scheduler(tmp_path, 8, 17, 5, 0.1, 5)
    scheduler.phase([source], 1)
    assert scheduler.max_running == 8
    assert (source.path.name, source.path.name) in scheduler.overlaps
    assert scheduler.completed
    assert len({row["seed"] for row in scheduler.completed}) == len(scheduler.completed)
    assert all(row["finished"] >= row["started"] for row in scheduler.completed)


def test_process_group_cleanup_kills_term_ignoring_descendant(tmp_path):
    child_pid = tmp_path / "child.pid"
    body = f"""
import subprocess,sys,time
subprocess.Popen([sys.executable,'-c',"import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); open({str(child_pid)!r},'w').write(str(os.getpid())); time.sleep(60)"])
time.sleep(60)
"""
    source = command(tmp_path, body=body)
    scheduler = scheduling.Scheduler(tmp_path, 1, 1, 5, 0.1, 5)
    scheduler.start(source)
    try:
        deadline = time.monotonic() + 5
        while not child_pid.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert child_pid.exists()
        pid = int(child_pid.read_text())
    finally:
        scheduler.reap(phase_end=True)
    stat = Path(f"/proc/{pid}/stat")
    deadline = time.monotonic() + 1
    while (
        stat.exists()
        and stat.read_text().rsplit(")", 1)[1].split()[0] != "Z"
        and time.monotonic() < deadline
    ):
        time.sleep(0.01)
    assert not stat.exists() or stat.read_text().rsplit(")", 1)[1].split()[0] == "Z"
    assert scheduler.completed[0]["incomplete"] == "phase_deadline"


def test_real_sdk_false_invariants_are_failures_even_with_exit_zero(tmp_path):
    pytest.importorskip("antithesis")
    source = command(
        tmp_path,
        body="""
from antithesis.assertions import always_or_unreachable, unreachable, sometimes
always_or_unreachable(False, 'actual wrong result')
unreachable('actual unexpected error')
sometimes(False, 'optional coverage is not an invariant failure')
""",
    )
    result = worker(source.path, 1, tmp_path / "real.sdk.jsonl")
    assert result.returncode == 0
    proof = runner.evidence(tmp_path)
    assert {row["assertion"]["message"] for row in proof["false_hits"]} == {
        "actual wrong result",
        "actual unexpected error",
    }


def test_declarations_false_guidance_and_other_rounds_do_not_fake_failures_or_witnesses(
    tmp_path,
):
    records = [
        {
            "antithesis_assert": {
                "id": "unhit",
                "message": "optional",
                "hit": False,
                "condition": False,
                "assert_type": "always",
            }
        },
        {"workload_coverage": {"message": "absent witness", "observed": False}},
        {
            "antithesis_interleaving": {
                "round": "a",
                "stage": "disruption",
                "outcome": "acked",
                "mode": "held_append",
            }
        },
        {
            "antithesis_interleaving": {
                "round": "b",
                "stage": "checked",
                "successful_reads": 1,
            }
        },
    ]
    path = tmp_path / "invocation.sdk.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in records))
    proof = runner.evidence(tmp_path)
    assert not proof["false_hits"] and not proof["evaluated_invariants"]
    assert len(proof["declared_only_invariants"]) == 1
    assert (
        not proof["positive_witnesses"] and not proof["diagnostic_checked_fault_rounds"]
    )
    with path.open("a") as output:
        output.write(
            json.dumps(
                {
                    "antithesis_interleaving": {
                        "round": "a",
                        "stage": "checked",
                        "successful_reads": 1,
                    }
                }
            )
            + "\n"
        )
    proof = runner.evidence(tmp_path)
    assert len(proof["diagnostic_checked_fault_rounds"]) == 1
    assert not proof["coherent_fault_rounds"]


def _history_protocol():
    metadata = {
        "profile": "transactions",
        "command": "parallel_driver_history_interleavings.py",
        "round": "71:123",
        "mode": "held_append",
        "epoch": 3,
        "key": 1,
        "read_key": None,
    }
    return [
        {**metadata, "stage": "start"},
        {
            **metadata,
            "stage": "disruption",
            "action": "delete",
            "outcome": "acked",
            "role": "clusterd",
            "pod_uid": "6ee856f5-508c-4b74-b803-df57fbf28340",
            "cluster_id": "u2",
        },
        {**metadata, "stage": "write", "write_id": 17, "outcome": "ok"},
        {
            **metadata,
            "stage": "checked",
            "write_id": 17,
            "read_id": 19,
            "successful_reads": 1,
            "observed_write": True,
        },
        {**metadata, "stage": "end", "outcome": "completed"},
    ]


def _protocol_evidence(tmp_path, records):
    (tmp_path / "protocol.sdk.jsonl").write_text(
        "".join(json.dumps({"antithesis_interleaving": row}) + "\n" for row in records)
    )
    return runner.evidence(tmp_path)


def test_coherent_protocol_does_not_claim_strict_fault_exposure(tmp_path):
    proof = _protocol_evidence(tmp_path, _history_protocol())
    assert len(proof["coherent_fault_rounds"]) == 1
    assert len(proof["diagnostic_checked_fault_rounds"]) == 1
    assert not proof["authoritative_fault_witnesses"]
    assert "checked_fault_rounds" not in proof


@pytest.mark.parametrize(
    "index,changes",
    [
        (3, {"observed_write": False}),
        (4, {"outcome": "unresolved"}),
        (3, {"epoch": 4}),
        (3, {"write_id": 18}),
        (1, {"role": "environmentd"}),
        (1, {"pod_uid": None}),
        (2, {"outcome": "rejected"}),
        (2, {"command": "parallel_driver_other.py"}),
    ],
)
def test_incomplete_or_conflicting_protocol_stays_diagnostic(tmp_path, index, changes):
    records = _history_protocol()
    records[index].update(changes)
    proof = _protocol_evidence(tmp_path, records)
    assert len(proof["diagnostic_checked_fault_rounds"]) == 1
    assert not proof["coherent_fault_rounds"]


@pytest.mark.parametrize(
    "mutation", ["missing_start", "missing_end", "early_check", "duplicate_write"]
)
def test_protocol_requires_complete_ordered_round(tmp_path, mutation):
    records = _history_protocol()
    if mutation == "missing_start":
        records.pop(0)
    elif mutation == "missing_end":
        records.pop()
    elif mutation == "early_check":
        records[1], records[3] = records[3], records[1]
    else:
        records.insert(3, dict(records[2]))
    proof = _protocol_evidence(tmp_path, records)
    assert len(proof["diagnostic_checked_fault_rounds"]) == 1
    assert not proof["coherent_fault_rounds"]


def test_racing_write_may_finish_before_delete_ack_without_proving_exposure(tmp_path):
    records = _history_protocol()
    for record in records:
        record["mode"] = "race_append"
    records[1], records[2] = records[2], records[1]
    proof = _protocol_evidence(tmp_path, records)
    assert len(proof["coherent_fault_rounds"]) == 1
    assert not proof["authoritative_fault_witnesses"]


def test_strict_exposure_requires_positive_authoritative_coverage(tmp_path):
    proof = _protocol_evidence(tmp_path, _history_protocol())
    assert not proof["authoritative_fault_witnesses"]
    message = "coverage: a later read checked a write unresolved throughout an active-generation delete"
    observation = {
        "message": message,
        "observed": True,
        "location": {"file": "/usr/lib/materialize/antithesis/telemetry_coverage.py"},
        "details": {
            "combinations": [
                ["Applied", "environmentd_active", "append", "index", "ok"]
            ],
            "reads": 1,
        },
    }
    path = tmp_path / "coverage.sdk.jsonl"
    for changes in ({"observed": False}, {"location": {"file": "/tmp/other.py"}}):
        path.write_text(
            json.dumps({"workload_coverage": {**observation, **changes}}) + "\n"
        )
        assert not runner.evidence(tmp_path)["authoritative_fault_witnesses"]
    path.write_text(json.dumps({"workload_coverage": observation}) + "\n")
    proof = runner.evidence(tmp_path)
    assert set(proof["authoritative_fault_witnesses"]) == {message}
    assert proof["fault_round_attribution_available"] is False


@pytest.mark.parametrize(
    "record",
    [
        {"harness_health": {"valid": False}},
        {"workload_harness_error": {"component": "ledger"}},
        {"antithesis_error": {"message": "invalid details"}},
        {"workload_coverage": {"message": "invalid boolean", "observed": "yes"}},
        {"antithesis_interleaving": None},
    ],
)
def test_invalid_health_sdk_and_harness_payloads_are_not_clean(tmp_path, record):
    if record == {"antithesis_interleaving": None}:
        record = {"antithesis_interleaving": {"round": 7}}
    (tmp_path / "bad.sdk.jsonl").write_text(json.dumps(record) + "\n")
    assert runner.evidence(tmp_path)["harness_errors"]


def test_counter_delta_uses_only_this_local_interval_and_rejects_reset():
    before = {
        "commands": [
            {"profile": "contracts", "command": "driver", "completed": 19, "cpu_s": 3.0}
        ]
    }
    after = {
        "commands": [
            {"profile": "contracts", "command": "driver", "completed": 21, "cpu_s": 3.2}
        ]
    }
    delta = runner.counter_delta(before, after, "commands", ("profile", "command"))
    assert delta[0]["completed"] == 2
    assert delta[0]["cpu_s"] == pytest.approx(0.2)
    with pytest.raises(ValueError, match="regressed"):
        runner.counter_delta(after, before, "commands", ("profile", "command"))


@pytest.fixture
def experiment(monkeypatch, tmp_path):
    monkeypatch.setenv("MZ_ANTITHESIS_STATE_DIR", str(tmp_path / "state"))
    setup = command(tmp_path, "first_configure", "configure", "first", "pass")
    driver = command(
        tmp_path,
        "parallel_driver_test",
        "test",
        "parallel",
        "import time; time.sleep(0.02)",
    )
    checker = command(tmp_path, "finally_check", "check", "finally", "pass")
    monkeypatch.setattr(runner, "discover", lambda directory: [setup, driver, checker])
    healthy = {
        "telemetry_available": True,
        "harness_health": {"valid": True, "errors": 0},
        "active_commands": 0,
    }
    monkeypatch.setattr(
        runner,
        "telemetry_snapshot",
        lambda output, label: {
            **healthy,
            "comparisons": (
                []
                if label == "before"
                else [
                    {
                        "profile": "transactions",
                        "command": driver.path.name,
                        "family": "test",
                        "path": "table",
                        "outcome": "passed",
                        "units": 1,
                    }
                ]
            ),
        },
    )
    args = argparse.Namespace(
        output=tmp_path / "result",
        templates=tmp_path,
        profile="transactions",
        duration=0.25,
        concurrency=2,
        seed=7,
        command_timeout=1,
        setup_timeout=2,
        shutdown_grace=0.05,
        check_interval=5,
        include=[],
        check=[],
        require_witness=[],
        skip_first=True,
    )
    return args, driver, checker, setup


def test_initial_false_sdk_assertion_stops_before_workloads(monkeypatch, experiment):
    args, driver, _, setup = experiment
    args.skip_first = False
    setup.path.write_text(
        "from antithesis.assertions import always_or_unreachable; always_or_unreachable(False,'bad initialization')"
    )
    assert runner.run(args) == 1
    report = json.loads((args.output / "summary.json").read_text())
    assert report["stage"] == "initialization" and report["false_hits"]
    assert not any(row["command"] == driver.path.name for row in report["schedule"])


def test_sticky_invalid_baseline_stops_before_workloads(monkeypatch, experiment):
    args, _, _, _ = experiment
    monkeypatch.setattr(
        runner,
        "telemetry_snapshot",
        lambda *args: {
            "telemetry_available": False,
            "harness_health": {"valid": False},
        },
    )
    assert runner.run(args) == 1
    assert json.loads((args.output / "summary.json").read_text())["stage"] == "baseline"
