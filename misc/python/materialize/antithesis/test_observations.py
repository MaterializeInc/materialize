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
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

pytest.importorskip("antithesis")

from antithesis import assertions
from antithesis.catalog import (  # pyright: ignore[reportMissingTypeStubs]
    scan_source,
    scan_tree,
)

from materialize.antithesis import observations


@pytest.fixture(autouse=True)
def clear_observations(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(observations, "_seen", {})
    monkeypatch.setattr(observations, "_numeric", {})


def test_workload_catalog_declares_only_optional_product_properties() -> None:
    entries = scan_tree(str(Path(__file__).parent))
    assert entries
    assert not [entry.message for entry in entries if entry.kind.must_hit]


def test_optional_product_catalog_keeps_invariants_and_excludes_coverage() -> None:
    entries = scan_source(
        """
from antithesis.assertions import always_or_unreachable as always, unreachable
from materialize.antithesis.observations import sometimes, reachable
always(True, "a product invariant")
unreachable("an unexpected product result")
sometimes(False, "an unobserved target")
reachable("a milestone")
""",
        "probe.py",
    )
    assert {entry.message: entry.kind.value for entry in entries} == {
        "a product invariant": "AlwaysOrUnreachable",
        "an unexpected product result": "Unreachable",
    }
    assert not any(entry.kind.must_hit for entry in entries)


def test_coverage_never_emits_product_failures_and_real_false_invariants_remain(
    tmp_path: Path,
) -> None:
    output = tmp_path / "sdk.jsonl"
    env = {**os.environ, "ANTITHESIS_SDK_LOCAL_OUTPUT": str(output)}
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
from antithesis.assertions import always_or_unreachable, unreachable
from antithesis.catalog import register, scan_source
from materialize.antithesis.observations import sometimes, reachable
register(scan_source('from antithesis.assertions import always_or_unreachable as always; always(True, "never evaluated")', 'optional.py'))
for _ in range(10):
    sometimes(False, "missed coverage")
sometimes(True, "missed coverage")
reachable("reached coverage", {"value": 7})
always_or_unreachable(False, "real wrong result", {"actual": 1, "expected": 2})
unreachable("real unexpected error")
""",
        ],
        env=env,
        check=True,
    )
    records = [json.loads(line) for line in output.read_text().splitlines()]
    product = [
        record["antithesis_assert"]
        for record in records
        if "antithesis_assert" in record
    ]
    assert {record["message"] for record in product} == {
        "never evaluated",
        "real wrong result",
        "real unexpected error",
    }
    assert not any(record["must_hit"] for record in product)
    wrong = next(
        record for record in product if record["message"] == "real wrong result"
    )
    assert wrong["hit"] and not wrong["condition"]
    assert wrong["assert_type"] == "always"
    unexpected = next(
        record for record in product if record["message"] == "real unexpected error"
    )
    assert unexpected["hit"] and not unexpected["condition"]
    assert unexpected["assert_type"] == "reachability"
    coverage = [
        record["workload_coverage"]
        for record in records
        if "workload_coverage" in record
    ]
    assert [(record["message"], record["observed"]) for record in coverage] == [
        ("missed coverage", False),
        ("missed coverage", True),
        ("reached coverage", True),
    ]


def test_boolean_guidance_matches_the_pinned_sdk_wire_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted = []
    monkeypatch.setattr(
        observations, "send_event", lambda key, value: emitted.append({key: value})
    )
    observations.sometimes(False, "a target")
    guidance = [
        record["antithesis_guidance"]
        for record in emitted
        if "antithesis_guidance" in record
    ]
    assert len(guidance) == 2
    assert not guidance[0]["hit"] and guidance[1]["hit"]
    runtime = guidance[1]
    expected = []
    monkeypatch.setattr(
        assertions, "dispatch_output", lambda raw: expected.append(json.loads(raw))
    )
    location = runtime["location"]
    getattr(assertions, "_boolean_guidance_raw")(
        {"observed": False},
        runtime["message"],
        True,
        location["file"],
        location["function"],
        location["class"],
        location["begin_line"],
        location["begin_column"],
        True,
        runtime["id"],
    )
    assert expected == [{"antithesis_guidance": runtime}]


def test_observation_emission_state_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(observations, "STATE_CAPACITY", 2)
    monkeypatch.setattr(observations, "send_event", lambda *args: None)
    for i in range(10):
        observations.sometimes(False, f"target-{i}")
    assert len(observations._seen) == 2


@pytest.mark.parametrize("maximize", [False, True])
def test_numeric_guidance_matches_sdk_and_retains_only_extreme_values(
    monkeypatch: pytest.MonkeyPatch, maximize: bool
) -> None:
    emitted = []
    monkeypatch.setattr(
        observations, "send_event", lambda key, value: emitted.append({key: value})
    )
    values = [3, 2, 4, 4, 1]
    for value in values:
        observations.numeric_guidance(
            value, 0, f"a numeric target {maximize}", maximize=maximize
        )
    runtime = [
        record["antithesis_guidance"]
        for record in emitted
        if record["antithesis_guidance"]["hit"]
    ]
    assert [record["guidance_data"]["left"] for record in runtime] == (
        [3, 4] if maximize else [3, 2, 1]
    )
    expected = []
    monkeypatch.setattr(
        assertions, "dispatch_output", lambda raw: expected.append(json.loads(raw))
    )
    sample = runtime[0]
    location = sample["location"]
    getattr(assertions, "_numeric_guidance_raw")(
        3,
        0,
        sample["message"],
        maximize,
        location["file"],
        location["function"],
        location["class"],
        location["begin_line"],
        location["begin_column"],
        True,
        sample["id"],
    )
    assert expected == [{"antithesis_guidance": sample}]


def test_numeric_guidance_does_not_turn_invalid_input_into_product_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitted = []
    monkeypatch.setattr(
        observations, "send_event", lambda key, value: emitted.append({key: value})
    )
    observations.numeric_guidance(float("nan"), 0, "nonfinite", maximize=True)
    observations.numeric_guidance(1, 0, "direction", maximize=True)
    observations.numeric_guidance(2, 0, "direction", maximize=False)
    assert not any("antithesis_assert" in record for record in emitted)
    invalid = [
        record["workload_harness_error"]
        for record in emitted
        if "workload_harness_error" in record
    ]
    assert len(invalid) == 2 and not any(record["valid"] for record in invalid)


def test_harness_errors_have_an_explicit_invalidity_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = Mock()
    monkeypatch.setattr(observations, "send_event", output)
    observations.harness_error(
        "telemetry", "database unavailable", {"error": "OSError"}
    )
    output.assert_called_once_with(
        "workload_harness_error",
        {
            "valid": False,
            "component": "telemetry",
            "message": "database unavailable",
            "details": {"error": "OSError"},
        },
    )


def test_harness_error_keeps_a_health_signal_with_unserializable_details(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MZ_ANTITHESIS_STATE_DIR", str(tmp_path))
    output = Mock()
    monkeypatch.setattr(observations, "send_event", output)
    observations.harness_error("test", "invalid diagnostic", {"value": object()})
    event = output.call_args.args[1]
    assert not event["valid"]
    assert event["details"] == {"details_serialization_error": "TypeError"}
    assert observations.durable_health() == event


def test_coverage_keeps_identity_and_condition_when_details_cannot_serialize(
    tmp_path: Path,
) -> None:
    output = tmp_path / "sdk.jsonl"
    env = {
        **os.environ,
        "ANTITHESIS_SDK_LOCAL_OUTPUT": str(output),
        "MZ_ANTITHESIS_STATE_DIR": str(tmp_path / "state"),
    }
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
from materialize.antithesis.observations import sometimes
cycle = {}
cycle['self'] = cycle
sometimes(False, "unsupported details", {"value": object()})
sometimes(True, "cyclic details", cycle)
sometimes(False, "large details", {"value": "x" * 10000})
""",
        ],
        env=env,
        check=True,
    )
    records = [json.loads(line) for line in output.read_text().splitlines()]
    coverage = {
        record["workload_coverage"]["message"]: record["workload_coverage"]
        for record in records
        if "workload_coverage" in record
    }
    assert {message: record["observed"] for message, record in coverage.items()} == {
        "unsupported details": False,
        "cyclic details": True,
        "large details": False,
    }
    assert coverage["unsupported details"]["details"] == {
        "details_serialization_error": "TypeError"
    }
    assert coverage["cyclic details"]["details"] == {
        "details_serialization_error": "ValueError"
    }
    assert coverage["large details"]["details"]["details_omitted"]
    errors = [
        record["workload_harness_error"]
        for record in records
        if "workload_harness_error" in record
    ]
    assert {record["details"]["target"] for record in errors} == {
        "unsupported details",
        "cyclic details",
    }
    assert not any(
        "antithesis_error" in record or "antithesis_assert" in record
        for record in records
    )


@pytest.mark.parametrize("contents", ["[]", "null", "5", '"text"', "true"])
def test_nonobject_health_markers_remain_explicitly_invalid(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, contents: str
) -> None:
    monkeypatch.setenv("MZ_ANTITHESIS_STATE_DIR", str(tmp_path))
    (tmp_path / observations.INVALID_MARKER).write_text(contents)
    health = observations.durable_health()
    assert health is not None and health["valid"] is False
    assert "malformed" in health["message"]


def test_large_harness_details_are_bounded_without_losing_invalidity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = Mock()
    monkeypatch.setattr(observations, "send_event", output)
    observations.harness_error("test", "large diagnostic", {"value": "x" * 10_000})
    event = output.call_args.args[1]
    assert not event["valid"] and event["details"]["details_omitted"]


def test_inventory_retains_unseen_targets_and_exposes_dynamic_calls(
    tmp_path: Path,
) -> None:
    (tmp_path / "driver.py").write_text(
        'sometimes(False, "unseen target")\n'
        'r.reachable(message="a milestone")\n'
        'sometimes(False, f"dynamic {value}")\n'
    )
    (tmp_path / "test_driver.py").write_text('sometimes(False, "test-only")\n')
    catalog = observations.inventory(tmp_path)
    assert {target["message"] for target in catalog["targets"]} == {
        "unseen target",
        "a milestone",
    }
    assert catalog["unresolved"] == [
        {
            "kind": "sometimes",
            "file": "driver.py",
            "line": 3,
            "reason": "message is not a string literal",
        }
    ]
