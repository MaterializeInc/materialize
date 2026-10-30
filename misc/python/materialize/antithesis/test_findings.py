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
import sys
from pathlib import Path
from typing import Any

import pytest

from materialize.antithesis import findings
from materialize.antithesis.exit_evidence import ExitDisposition


def prop(
    identity: str,
    group: str | None = "Always: Always",
    *,
    name: str | None = None,
    status: str = "Failing",
    examples: int = 0,
    counterexamples: int = 1,
) -> dict[str, Any]:
    return {
        "id": identity,
        "group": group,
        "name": name or f"{group}: {identity}",
        "status": status,
        "example_count": examples,
        "counterexample_count": counterexamples,
        "counterexamples": (
            [{"moment": {"input_hash": "sample", "vtime": 17.0}}]
            if counterexamples
            else []
        ),
    }


def passing() -> dict[str, Any]:
    return prop("read_agreement", status="Passing", examples=4, counterexamples=0)


def exit_prop(code: int = 166, *, count: int = 1) -> dict[str, Any]:
    return prop(
        "exit",
        "No unexpected container exits",
        name=f"No unexpected container exits: container: environmentd, namespace: test, exit code: {code}",
        counterexamples=count,
    )


def test_historical_finding_shape_preserves_every_category() -> None:
    properties = [prop(f"coverage_{i}", "Sometimes: Sometimes") for i in range(136)]
    properties += [
        prop(f"missed_{i}", "Always: Missed Always", counterexamples=0)
        for i in range(9)
    ]
    properties += [
        prop(name, None, name=name)
        for name in (
            "All commands were started at least once",
            "All commands were run to completion at least once",
            "Every instrumented module has symbols",
        )
    ]
    panic = exit_prop(134)
    panic["name"] = panic["name"].replace("environmentd", "clusterd")
    properties += [
        panic,
        prop("crash", "No unexpected crashes", name="No unexpected crashes: clusterd"),
        {**exit_prop(count=9), "id": "halt"},
    ]
    saved = copy.deepcopy(properties)
    report = findings.summarize(properties, events=[])
    assert report["counts"] == {
        "input_properties": 151,
        "raw_failing_properties": 151,
        "product_candidate_properties": 2,
        "product_failure_properties": 0,
        "evaluated_product_properties": 2,
        "coverage": 147,
        "harness_health": 1,
        "unresolved": 1,
        "accepted_events": 0,
    }
    assert report["verdict"] == "inconclusive"
    assert len(report["product_candidates"]) == 2
    assert len(report["incomplete_coverage"]["missing_properties"]) == 147
    assert report["unresolved"][0]["property"]["counterexample_count"] == 9
    retained = [
        record["property"]
        for key in ("product_checks", "coverage", "harness_health", "unresolved")
        for record in report[key]
    ]
    assert {record["id"] for record in retained} == {record["id"] for record in saved}
    assert properties == saved


@pytest.mark.parametrize(
    "group", ["Always: Always", "Always: AlwaysOrUnreachable", "Always: Unreachable"]
)
def test_concrete_sdk_counterexample_remains_a_product_failure(group: str) -> None:
    report = findings.summarize([prop("wrong_rows", group)], events=[])
    assert report["verdict"] == "product_failure"
    assert report["product_failures"][0]["property"]["id"] == "wrong_rows"


def test_coverage_misses_do_not_accuse_the_product_after_evaluated_checks() -> None:
    report = findings.summarize(
        [
            passing(),
            prop("promotion", "Sometimes: Reachable"),
            prop("conditional", "Always: Missed Always", counterexamples=0),
        ],
        events=[],
    )
    assert report["verdict"] == "no_product_findings"
    assert not report["product_failures"]
    assert report["incomplete_coverage"]["missing_properties"] == [
        "promotion",
        "conditional",
    ]


@pytest.mark.parametrize(
    "properties",
    [
        [],
        [prop("promotion", "Sometimes: Sometimes")],
        [
            prop(
                "optional",
                "Always: AlwaysOrUnreachable",
                status="Passing",
                counterexamples=0,
            )
        ],
    ],
)
def test_an_export_without_evaluated_product_checks_is_inconclusive(properties) -> None:
    report = findings.summarize(properties, events=[])
    assert report["verdict"] == "inconclusive"
    assert not report["product_failures"]


def test_properties_alone_do_not_claim_harness_health_was_assessed() -> None:
    report = findings.summarize([passing()])
    assert report["verdict"] == "inconclusive"
    assert report["property_disposition"] == "no_product_findings"
    assert report["evidence_scope"]["health_events_provided"] is False


def test_missing_debug_symbols_does_not_hide_a_concrete_product_violation() -> None:
    symbols = prop("symbols", None, name="Every instrumented module has symbols")
    report = findings.summarize([prop("wrong_rows"), symbols], events=[])
    assert report["verdict"] == "product_failure"
    assert len(report["product_failures"]) == 1
    assert report["harness_health"][0]["severity"] == "advisory"


def designed_panic(container: str = "environmentd") -> dict[str, Any]:
    return {
        "source": {"container": f"pod/{container}", "namespace": "test"},
        "moment": {"input_hash": "sample", "vtime": 16.99},
        "workload_coverage": {
            "message": "Materialize process panicked by design",
            "observed": True,
        },
    }


@pytest.mark.parametrize(
    "native",
    [
        exit_prop(134),
        prop(
            "crash", "No unexpected crashes", name="No unexpected crashes: environmentd"
        ),
    ],
)
def test_designed_panic_context_does_not_become_a_confirmed_native_bug(native) -> None:
    report = findings.summarize([passing(), native], events=[designed_panic()])
    assert report["verdict"] == "inconclusive"
    assert not report["product_failures"]
    assert report["product_candidates"][0]["property"] == native
    assert report["unresolved"][0]["attribution_only"] is True


def test_designed_panic_conflict_preserves_independent_unexpected_sdk_panic() -> None:
    unexpected = prop(
        "unexpected_panic",
        "Always: Unreachable",
        name="Always: Unreachable: Materialize process panicked",
    )
    report = findings.summarize([unexpected, exit_prop(134)], events=[designed_panic()])
    assert report["verdict"] == "product_failure"
    assert [record["property"]["id"] for record in report["product_failures"]] == [
        "unexpected_panic"
    ]
    assert len(report["product_candidates"]) == 2


def test_designed_panic_from_another_known_container_does_not_hide_native_crash() -> (
    None
):
    report = findings.summarize([exit_prop(134)], events=[designed_panic("clusterd")])
    assert report["verdict"] == "product_failure"


@pytest.mark.parametrize(
    "name,group", [("future property", "Future: New"), ("Peak memory usage", None)]
)
def test_unknown_failed_properties_are_never_silently_filtered(name, group) -> None:
    raw = prop("future", group, name=name)
    report = findings.summarize([passing(), raw], events=[])
    assert report["verdict"] == "inconclusive"
    assert report["unresolved"][0]["property"] == raw


@pytest.mark.parametrize(
    "changes",
    [
        {"counterexample_count": None},
        {"counterexample_count": True},
        {"counterexample_count": 0},
        {"group": []},
        {"status": "Unrecognized"},
    ],
)
def test_malformed_failed_invariant_cannot_become_a_product_finding(changes) -> None:
    report = findings.summarize([passing(), {**prop("broken"), **changes}], events=[])
    assert report["verdict"] == "inconclusive"
    assert len(report["unresolved"]) == 1
    assert not report["product_failures"]


def test_duplicate_property_ids_are_visible() -> None:
    report = findings.summarize([passing(), passing()], events=[])
    assert report["verdict"] == "inconclusive"
    assert "duplicated" in report["unresolved"][0]["reason"]


@pytest.mark.parametrize("wrapped", [False, True])
def test_harness_error_event_invalidates_observations_without_product_failure(
    wrapped: bool,
) -> None:
    payload = {
        "workload_harness_error": {
            "valid": False,
            "component": "memory_budget",
            "message": "node is undersized",
            "details": {"free_bytes": 0},
        }
    }
    raw = (
        {
            "moment": {"input_hash": "sample", "vtime": 12},
            "output_text": json.dumps(payload),
        }
        if wrapped
        else payload
    )
    report = findings.summarize([passing()], events=[raw])
    assert report["verdict"] == "inconclusive"
    assert not report["product_failures"]
    assert report["health_events"][0]["event"] == raw


@pytest.mark.parametrize(
    "events",
    [
        None,
        [
            {
                "workload_harness_error": {
                    "valid": False,
                    "component": "model",
                    "message": "model state was reset",
                }
            }
        ],
    ],
)
def test_invalid_or_unassessed_harness_retains_oracle_failure_as_candidate(
    events,
) -> None:
    raw = prop("wrong_rows")
    report = findings.summarize([raw], events=events)
    assert report["verdict"] == "inconclusive"
    assert not report["product_failures"]
    assert report["product_candidates"][0]["property"] == raw


@pytest.mark.parametrize(
    "event",
    [
        {"workload_harness_future": {}},
        {"workload_harness_error": {}},
        {"output_text": '{"workload_harness_error":'},
        {"antithesis_exit_evidence": {"termination": {}}},
    ],
)
def test_unknown_or_malformed_health_evidence_is_unresolved(event) -> None:
    report = findings.summarize([passing()], events=[event])
    assert report["verdict"] == "inconclusive"
    assert report["unresolved"][0]["event"] == event


def test_persistent_harness_health_cannot_be_cleared_by_a_later_healthy_summary() -> (
    None
):
    unhealthy = {"harness_health": {"valid": False, "errors": 1, "diagnostics": []}}
    healthy = {"harness_health": {"valid": True, "errors": 0, "diagnostics": []}}
    report = findings.summarize([passing()], events=[unhealthy, healthy])
    assert report["verdict"] == "inconclusive"
    assert report["health_events"][0]["event"] == unhealthy
    assert not report["product_failures"]


def test_incomplete_resource_attribution_survives_a_later_healthy_summary() -> None:
    event = {
        "workload_harness_unresolved": {
            "component": "resource attribution",
            "message": "memory-limit kill attribution is incomplete",
            "details": {"finding": "container-termination"},
        }
    }
    healthy = {"harness_health": {"valid": True, "errors": 0, "diagnostics": []}}
    report = findings.summarize([passing()], events=[event, healthy])
    assert report["verdict"] == "inconclusive"
    assert (
        report["unresolved"][0]["reason"]
        == "memory-limit kill attribution is incomplete"
    )
    assert report["unresolved"][0]["event"] == event
    assert not report["product_failures"]


@pytest.mark.parametrize(
    "state", [{}, {"valid": True}, {"valid": True, "errors": 1, "diagnostics": []}]
)
def test_malformed_health_summary_remains_unresolved(state) -> None:
    event = {"harness_health": state}
    report = findings.summarize([passing()], events=[event])
    assert report["verdict"] == "inconclusive"
    assert report["unresolved"][0]["event"] == event


def test_coverage_events_and_cumulative_snapshots_are_not_summed() -> None:
    snapshots = [
        {
            "coverage_catalog_available": True,
            "coverage_targets": [
                {"message": "promotion", "observed": 0},
                {"message": "replica", "observed": 2},
            ],
            "coverage_catalog_unresolved": [
                {"file": "driver.py", "reason": "dynamic message"}
            ],
        },
        {"workload_coverage": {"message": "promotion", "observed": True}},
        {
            "coverage_catalog_available": True,
            "coverage_targets": [
                {"message": "promotion", "observed": 0},
                {"message": "never_reached", "observed": 0},
            ],
            "coverage_catalog_unresolved": [],
        },
    ]
    report = findings.summarize([passing()], events=snapshots)
    assert report["verdict"] == "no_product_findings"
    coverage = report["incomplete_coverage"]
    assert coverage["inventory_assessed"] is True
    assert coverage["unobserved_targets"] == ["never_reached"]
    assert coverage["unresolved_targets"] == [
        {"file": "driver.py", "reason": "dynamic message"}
    ]


def test_repeated_coverage_inventories_keep_each_unresolved_callsite_once() -> None:
    first = {
        "kind": "sometimes",
        "file": "driver.py",
        "line": 7,
        "reason": "message is not a string literal",
    }
    sites = [first, {**first, "line": 11}, {**first, "file": "other.py"}]
    snapshots = [
        {
            "coverage_catalog_available": True,
            "coverage_targets": [],
            "coverage_catalog_unresolved": [dict(site) for site in sites],
        }
        for _ in range(39)
    ]

    report = findings.summarize([passing()], events=snapshots)

    assert report["verdict"] == "no_product_findings"
    assert report["incomplete_coverage"]["unresolved_targets"] == sites


@pytest.mark.parametrize(
    "disposition,verdict",
    [
        ("designed", "no_product_findings"),
        ("product", "product_failure"),
        ("unresolved", "inconclusive"),
    ],
)
def test_native_halt_property_uses_correlated_evidence_verdict(
    monkeypatch: pytest.MonkeyPatch, disposition: str, verdict: str
) -> None:
    occurrence = {
        "termination": {
            "namespace": "test",
            "container": "environmentd",
            "exit_code": 166,
        }
    }
    seen = []

    def classify(raw, occurrences):
        seen.extend(occurrences)
        return ExitDisposition(disposition, "reviewed evidence", 1)  # type: ignore[arg-type]

    monkeypatch.setattr(findings, "classify_exit_property", classify)
    event = {"output_text": json.dumps({"antithesis_exit_evidence": occurrence})}
    report = findings.summarize([passing(), exit_prop()], events=[event])
    assert seen == [occurrence]
    assert report["verdict"] == verdict
    assert report["exit_evidence_events"] == [event]
    category = {
        "designed": "accepted_events",
        "product": "product_failures",
        "unresolved": "unresolved",
    }[disposition]
    assert report[category][0]["covered_occurrences"] == 1


def test_one_designed_halt_cannot_explain_nine_reported_exits() -> None:
    termination = {
        "namespace": "test",
        "pod_uid": "pod",
        "container": "environmentd",
        "termination_count": 1,
        "finished_at": "2026-01-01T00:00:00+00:00",
        "exit_code": 166,
    }
    occurrence = {
        "termination": termination,
        "halt": {
            "termination": termination,
            "complete": True,
            "message": "fenced out old deployment; rebooting as leader",
        },
    }
    report = findings.summarize(
        [passing(), exit_prop(count=9)],
        events=[{"antithesis_exit_evidence": occurrence}],
    )
    assert report["verdict"] == "inconclusive"
    assert not report["accepted_events"]
    assert report["unresolved"][0]["property"]["counterexample_count"] == 9


@pytest.mark.parametrize(
    "case,code", [("coverage", 0), ("product", 1), ("unresolved", 2)]
)
def test_cli_exit_code_and_json_preserve_verdict(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    case: str,
    code: int,
) -> None:
    path, events = tmp_path / "properties.ndjson", tmp_path / "events.ndjson"
    rows = [passing(), prop("coverage", "Sometimes: Sometimes")]
    if case == "product":
        rows.append(prop("wrong_rows"))
    elif case == "unresolved":
        rows.append(prop("future", "Future: New"))
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    events.write_text("")
    monkeypatch.setattr(
        sys, "argv", ["findings", "--properties", str(path), "--events", str(events)]
    )
    assert findings.main() == code
    report = json.loads(capsys.readouterr().out)
    assert report["counts"]["input_properties"] == len(rows)


def test_cli_rejects_overwriting_source_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "properties.ndjson"
    original = json.dumps(passing()) + "\n"
    path.write_text(original)
    monkeypatch.setattr(
        sys, "argv", ["findings", "--properties", str(path), "--output", str(path)]
    )
    with pytest.raises(SystemExit) as error:
        findings.main()
    assert error.value.code == 2
    assert path.read_text() == original
