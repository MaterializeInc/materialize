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
from pathlib import Path
from typing import Any

import pytest

from materialize.antithesis import exit_evidence as evidence

DESIGNED = "fenced out old deployment; rebooting as leader"
RULES = (evidence.HaltRule((DESIGNED,), "a promoted generation reboots as leader"),)


def prop(count: Any = 1, code: int = 166) -> dict[str, Any]:
    return {
        "name": "No unexpected container exits: container: environmentd, namespace: mz , "
        f"exit code: {code}",
        "counterexample_count": count,
    }


def occurrence(count: int = 1, message: str = DESIGNED) -> dict[str, Any]:
    termination = {
        "namespace": "mz",
        "pod_uid": "pod-uid",
        "container": "environmentd",
        "termination_count": count,
        "finished_at": f"2026-10-30T12:00:{count:02d}+00:00",
        "exit_code": 166,
    }
    return {
        "termination": termination,
        "halt": {
            "termination": dict(termination),
            "message": message,
            "complete": True,
        },
    }


def classify(properties: dict[str, Any], occurrences: Any) -> evidence.ExitDisposition:
    return evidence.classify_exit_property(properties, occurrences, policy=RULES)


@pytest.mark.parametrize("prefix", ["", "halting process: "])
def test_complete_designed_halt_is_informational(prefix: str) -> None:
    result = classify(prop(), [occurrence(message=prefix + DESIGNED)])
    assert result.disposition == "designed"
    assert result.covered_occurrences == 1


def test_one_representative_cannot_explain_nine_exits() -> None:
    result = classify(prop(9), [occurrence()])
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 1
    assert "1 of 9" in result.reason


def test_all_occurrences_must_be_explained_independently() -> None:
    result = classify(prop(9), [occurrence(i) for i in range(1, 10)])
    assert result.disposition == "designed"
    assert result.covered_occurrences == 9


def test_repeated_delivery_of_the_same_evidence_is_idempotent() -> None:
    result = classify(prop(), [occurrence(), occurrence()])
    assert result.disposition == "designed"
    assert result.covered_occurrences == 1


@pytest.mark.parametrize("change_time", [False, True])
def test_duplicate_evidence_does_not_increase_occurrence_coverage(
    change_time: bool,
) -> None:
    original = occurrence()
    duplicate = copy.deepcopy(original)
    if change_time:
        for section in (duplicate["termination"], duplicate["halt"]["termination"]):
            section["finished_at"] = "2026-10-30T12:30:00+00:00"
    result = classify(prop(2), [original, duplicate])
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("namespace", "other"),
        ("pod_uid", "other-uid"),
        ("container", "clusterd"),
        ("termination_count", 2),
        ("finished_at", "2026-10-30T12:30:00+00:00"),
        ("exit_code", 134),
    ],
)
def test_halt_from_another_termination_cannot_explain_exit(
    field: str, value: Any
) -> None:
    record = occurrence()
    record["halt"]["termination"][field] = value
    result = classify(prop(), [record])
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 0


@pytest.mark.parametrize(
    "field,value", [("namespace", "other"), ("container", "clusterd")]
)
def test_evidence_for_another_property_does_not_explain_exit(
    field: str, value: str
) -> None:
    record = occurrence()
    record["termination"][field] = value
    record["halt"]["termination"][field] = value
    assert classify(prop(), [record]).disposition == "unresolved"


@pytest.mark.parametrize(
    "field,value",
    [
        ("pod_uid", ""),
        ("termination_count", True),
        ("termination_count", 0),
        ("finished_at", "not-a-timestamp"),
        ("finished_at", "2026-10-30T12:30:00"),
        ("exit_code", 42),
    ],
)
def test_partial_or_malformed_identity_is_unresolved(field: str, value: Any) -> None:
    record = occurrence()
    record["termination"][field] = value
    record["halt"]["termination"][field] = value
    assert classify(prop(), [record]).disposition == "unresolved"


@pytest.mark.parametrize("complete", [False, None])
def test_truncated_halt_evidence_is_unresolved(complete: Any) -> None:
    record = occurrence()
    record["halt"]["complete"] = complete
    assert classify(prop(), [record]).disposition == "unresolved"


@pytest.mark.parametrize("records", [[], [None], ["halt"], None, "halt", {}])
def test_missing_or_malformed_occurrences_are_unresolved(records: Any) -> None:
    assert classify(prop(), records).disposition == "unresolved"


@pytest.mark.parametrize("count", [0, None, "1", True])
def test_missing_or_malformed_occurrence_count_is_unresolved(count: Any) -> None:
    assert classify(prop(count), [occurrence()]).disposition == "unresolved"


def test_extra_occurrences_cannot_establish_complete_coverage() -> None:
    assert classify(prop(), [occurrence(1), occurrence(2)]).disposition == "unresolved"


@pytest.mark.parametrize(
    "message",
    [
        "a new halt reason nobody reviewed",
        "cannot serve requested as_of Antichain { elements: [42] }",
        "blob is missing but its reader is still present in state",
        "dependency since frontier is empty while dependent upper is writable",
    ],
)
def test_unknown_and_known_bug_halts_remain_product_findings(message: str) -> None:
    result = classify(prop(9), [occurrence(message=message)])
    assert result.disposition == "product"
    assert result.covered_occurrences == 1


def test_native_panic_is_never_accepted_as_a_designed_halt() -> None:
    assert classify(prop(code=134), []).disposition == "product"


def test_native_exit_attribution_does_not_parse_other_properties() -> None:
    assert evidence.native_exit(prop()) == ("environmentd", "mz", 166)
    assert evidence.native_exit(prop(code=134)) == ("environmentd", "mz", 134)
    for name in ("No unexpected crashes: clusterd", "exit code: 166", None):
        assert evidence.native_exit({"name": name}) is None


def test_missing_policy_cannot_explain_halts(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing():
        raise FileNotFoundError("policy source is absent")

    monkeypatch.setattr(evidence, "load_halt_rules", missing)
    assert (
        evidence.classify_exit_property(prop(), [occurrence()]).disposition
        == "unresolved"
    )


def test_runtime_policy_uses_the_reviewed_sut_rules() -> None:
    rules = evidence.load_halt_rules()
    assert rules
    assert any(DESIGNED in rule.matches for rule in rules)
    for rule in rules:
        result = evidence.classify_exit_property(
            prop(), [occurrence(message=" ".join(rule.matches))]
        )
        assert result.disposition == "designed", rule


def test_policy_reader_rejects_unrecognized_rust_syntax(tmp_path: Path) -> None:
    source = tmp_path / "policy.rs"
    source.write_text(
        "pub const DESIGNED_HALTS: &[DesignedHalt] = &[\n"
        '    DesignedHalt { matches: &[r"reason"], reason: "explanation" },\n'
        "];\n"
    )
    with pytest.raises(ValueError, match="syntax"):
        evidence.load_halt_rules(source)


def test_policy_reader_decodes_rust_escaped_newlines(tmp_path: Path) -> None:
    source = tmp_path / "policy.rs"
    source.write_text(
        "pub const DESIGNED_HALTS: &[DesignedHalt] = &[\n"
        '    DesignedHalt { matches: &["one", "two"], reason: "first \\\n'
        '        second" },\n'
        "];\n"
    )
    assert evidence.load_halt_rules(source) == (
        evidence.HaltRule(("one", "two"), "first second"),
    )
