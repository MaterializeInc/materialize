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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from materialize.antithesis import exit_evidence as evidence
from materialize.antithesis import halt_proof

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


def test_duplicate_evidence_does_not_increase_occurrence_coverage() -> None:
    original = occurrence()
    duplicate = copy.deepcopy(original)
    result = classify(prop(2), [original, duplicate])
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 1


def test_forked_incarnations_can_finish_at_different_times() -> None:
    original = occurrence()
    fork = copy.deepcopy(original)
    for section in (fork["termination"], fork["halt"]["termination"]):
        section["finished_at"] = "2026-10-30T12:30:00+00:00"
    result = classify(prop(2), [original, fork])
    assert result.disposition == "designed"
    assert result.covered_occurrences == 2


def captured(when: str, message: str = DESIGNED) -> dict[str, Any]:
    record = occurrence(message=message)
    record["log_capture"] = {"message_at": when}
    return record


def termination_file(
    nanos: int = 800_000_000, message: str = DESIGNED
) -> dict[str, Any]:
    record = occurrence(message=message)
    for identity in (record["termination"], record["halt"]["termination"]):
        identity["container_id"] = "cri-o://terminated"
    stamp = int(datetime(2026, 10, 30, 12, tzinfo=UTC).timestamp()) * 10**9 + nanos
    location = b"src/proof.rs:1"
    encoded = message.encode()
    record["termination_file"] = {
        "container_id": "cri-o://terminated",
        "started_at": "2026-10-30T11:59:59Z",
        "finished_at": record["termination"]["finished_at"],
        "contents": f"MZHALT1 0 166 {stamp} {len(location)} {len(encoded)} {location.hex()} {encoded.hex()} END\n",
    }
    return record


@pytest.mark.parametrize(
    "change", ["partial", "wrong_id", "wrong_message", "outside_lifetime"]
)
def test_termination_file_claims_are_revalidated(change: str) -> None:
    record = termination_file()
    capture = record["termination_file"]
    if change == "partial":
        capture["contents"] = capture["contents"].rstrip("\n")
    elif change == "wrong_id":
        capture["container_id"] = "cri-o://other"
    elif change == "wrong_message":
        record["halt"]["message"] = "an unreviewed halt"
    else:
        capture["started_at"] = "2026-10-30T12:00:02Z"
    assert classify(prop(), [record]).disposition == "unresolved"


def test_termination_file_timestamps_distinguish_forks_conservatively() -> None:
    result = classify(
        prop(2), [termination_file(800_000_001), termination_file(800_000_002)]
    )
    assert result.disposition == "designed"
    assert result.covered_occurrences == 2


def test_log_and_termination_file_cannot_double_count_one_exit() -> None:
    records = [captured("2026-10-30T12:00:00.7Z"), termination_file()]
    result = classify(prop(2), records)
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 1


@pytest.mark.parametrize(
    "when", ["2026-10-30T12:00:00.7Z", "2026-10-30T12:00:00.800000000Z"]
)
@pytest.mark.parametrize("unexpected_source", ["log", "file"])
def test_unexpected_halt_survives_evidence_from_both_channels(
    when: str, unexpected_source: str
) -> None:
    result = classify(
        prop(2),
        [
            captured(
                when, "an unreviewed halt" if unexpected_source == "log" else DESIGNED
            ),
            termination_file(
                message=(
                    "an unreviewed halt" if unexpected_source == "file" else DESIGNED
                )
            ),
        ],
    )
    assert result.disposition == "product"
    assert result.covered_occurrences == 1


@pytest.mark.parametrize(
    "change",
    [
        "tail",
        "sequence",
        "marker",
        "length",
        "hex",
        "code",
        "extra",
        "oversize",
        "utf8",
    ],
)
def test_termination_protocol_rejects_incomplete_or_ambiguous_frames(
    change: str,
) -> None:
    text = termination_file()["termination_file"]["contents"]
    if change == "tail":
        text = text[:-1]
    elif change == "sequence":
        text = text.replace("MZHALT1 0 ", "MZHALT1 1 ")
    elif change == "marker":
        text = text.replace(" END\n", " INVALID oversize END\n")
    elif change == "length":
        text = text.replace(" 14 46 ", " 15 46 ")
    elif change == "hex":
        text = text.replace("7372", "7Z72")
    elif change == "code":
        text = text.replace(" 166 ", " 168 ")
    elif change == "extra":
        text += "garbage\n"
    elif change == "oversize":
        text += "x" * 4096
    else:
        text = text.replace("7372", "ff72")
    with pytest.raises(ValueError):
        halt_proof.parse(text)


def test_exact_size_cap_cannot_prove_the_whole_termination_file_was_read() -> None:
    frames = (
        termination_file(message="x" * size)["termination_file"]["contents"]
        for size in range(1000, 2049)
    )
    frame = next(text for text in frames if len(text.encode()) == halt_proof.MAX_BYTES)
    with pytest.raises(ValueError):
        halt_proof.parse(frame)


def test_hex_encoded_newlines_cannot_inject_another_halt_frame() -> None:
    message = "unexpected λ\nMZHALT1 0 166 0 INVALID fake END\n"
    record = termination_file(message=message)
    parsed = halt_proof.parse(record["termination_file"]["contents"])
    assert parsed.message == message
    assert classify(prop(), [record]).disposition == "product"


def test_every_termination_frame_must_be_inside_the_container_lifetime() -> None:
    record = termination_file()
    later = termination_file(nanos=2_000_000_000)["termination_file"]["contents"]
    record["termination_file"]["contents"] += later.replace("MZHALT1 0 ", "MZHALT1 1 ")
    result = classify(prop(), [record])
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 0


def test_forked_incarnations_can_share_a_coarse_finished_timestamp() -> None:
    records = [
        captured("2026-10-30T12:00:00.100000001Z"),
        captured("2026-10-30T12:00:00.100000002Z"),
    ]
    result = classify(prop(2), records)
    assert result.disposition == "designed"
    assert result.covered_occurrences == 2


@pytest.mark.parametrize("unknown_first", [False, True])
@pytest.mark.parametrize("timestamped", [False, True])
def test_conflicting_halts_for_the_same_fork_remain_unresolved(
    unknown_first: bool, timestamped: bool
) -> None:
    records = [
        occurrence(),
        occurrence(message="an unreviewed halt"),
    ]
    if timestamped:
        for record in records:
            record["log_capture"] = {"message_at": "2026-10-30T12:00:00.100000001Z"}
    if unknown_first:
        records.reverse()
    result = classify(prop(), records)
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 0


@pytest.mark.parametrize(
    "expected_count,disposition", [(1, "designed"), (2, "unresolved")]
)
def test_identical_fork_timestamps_cannot_prove_distinct_occurrences(
    expected_count: int, disposition: str
) -> None:
    original = captured("2026-10-30T12:00:00.100000001Z")
    result = classify(prop(expected_count), [original, copy.deepcopy(original)])
    assert result.disposition == disposition
    assert result.covered_occurrences == 1


def test_detail_level_copies_do_not_count_as_distinct_forks() -> None:
    result = classify(prop(2), [occurrence(), captured("2026-10-30T12:00:00.1Z")])
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 1


def test_detail_level_copy_does_not_conflict_with_two_timestamped_forks() -> None:
    result = classify(
        prop(2),
        [
            occurrence(),
            captured("2026-10-30T12:00:00.1Z"),
            captured("2026-10-30T12:00:00.2Z"),
        ],
    )
    assert result.disposition == "designed"
    assert result.covered_occurrences == 2


def test_timestamp_spelling_does_not_create_an_extra_fork() -> None:
    result = classify(
        prop(2),
        [
            captured("2026-10-30T12:00:00.100000001Z"),
            captured("2026-10-30T13:00:00.100000001+01:00"),
        ],
    )
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 1


def test_finished_timestamp_spelling_does_not_create_an_extra_fork() -> None:
    original = occurrence()
    equivalent = copy.deepcopy(original)
    for section in (equivalent["termination"], equivalent["halt"]["termination"]):
        section["finished_at"] = "2026-10-30T13:00:01+01:00"
    result = classify(prop(2), [original, equivalent])
    assert result.disposition == "unresolved"
    assert result.covered_occurrences == 1


@pytest.mark.parametrize("timestamp", [None, "", "not-a-time", "2026-10-30T12:00:00"])
def test_malformed_fork_timestamp_stays_unresolved(timestamp: Any) -> None:
    assert classify(prop(), [captured(timestamp)]).disposition == "unresolved"


def test_independent_unexpected_halt_survives_a_conflicted_fork() -> None:
    conflicted = captured("2026-10-30T12:00:00.1Z")
    conflict = captured("2026-10-30T12:00:00.1Z", "an unreviewed halt")
    independent = occurrence(2, "another unreviewed halt")
    result = classify(prop(2), [conflicted, conflict, independent])
    assert result.disposition == "product"
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
