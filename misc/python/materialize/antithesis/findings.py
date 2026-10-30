# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Separate observed product violations from coverage and harness findings."""

from __future__ import annotations

import argparse
import copy
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from materialize.antithesis.exit_evidence import classify_exit_property, native_exit

_SUT_CONTAINERS = {"environmentd", "clusterd", "orchestratord", "persistcli"}
_PANIC_EXITS = {101, 132, 134, 135, 136, 139}
_INVARIANT_GROUPS = {
    "Always: Always",
    "Always: AlwaysOrUnreachable",
    "Always: Unreachable",
}
_HEALTH_GROUPS = {
    "Always: Commands finish with zero exit code",
    "Always: SDK Minimum Version",
}
_COMMAND_COVERAGE = {
    "All commands were run to completion at least once",
    "All commands were started at least once",
}
_ADVISORY_HEALTH = {
    "Customer output volume",
    "Every instrumented module has symbols",
    "Logging output was limited",
    "No Antithesis build warnings",
    "Recent software version provided",
    "Test ran with full parallelism",
    "Unique commands found",
    "Virtual time per wall time",
}
_HEALTH_PROPERTIES = {
    "Containers joined the Antithesis network",
    "Customer output volume",
    "Every instrumented module has symbols",
    "Invalid commands found",
    "Invalid paths found during Antithesis instrumention",
    "Logging output was limited",
    "Never: Invalid commands found",
    "No Antithesis build errors",
    "No Antithesis build warnings",
    "No Antithesis errors",
    "No Antithesis session errors",
    "No invalid paths found during Antithesis instrumentation",
    "Paths found for Antithesis instrumentation",
    "Recent software version provided",
    "Software images match expected platform",
    "Software was instrumented",
    "Test ran with full parallelism",
    "The Antithesis Fault Injector was started",
    "Thread pausing was enabled",
    "Unique commands found",
    "Virtual time per wall time",
}
_HARNESS_ASSERTIONS = {
    "workload state was reset after setup",
    "workload telemetry records measurements without errors",
}


def _record(prop: Mapping[str, Any], reason: str, **details: Any) -> dict[str, Any]:
    return {"reason": reason, "property": copy.deepcopy(dict(prop)), **details}


def _classify(
    prop: Mapping[str, Any], occurrences: Sequence[Mapping[str, Any]]
) -> tuple[str, dict[str, Any]]:
    status, name, group = prop.get("status"), prop.get("name"), prop.get("group", "")
    if status not in ("Passing", "Failing") or not isinstance(name, str):
        return "unresolved", _record(prop, "property metadata is incomplete")
    if group is None:
        group = ""
    if not isinstance(group, str):
        return "unresolved", _record(prop, "property group is invalid")
    for field in ("example_count", "counterexample_count"):
        value = prop.get(field)
        if type(value) is not int or value < 0:
            return "unresolved", _record(prop, f"invalid {field}")
    failed = status == "Failing"
    if (
        group
        in (
            "Sometimes: Sometimes",
            "Sometimes: Reachable",
            "Always: Missed Always",
        )
        or name in _COMMAND_COVERAGE
    ):
        return "coverage", _record(prop, "coverage obligation, not a product violation")
    message = name.removeprefix(f"{group}: ")
    if (
        group in _HEALTH_GROUPS
        or name in _HEALTH_PROPERTIES
        or message in _HARNESS_ASSERTIONS
    ):
        return "harness_health", _record(
            prop,
            "test environment or workload health",
            severity="advisory" if name in _ADVISORY_HEALTH else "invalid",
        )
    if group in _INVARIANT_GROUPS:
        if failed and prop["counterexample_count"] == 0:
            return "unresolved", _record(prop, "failed invariant has no counterexample")
        if not failed and prop["counterexample_count"]:
            return "unresolved", _record(prop, "passing invariant has counterexamples")
        return "product_checks", _record(prop, "evaluated product invariant")
    if group == "No unexpected container exits":
        parsed = native_exit(prop)
        if parsed is None or parsed[0] not in _SUT_CONTAINERS:
            return "unresolved" if failed else "other", _record(
                prop, "exit attribution is unavailable"
            )
        if failed and prop["counterexample_count"] == 0:
            return "unresolved", _record(prop, "failed exit property has no occurrence")
        if parsed[2] == 166 and failed:
            disposition = classify_exit_property(prop, occurrences)
            category = {
                "designed": "accepted_events",
                "product": "product_checks",
                "unresolved": "unresolved",
            }[disposition.disposition]
            return category, _record(
                prop,
                disposition.reason,
                covered_occurrences=disposition.covered_occurrences,
                evidence=copy.deepcopy(list(occurrences)),
            )
        if parsed[2] in _PANIC_EXITS:
            return "product_checks", _record(
                prop, "product process panic or fatal signal"
            )
        return "unresolved" if failed else "other", _record(
            prop, "exit requires contextual evidence"
        )
    if group == "No unexpected crashes":
        container = name.removeprefix("No unexpected crashes: ")
        if container in _SUT_CONTAINERS and (
            not failed or prop["counterexample_count"] > 0
        ):
            return "product_checks", _record(prop, "product process crashed")
        return "unresolved" if failed else "other", _record(
            prop, "crash attribution is unavailable"
        )
    return "unresolved" if failed else "other", _record(
        prop, "unrecognized property type"
    )


def _payload(event: Mapping[str, Any]) -> Mapping[str, Any]:
    text = event.get("output_text")
    if isinstance(text, str) and text.lstrip().startswith("{"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            if "workload_harness_" in text:
                return {"workload_harness_malformed": text}
        else:
            if isinstance(parsed, dict):
                return parsed
    return event


def _panic_conflicts(
    prop: Mapping[str, Any], events: Sequence[Mapping[str, Any]]
) -> list[Mapping[str, Any]]:
    native = native_exit(prop)
    if native is not None and native[2] in (101, 134):
        container, namespace, _ = native
    elif prop.get("group") == "No unexpected crashes":
        container = str(prop.get("name", "")).removeprefix("No unexpected crashes: ")
        namespace = None
    else:
        return []
    conflicts = []
    for event in events:
        source = event.get("source")
        source = source if isinstance(source, dict) else {}
        reported = source.get("container")
        if isinstance(reported, str) and reported.rsplit("/", 1)[-1] != container:
            continue
        if namespace is not None and source.get("namespace") not in (None, namespace):
            continue
        conflicts.append(event)
    return conflicts


def summarize(
    properties: Iterable[Mapping[str, Any]],
    *,
    events: Iterable[Mapping[str, Any]] | None = None,
    exit_evidence: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Classify complete exports, retaining raw records and their observation scope."""
    result: dict[str, Any] = {
        key: []
        for key in (
            "product_checks",
            "coverage",
            "harness_health",
            "unresolved",
            "accepted_events",
            "other",
        )
    }
    result.update(
        version=1, health_events=[], coverage_events=[], exit_evidence_events=[]
    )
    props = list(properties)
    evidence = {} if exit_evidence is None else exit_evidence
    targets: dict[str, bool] = {}
    unresolved_targets = []
    inventory_assessed = False
    automatic_exits = []
    designed_panics = []
    if events is not None:
        for event in events:
            payload = _payload(event)
            if "harness_health" in payload:
                state = payload["harness_health"]
                if isinstance(state, dict) and state.get("valid") is False:
                    result["health_events"].append(
                        {
                            "reason": "persistent harness health is invalid",
                            "event": copy.deepcopy(dict(event)),
                        }
                    )
                elif not (
                    isinstance(state, dict)
                    and state.get("valid") is True
                    and type(state.get("errors")) is int
                    and state["errors"] == 0
                    and state.get("diagnostics") == []
                ):
                    result["unresolved"].append(
                        {
                            "reason": "malformed harness health summary",
                            "event": copy.deepcopy(dict(event)),
                        }
                    )
            health = [key for key in payload if key.startswith("workload_harness_")]
            for key in health:
                entry = payload[key]
                if (
                    key == "workload_harness_unresolved"
                    and isinstance(entry, dict)
                    and all(
                        isinstance(entry.get(field), str) and entry[field]
                        for field in ("component", "message")
                    )
                ):
                    result["unresolved"].append(
                        {
                            "reason": entry["message"],
                            "component": entry["component"],
                            "event": copy.deepcopy(dict(event)),
                        }
                    )
                elif (
                    key == "workload_harness_error"
                    and isinstance(entry, dict)
                    and entry.get("valid") is False
                    and all(
                        isinstance(entry.get(field), str) and entry[field]
                        for field in ("component", "message")
                    )
                ):
                    result["health_events"].append(
                        {
                            "reason": "workload reported invalid harness observations",
                            "event": copy.deepcopy(dict(event)),
                        }
                    )
                else:
                    result["unresolved"].append(
                        {
                            "reason": "unrecognized or malformed harness event",
                            "event": copy.deepcopy(dict(event)),
                        }
                    )
            observation = payload.get("workload_coverage")
            if (
                isinstance(observation, dict)
                and isinstance(observation.get("message"), str)
                and type(observation.get("observed")) is bool
            ):
                message = observation["message"]
                targets[message] = (
                    targets.get(message, False) or observation["observed"]
                )
                result["coverage_events"].append(copy.deepcopy(dict(event)))
                if (
                    message == "Materialize process panicked by design"
                    and observation["observed"]
                ):
                    designed_panics.append(event)
            elif "workload_coverage" in payload:
                result["unresolved"].append(
                    {
                        "reason": "malformed coverage observation",
                        "event": copy.deepcopy(dict(event)),
                    }
                )
            if "antithesis_exit_evidence" in payload:
                occurrence = payload["antithesis_exit_evidence"]
                termination = (
                    occurrence.get("termination")
                    if isinstance(occurrence, dict)
                    else None
                )
                if (
                    isinstance(termination, dict)
                    and all(
                        isinstance(termination.get(field), str) and termination[field]
                        for field in ("namespace", "container")
                    )
                    and type(termination.get("exit_code")) is int
                ):
                    automatic_exits.append(occurrence)
                    result["exit_evidence_events"].append(copy.deepcopy(dict(event)))
                else:
                    result["unresolved"].append(
                        {
                            "reason": "malformed exit evidence event",
                            "event": copy.deepcopy(dict(event)),
                        }
                    )
            if payload.get("coverage_catalog_available") is True:
                listed, dynamic = payload.get("coverage_targets"), payload.get(
                    "coverage_catalog_unresolved"
                )
                if not isinstance(listed, list) or not isinstance(dynamic, list):
                    result["unresolved"].append(
                        {
                            "reason": "malformed coverage inventory",
                            "event": copy.deepcopy(dict(event)),
                        }
                    )
                    continue
                inventory_assessed = True
                for target in listed:
                    if (
                        isinstance(target, dict)
                        and isinstance(target.get("message"), str)
                        and type(target.get("observed")) is int
                        and target["observed"] >= 0
                    ):
                        message = target["message"]
                        targets[message] = (
                            targets.get(message, False) or target["observed"] > 0
                        )
                    else:
                        result["unresolved"].append(
                            {
                                "reason": "malformed coverage target",
                                "event": copy.deepcopy(dict(event)),
                            }
                        )
                unresolved_targets.extend(dynamic)
    ids = set()
    matched_exits = set()
    for prop in props:
        identity = prop.get("id")
        if not isinstance(identity, str) or not identity or identity in ids:
            result["unresolved"].append(
                _record(prop, "property ID is missing or duplicated")
            )
            continue
        ids.add(identity)
        occurrences = list(evidence.get(identity, []))
        parsed = native_exit(prop)
        if parsed is not None:
            for index, occurrence in enumerate(automatic_exits):
                termination = occurrence["termination"]
                if (
                    termination.get("container"),
                    termination.get("namespace"),
                    termination.get("exit_code"),
                ) == parsed:
                    matched_exits.add(index)
                    occurrences.append(occurrence)
        category, record = _classify(prop, occurrences)
        result[category].append(record)
        if category == "product_checks" and prop.get("status") == "Failing":
            conflicts = _panic_conflicts(prop, designed_panics)
            if conflicts:
                record["designed_panic_conflicts"] = copy.deepcopy(conflicts)
                result["unresolved"].append(
                    _record(
                        prop,
                        "native crash may overlap an explicitly designed panic; incident attribution is unavailable",
                        attribution_only=True,
                        evidence=copy.deepcopy(conflicts),
                    )
                )
    for index, event in enumerate(result["exit_evidence_events"]):
        if index not in matched_exits:
            result["unresolved"].append(
                {"reason": "exit evidence has no matching property", "event": event}
            )
    product_candidates = [
        record
        for record in result["product_checks"]
        if record["property"]["status"] == "Failing"
    ]
    evaluated = sum(
        record["property"]["example_count"] + record["property"]["counterexample_count"]
        > 0
        for record in result["product_checks"]
    )
    reasons = []
    if not props:
        reasons.append("no property results were exported")
    if not evaluated:
        reasons.append("no evaluated product checks were exported")
    if any(not record.get("attribution_only") for record in result["unresolved"]):
        reasons.append("some findings lack a safe classification")
    if (
        any(
            record["property"]["status"] == "Failing"
            and record["severity"] == "invalid"
            for record in result["harness_health"]
        )
        or result["health_events"]
    ):
        reasons.append("the harness or test environment reported a failure")
    unused = sorted(set(evidence) - ids)
    if unused:
        reasons.append("exit evidence refers to properties absent from the export")
    property_disposition = (
        "product_candidates"
        if product_candidates
        else "inconclusive" if reasons else "no_product_findings"
    )
    if events is None:
        reasons.append("no harness health event export was provided")
    # Aggregate exports do not prove which model failures share a timeline with
    # a counterexample, so invalid or missing harness evidence blocks attribution.
    product_failures = (
        []
        if reasons
        else [
            record
            for record in product_candidates
            if not record.get("designed_panic_conflicts")
        ]
    )
    if any(record.get("attribution_only") for record in result["unresolved"]):
        reasons.append(
            "native panic evidence includes unresolved designed-panic context"
        )
    result.update(
        verdict=(
            "product_failure"
            if product_failures
            else "inconclusive" if reasons else "no_product_findings"
        ),
        property_disposition=property_disposition,
        product_candidates=product_candidates,
        product_failures=product_failures,
        inconclusive_reasons=reasons,
        incomplete_coverage={
            "missing_properties": [
                record["property"]["id"]
                for record in result["coverage"]
                if record["property"]["status"] == "Failing"
            ],
            "unobserved_targets": sorted(
                message for message, observed in targets.items() if not observed
            ),
            "inventory_assessed": inventory_assessed,
            "unresolved_targets": unresolved_targets,
        },
        evidence_scope={
            "properties_provided": True,
            "health_events_provided": events is not None,
            "unused_exit_evidence_ids": unused,
        },
        counts={
            "input_properties": len(props),
            "raw_failing_properties": sum(
                prop.get("status") == "Failing" for prop in props
            ),
            "product_candidate_properties": len(product_candidates),
            "product_failure_properties": len(product_failures),
            "evaluated_product_properties": evaluated,
            **{
                key: len(result[key])
                for key in (
                    "coverage",
                    "harness_health",
                    "unresolved",
                    "accepted_events",
                )
            },
        },
    )
    return result


def _ndjson(path: Path) -> Iterator[dict[str, Any]]:
    with path.open() as source:
        for number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"invalid JSON on line {number}") from error
            if not isinstance(row, dict):
                raise ValueError(f"expected an object on line {number}")
            yield row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--properties", type=Path, required=True)
    parser.add_argument("--events", type=Path)
    parser.add_argument("--exit-evidence", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        if args.output is not None and args.output.resolve() in {
            path.resolve()
            for path in (args.properties, args.events, args.exit_evidence)
            if path is not None
        }:
            raise ValueError("output must differ from the evidence files")
        evidence = (
            json.loads(args.exit_evidence.read_text()) if args.exit_evidence else None
        )
        if evidence is not None and (
            not isinstance(evidence, dict)
            or any(
                not isinstance(value, list)
                or any(not isinstance(record, dict) for record in value)
                for value in evidence.values()
            )
        ):
            raise ValueError("exit evidence must map property IDs to occurrence lists")
        result = summarize(
            _ndjson(args.properties),
            events=_ndjson(args.events) if args.events is not None else None,
            exit_evidence=evidence,
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))
    text = json.dumps(result, indent=2) + "\n"
    if args.output is None:
        print(text, end="")
    else:
        args.output.write_text(text)
    return {"no_product_findings": 0, "product_failure": 1, "inconclusive": 2}[
        result["verdict"]
    ]


if __name__ == "__main__":
    raise SystemExit(main())
