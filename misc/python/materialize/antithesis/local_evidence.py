# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Assess local workload SDK events and telemetry snapshots."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


def telemetry_snapshot(output: Path, label: str) -> dict[str, Any]:
    destination = output / f"telemetry-{label}.json"
    env = {
        **os.environ,
        "ANTITHESIS_SDK_LOCAL_OUTPUT": str(output / f"telemetry-{label}.sdk.jsonl"),
    }
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json; from materialize.antithesis.telemetry import summary; print(json.dumps(summary()))",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode:
        raise RuntimeError(f"telemetry snapshot failed: {result.stderr[-2000:]}")
    records = []
    for line in result.stdout.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and "telemetry_available" in value:
            records.append(value)
    if len(records) != 1:
        raise RuntimeError("telemetry snapshot did not return one result")
    destination.write_text(json.dumps(records[0], indent=2) + "\n")
    return records[0]


def snapshot_valid(value: dict) -> bool:
    health = value.get("harness_health")
    return (
        value.get("telemetry_available") is True
        and isinstance(health, dict)
        and health.get("valid") is True
        and health.get("errors", 0) == 0
    )


def counter_delta(
    before: dict, after: dict, table: str, keys: tuple[str, ...]
) -> list[dict]:
    old = {tuple(row[key] for key in keys): row for row in before.get(table, [])}
    result = []
    for row in after.get(table, []):
        identity = tuple(row[key] for key in keys)
        previous = old.get(identity, {})
        delta = {key: row[key] for key in keys}
        for key, value in row.items():
            if key in keys:
                continue
            if isinstance(value, (float, int)):
                delta[key] = value - previous.get(key, 0)
                if delta[key] < -1e-8:
                    raise ValueError(
                        f"telemetry counter regressed: {table}/{identity}/{key}"
                    )
        if any(value > 0 for key, value in delta.items() if key not in keys):
            result.append(delta)
    return result


def evidence(output: Path) -> dict[str, Any]:
    assertions = {}
    false_hits = []
    health = []
    witnesses = {}
    protocols = defaultdict(list)
    sdk_files = 0
    for path in sorted(output.glob("*.sdk.jsonl")):
        sdk_files += 1
        if path.stat().st_size == 0:
            health.append({"file": path.name, "error": "empty SDK capture"})
        for number, line in enumerate(path.open(), 1):
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("not an object")
            except ValueError:
                health.append(
                    {"file": path.name, "line": number, "error": "malformed SDK JSON"}
                )
                continue
            context = {"file": path.name, "line": number}
            assertion = record.get("antithesis_assert")
            if assertion is not None:
                if (
                    not isinstance(assertion, dict)
                    or type(assertion.get("hit")) is not bool
                    or type(assertion.get("condition")) is not bool
                ):
                    health.append({**context, "error": "malformed SDK assertion"})
                else:
                    key = assertion.get("id", assertion.get("message", "unknown"))
                    state = assertions.setdefault(
                        key,
                        {
                            "message": assertion.get("message"),
                            "assert_type": assertion.get("assert_type"),
                            "hit": False,
                            "failed": False,
                        },
                    )
                    state["hit"] |= assertion["hit"]
                    failed = (
                        assertion["hit"]
                        and not assertion["condition"]
                        and assertion.get("assert_type") in ("always", "reachability")
                    )
                    state["failed"] |= failed
                    if failed:
                        false_hits.append({**context, "assertion": assertion})
            for key in ("antithesis_error", "workload_harness_error"):
                if key in record:
                    health.append({**context, "error": key, "details": record[key]})
            if "harness_health" in record:
                value = record["harness_health"]
                if (
                    not isinstance(value, dict)
                    or value.get("valid") is not True
                    or value.get("errors", 0)
                ):
                    health.append(
                        {**context, "error": "invalid harness health", "details": value}
                    )
            observation = record.get("workload_coverage")
            if observation is not None:
                if (
                    not isinstance(observation, dict)
                    or type(observation.get("observed")) is not bool
                    or not isinstance(observation.get("message"), str)
                ):
                    health.append(
                        {**context, "error": "malformed coverage observation"}
                    )
                elif observation["observed"]:
                    witnesses.setdefault(
                        observation["message"], {**context, "observation": observation}
                    )
            protocol = record.get("antithesis_interleaving")
            if protocol is not None:
                if isinstance(protocol, dict) and isinstance(
                    protocol.get("round"), str
                ):
                    protocols[(path.name, protocol["round"])].append(protocol)
                else:
                    health.append(
                        {**context, "error": "malformed interleaving observation"}
                    )
    diagnostic_faults = []
    coherent_faults = []
    for (source, token), records in protocols.items():
        checked = [
            row
            for row in records
            if row.get("stage") == "checked"
            and type(row.get("successful_reads")) is int
            and row["successful_reads"] > 0
        ]
        faults = [
            row
            for row in records
            if row.get("stage") == "disruption" and row.get("outcome") == "acked"
        ]
        if checked and faults:
            diagnostic = {
                "file": source,
                "round": token,
                "faults": faults,
                "checks": checked,
            }
            diagnostic_faults.append(diagnostic)
            stages = {
                stage: [i for i, row in enumerate(records) if row.get("stage") == stage]
                for stage in ("start", "write", "checked", "end")
            }
            if (
                not all(len(indices) == 1 for indices in stages.values())
                or len(faults) != 1
            ):
                continue
            first, write, check, last = (
                records[stages[stage][0]]
                for stage in ("start", "write", "checked", "end")
            )
            fault = faults[0]
            fault_index = next(i for i, row in enumerate(records) if row is fault)
            if not (
                stages["start"][0] == 0
                and stages["end"][0] == len(records) - 1
                and 0 < stages["write"][0] < stages["checked"][0] < stages["end"][0]
                and 0 < fault_index < stages["checked"][0]
                and first.get("mode") in ("held_append", "race_append", "race_rmw")
                and type(first.get("epoch")) is int
                and first["epoch"] >= 0
                and all(field in first for field in ("key", "read_key"))
                and all(
                    isinstance(first.get(field), str) and first[field]
                    for field in ("profile", "command")
                )
                and all(
                    row.get(field) == first[field]
                    for row in records
                    for field in (
                        "profile",
                        "command",
                        "mode",
                        "epoch",
                        "key",
                        "read_key",
                    )
                )
                and type(write.get("write_id")) is int
                and write["write_id"] > 0
                and write.get("outcome") in ("ok", "indeterminate")
                and type(check.get("write_id")) is int
                and check.get("write_id") == write["write_id"]
                and type(check.get("read_id")) is int
                and check["read_id"] > write["write_id"]
                and check.get("observed_write") is True
                and fault.get("action") == "delete"
                and fault.get("role")
                in ("clusterd", "environmentd_active", "environmentd_candidate")
                and isinstance(fault.get("pod_uid"), str)
                and bool(fault["pod_uid"])
                and last.get("outcome") == "completed"
                and (first["mode"] != "held_append" or fault_index < stages["write"][0])
            ):
                continue
            coherent_faults.append(
                {**diagnostic, "start": first, "write": write, "end": last}
            )

    # Protocol events omit the write/DELETE intervals used by coverage_model.
    # Keep their coherent rounds diagnostic and retain the oracle's own proof.
    authoritative_faults = {}
    for message, witness in witnesses.items():
        observation = witness["observation"]
        location = observation.get("location")
        details = observation.get("details")
        if not (
            isinstance(location, dict)
            and isinstance(location.get("file"), str)
            and location["file"].endswith(
                "/materialize/antithesis/telemetry_coverage.py"
            )
            and isinstance(details, dict)
            and isinstance(details.get("combinations"), list)
        ):
            continue
        if any(
            isinstance(case, list)
            and len(case) == 5
            and isinstance(case[1], str)
            and case[1]
            for case in details["combinations"]
        ):
            authoritative_faults[message] = witness
    return {
        "sdk_files": sdk_files,
        "false_hits": false_hits,
        "harness_errors": health,
        "evaluated_invariants": [
            row
            for row in assertions.values()
            if row["hit"] and row["assert_type"] in ("always", "reachability")
        ],
        "declared_only_invariants": [
            row
            for row in assertions.values()
            if not row["hit"] and row["assert_type"] in ("always", "reachability")
        ],
        "positive_witnesses": witnesses,
        "diagnostic_checked_fault_rounds": diagnostic_faults,
        "coherent_fault_rounds": coherent_faults,
        "authoritative_fault_witnesses": authoritative_faults,
        "fault_round_attribution_available": False,
        "native_sut_output_captured": False,
    }
