# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Coverage observations and harness health, separate from product assertions."""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import sys
import threading
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

from antithesis.lifecycle import (  # pyright: ignore[reportMissingModuleSource]
    send_event,
)

CATALOG_ENV = "MZ_ANTITHESIS_COVERAGE_CATALOG"
STATE_CAPACITY = 2048
DETAIL_BYTES = 4096
INVALID_MARKER = "harness-invalid.json"
_seen: dict[str, int] = {}
_numeric: dict[str, tuple[bool, float]] = {}
_lock = threading.Lock()


def _location(depth: int) -> dict[str, Any]:
    frame = sys._getframe(depth)
    return {
        "file": frame.f_code.co_filename,
        "function": frame.f_code.co_name,
        "class": "",
        "begin_line": frame.f_lineno,
        "begin_column": 0,
    }


def _guidance(
    kind: str,
    message: str,
    location: Mapping[str, Any],
    maximize: bool,
    values: Mapping[str, Any],
    first: bool,
) -> None:
    prefix = "workload coverage" if kind == "boolean" else "workload numeric"
    guidance = {
        "guidance_type": kind,
        "id": f"{prefix}: {message}",
        "message": f"{prefix}: {message}",
        "location": location,
        "maximize": maximize,
        "hit": False,
    }
    # Guidance has its own catalog and does not require a failing property.
    if first:
        send_event("antithesis_guidance", guidance)
    send_event(
        "antithesis_guidance",
        {**guidance, "hit": True, "guidance_data": values},
    )


def _observe(condition: bool, message: str, details: Mapping[str, Any] | None) -> None:
    from materialize.antithesis import telemetry

    condition = bool(condition)
    telemetry.observation(message, condition)
    bit = 2 if condition else 1
    with _lock:
        previous = _seen.get(message, 0)
        if previous & bit:
            return
        if not previous and len(_seen) >= STATE_CAPACITY:
            del _seen[next(iter(_seen))]
        _seen[message] = previous | bit

    location = _location(3)
    _guidance("boolean", message, location, True, {"observed": condition}, not previous)
    safe_details, error = _bounded_details(details)
    if error is not None:
        harness_error(
            "coverage",
            "observation details are not serializable",
            {"target": message, "error": error},
        )
    send_event(
        "workload_coverage",
        {
            "message": message,
            "observed": condition,
            "location": location,
            "profile": os.environ.get("MZ_ANTITHESIS_TEMPLATE_PROFILE", "main"),
            "details": safe_details,
        },
    )


def sometimes(
    condition: bool, message: str, details: Mapping[str, Any] | None = None
) -> None:
    """Count a coverage condition and guide exploration toward observing it."""
    _observe(condition, message, details)


def reachable(message: str, details: Mapping[str, Any] | None = None) -> None:
    """Record that a coverage milestone was reached."""
    _observe(True, message, details)


def numeric_guidance(
    left: int | float,
    right: int | float,
    message: str,
    *,
    maximize: bool,
) -> None:
    """Guide exploration toward extreme operands without asserting reachability."""
    try:
        gap = float(left) - float(right)
        finite = math.isfinite(left) and math.isfinite(right)
    except (ValueError, OverflowError):
        finite = False
        gap = 0.0
    if not finite:
        harness_error(
            "guidance", "numeric operands are not finite", {"target": message}
        )
        return
    changed_direction = False
    with _lock:
        previous = _numeric.get(message)
        if previous is not None:
            direction, mark = previous
            changed_direction = direction != maximize
            if not changed_direction and (gap <= mark if maximize else gap >= mark):
                return
        if not changed_direction:
            if previous is None and len(_numeric) >= STATE_CAPACITY:
                del _numeric[next(iter(_numeric))]
            _numeric[message] = (maximize, gap)
    if changed_direction:
        harness_error(
            "guidance", "numeric target changed direction", {"target": message}
        )
        return
    _guidance(
        "numeric",
        message,
        _location(2),
        maximize,
        {"left": left, "right": right},
        previous is None,
    )


def _bounded_details(
    details: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], str | None]:
    try:
        encoded = json.dumps({} if details is None else details, allow_nan=False)
        encoded_size = len(encoded.encode())
        if encoded_size > DETAIL_BYTES:
            return {"details_omitted": True, "encoded_bytes": encoded_size}, None
        decoded = json.loads(encoded)
        if not isinstance(decoded, dict):
            return {"details_serialization_error": "TypeError"}, "TypeError"
        return decoded, None
    except (ValueError, TypeError) as error:
        name = type(error).__name__
        return {"details_serialization_error": name}, name


def harness_error(
    component: str, message: str, details: Mapping[str, Any] | None = None
) -> None:
    """Record an invalid harness observation without accusing the product."""
    from materialize.antithesis import telemetry

    safe_details, _ = _bounded_details(details)
    record = {
        "valid": False,
        "component": component,
        "message": message,
        "details": safe_details,
    }
    telemetry.record_harness_error(record)
    state_dir = os.environ.get("MZ_ANTITHESIS_STATE_DIR")
    if state_dir is not None:
        try:
            directory = Path(state_dir)
            directory.mkdir(parents=True, exist_ok=True)
            # The sticky marker survives an error in the telemetry database.
            with (directory / INVALID_MARKER).open("x") as output:
                output.write(json.dumps(record))
        except FileExistsError:
            pass
        except OSError as error:
            record["durable_marker_error"] = type(error).__name__
    send_event("workload_harness_error", record)


def durable_health() -> dict[str, Any] | None:
    """Read the first harness error, independently of the telemetry database."""
    directory = os.environ.get("MZ_ANTITHESIS_STATE_DIR")
    if directory is None:
        return None
    try:
        record = json.loads((Path(directory) / INVALID_MARKER).read_text())
    except FileNotFoundError:
        return None
    except (ValueError, UnicodeError):
        return {"valid": False, "message": "harness invalidity marker is incomplete"}
    if not isinstance(record, dict):
        return {"valid": False, "message": "harness invalidity marker is malformed"}
    return {**record, "valid": False}


def inventory(source: Path) -> dict[str, Any]:
    """List literal observation targets and unresolved messages in runtime sources."""
    targets: dict[str, dict[str, Any]] = {}
    unresolved = []
    for path in sorted(source.rglob("*.py")):
        if path.name.startswith("test_"):
            continue
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if not isinstance(node, ast.Call):
                continue
            name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr if isinstance(node.func, ast.Attribute) else ""
            )
            if name not in ("sometimes", "reachable"):
                continue
            position = 1 if name == "sometimes" else 0
            message = node.args[position] if len(node.args) > position else None
            if message is None:
                message = next(
                    (value.value for value in node.keywords if value.arg == "message"),
                    None,
                )
            location = {
                "kind": name,
                "file": str(path.relative_to(source)),
                "line": node.lineno,
            }
            if isinstance(message, ast.Constant) and isinstance(message.value, str):
                targets.setdefault(
                    message.value, {"message": message.value, **location}
                )
            else:
                unresolved.append(
                    {**location, "reason": "message is not a string literal"}
                )
    return {
        "version": 1,
        "targets": [targets[key] for key in sorted(targets)],
        "unresolved": unresolved,
    }


@lru_cache(maxsize=1)
def _load_catalog(path: str) -> dict[str, Any]:
    catalog = json.loads(Path(path).read_text())
    if catalog.get("version") != 1 or not all(
        isinstance(catalog.get(key), list) for key in ("targets", "unresolved")
    ):
        raise ValueError("invalid workload coverage catalog")
    return catalog


def catalog() -> dict[str, Any] | None:
    """Read the image's coverage inventory when it was configured."""
    path = os.environ.get(CATALOG_ENV)
    return _load_catalog(path) if path is not None else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(inventory(args.source), indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
