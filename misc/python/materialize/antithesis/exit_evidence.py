# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Explain native halt exits only from complete, termination-correlated evidence."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from materialize import MZ_ROOT


@dataclass(frozen=True)
class HaltRule:
    matches: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class ExitDisposition:
    disposition: Literal["designed", "product", "unresolved"]
    reason: str
    covered_occurrences: int = 0


_STRING = r'"(?:[^"\\]|\\.)*"'
_RULE = re.compile(
    r"\s*DesignedHalt\s*\{\s*matches\s*:\s*&\s*\[(?P<matches>.*?)\]"
    rf"\s*,\s*reason\s*:\s*(?P<reason>{_STRING})\s*,?\s*\}}\s*,?",
    re.DOTALL,
)
_EXIT_PROPERTY = re.compile(
    r"No unexpected container exits: container:\s*([^,]+),"
    r"\s*namespace:\s*([^,]+),\s*exit code:\s*(-?\d+)"
)
_IDENTITY_FIELDS = (
    "namespace",
    "pod_uid",
    "container",
    "termination_count",
    "finished_at",
)


def _string(literal: str) -> str:
    # Rust's escaped newline also removes the indentation on the next line.
    value = json.loads(re.sub(r"\\\r?\n\s*", "", literal))
    if not isinstance(value, str) or not value:
        raise ValueError("halt policy strings must be nonempty")
    return value


def _matches(source: str) -> tuple[str, ...]:
    values = []
    remaining = source.strip()
    while remaining:
        match = re.match(_STRING, remaining, re.DOTALL)
        if match is None:
            raise ValueError("unsupported halt policy match syntax")
        values.append(_string(match[0]))
        remaining = remaining[match.end() :].strip()
        if remaining:
            if not remaining.startswith(","):
                raise ValueError("halt policy matches must be comma-separated")
            remaining = remaining[1:].strip()
    if not values:
        raise ValueError("halt policy rules must have match strings")
    return tuple(values)


def load_halt_rules(source: Path | None = None) -> tuple[HaltRule, ...]:
    """Read the reviewed SUT rules, rejecting unsupported source syntax."""
    source = source or MZ_ROOT / "src" / "ore" / "src" / "antithesis.rs"
    text = source.read_text()
    table = re.search(
        r"pub\s+const\s+DESIGNED_HALTS\s*:\s*&\s*\[\s*DesignedHalt\s*\]"
        r"\s*=\s*&\s*\[(.*?)\n\s*\];",
        text,
        re.DOTALL,
    )
    if table is None:
        raise ValueError("reviewed SUT halt table was not found")
    remaining = table[1]
    rules = []
    while remaining.strip():
        entry = _RULE.match(remaining)
        if entry is None:
            raise ValueError("unsupported reviewed SUT halt table syntax")
        rules.append(HaltRule(_matches(entry["matches"]), _string(entry["reason"])))
        remaining = remaining[entry.end() :]
    if not rules:
        raise ValueError("reviewed SUT halt table is empty")
    return tuple(rules)


def _termination(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, Mapping):
        raise ValueError("termination identity is missing")
    for field in ("namespace", "pod_uid", "container", "finished_at"):
        if not isinstance(value.get(field), str) or not value[field]:
            raise ValueError(f"termination {field} is missing")
    if (
        type(value.get("termination_count")) is not int
        or value["termination_count"] < 1
    ):
        raise ValueError("termination_count must identify a completed termination")
    if type(value.get("exit_code")) is not int or value["exit_code"] != 166:
        raise ValueError("termination evidence is not for exit 166")
    finished = datetime.fromisoformat(value["finished_at"])
    if finished.tzinfo is None:
        raise ValueError("termination finished_at has no timezone")
    return tuple(value[field] for field in _IDENTITY_FIELDS)


def native_exit(prop: Mapping[str, Any]) -> tuple[str, str, int] | None:
    """Return the container, namespace and code named by a native exit property."""
    name = prop.get("name")
    match = _EXIT_PROPERTY.fullmatch(name) if isinstance(name, str) else None
    if match is None:
        return None
    container, namespace, code = (part.strip() for part in match.groups())
    return container, namespace, int(code)


def classify_exit_property(
    prop: Mapping[str, Any],
    occurrences: Sequence[Mapping[str, Any]],
    *,
    policy: Sequence[HaltRule] | None = None,
) -> ExitDisposition:
    """Classify a native halt property without accepting partial occurrence coverage.

    Each occurrence supplies a Kubernetes termination identity and a complete
    halt message bound to that same termination. A valid unexpected halt is
    product evidence even when other occurrences remain unexplained.
    """
    attribution = native_exit(prop)
    if attribution is None:
        return ExitDisposition(
            "unresolved", "native exit property attribution is missing"
        )
    container, namespace, code = attribution
    if code != 166:
        return ExitDisposition(
            "product", "native exit is not a designed-halt candidate"
        )
    total = prop.get("counterexample_count")
    if type(total) is not int or total < 1:
        return ExitDisposition("unresolved", "native exit occurrence count is missing")
    try:
        rules = tuple(policy) if policy is not None else load_halt_rules()
        if not rules or any(
            not isinstance(rule, HaltRule)
            or not rule.matches
            or any(not isinstance(part, str) or not part for part in rule.matches)
            or not rule.reason
            for rule in rules
        ):
            raise ValueError("reviewed SUT halt policy is incomplete")
    except (OSError, ValueError) as error:
        return ExitDisposition(
            "unresolved", f"reviewed SUT halt policy unavailable: {error}"
        )
    if not isinstance(occurrences, Sequence) or isinstance(occurrences, (str, bytes)):
        return ExitDisposition(
            "unresolved", "native exit occurrence evidence is not a list"
        )

    covered: dict[tuple[Any, ...], tuple[tuple[Any, ...], str]] = {}
    unexpected = False
    errors = []
    for occurrence in occurrences:
        try:
            if not isinstance(occurrence, Mapping):
                raise ValueError("occurrence evidence is not an object")
            termination = _termination(occurrence.get("termination"))
            if termination[0] != namespace or termination[2] != container:
                raise ValueError(
                    "termination does not belong to the native exit property"
                )
            halt = occurrence.get("halt")
            if not isinstance(halt, Mapping) or halt.get("complete") is not True:
                raise ValueError("complete halt evidence is missing")
            if _termination(halt.get("termination")) != termination:
                raise ValueError("halt evidence is from another termination")
            message = halt.get("message")
            if not isinstance(message, str) or not message:
                raise ValueError("halt message is missing")
            # A different timestamp cannot make the same pod restart a second occurrence.
            identity = termination[:-1]
            message = message.removeprefix("halting process: ")
            if identity in covered:
                if covered[identity] != (termination, message):
                    raise ValueError("termination evidence conflicts")
                continue
            covered[identity] = (termination, message)
            if not any(all(part in message for part in rule.matches) for rule in rules):
                unexpected = True
        except (TypeError, ValueError) as error:
            errors.append(str(error))

    count = len(covered)
    if unexpected:
        return ExitDisposition(
            "product", "an exit-correlated halt is not a reviewed designed halt", count
        )
    if errors or count != total:
        reason = (
            f"designed halt evidence covers {count} of {total} native exit occurrences"
        )
        if errors:
            reason += f": {errors[0]}"
        return ExitDisposition("unresolved", reason, count)
    return ExitDisposition(
        "designed",
        "every native exit has complete, correlated designed-halt evidence",
        count,
    )
