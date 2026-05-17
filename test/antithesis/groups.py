# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Workload-group manifest loader, shared by export-compose.py (build
time) and select_scripts.py (workload-container runtime).

The manifest itself lives in `test/antithesis/groups.yaml`. This module
is the only code that knows the manifest's on-disk shape; both consumers
go through `load_manifest()` so a schema change touches one place.
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MANIFEST_PATH = Path(__file__).resolve().parent / "groups.yaml"


@dataclasses.dataclass(frozen=True)
class Group:
    name: str
    description: str
    extra_services: tuple[str, ...]
    setup: tuple[str, ...]
    drivers: tuple[str, ...]
    anytime: tuple[str, ...]

    def all_services(self, universal: tuple[str, ...]) -> tuple[str, ...]:
        """Concatenation of universal + group-specific services in declaration order, no duplicates."""
        seen: set[str] = set()
        result: list[str] = []
        for name in (*universal, *self.extra_services):
            if name not in seen:
                seen.add(name)
                result.append(name)
        return tuple(result)

    def all_scripts(
        self,
        default_anytime: tuple[str, ...],
        default_drivers: tuple[str, ...] = (),
    ) -> tuple[str, ...]:
        """Every script Test Composer should see for this group: setup + drivers + anytime, plus the auto-included defaults."""
        seen: set[str] = set()
        result: list[str] = []
        for name in (
            *self.setup,
            *self.drivers,
            *default_drivers,
            *self.anytime,
            *default_anytime,
        ):
            if name not in seen:
                seen.add(name)
                result.append(name)
        return tuple(result)


@dataclasses.dataclass(frozen=True)
class Manifest:
    universal_services: tuple[str, ...]
    default_anytime: tuple[str, ...]
    default_drivers: tuple[str, ...]
    groups: dict[str, Group]

    def group(self, name: str) -> Group:
        try:
            return self.groups[name]
        except KeyError:
            valid = ", ".join(sorted(self.groups))
            raise KeyError(
                f"unknown workload group {name!r}; valid groups: {valid}"
            ) from None


def load_manifest(path: Path | str | None = None) -> Manifest:
    """Parse the YAML manifest at `path` (or the default in this directory)."""
    p = Path(path) if path is not None else DEFAULT_MANIFEST_PATH
    raw = yaml.safe_load(p.read_text())
    universal = tuple(raw.get("universal_services", []))
    default_anytime = tuple(raw.get("default_anytime", []))
    default_drivers = tuple(raw.get("default_drivers", []))
    groups: dict[str, Group] = {}
    for name, spec in raw.get("groups", {}).items():
        groups[name] = Group(
            name=name,
            description=spec.get("description", ""),
            extra_services=tuple(spec.get("services", []) or []),
            setup=tuple(spec.get("setup", []) or []),
            drivers=tuple(spec.get("drivers", []) or []),
            anytime=tuple(spec.get("anytime", []) or []),
        )
    return Manifest(
        universal_services=universal,
        default_anytime=default_anytime,
        default_drivers=default_drivers,
        groups=groups,
    )


def resolve_group_from_env(
    manifest: Manifest,
    env_var: str = "ANTITHESIS_WORKLOAD_GROUP",
    default: str = "combined",
) -> Group:
    """Return the active Group, defaulting to `combined` if the env var is unset.

    Surfaces a clear error if the env var names a group that isn't in the manifest.
    """
    name = os.environ.get(env_var, default)
    return manifest.group(name)


def script_filename(name: str, extension_hint: dict[str, str] | None = None) -> str:
    """Map a manifest script name to its on-disk filename.

    Most scripts are `.py`; `anytime_health_check` is `.sh`. The manifest
    omits the extension so the YAML stays terse; this resolver is the
    one place that knows the exception list.
    """
    overrides = extension_hint or {"anytime_health_check": "anytime_health_check.sh"}
    if name in overrides:
        return overrides[name]
    return f"{name}.py"


def all_known_scripts(manifest: Manifest) -> set[str]:
    """Every script name referenced anywhere in the manifest. Used by
    validation to catch typos (a manifest entry naming a script that
    doesn't exist on disk) and orphans (a script on disk not referenced
    by any group)."""
    seen: set[str] = set()
    for g in manifest.groups.values():
        seen.update(g.setup)
        seen.update(g.drivers)
        seen.update(g.anytime)
    seen.update(manifest.default_anytime)
    seen.update(manifest.default_drivers)
    return seen


def _self_check() -> int:
    """Internal sanity check: every group resolves; groups have at least one script."""
    m = load_manifest()
    rc = 0
    for name, g in m.groups.items():
        services = g.all_services(m.universal_services)
        scripts = g.all_scripts(m.default_anytime, m.default_drivers)
        if not scripts:
            print(f"WARN: group {name!r} has no scripts at all")
            rc = 1
        if not services:
            print(f"WARN: group {name!r} has no services at all")
            rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(_self_check())
