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
from pathlib import Path

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


def all_known_scripts(manifest: Manifest) -> set[str]:
    """Every script name referenced anywhere in the manifest.

    Used by `_self_check` (below) to catch typos in `groups.yaml`
    (a manifest entry naming a script that doesn't exist on disk)
    and orphans (a `workload/test/<template>/<file>` not referenced
    by any manifest entry — silent dead code in the image).
    """
    seen: set[str] = set()
    for g in manifest.groups.values():
        seen.update(g.setup)
        seen.update(g.drivers)
        seen.update(g.anytime)
    seen.update(manifest.default_anytime)
    seen.update(manifest.default_drivers)
    return seen


def _self_check() -> int:
    """Manifest sanity check: every group resolves, every manifest entry
    points at an existing file under `workload/test/`, and no template
    script is orphaned (in `workload/test/<template>/` but absent from
    the manifest).  Invokable via `python3 test/antithesis/groups.py`
    locally or as a CI lint.
    """
    m = load_manifest()
    rc = 0
    test_root = Path(__file__).resolve().parent / "workload" / "test"

    for name, g in m.groups.items():
        services = g.all_services(m.universal_services)
        scripts = g.all_scripts(m.default_anytime, m.default_drivers)
        if not scripts:
            print(f"WARN: group {name!r} has no scripts at all")
            rc = 1
        if not services:
            print(f"WARN: group {name!r} has no services at all")
            rc = 1

    # Anytime drivers may be `.py` or `.sh`; everything else is `.py`.
    # Bake-script (workload/scripts-for-group.py) keeps the same rule;
    # mirror it here so the lint matches what the image actually copies.
    referenced_paths: set[Path] = set()
    for entry in all_known_scripts(m):
        template, basename = entry.split("/", 1)
        ext = ".sh" if basename == "anytime_health_check" else ".py"
        p = test_root / template / f"{basename}{ext}"
        referenced_paths.add(p)
        if not p.is_file():
            print(f"ERROR: manifest entry {entry!r} → {p} not found on disk")
            rc = 1

    # Orphan scan: every `first_`/`parallel_driver_`/`singleton_driver_`/
    # `anytime_` file under `workload/test/<template>/` should be
    # referenced by some manifest entry.  `helper_*` files are
    # intentionally not in the manifest (they're imported by drivers).
    for path in test_root.rglob("*"):
        if not path.is_file():
            continue
        if path.name.startswith(("helper_", "__")):
            continue
        if path.suffix not in (".py", ".sh"):
            continue
        # Skip `helpers/` directory entirely (shared across templates).
        if "helpers" in path.parts:
            continue
        if path not in referenced_paths:
            rel = path.relative_to(test_root)
            print(f"WARN: orphan script {rel} not referenced by any manifest entry")
            rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(_self_check())
