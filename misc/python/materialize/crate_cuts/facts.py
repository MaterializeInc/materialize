# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The extracted packages and module reference graph, the input of the model."""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Package:
    id: str
    name: str
    is_workspace: bool
    is_proc_macro: bool = False
    # Normal and build dependencies, as package ids, for the host platform.
    deps: frozenset[str] = frozenset()
    # The subset of `deps` that build scripts use. Build scripts are not
    # indexed, so a split keeps them in both parts.
    build_deps: frozenset[str] = frozenset()


@dataclass
class Module:
    """A source file of a workspace package, the unit a split moves."""

    path: str
    package: str
    # `lib` or `bin`. Only library modules can move into a split-off crate.
    role: str
    loc: int
    # Referenced workspace modules, with occurrence counts. Whether a
    # reference crosses a crate boundary depends on the current assignment
    # of modules to crates, which splits change.
    refs: Counter[str] = field(default_factory=Counter)
    # Referenced packages outside the workspace, by package id.
    external: Counter[str] = field(default_factory=Counter)
    # Other workspace packages named in paths, as package ids. Naming a
    # package requires depending on it even when the items behind the path
    # are re-exports defined elsewhere.
    named: set[str] = field(default_factory=set)
    # Referenced symbols per referenced module of another package, for
    # reporting.
    symbols: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # Items of this module that reference each module of another package,
    # for reporting.
    referrers: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # Modules this module must stay in the same crate with, because of an
    # impl the orphan rule ties to them.
    pinned: set[str] = field(default_factory=set)


@dataclass
class Facts:
    packages: dict[str, Package]
    modules: dict[str, Module]

    def to_json(self) -> dict[str, Any]:
        return {
            "packages": [
                {
                    "id": p.id,
                    "name": p.name,
                    "workspace": p.is_workspace,
                    "proc_macro": p.is_proc_macro,
                    "deps": sorted(p.deps),
                    "build_deps": sorted(p.build_deps),
                }
                for p in sorted(self.packages.values(), key=lambda p: p.id)
            ],
            "modules": [
                {
                    "path": m.path,
                    "package": m.package,
                    "role": m.role,
                    "loc": m.loc,
                    "refs": dict(sorted(m.refs.items())),
                    "external": dict(sorted(m.external.items())),
                    "named": sorted(m.named),
                    "pinned": sorted(m.pinned),
                    "symbols": {t: sorted(s) for t, s in sorted(m.symbols.items())},
                    "referrers": {t: sorted(s) for t, s in sorted(m.referrers.items())},
                }
                for m in sorted(self.modules.values(), key=lambda m: m.path)
            ],
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "Facts":
        packages = {
            p["id"]: Package(
                id=p["id"],
                name=p["name"],
                is_workspace=p["workspace"],
                is_proc_macro=p["proc_macro"],
                deps=frozenset(p["deps"]),
                build_deps=frozenset(p["build_deps"]),
            )
            for p in data["packages"]
        }
        modules = {
            m["path"]: Module(
                path=m["path"],
                package=m["package"],
                role=m["role"],
                loc=m["loc"],
                refs=Counter(m["refs"]),
                external=Counter(m["external"]),
                named=set(m["named"]),
                symbols=defaultdict(set, {t: set(s) for t, s in m["symbols"].items()}),
                referrers=defaultdict(
                    set, {t: set(s) for t, s in m["referrers"].items()}
                ),
                pinned=set(m["pinned"]),
            )
            for m in data["modules"]
        }
        return cls(packages, modules)
