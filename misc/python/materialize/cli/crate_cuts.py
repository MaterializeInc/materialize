#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Find crate splits and dependency edges that reduce crate connectivity.

Builds a module-level reference graph of the workspace from a SCIP index
produced by rust-analyzer, overlays it on the Cargo dependency graph, and
ranks two kinds of refactorings by how much they shrink the transitive
dependency closure of workspace crates:

* Crate splits. A crate is cut into a `core` part, the modules that do not
  reach a set of dependencies, and a `rest` part. Consumers that only use
  `core` modules stop depending on those dependencies.
* Edge cuts. A dependency edge whose references are few is a candidate for
  moving the referencing code or the referenced items.

Indexing needs `rust-analyzer` (`rustup component add rust-analyzer`) and
takes several minutes. The index is cached in the output directory, which
also receives `report.md`, `report.json`, the module graph `modules.json`, and
the crate graph `crates.dot`. Pass `--reindex` after changing code.

Proposals are candidates derived from references under the features Cargo
resolves for the workspace and `cfg(not(test))`. Code that only macro
expansions reference can be missed. Verify each proposal by performing the
split and running `cargo check`.
"""

import argparse
import bisect
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path

from materialize import MZ_ROOT, buildkite, scip, spawn, ui

# Packages whose symbols never imply a Cargo dependency.
SYSROOT_PACKAGES = {"std", "core", "alloc", "proc_macro", "test"}

# rust-analyzer configuration for indexing: library code as `cargo build`
# sees it. Test modules inside `src/` would otherwise attribute dev-dependency
# references to library modules.
RUST_ANALYZER_CONFIG = {"cfg": {"setTest": False}}


@dataclass
class Package:
    id: str
    name: str
    version: str
    manifest_dir: Path
    is_workspace: bool
    has_lib: bool
    is_proc_macro: bool = False
    # The package this one was split off from, if any.
    origin: str | None = None
    # Normal and build dependencies, as package ids, for the host platform.
    deps: set[str] = field(default_factory=set)
    # The subset of `deps` that build scripts use. Build scripts are not
    # indexed, so a split keeps them in both parts.
    build_deps: set[str] = field(default_factory=set)


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
    # of modules to packages, which splits change.
    refs: Counter[str] = field(default_factory=Counter)
    # Referenced packages outside the workspace, by package id.
    external: Counter[str] = field(default_factory=Counter)
    # Other workspace packages named in paths, as package ids before any
    # split. Naming a package requires depending on it even when the items
    # behind the path are re-exports defined elsewhere.
    named: set[str] = field(default_factory=set)
    # Referenced symbols per referenced module of another package at load
    # time, for reporting.
    symbols: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # Items of this module that reference each module of another package,
    # for reporting.
    referrers: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # Modules this module must stay in the same crate with, because of an
    # impl the orphan rule ties to them.
    pinned: set[str] = field(default_factory=set)


class Model:
    """The Cargo graph, the module graph, and the assignment between them."""

    def __init__(self, packages: dict[str, Package], modules: dict[str, Module]):
        self.packages = packages
        self.modules = modules
        self.by_package: dict[str, set[str]] = defaultdict(set)
        for m in modules.values():
            self.by_package[m.package].add(m.path)
        self.bit: dict[str, int] = {}
        for p in sorted(packages):
            self._register(p)
        # Closures as bitsets over `bit`. External packages never depend on
        # workspace packages, so their entries survive changes to the
        # workspace graph.
        self._closure: dict[str, int] = {}

    def _register(self, pkg_id: str) -> None:
        self.bit[pkg_id] = 1 << len(self.bit)

    def workspace(self) -> list[str]:
        return sorted(p.id for p in self.packages.values() if p.is_workspace)

    def workspace_mask(self) -> int:
        mask = 0
        for p in self.workspace():
            mask |= self.bit[p]
        return mask

    def invalidate(self) -> None:
        for p in self.workspace():
            self._closure.pop(p, None)

    def closure(self, pkg_id: str) -> int:
        """Transitive dependencies of `pkg_id` as a bitset, excluding itself."""
        hit = self._closure.get(pkg_id)
        if hit is not None:
            return hit
        # Iterative post-order. Normal and build dependencies form a DAG.
        stack = [(pkg_id, False)]
        while stack:
            p, done = stack.pop()
            if p in self._closure:
                continue
            deps = self.packages[p].deps
            if done:
                acc = 0
                for d in deps:
                    acc |= self.bit[d] | self._closure[d]
                self._closure[p] = acc
                continue
            stack.append((p, True))
            for d in deps:
                if d not in self._closure:
                    stack.append((d, False))
        return self._closure[pkg_id]

    def reaches(self, pkg_id: str, target: str) -> bool:
        return bool(self.closure(pkg_id) & self.bit[target])

    def module_needs(self, path: str) -> set[str]:
        """Packages a module needs as direct dependencies of its crate."""
        m = self.modules[path]
        pkg = self.packages[m.package]
        needed = {self.modules[t].package for t in m.refs} | set(m.external)
        origins = {self.packages[n].origin or n for n in needed}
        # Without an item reference into a split package, the path could
        # name either part. `rest` keeps the original id and is conservative.
        needed |= m.named - origins
        needed.discard(pkg.id)
        direct = needed & pkg.deps
        # A reference can resolve to a package that is not a direct
        # dependency, for example through a re-export. Attribute it to a
        # direct dependency that is already needed and reaches it, else to
        # the reaching dependency with the smallest closure.
        for target in sorted(needed - pkg.deps):
            if any(self.reaches(d, target) for d in direct):
                continue
            providers = [d for d in pkg.deps if self.reaches(d, target)]
            if providers:
                direct.add(
                    min(providers, key=lambda d: (self.closure(d).bit_count(), d))
                )
        return direct

    def split(self, pkg_id: str, core: set[str]) -> str:
        """Moves `core` modules of `pkg_id` into a new package and rewires.

        Returns the new package id. Consumers that reference only `core`
        modules depend on the new package instead of `pkg_id`. Consumers
        that reference both parts depend on both. Consumers that reference
        neither keep their declared dependency on `pkg_id`.
        """
        pkg = self.packages[pkg_id]
        consumers = self.consumers(pkg_id)
        own = self.by_package[pkg_id]
        used = {c: self.references(c, own) for c in consumers}
        n = sum(1 for p in self.packages if p.startswith(pkg_id + "+core"))
        core_id = f"{pkg_id}+core{n}"
        core_deps = set(pkg.build_deps)
        for path in core:
            core_deps |= self.module_needs(path)
        self.packages[core_id] = Package(
            id=core_id,
            name=f"{pkg.name}-core" + (str(n) if n else ""),
            version=pkg.version,
            manifest_dir=pkg.manifest_dir,
            is_workspace=True,
            has_lib=True,
            deps=core_deps,
            build_deps=set(pkg.build_deps),
            origin=pkg.origin or pkg.id,
        )
        self._register(core_id)
        for path in core:
            self.modules[path].package = core_id
        self.by_package[core_id] = set(core)
        self.by_package[pkg_id] = own - core
        pkg.deps.add(core_id)
        for c in consumers:
            if not used[c] & core:
                continue
            self.packages[c].deps.add(core_id)
            if used[c] <= core:
                self.packages[c].deps.discard(pkg_id)
        self.invalidate()
        return core_id

    def consumers(self, pkg_id: str) -> list[str]:
        return [
            p.id
            for p in self.packages.values()
            if p.is_workspace and pkg_id in p.deps and p.id != pkg_id
        ]

    def references(self, consumer: str, targets: set[str]) -> set[str]:
        """Modules in `targets` referenced from modules of `consumer`."""
        out: set[str] = set()
        for path in self.by_package[consumer]:
            out |= targets & self.modules[path].refs.keys()
        return out

    def consumers_using_only(self, pkg_id: str, core: set[str]) -> list[str]:
        own = self.by_package[pkg_id]
        out = []
        for c in self.consumers(pkg_id):
            used = self.references(c, own)
            if used and used <= core:
                out.append(c)
        return out

    def snapshot(self) -> tuple:
        return (
            {k: set(p.deps) for k, p in self.packages.items()},
            {p: m.package for p, m in self.modules.items()},
            {k: set(v) for k, v in self.by_package.items()},
            dict(self.bit),
        )

    def restore(self, snap: tuple) -> None:
        deps, owners, by_package, bit = snap
        for k in list(self.packages):
            if k not in deps:
                del self.packages[k]
                self._closure.pop(k, None)
        for k, d in deps.items():
            self.packages[k].deps = d
        for p, owner in owners.items():
            self.modules[p].package = owner
        self.by_package.clear()
        self.by_package.update(by_package)
        self.bit = bit
        self.invalidate()


def load_packages(root: Path) -> dict[str, Package]:
    meta = json.loads(
        spawn.capture(
            [
                "cargo",
                "metadata",
                "--format-version=1",
                f"--filter-platform={host_triple()}",
            ],
            cwd=root,
        )
    )
    members = set(meta["workspace_members"])
    packages = {}
    for p in meta["packages"]:
        packages[p["id"]] = Package(
            id=p["id"],
            name=p["name"],
            version=p["version"],
            manifest_dir=Path(p["manifest_path"]).parent,
            is_workspace=p["id"] in members,
            has_lib=any(
                k in ("lib", "rlib", "proc-macro")
                for t in p["targets"]
                for k in t["kind"]
            ),
            is_proc_macro=any("proc-macro" in t["kind"] for t in p["targets"]),
        )
    for node in meta["resolve"]["nodes"]:
        for dep in node["deps"]:
            kinds = {k["kind"] for k in dep["dep_kinds"]}
            if kinds & {None, "build"}:
                packages[node["id"]].deps.add(dep["pkg"])
            if "build" in kinds:
                packages[node["id"]].build_deps.add(dep["pkg"])
    for p in packages.values():
        if not p.is_workspace and p.deps & members:
            # `Model` caches closures of external packages across splits.
            raise RuntimeError(f"external package {p.name} depends on the workspace")
    # Only resolved packages exist for the host platform.
    resolved = {n["id"] for n in meta["resolve"]["nodes"]}
    return {k: v for k, v in packages.items() if k in resolved}


def host_triple() -> str:
    for line in spawn.capture(["rustc", "-vV"]).splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ")
    raise RuntimeError("rustc -vV did not report a host triple")


def classify(rel: Path, pkg: Package) -> str | None:
    """Returns the role of a package-relative path, `None` for non-build code."""
    parts = rel.parts
    if parts[0] in ("tests", "benches", "examples") or rel == Path("build.rs"):
        return None
    if not pkg.has_lib or parts[:2] == ("src", "bin") or rel == Path("src/main.rs"):
        return "bin"
    return "lib"


# A `use` declaration. Visibility-qualified ones re-export.
USE_DECL = re.compile(
    r"^[ \t]*(?P<vis>pub(?:\s*\([^)]*\))?\s+)?use\s[^;]*;", re.MULTILINE
)
GLOB_SUFFIX = re.compile(r"\s*::\s*\*")


@dataclass
class SourceText:
    """Line-addressed source text with the spans of its `use` declarations."""

    lines: list[str]
    # `(start, end, is_reexport)` with `(line, column)` bounds.
    uses: list[tuple[tuple[int, int], tuple[int, int], bool]]

    @classmethod
    def read(cls, path: Path) -> "SourceText":
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return cls([], [])
        starts = [0]
        for i, c in enumerate(text):
            if c == "\n":
                starts.append(i + 1)

        def pos(offset: int) -> tuple[int, int]:
            line = bisect.bisect_right(starts, offset) - 1
            return line, offset - starts[line]

        uses = [
            (pos(m.start()), pos(m.end()), m.group("vis") is not None)
            for m in USE_DECL.finditer(text)
        ]
        return cls(text.split("\n"), uses)

    def use_at(self, line: int, col: int) -> bool | None:
        """`None` outside `use` declarations, else whether it re-exports."""
        at = (line, col)
        i = bisect.bisect_right(self.uses, (at, (sys.maxsize, 0), True)) - 1
        if i >= 0 and self.uses[i][0] <= at < self.uses[i][1]:
            return self.uses[i][2]
        return None

    def is_glob(self, line: int, end_col: int) -> bool:
        """Whether the path segment ending at `(line, end_col)` is glob-imported."""
        return line < len(self.lines) and bool(
            GLOB_SUFFIX.match(self.lines[line], end_col)
        )


def _start_end(r: list[int]) -> tuple[tuple[int, int], tuple[int, int]]:
    if len(r) == 3:
        return (r[0], r[1]), (r[0], r[2])
    return (r[0], r[1]), (r[2], r[3])


class ItemLocator:
    """Finds the innermost item definition enclosing a position of a document."""

    def __init__(self, doc: scip.Document):
        spans = []
        for occ in doc.occurrences:
            if not (occ.roles & scip.ROLE_DEFINITION and occ.enclosing_range):
                continue
            sym = scip.parse_symbol(occ.symbol)
            if sym is None or sym.is_module:
                continue
            start, end = _start_end(occ.enclosing_range)
            spans.append((start, end, sym.descriptors))
        # Definitions nest, so sorting by start and then by decreasing end puts
        # every span after the spans enclosing it.
        spans.sort(key=lambda s: (s[0], (-s[1][0], -s[1][1])))
        self.spans = spans
        self.starts = [s[0] for s in spans]
        self.parent: list[int] = []
        open_spans: list[int] = []
        for i, (start, _end, _name) in enumerate(spans):
            while open_spans and spans[open_spans[-1]][1] <= start:
                open_spans.pop()
            self.parent.append(open_spans[-1] if open_spans else -1)
            open_spans.append(i)

    def item_at(self, r: list[int]) -> str | None:
        pos, _ = _start_end(r)
        # The innermost span containing `pos` is the last span starting at or
        # before `pos`, or one of its ancestors.
        i = bisect.bisect_right(self.starts, pos) - 1
        while i >= 0:
            _start, end, name = self.spans[i]
            if pos < end:
                return name
            i = self.parent[i]
        return None


def load_modules(
    root: Path, index: Path, packages: dict[str, Package]
) -> dict[str, Module]:
    ws = sorted(
        (p for p in packages.values() if p.is_workspace),
        key=lambda p: len(p.manifest_dir.parts),
        reverse=True,
    )

    def owner(path: Path) -> Package | None:
        for p in ws:
            if path.is_relative_to(p.manifest_dir):
                return p
        return None

    by_name_version = {(p.name, p.version): p.id for p in packages.values()}
    by_name: dict[str, list[str]] = defaultdict(list)
    for p in packages.values():
        by_name[p.name].append(p.id)
        by_name[p.name.replace("-", "_")].append(p.id)

    def package_of(sym: scip.Symbol) -> str | None:
        if sym.package in SYSROOT_PACKAGES:
            return None
        hit = by_name_version.get((sym.package, sym.version))
        if hit is None:
            hit = by_name_version.get((sym.package.replace("_", "-"), sym.version))
        if hit is None and len(by_name.get(sym.package, [])) == 1:
            hit = by_name[sym.package][0]
        return hit

    ui.say(f"reading {index}")
    docs = list(scip.read_documents(index.read_bytes()))

    modules: dict[str, Module] = {}
    texts: dict[str, SourceText] = {}
    definitions: dict[str, str] = {}
    # The file of each file-backed or inline module, by package and module
    # path (`a/b/`, the crate root is `crate/`).
    module_files: dict[tuple[str, str], str] = {}
    whole_file: set[tuple[str, str]] = set()
    # Modules defining a top-level type or trait, by package and name.
    types: dict[tuple[str, str], set[str]] = defaultdict(set)
    for doc in docs:
        path = root / doc.relative_path
        pkg = owner(path)
        if pkg is None:
            continue
        role = classify(path.relative_to(pkg.manifest_dir), pkg)
        if role is None:
            continue
        text = SourceText.read(path)
        texts[doc.relative_path] = text
        modules[doc.relative_path] = Module(
            doc.relative_path, pkg.id, role, len(text.lines)
        )
        for occ in doc.occurrences:
            if not occ.roles & scip.ROLE_DEFINITION:
                continue
            definitions.setdefault(occ.symbol, doc.relative_path)
            sym = scip.parse_symbol(occ.symbol)
            if sym is None:
                continue
            if (name := sym.type_name()) is not None:
                types[(pkg.id, name)].add(doc.relative_path)
            if sym.is_module:
                key = (pkg.id, sym.descriptors)
                # A file-backed module's definition spans its file from the
                # start. Its `mod` declaration in the parent is also a
                # definition. Library roots win over binary roots, which share
                # the `crate/` path.
                whole = occ.range[:2] == [0, 0]
                prev = module_files.get(key)
                if (
                    prev is None
                    or (whole and key not in whole_file)
                    or (whole and role == "lib" and modules[prev].role != "lib")
                ):
                    module_files[key] = doc.relative_path
                    if whole:
                        whole_file.add(key)

    def resolve(sym: scip.Symbol, raw: str) -> str | None:
        """The defining module file of a workspace symbol, if known."""
        target = definitions.get(raw)
        if target is not None and target in modules:
            return target
        # Items generated by macros have no definition occurrence. Their
        # symbol still names the module they are generated into.
        pkg = package_of(sym)
        if pkg is None or not packages[pkg].is_workspace:
            return None
        path = sym.descriptors.rsplit("/", 1)[0] + "/" if "/" in sym.descriptors else ""
        return module_files.get((pkg, path or "crate/"))

    # Targets of re-exporting glob imports, `pub use x::*`, per module.
    pub_globs: dict[str, set[str]] = defaultdict(set)
    # Targets of private glob imports, per module.
    glob_imports: dict[str, set[str]] = defaultdict(set)
    for doc in docs:
        m = modules.get(doc.relative_path)
        if m is None:
            continue
        text = texts[doc.relative_path]
        enclosing = ItemLocator(doc)
        impls: set[tuple[str, str | None]] = set()
        for occ in doc.occurrences:
            sym = scip.parse_symbol(occ.symbol)
            if sym is None:
                continue
            if occ.roles & scip.ROLE_DEFINITION:
                if (impl := sym.impl_header()) is not None:
                    impls.add(impl)
                continue
            line, col = occ.range[0], occ.range[1]
            end_col = occ.range[2] if len(occ.range) == 3 else occ.range[3]
            reexport = text.use_at(line, col)
            if sym.is_module:
                pkg = package_of(sym)
                if pkg is None:
                    continue
                if not packages[pkg].is_workspace:
                    if pkg != m.package:
                        m.external[pkg] += 1
                    continue
                if pkg != m.package:
                    m.named.add(pkg)
                # Other module path segments are structure, not a use of an
                # item. A glob import uses every item of the module, which
                # matters where macro-generated references lack occurrences.
                if reexport is not None and text.is_glob(line, end_col):
                    target = module_files.get((pkg, sym.descriptors))
                    if target is not None and target != m.path:
                        (pub_globs if reexport else glob_imports)[m.path].add(target)
                continue
            target = resolve(sym, occ.symbol)
            if target is not None:
                if target == m.path:
                    continue
                same = modules[target].package == m.package
                # A re-export within the crate is not a use of the item. In a
                # split, the re-export moves with the item.
                if same and reexport:
                    continue
                m.refs[target] += 1
                if not same:
                    m.symbols[target].add(sym.descriptors)
                    # Impl headers and attributes lie outside any item span.
                    item = enclosing.item_at(occ.range) or f"line {line + 1}"
                    m.referrers[target].add(item)
                continue
            ext = package_of(sym)
            if ext is not None and ext != m.package:
                m.external[ext] += 1
        for self_ty, trait in sorted(impls, key=str):
            pin_impl(m, self_ty, trait, types)

    # A glob import reaches through the re-exporting globs of its target.
    for path, targets in glob_imports.items():
        seen: set[str] = set()
        stack = list(targets)
        while stack:
            t = stack.pop()
            if t in seen:
                continue
            seen.add(t)
            stack.extend(pub_globs.get(t, ()))
        seen.discard(path)
        for t in seen:
            modules[path].refs[t] += 1
    return modules


def pin_impl(
    m: Module,
    self_ty: str,
    trait: str | None,
    types: dict[tuple[str, str], set[str]],
) -> None:
    """Records the placement constraint of an impl defined in `m`.

    An inherent impl must live in the crate of its self type, a trait impl in
    the crate of its self type or its trait. A split that separated `m` from
    that module would not compile, so `m` is pinned to it. When both are
    local, the self type wins, which is conservative.
    """

    def owner(name: str | None) -> set[str]:
        if name is None:
            return set()
        found = types.get((m.package, name), set())
        if m.path in found:
            return {m.path}
        # Type names are not unique within a package. Prefer definitions the
        # impl's module references.
        referenced = found & m.refs.keys()
        return referenced or found

    self_mods = owner(self_ty)
    trait_mods = owner(trait)
    if m.path in self_mods or m.path in trait_mods:
        return
    m.pinned |= self_mods or trait_mods


def module_reach(model: Model, pkg_id: str) -> dict[str, set[str]]:
    """Direct dependencies of `pkg_id` each of its modules needs transitively.

    Internal references and impl pins are followed, so a module's set
    includes the needs of every same-package module it requires.
    """
    own = model.by_package[pkg_id]
    paths = sorted(own)
    succ: dict[str, set[str]] = {}
    for p in paths:
        m = model.modules[p]
        succ[p] = (m.refs.keys() | m.pinned) & own
    # Pins are symmetric placement constraints.
    for p in paths:
        for t in model.modules[p].pinned & own:
            succ[t].add(p)
    needs = {p: model.module_needs(p) for p in paths}
    out: dict[str, set[str]] = {}
    for comp in sccs(paths, succ):
        acc: set[str] = set()
        members = set(comp)
        for p in comp:
            acc |= needs[p]
            for t in succ[p]:
                if t not in members:
                    acc |= out[t]
        for p in comp:
            out[p] = acc
    return out


def sccs(nodes: list[str], succ: dict[str, set[str]]) -> list[list[str]]:
    """Strongly connected components in reverse topological order (sinks first)."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    out: list[list[str]] = []
    counter = 0
    for root in nodes:
        if root in index:
            continue
        work = [(root, iter(sorted(succ[root])))]
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            v, it = work[-1]
            advanced = False
            for w in it:
                if w not in index:
                    index[w] = low[w] = counter
                    counter += 1
                    stack.append(w)
                    on_stack.add(w)
                    work.append((w, iter(sorted(succ[w]))))
                    advanced = True
                    break
                elif w in on_stack:
                    low[v] = min(low[v], index[w])
            if advanced:
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[v])
            if low[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.append(w)
                    if w == v:
                        break
                out.append(comp)
    return out


@dataclass
class Metrics:
    # Pairs `(X, Y)` of workspace crates where X transitively depends on Y.
    # Equals the number of crates rebuilt when editing every crate once.
    workspace_pairs: int
    # Expected lines of workspace crates rebuilt when editing a uniformly
    # random line of a workspace crate, including the edited crate itself.
    # Splitting a crate preserves its lines, so unlike `workspace_pairs` this
    # does not reward or penalize the number of crates as such.
    rebuild_lines: int
    # Pairs `(X, Y)` where X is a workspace crate and Y any crate.
    all_pairs: int

    def minus(self, other: "Metrics") -> "Metrics":
        return Metrics(
            self.workspace_pairs - other.workspace_pairs,
            self.rebuild_lines - other.rebuild_lines,
            self.all_pairs - other.all_pairs,
        )

    def key(self, objective: str) -> tuple[int, ...]:
        """Sort key, larger is better, for a delta under `objective`."""
        if objective == "lines":
            return (self.rebuild_lines, self.workspace_pairs, self.all_pairs)
        return (self.workspace_pairs, self.rebuild_lines, self.all_pairs)


def metrics(model: Model) -> Metrics:
    ws = model.workspace()
    mask = model.workspace_mask()
    loc = {p: sum(model.modules[m].loc for m in model.by_package[p]) for p in ws}
    total = sum(loc.values()) or 1
    wp = ap = weighted = 0
    for x in ws:
        c = model.closure(x)
        ap += c.bit_count()
        wp += (c & mask).bit_count()
        # Editing x or any workspace crate it depends on rebuilds x.
        edited = loc[x] + sum(loc[y] for y in ws if c & model.bit[y])
        weighted += loc[x] * edited
    return Metrics(wp, round(weighted / total), ap)


@dataclass
class Cut:
    package: str
    avoided: list[str]
    core: list[str]
    rest_lib: list[str]
    core_loc: int
    rest_loc: int
    consumers: list[str]
    delta: Metrics


def candidate_cuts(model: Model, pkg_id: str) -> Iterable[tuple[list[str], set[str]]]:
    """Yields `(avoided dependencies, core modules)` candidates for a package."""
    reach = module_reach(model, pkg_id)
    lib = {p for p in model.by_package[pkg_id] if model.modules[p].role == "lib"}
    if not lib:
        return
    seen: set[frozenset[str]] = set()

    def emit(avoided: set[str]) -> Iterable[tuple[list[str], set[str]]]:
        core = frozenset(p for p in lib if not (reach[p] & avoided))
        if not core or core == lib or core in seen:
            return
        seen.add(core)
        yield sorted(avoided), set(core)

    used = set().union(*(reach[p] for p in lib))
    for dep in sorted(used):
        yield from emit({dep})
    # The cut that serves one consumer exactly: avoid everything its used
    # modules do not need.
    own = model.by_package[pkg_id]
    for c in sorted(model.consumers(pkg_id)):
        used_mods = model.references(c, own) & lib
        if not used_mods:
            continue
        need = set().union(*(reach[p] for p in used_mods))
        if used - need:
            yield from emit(used - need)


def evaluate(
    model: Model, before: Metrics, pkg_id: str, avoided: list[str], core: set[str]
) -> Cut:
    consumers = model.consumers_using_only(pkg_id, core)
    own = set(model.by_package[pkg_id])
    lib = {p for p in own if model.modules[p].role == "lib"}
    snap = model.snapshot()
    model.split(pkg_id, core)
    after = metrics(model)
    model.restore(snap)
    return Cut(
        package=pkg_id,
        avoided=avoided,
        core=sorted(core),
        rest_lib=sorted(lib - core),
        core_loc=sum(model.modules[p].loc for p in core),
        rest_loc=sum(model.modules[p].loc for p in own - core),
        consumers=sorted(consumers),
        delta=before.minus(after),
    )


@dataclass
class EdgeCut:
    package: str
    dependency: str
    files: list[str]
    # Items of `package` that reference `dependency`, as `file: descriptor`.
    referrers: list[str]
    # Items of `dependency` that `package` references.
    symbols: list[str]
    delta: Metrics


def edge_cuts(model: Model) -> list[EdgeCut]:
    """Workspace edges with the closure reduction of removing them.

    Edges to procedural macro crates are left out: removing one means not
    using the macro, which no reference list helps with.
    """
    before = metrics(model)
    out = []
    for pkg_id in model.workspace():
        pkg = model.packages[pkg_id]
        for dep in sorted(pkg.deps):
            dep_pkg = model.packages[dep]
            if not dep_pkg.is_workspace or dep_pkg.is_proc_macro:
                continue
            files: list[str] = []
            symbols: set[str] = set()
            referrers: set[str] = set()
            for path in sorted(model.by_package[pkg_id]):
                m = model.modules[path]
                hit = False
                for target, syms in m.symbols.items():
                    if model.modules[target].package == dep:
                        hit = True
                        symbols |= syms
                        referrers |= {f"{path}: {r}" for r in m.referrers[target]}
                if hit:
                    files.append(path)
            pkg.deps.discard(dep)
            model.invalidate()
            after = metrics(model)
            pkg.deps.add(dep)
            model.invalidate()
            out.append(
                EdgeCut(
                    pkg_id,
                    dep,
                    files,
                    sorted(referrers),
                    sorted(symbols),
                    before.minus(after),
                )
            )
    return out


def plan(model: Model, rounds: int, objective: str) -> tuple[list[Cut], list[Cut]]:
    """Greedily applies the best split `rounds` times.

    Returns the ranking of all first-round candidates and the applied cuts.
    """
    ranking: list[Cut] = []
    applied: list[Cut] = []
    for r in range(rounds):
        before = metrics(model)
        cuts = []
        for pkg_id in model.workspace():
            for avoided, core in candidate_cuts(model, pkg_id):
                cuts.append(evaluate(model, before, pkg_id, avoided, core))
        cuts.sort(key=lambda c: (tuple(-k for k in c.delta.key(objective)), c.package))
        if r == 0:
            ranking = cuts
        # A split only pays for itself if the objective strictly improves.
        if not cuts or cuts[0].delta.key(objective)[0] <= 0:
            break
        best = cuts[0]
        ui.say(
            f"round {r + 1}: split {model.packages[best.package].name}, saves "
            f"{best.delta.workspace_pairs} workspace pairs, "
            f"{best.delta.rebuild_lines} rebuild lines"
        )
        model.split(best.package, set(best.core))
        applied.append(best)
    return ranking, applied


def unreferenced_deps(model: Model) -> list[tuple[str, str]]:
    """Declared dependencies of workspace packages that no module references."""
    out = []
    for pkg_id in model.workspace():
        needed: set[str] = set()
        for path in model.by_package[pkg_id]:
            needed |= model.module_needs(path)
        pkg = model.packages[pkg_id]
        for dep in sorted(pkg.deps - pkg.build_deps - needed):
            out.append((pkg_id, dep))
    return out


def run_index(root: Path, rust_analyzer: str, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    config = out.parent / "rust-analyzer.json"
    config.write_text(json.dumps(RUST_ANALYZER_CONFIG))
    log = out.parent / "rust-analyzer.log"
    ui.say(f"indexing {root} with {rust_analyzer}, logging to {log}")
    ui.say("this takes several minutes and several GiB of memory")
    with log.open("w") as fh:
        subprocess.run(
            [
                rust_analyzer,
                "scip",
                str(root),
                "--output",
                str(out),
                "--config-path",
                str(config),
                "--exclude-vendored-libraries",
            ],
            env={**os.environ, "CARGO_INCREMENTAL": "0"},
            stdout=fh,
            stderr=subprocess.STDOUT,
            check=True,
        )


def find_rust_analyzer() -> str:
    found = shutil.which("rust-analyzer")
    if found:
        try:
            subprocess.run([found, "--version"], check=True, capture_output=True)
            return found
        except subprocess.CalledProcessError:
            pass
    raise SystemExit(
        "rust-analyzer not found. Install it with "
        "`rustup component add rust-analyzer` or pass --rust-analyzer."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="crate-cuts",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=MZ_ROOT / "target" / "crate-cuts",
        help="directory for the index and reports",
    )
    parser.add_argument("--index", type=Path, help="use an existing SCIP index")
    parser.add_argument("--reindex", action="store_true", help="rebuild the SCIP index")
    parser.add_argument("--rust-analyzer", help="rust-analyzer binary to use")
    parser.add_argument(
        "--rounds", type=int, default=10, help="greedy split rounds to plan"
    )
    parser.add_argument(
        "--objective",
        choices=["pairs", "lines"],
        default="pairs",
        help="rank by workspace pairs or by rebuild lines",
    )
    parser.add_argument(
        "--top", type=int, default=40, help="entries per report section"
    )
    parser.add_argument(
        "--annotate",
        action="store_true",
        help="post a summary as a Buildkite annotation",
    )
    args = parser.parse_args()

    index = args.index or args.out_dir / "index.scip"
    if args.index is None and (args.reindex or not index.exists()):
        run_index(MZ_ROOT, args.rust_analyzer or find_rust_analyzer(), index)

    packages = load_packages(MZ_ROOT)
    modules = load_modules(MZ_ROOT, index, packages)
    model = Model(packages, modules)
    baseline = metrics(model)
    unreferenced = unreferenced_deps(model)
    edges = edge_cuts(model)
    graph = crate_graph(model)
    ranking, applied = plan(model, args.rounds, args.objective)
    final = metrics(model)

    report = render(
        packages,
        baseline,
        final,
        unreferenced,
        edges,
        ranking,
        applied,
        args.top,
        args.objective,
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "report.md").write_text(report)
    (args.out_dir / "report.json").write_text(
        json.dumps(
            {
                "baseline": asdict(baseline),
                "planned": asdict(final),
                "applied": [cut_json(c, packages) for c in applied],
                "candidates": [cut_json(c, packages) for c in ranking],
                "edges": [
                    {
                        "crate": packages[e.package].name,
                        "dependency": packages[e.dependency].name,
                        "delta": asdict(e.delta),
                        "files": e.files,
                        "referrers": e.referrers,
                        "symbols": e.symbols,
                    }
                    for e in edges
                ],
                "unreferenced": [
                    [packages[a].name, packages[b].name] for a, b in unreferenced
                ],
            },
            indent=1,
        )
    )
    (args.out_dir / "modules.json").write_text(
        json.dumps(
            {
                p: {
                    "package": (
                        packages[m.package].name if m.package in packages else m.package
                    ),
                    "role": m.role,
                    "loc": m.loc,
                    "refs": dict(m.refs),
                    "external": {packages[e].name: n for e, n in m.external.items()},
                }
                for p, m in sorted(modules.items())
            },
            indent=1,
        )
    )
    (args.out_dir / "crates.dot").write_text(graph)
    if args.annotate:
        summary = render(
            packages,
            baseline,
            final,
            unreferenced,
            edges,
            ranking,
            applied,
            10,
            args.objective,
        )
        buildkite.add_annotation(
            "info",
            f"Crate cuts: {baseline.workspace_pairs} workspace pairs, "
            f"{len(applied)} planned splits save "
            f"{baseline.workspace_pairs - final.workspace_pairs}",
            summary,
            context="crate-cuts",
        )
    ui.say(
        f"wrote report.md, report.json, modules.json, and crates.dot to {args.out_dir}"
    )


def crate_graph(model: Model) -> str:
    """Renders workspace dependency edges as Graphviz, before any split.

    Edges are labeled with the number of distinct referenced items. Edges
    without references are dashed.
    """
    lines = ["digraph crates {", "  rankdir=LR;", "  node [shape=box];"]
    for pkg_id in model.workspace():
        pkg = model.packages[pkg_id]
        counts: Counter[str] = Counter()
        for path in model.by_package[pkg_id]:
            m = model.modules[path]
            for target, syms in m.symbols.items():
                counts[model.modules[target].package] += len(syms)
        for dep in sorted(pkg.deps):
            if not model.packages[dep].is_workspace:
                continue
            n = counts.get(dep, 0)
            style = f'label="{n}"' if n else "style=dashed"
            lines.append(f'  "{pkg.name}" -> "{model.packages[dep].name}" [{style}];')
    lines.append("}")
    return "\n".join(lines) + "\n"


def cut_json(c: Cut, packages: dict[str, Package]) -> dict:
    def name(p: str) -> str:
        return packages[p].name if p in packages else p

    return {
        "crate": name(c.package),
        "avoided": [name(a) for a in c.avoided],
        "core": c.core,
        "rest": c.rest_lib,
        "core_lines": c.core_loc,
        "rest_lines": c.rest_loc,
        "consumers": [name(x) for x in c.consumers],
        "delta": asdict(c.delta),
    }


def render(
    packages: dict[str, Package],
    baseline: Metrics,
    final: Metrics,
    unreferenced: list[tuple[str, str]],
    edges: list[EdgeCut],
    ranking: list[Cut],
    applied: list[Cut],
    top: int,
    objective: str,
) -> str:
    def name(pkg_id: str) -> str:
        return packages[pkg_id].name if pkg_id in packages else pkg_id

    def names(ids: Iterable[str], limit: int = 8) -> str:
        ids = list(ids)
        shown = ", ".join(f"`{name(i)}`" for i in ids[:limit])
        return shown + (f" and {len(ids) - limit} more" if len(ids) > limit else "")

    def paths(ps: list[str], limit: int = 6) -> str:
        shown = ", ".join(f"`{p}`" for p in ps[:limit])
        return shown + (f" and {len(ps) - limit} more" if len(ps) > limit else "")

    def fmt(m: Metrics) -> str:
        return (
            f"{m.workspace_pairs} workspace pairs, {m.rebuild_lines} rebuild "
            f"lines, {m.all_pairs} all pairs"
        )

    lines = [
        "# Crate cuts",
        "",
        "Generated by `bin/crate-cuts`.",
        "Workspace pairs count the pairs of workspace crates where the first",
        "transitively depends on the second, which is the number of crate",
        "rebuilds when editing every workspace crate once.",
        "Rebuild lines weight each pair by the lines of the rebuilt crate.",
        "All pairs also count external dependencies.",
        f"Splits are ranked by {'rebuild lines' if objective == 'lines' else 'workspace pairs'}.",
        "",
        f"* Baseline: {fmt(baseline)}.",
        f"* After the planned splits: {fmt(final)}.",
        "",
        "## Planned splits",
        "",
        "Each round applies the best split and re-ranks, so later rounds can",
        "split a crate created by an earlier round.",
        "",
    ]
    for i, c in enumerate(applied, 1):
        lines += render_cut(i, c, name, names, paths)
    lines += [
        "## Split candidates",
        "",
        "The best first-round split per crate, evaluated independently",
        "against the baseline.",
        "",
    ]
    best_per_crate: dict[str, Cut] = {}
    for c in ranking:
        best_per_crate.setdefault(c.package, c)
    shown = [c for c in best_per_crate.values() if c.delta.key(objective)[0] > 0]
    for i, c in enumerate(shown[:top], 1):
        lines += render_cut(i, c, name, names, paths)
    lines += [
        "## Edge cuts",
        "",
        "Workspace dependency edges whose removal shrinks the closure, ranked",
        "by workspace pairs saved per referenced item. Removing an edge means",
        "moving the referencing items, or the referenced items, across the",
        "crate boundary.",
        "",
    ]
    edges = sorted(
        (e for e in edges if e.delta.workspace_pairs > 0 and e.symbols),
        key=lambda e: (
            -e.delta.workspace_pairs / len(e.symbols),
            e.package,
            e.dependency,
        ),
    )

    def listing(items: list[str], limit: int = 6) -> str:
        shown = ", ".join(f"`{i}`" for i in items[:limit])
        return shown + (f" and {len(items) - limit} more" if len(items) > limit else "")

    for i, e in enumerate(edges[:top], 1):
        lines += [
            f"### {i}. `{name(e.package)}` to `{name(e.dependency)}`",
            "",
            f"* Saves {e.delta.workspace_pairs} workspace pairs, "
            f"{e.delta.rebuild_lines} rebuild lines, {e.delta.all_pairs} all pairs.",
            f"* Referencing items: {listing(e.referrers) or 'none located'}.",
            f"* Referenced items: {listing(e.symbols)}.",
            "",
        ]
    lines += [
        "## Unreferenced dependencies",
        "",
        "Declared normal or build dependencies that no library or binary",
        "module references under default features. Some are needed for",
        "linking, feature unification, or macro expansion.",
        "",
    ]
    grouped: dict[str, list[str]] = defaultdict(list)
    for pkg, dep in unreferenced:
        grouped[pkg].append(dep)
    for pkg in sorted(grouped, key=name):
        lines.append(f"* `{name(pkg)}`: {names(sorted(grouped[pkg], key=name), 50)}")
    lines.append("")
    return "\n".join(lines)


def render_cut(i, c: Cut, name, names, paths) -> list[str]:
    return [
        f"### {i}. `{name(c.package)}` without {names(c.avoided)}",
        "",
        f"* Saves {c.delta.workspace_pairs} workspace pairs, "
        f"{c.delta.rebuild_lines} rebuild lines, {c.delta.all_pairs} all pairs.",
        f"* Core: {len(c.core)} modules, {c.core_loc} lines: {paths(c.core)}.",
        f"* Rest: {len(c.rest_lib)} library modules, {c.rest_loc} lines: "
        f"{paths(c.rest_lib)}.",
        f"* Consumers that only need core: {names(c.consumers) or 'none'}.",
        "",
    ]


if __name__ == "__main__":
    sys.exit(main())
