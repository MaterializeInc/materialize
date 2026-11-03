# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Extracts packages and the module reference graph from Cargo and SCIP."""

import bisect
import json
import os
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from materialize import spawn, ui
from materialize.crate_cuts import scip
from materialize.crate_cuts.facts import Facts, Module, Package

# Packages whose symbols never imply a Cargo dependency.
SYSROOT_PACKAGES = {"std", "core", "alloc", "proc_macro", "test"}

# rust-analyzer configuration for indexing: library code as `cargo build`
# sees it. Test modules inside `src/` would otherwise attribute dev-dependency
# references to library modules.
RUST_ANALYZER_CONFIG = {"cfg": {"setTest": False}}


@dataclass
class CargoPackage:
    id: str
    name: str
    version: str
    manifest_dir: Path
    is_workspace: bool
    has_lib: bool
    is_proc_macro: bool = False
    # Normal and build dependencies, as package ids, for the host platform.
    deps: set[str] = field(default_factory=set)
    # The subset of `deps` that build scripts use.
    build_deps: set[str] = field(default_factory=set)


def extract(root: Path, index: Path) -> Facts:
    cargo = load_packages(root)
    modules = load_modules(root, index, cargo)
    packages = {
        p.id: Package(
            id=p.id,
            name=p.name,
            is_workspace=p.is_workspace,
            is_proc_macro=p.is_proc_macro,
            deps=frozenset(p.deps),
            build_deps=frozenset(p.build_deps),
        )
        for p in cargo.values()
    }
    return Facts(packages, modules)


def load_packages(root: Path) -> dict[str, CargoPackage]:
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
        packages[p["id"]] = CargoPackage(
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
    # Only resolved packages exist for the host platform.
    resolved = {n["id"] for n in meta["resolve"]["nodes"]}
    return {k: v for k, v in packages.items() if k in resolved}


def host_triple() -> str:
    for line in spawn.capture(["rustc", "-vV"]).splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ")
    raise RuntimeError("rustc -vV did not report a host triple")


def classify(rel: Path, pkg: CargoPackage) -> str | None:
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
    root: Path, index: Path, packages: dict[str, CargoPackage]
) -> dict[str, Module]:
    ws = sorted(
        (p for p in packages.values() if p.is_workspace),
        key=lambda p: len(p.manifest_dir.parts),
        reverse=True,
    )

    def owner(path: Path) -> CargoPackage | None:
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
