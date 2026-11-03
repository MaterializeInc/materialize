# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The current assignment of modules to crates and what it implies."""

from collections import defaultdict
from dataclasses import dataclass, field

from materialize.crate_cuts.facts import Facts


@dataclass
class Crate:
    """A package of the current assignment, possibly split off another one."""

    id: str
    name: str
    is_workspace: bool
    is_proc_macro: bool
    # The package of the facts this crate was split off from, or its own id.
    origin: str
    deps: set[str] = field(default_factory=set)
    build_deps: set[str] = field(default_factory=set)


class Model:
    """The assignment of modules to crates and the crate dependency edges.

    Starts from the packages of the facts. Splits add crates and move
    modules. The facts themselves are never modified.
    """

    def __init__(self, facts: Facts):
        self.facts = facts
        self.modules = facts.modules
        self.crates = {
            p.id: Crate(
                id=p.id,
                name=p.name,
                is_workspace=p.is_workspace,
                is_proc_macro=p.is_proc_macro,
                origin=p.id,
                deps=set(p.deps),
                build_deps=set(p.build_deps),
            )
            for p in facts.packages.values()
        }
        workspace = {p for p, c in self.crates.items() if c.is_workspace}
        for c in self.crates.values():
            if not c.is_workspace and c.deps & workspace:
                # Closures of external crates are cached across splits.
                raise RuntimeError(
                    f"external package {c.name} depends on the workspace"
                )
        self.owner = {path: m.package for path, m in facts.modules.items()}
        self.by_crate: dict[str, set[str]] = defaultdict(set)
        for path, owner in self.owner.items():
            self.by_crate[owner].add(path)
        self.bit: dict[str, int] = {}
        for p in sorted(self.crates):
            self._register(p)
        # Closures as bitsets over `bit`. External packages never depend on
        # workspace packages, so their entries survive changes to the
        # workspace graph.
        self._closure: dict[str, int] = {}

    def _register(self, crate_id: str) -> None:
        self.bit[crate_id] = 1 << len(self.bit)

    def workspace(self) -> list[str]:
        return sorted(c.id for c in self.crates.values() if c.is_workspace)

    def workspace_mask(self) -> int:
        mask = 0
        for p in self.workspace():
            mask |= self.bit[p]
        return mask

    def name(self, crate_id: str) -> str:
        return self.crates[crate_id].name

    def invalidate(self) -> None:
        for p in self.workspace():
            self._closure.pop(p, None)

    def closure(self, crate_id: str) -> int:
        """Transitive dependencies of `crate_id` as a bitset, excluding itself."""
        hit = self._closure.get(crate_id)
        if hit is not None:
            return hit
        # Iterative post-order. Normal and build dependencies form a DAG.
        stack = [(crate_id, False)]
        while stack:
            p, done = stack.pop()
            if p in self._closure:
                continue
            deps = self.crates[p].deps
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
        return self._closure[crate_id]

    def reaches(self, crate_id: str, target: str) -> bool:
        return bool(self.closure(crate_id) & self.bit[target])

    def module_needs(self, path: str) -> set[str]:
        """Crates a module needs as direct dependencies of its crate."""
        m = self.modules[path]
        crate = self.crates[self.owner[path]]
        needed = {self.owner[t] for t in m.refs} | set(m.external)
        origins = {self.crates[n].origin for n in needed}
        # Without an item reference into a split package, the path could
        # name either part. `rest` keeps the original id and is conservative.
        needed |= m.named - origins
        needed.discard(crate.id)
        direct = needed & crate.deps
        # A reference can resolve to a crate that is not a direct
        # dependency, for example through a re-export. Attribute it to a
        # direct dependency that is already needed and reaches it, else to
        # the reaching dependency with the smallest closure.
        covered = 0
        for d in direct:
            covered |= self.closure(d)
        for target in needed - crate.deps:
            if covered & self.bit[target]:
                continue
            providers = [d for d in crate.deps if self.reaches(d, target)]
            if providers:
                direct.add(
                    min(providers, key=lambda d: (self.closure(d).bit_count(), d))
                )
        return direct

    def split(self, crate_id: str, core: set[str]) -> str:
        """Moves `core` modules of `crate_id` into a new crate and rewires.

        Returns the new crate id. Consumers that reference only `core`
        modules depend on the new crate instead of `crate_id`. Consumers
        that reference both parts depend on both. Consumers that reference
        neither keep their declared dependency on `crate_id`.
        """
        crate = self.crates[crate_id]
        consumers = self.consumers(crate_id)
        own = self.by_crate[crate_id]
        used = {c: self.references(c, own) for c in consumers}
        n = sum(1 for p in self.crates if p.startswith(crate_id + "+core"))
        core_id = f"{crate_id}+core{n}"
        core_deps = set(crate.build_deps)
        for path in core:
            core_deps |= self.module_needs(path)
        self.crates[core_id] = Crate(
            id=core_id,
            name=f"{crate.name}-core" + (str(n) if n else ""),
            is_workspace=True,
            is_proc_macro=False,
            origin=crate.origin,
            deps=core_deps,
            build_deps=set(crate.build_deps),
        )
        self._register(core_id)
        for path in core:
            self.owner[path] = core_id
        self.by_crate[core_id] = set(core)
        self.by_crate[crate_id] = own - core
        crate.deps.add(core_id)
        for c in consumers:
            if not used[c] & core:
                continue
            self.crates[c].deps.add(core_id)
            if used[c] <= core:
                self.crates[c].deps.discard(crate_id)
        self.invalidate()
        return core_id

    def consumers(self, crate_id: str) -> list[str]:
        return [
            c.id
            for c in self.crates.values()
            if c.is_workspace and crate_id in c.deps and c.id != crate_id
        ]

    def references(self, consumer: str, targets: set[str]) -> set[str]:
        """Modules in `targets` referenced from modules of `consumer`."""
        out: set[str] = set()
        for path in self.by_crate[consumer]:
            out |= targets & self.modules[path].refs.keys()
        return out

    def consumers_using_only(self, crate_id: str, core: set[str]) -> list[str]:
        own = self.by_crate[crate_id]
        out = []
        for c in self.consumers(crate_id):
            used = self.references(c, own)
            if used and used <= core:
                out.append(c)
        return out

    def snapshot(self) -> tuple:
        return (
            {k: set(c.deps) for k, c in self.crates.items()},
            dict(self.owner),
            {k: set(v) for k, v in self.by_crate.items()},
            dict(self.bit),
        )

    def restore(self, snap: tuple) -> None:
        deps, owner, by_crate, bit = snap
        for k in list(self.crates):
            if k not in deps:
                del self.crates[k]
                self._closure.pop(k, None)
        for k, d in deps.items():
            self.crates[k].deps = d
        self.owner = dict(owner)
        self.by_crate.clear()
        self.by_crate.update(by_crate)
        self.bit = bit
        self.invalidate()


def module_reach(model: Model, pkg_id: str) -> dict[str, set[str]]:
    """Direct dependencies of `pkg_id` each of its modules needs transitively.

    Internal references and impl pins are followed, so a module's set
    includes the needs of every same-package module it requires.
    """
    own = model.by_crate[pkg_id]
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
    loc = {p: sum(model.modules[m].loc for m in model.by_crate[p]) for p in ws}
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


def unreferenced_deps(model: Model) -> list[tuple[str, str]]:
    """Declared dependencies of workspace packages that no module references."""
    out = []
    for pkg_id in model.workspace():
        needed: set[str] = set()
        for path in model.by_crate[pkg_id]:
            needed |= model.module_needs(path)
        pkg = model.crates[pkg_id]
        for dep in sorted(pkg.deps - pkg.build_deps - needed):
            out.append((pkg_id, dep))
    return out
