# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Ranks crate splits and edge cuts, and plans splits greedily."""

from collections.abc import Iterable
from dataclasses import dataclass

from materialize import ui
from materialize.crate_cuts.model import (
    Metrics,
    Model,
    metrics,
    module_reach,
    unreferenced_deps,
)


@dataclass
class Cut:
    crate: str
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
    lib = {p for p in model.by_crate[pkg_id] if model.modules[p].role == "lib"}
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
    own = model.by_crate[pkg_id]
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
    own = set(model.by_crate[pkg_id])
    lib = {p for p in own if model.modules[p].role == "lib"}
    snap = model.snapshot()
    model.split(pkg_id, core)
    after = metrics(model)
    model.restore(snap)
    return Cut(
        crate=pkg_id,
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
    crate: str
    dependency: str
    files: list[str]
    # Items of `crate` that reference `dependency`, as `file: descriptor`.
    referrers: list[str]
    # Items of `dependency` that `crate` references.
    symbols: list[str]
    delta: Metrics
    # False when `crate` is the only consumer of `dependency` and removing the
    # edge drops nothing but `dependency` itself from closures. The savings
    # then only materialize by absorbing `dependency` into `crate`, after
    # which edits to its code rebuild the same crates as before.
    decouples: bool


def edge_cuts(model: Model) -> list[EdgeCut]:
    """Workspace edges with the closure reduction of removing them.

    Edges to procedural macro crates are left out: removing one means not
    using the macro, which no reference list helps with.
    """
    before = metrics(model)
    workspace = model.workspace()
    closures = {x: model.closure(x) for x in workspace}
    out = []
    for pkg_id in workspace:
        pkg = model.crates[pkg_id]
        for dep in sorted(pkg.deps):
            dep_pkg = model.crates[dep]
            if not dep_pkg.is_workspace or dep_pkg.is_proc_macro:
                continue
            files: list[str] = []
            symbols: set[str] = set()
            referrers: set[str] = set()
            for path in sorted(model.by_crate[pkg_id]):
                m = model.modules[path]
                hit = False
                for target, syms in m.symbols.items():
                    if model.owner[target] == dep:
                        hit = True
                        symbols |= syms
                        referrers |= {f"{path}: {r}" for r in m.referrers[target]}
                if hit:
                    files.append(path)
            pkg.deps.discard(dep)
            model.invalidate()
            after = metrics(model)
            others = ~model.bit[dep]
            decouples = any(
                dep in model.crates[c].deps for c in workspace if c != pkg_id
            ) or any(closures[x] & ~model.closure(x) & others for x in workspace)
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
                    decouples,
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
        cuts.sort(key=lambda c: (tuple(-k for k in c.delta.key(objective)), c.crate))
        if r == 0:
            ranking = cuts
        # A split only pays for itself if the objective strictly improves.
        if not cuts or cuts[0].delta.key(objective)[0] <= 0:
            break
        best = cuts[0]
        ui.say(
            f"round {r + 1}: split {model.crates[best.crate].name}, saves "
            f"{best.delta.workspace_pairs} workspace pairs, "
            f"{best.delta.rebuild_lines} rebuild lines"
        )
        model.split(best.crate, set(best.core))
        applied.append(best)
    return ranking, applied


@dataclass
class Results:
    baseline: Metrics
    final: Metrics
    unreferenced: list[tuple[str, str]]
    edges: list[EdgeCut]
    ranking: list[Cut]
    applied: list[Cut]
    objective: str


def analyze(model: Model, rounds: int, objective: str) -> Results:
    """Computes all results. Applies the planned splits to `model`."""
    baseline = metrics(model)
    unreferenced = unreferenced_deps(model)
    edges = edge_cuts(model)
    ranking, applied = plan(model, rounds, objective)
    return Results(
        baseline, metrics(model), unreferenced, edges, ranking, applied, objective
    )
