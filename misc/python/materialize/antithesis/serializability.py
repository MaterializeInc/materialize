# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Elle-style checker for histories of appends to, and reads of, several keys.

Pure Python without the Antithesis SDK, so it is unit tested directly
(`test_serializability.py`). The client history driver
(`drivers/history.py`) records the history and turns a `Report` into
assertions.

Model. Every key is an append-only set of unique values. A write transaction
appends one value, its own op id, to one key: a blind append, or a
read-modify-write that also stores how many values its read key held when it
read (its count). A read observes the full set of one or more keys at one
timestamp. A row of a read-modify-write that a read returns carries that
count, which is how the checker learns what the read-modify-write read.

Version order. A table returns a set, not a list, so the order of each key's
values is inferred. The distinct sets that reads observed must form a chain
under inclusion (otherwise: incompatible orders). Values that first appear in
the same set of the chain form a layer whose internal order is unknown, which
is not an error, because group commit applies several appends at one
timestamp. Committed writes that no read observed form one more layer after
the last. Every edge below holds in every serial order that is consistent
with the observations, so a correct system yields an acyclic graph:

* ww: each writer of a layer precedes each writer of the next layer.
* wr: the writers of the last layer of an observed set precede its reader.
* rw: a reader precedes the writers of the layer after its set.
* realtime: between `strict serializable` operations, A precedes B if A
  completed before B was invoked.
* process: between consecutive operations of one `strong session
  serializable` session.

Writers of earlier layers and later layers are reached through the ww chain,
which keeps the graph near linear in the history. Realtime edges are a
transitive reduction of the realtime order, as in Elle: B gets an edge only
from operations that completed before B's invoke and are not already ordered
before another such operation.

Cycles are found per strongly connected component and classified by the
first of `ANOMALIES` whose edge constraint admits a cycle in the component,
so a component that has a cycle of data edges alone is never reported as a
realtime anomaly. G1b (reading an intermediate version) needs a transaction
that writes one key twice, which this workload never issues, so it is not
checked.

Indeterminate writes may or may not have committed. They join the graph only
when a read observed them, and never have an outgoing realtime edge, because
their commit may follow the error the client saw.
"""

from __future__ import annotations

import bisect
import enum
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

STRICT = "strict serializable"
SERIALIZABLE = "serializable"
STRONG_SESSION = "strong session serializable"

WW = 1
WR = 2
RW = 4
RT = 8
PROCESS = 16
DATA = WW | WR | RW
ALL = DATA | RT | PROCESS

# Label preference when an edge carries several kinds: a reported cycle shows
# as few rw edges as its class allows.
_LABELS = ((WW, "ww"), (WR, "wr"), (PROCESS, "process"), (RT, "realtime"), (RW, "rw"))

REACHABILITY_COMPONENTS_LIMIT = 20_000
"""Above this many components the G-single search, which keeps one bit set per
component, is skipped and the cycle is classified as G2: still reported, only
less precisely. Epoch-sized histories stay far below it."""
CYCLES_KEPT = 5
"""Cycles kept per anomaly class in a report; `Report.anomaly_counts` counts all."""
CYCLE_STEPS_SHOWN = 24
SAMPLE = 20


class Kind(enum.Enum):
    APPEND = "append"
    RMW = "rmw"
    READ = "read"


class Outcome(enum.Enum):
    OK = "ok"
    FAIL = "fail"
    """Definitely not applied."""
    INFO = "info"
    """Unknown: the connection broke, the statement timed out, or the client
    process died before recording the outcome."""


@dataclass(frozen=True)
class Anomaly:
    name: str
    allowed: int
    single_rw: bool
    """Exactly one rw edge, the rest from `allowed`."""
    description: str


ANOMALIES: tuple[Anomaly, ...] = (
    Anomaly("G0", WW, False, "cycle of ww edges"),
    Anomaly("G1c", WW | WR, False, "cycle of ww and wr edges"),
    Anomaly("G-single", WW | WR, True, "cycle with exactly one rw edge"),
    Anomaly("G2", DATA, False, "cycle with rw edges"),
    Anomaly("G0-realtime", WW | RT | PROCESS, False, "cycle of ww and real-time edges"),
    Anomaly(
        "G1c-realtime",
        WW | WR | RT | PROCESS,
        False,
        "cycle of ww, wr and real-time edges",
    ),
    Anomaly(
        "G-single-realtime",
        WW | WR | RT | PROCESS,
        True,
        "cycle with real-time edges and exactly one rw edge",
    ),
    Anomaly("G2-realtime", ALL, False, "cycle with real-time and rw edges"),
)


@dataclass(frozen=True)
class Observation:
    """What one read path returned for one key."""

    key: int
    path: str
    values: tuple[int, ...]
    """As returned, duplicates kept."""
    counts: tuple[tuple[int, int], ...] = ()
    """`(value, count)` for each returned row written by a read-modify-write."""


@dataclass(frozen=True)
class Op:
    op_id: int
    kind: Kind
    outcome: Outcome
    invoke: float
    complete: float | None = None
    """Ignored unless the outcome is `OK`."""
    session: str = ""
    isolation: str = STRICT
    write_key: int | None = None
    read_key: int | None = None
    """The key a read-modify-write counted."""
    observations: tuple[Observation, ...] = ()
    ts: int | None = None
    """`mz_now()` of a read."""
    uptime_s: float | None = None
    endpoint: str = "service"
    """`service`, or `<role>:<generation>` for a session on one environmentd pod."""

    @property
    def is_write(self) -> bool:
        return self.kind is not Kind.READ

    @property
    def acked(self) -> bool:
        return self.outcome is Outcome.OK and self.complete is not None


@dataclass
class Stats:
    ops: int = 0
    graph_nodes: int = 0
    graph_edges: int = 0
    multi_key_strict_reads: int = 0
    paths: set[str] = field(default_factory=set)
    non_active_ops: int = 0
    """Graph nodes issued on a pod of a generation that was not active."""
    rmw_reads_placed: int = 0
    indeterminate_observed: int = 0
    strict_reads_after_ack: int = 0
    min_ack_gap_s: float | None = None
    """Smallest gap between a write's acknowledgement and the invoke of a
    strict read that was checked against it."""
    inclusion_checks_nonempty: int = 0


@dataclass
class Report:
    phantom_reads: list[dict[str, Any]] = field(default_factory=list)
    """Values no attempted write could have produced by the time the read
    completed: unknown, written to another key, or invoked after the read."""
    aborted_reads: list[dict[str, Any]] = field(default_factory=list)
    """G1a: values of writes that failed with a definite error."""
    duplicates: list[dict[str, Any]] = field(default_factory=list)
    internal: list[dict[str, Any]] = field(default_factory=list)
    """One operation saw two different sets for a key, or two counts for one
    read-modify-write."""
    incompatible_orders: list[dict[str, Any]] = field(default_factory=list)
    rmw_self_reads: list[dict[str, Any]] = field(default_factory=list)
    stale_reads: list[dict[str, Any]] = field(default_factory=list)
    lost_writes: list[dict[str, Any]] = field(default_factory=list)
    timestamp_regressions: list[dict[str, Any]] = field(default_factory=list)
    indeterminate_regressions: list[dict[str, Any]] = field(default_factory=list)
    same_timestamp_mismatches: list[dict[str, Any]] = field(default_factory=list)
    cycles: dict[str, list[dict[str, Any]]] = field(
        default_factory=lambda: {a.name: [] for a in ANOMALIES}
    )
    anomaly_counts: dict[str, int] = field(
        default_factory=lambda: {a.name: 0 for a in ANOMALIES}
    )
    stats: Stats = field(default_factory=Stats)


def _sample(values: Iterable[int]) -> list[int]:
    return sorted(values)[:SAMPLE]


Graph = dict[int, dict[int, int]]


def strongly_connected_components(
    nodes: Iterable[int], successors: Callable[[int], Iterable[int]]
) -> list[list[int]]:
    """Tarjan's algorithm, iterative so deep graphs do not hit the recursion limit."""
    index: dict[int, int] = {}
    low: dict[int, int] = {}
    on_stack: set[int] = set()
    stack: list[int] = []
    components: list[list[int]] = []
    counter = 0
    for root in nodes:
        if root in index:
            continue
        index[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        work = [(root, iter(successors(root)))]
        while work:
            v, it = work[-1]
            descended = False
            for w in it:
                if w not in index:
                    index[w] = low[w] = counter
                    counter += 1
                    stack.append(w)
                    on_stack.add(w)
                    work.append((w, iter(successors(w))))
                    descended = True
                    break
                if w in on_stack:
                    low[v] = min(low[v], index[w])
            if descended:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[v])
            if low[v] == index[v]:
                component = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    component.append(w)
                    if w == v:
                        break
                components.append(component)
    return components


def _path(
    graph: Graph, start: int, goal: int, members: set[int], allowed: int
) -> list[int] | None:
    """Shortest path `start ... goal` over `allowed` edges within `members`."""
    if start == goal:
        return [start]
    parent: dict[int, int] = {start: start}
    frontier = deque([start])
    while frontier:
        u = frontier.popleft()
        for v, mask in graph.get(u, {}).items():
            if not mask & allowed or v not in members or v in parent:
                continue
            parent[v] = u
            if v == goal:
                path = [v]
                while path[-1] != start:
                    path.append(parent[path[-1]])
                return path[::-1]
            frontier.append(v)
    return None


def _cycle_through(
    graph: Graph, start: int, members: set[int], allowed: int
) -> list[int] | None:
    """Shortest cycle through `start`, as nodes whose last closes to the first."""
    parent: dict[int, int] = {start: start}
    frontier = deque([start])
    while frontier:
        u = frontier.popleft()
        for v, mask in graph.get(u, {}).items():
            if not mask & allowed or v not in members:
                continue
            if v == start:
                path = [u]
                while path[-1] != start:
                    path.append(parent[path[-1]])
                return path[::-1]
            if v not in parent:
                parent[v] = u
                frontier.append(v)
    return None


def find_cycle(graph: Graph, members: set[int], allowed: int) -> list[int] | None:
    """A short cycle within `members` using only `allowed` edges, if any."""

    def successors(u: int) -> list[int]:
        return [v for v, m in graph.get(u, {}).items() if m & allowed and v in members]

    for component in strongly_connected_components(sorted(members), successors):
        if len(component) < 2:
            continue
        inner = set(component)
        best: list[int] | None = None
        # A few starting points usually find a shorter cycle for the report.
        for start in sorted(component)[:4]:
            cycle = _cycle_through(graph, start, inner, allowed)
            if cycle is not None and (best is None or len(cycle) < len(best)):
                best = cycle
        return best
    return None


def find_single_rw_cycle(
    graph: Graph, members: set[int], allowed: int
) -> tuple[list[int], int] | None:
    """A cycle with exactly one rw edge, the others from `allowed`.

    An rw edge `u -> v` closes such a cycle iff `v` reaches `u` over `allowed`
    edges. Reachability between the components of the `allowed` subgraph is
    computed once, as bit sets, so every rw edge is tested in constant time.
    Returns the nodes and the index of the step that is the rw edge.
    """

    def successors(u: int) -> list[int]:
        return [v for v, m in graph.get(u, {}).items() if m & allowed and v in members]

    components = strongly_connected_components(sorted(members), successors)
    if len(components) > REACHABILITY_COMPONENTS_LIMIT:
        return None
    component_of = {u: i for i, c in enumerate(components) for u in c}
    # Tarjan emits components in reverse topological order, so every
    # successor component is complete before its predecessors.
    reach: list[int] = []
    for i, component in enumerate(components):
        bits = 1 << i
        for u in component:
            for v in successors(u):
                j = component_of[v]
                if j != i:
                    bits |= reach[j]
        reach.append(bits)
    for u in sorted(members):
        for v, mask in sorted(graph.get(u, {}).items()):
            if not mask & RW or v not in members:
                continue
            if reach[component_of[v]] >> component_of[u] & 1:
                path = _path(graph, v, u, members, allowed)
                assert path is not None
                return [u, *path[:-1]], 0
    return None


def classify_component(
    graph: Graph, members: set[int]
) -> tuple[Anomaly, list[int], int | None] | None:
    """The first anomaly class in `ANOMALIES` with a cycle in `members`.

    Returns the class, the cycle, and the index of its single rw step for the
    G-single classes.
    """
    for anomaly in ANOMALIES:
        if anomaly.single_rw:
            found = find_single_rw_cycle(graph, members, anomaly.allowed)
            if found is not None:
                return anomaly, found[0], found[1]
        else:
            cycle = find_cycle(graph, members, anomaly.allowed)
            if cycle is not None:
                return anomaly, cycle, None
    return None


def realtime_edges(ops: Iterable[Op]) -> list[tuple[int, int]]:
    """A transitive reduction of the realtime order over `ops`.

    A precedes B when A is acknowledged and A completed strictly before B was
    invoked. Only acknowledged operations have a completion.
    """
    events: list[tuple[float, int, int]] = []
    for op in ops:
        events.append((op.invoke, 0, op.op_id))
        if op.acked:
            assert op.complete is not None
            events.append((op.complete, 1, op.op_id))
    # Invokes sort before completions at the same instant, so equal instants
    # never order two operations.
    events.sort()
    frontier: set[int] = set()
    preds: dict[int, frozenset[int]] = {}
    edges: list[tuple[int, int]] = []
    for _, kind, op_id in events:
        if kind == 0:
            preds[op_id] = frozenset(frontier)
            edges.extend((p, op_id) for p in sorted(frontier))
        else:
            frontier -= preds.get(op_id, frozenset())
            frontier.add(op_id)
    return edges


class _Analysis:
    def __init__(self, ops: Sequence[Op]) -> None:
        self.ops = ops
        self.by_id = {op.op_id: op for op in ops}
        self.writers = {op.op_id: op for op in ops if op.is_write}
        self.report = Report()
        self.report.stats.ops = len(ops)
        self.reads: dict[int, dict[int, frozenset[int]]] = {}
        """Read op id to the set it observed per key, valid values only."""
        self.rmw_counts: dict[int, int] = {}
        self.key_reads: dict[int, list[tuple[int, frozenset[int]]]] = defaultdict(list)
        self.bad_keys: set[int] = set()
        self.layers: dict[int, list[list[int]]] = {}
        """Per key `[L_1, ..., L_m, U]`: the layers, then the acknowledged
        writes no read observed."""
        self.cumulative: dict[int, list[int]] = {}
        """Per key, the size of each observed set of the chain, from 0."""
        self.set_index: dict[int, dict[frozenset[int], int]] = {}
        self.layer_of: dict[int, dict[int, int]] = {}
        self.graph: Graph = defaultdict(dict)
        self.nodes: set[int] = set()

    def run(self) -> Report:
        self._collect_reads()
        self._infer_orders()
        self._data_edges()
        self._rmw_edges()
        self._order_edges()
        self._cycles()
        self._stale_and_lost()
        self._timestamps()
        return self.report

    def _collect_reads(self) -> None:
        r = self.report
        for op in self.ops:
            if op.kind is not Kind.READ or not op.acked:
                continue
            assert op.complete is not None
            per_key: dict[int, frozenset[int]] = {}
            conflicted: set[int] = set()
            for obs in op.observations:
                repeated = [v for v, n in Counter(obs.values).items() if n > 1]
                if repeated:
                    r.duplicates.append(
                        {
                            "read": op.op_id,
                            "key": obs.key,
                            "path": obs.path,
                            "values": _sample(repeated),
                        }
                    )
                kept: set[int] = set()
                for v in set(obs.values):
                    w = self.writers.get(v)
                    if w is None or w.write_key != obs.key or w.invoke >= op.complete:
                        r.phantom_reads.append(
                            {
                                "read": op.op_id,
                                "key": obs.key,
                                "path": obs.path,
                                "value": v,
                                "writer_key": None if w is None else w.write_key,
                                "writer_invoke": None if w is None else w.invoke,
                                "read_complete": op.complete,
                            }
                        )
                    elif w.outcome is Outcome.FAIL:
                        r.aborted_reads.append(
                            {
                                "read": op.op_id,
                                "key": obs.key,
                                "path": obs.path,
                                "value": v,
                                "writer_endpoint": w.endpoint,
                                "read_endpoint": op.endpoint,
                            }
                        )
                    else:
                        kept.add(v)
                for v, count in obs.counts:
                    w = self.writers.get(v)
                    if w is None or w.kind is not Kind.RMW or v not in kept:
                        continue
                    prior = self.rmw_counts.setdefault(v, count)
                    if prior != count:
                        r.internal.append(
                            {
                                "read": op.op_id,
                                "rmw": v,
                                "counts": [prior, count],
                            }
                        )
                observed = frozenset(kept)
                if obs.key in per_key and per_key[obs.key] != observed:
                    r.internal.append(
                        {
                            "read": op.op_id,
                            "key": obs.key,
                            "paths": [
                                o.path for o in op.observations if o.key == obs.key
                            ],
                            "only_first": _sample(per_key[obs.key] - observed),
                            "only_second": _sample(observed - per_key[obs.key]),
                        }
                    )
                    conflicted.add(obs.key)
                per_key.setdefault(obs.key, observed)
            for k in conflicted:
                del per_key[k]
            if per_key:
                self.reads[op.op_id] = per_key
                for k, s in per_key.items():
                    self.key_reads[k].append((op.op_id, s))

    def _infer_orders(self) -> None:
        keys = set(self.key_reads) | {
            w.write_key for w in self.writers.values() if w.write_key is not None
        }
        for k in sorted(keys):
            entries = self.key_reads.get(k, [])
            readers: dict[frozenset[int], int] = {}
            for read_id, s in entries:
                readers.setdefault(s, read_id)
            chain = sorted(readers, key=len)
            incompatible = False
            for a, b in zip(chain, chain[1:]):
                if not a <= b:
                    self.report.incompatible_orders.append(
                        {
                            "key": k,
                            "read": readers[a],
                            "other_read": readers[b],
                            "only_in_read": _sample(a - b),
                            "only_in_other": _sample(b - a),
                        }
                    )
                    incompatible = True
                    break
            if incompatible:
                self.bad_keys.add(k)
                continue
            observed: frozenset[int] = chain[-1] if chain else frozenset()
            layers: list[list[int]] = []
            previous: frozenset[int] = frozenset()
            index = {frozenset(): 0}
            layer_of: dict[int, int] = {}
            for s in chain:
                if not s:
                    continue
                layer = sorted(s - previous)
                layers.append(layer)
                index[s] = len(layers)
                for v in layer:
                    layer_of[v] = len(layers)
                previous = s
            unobserved = sorted(
                w.op_id
                for w in self.writers.values()
                if w.write_key == k and w.acked and w.op_id not in observed
            )
            self.layers[k] = [*layers, unobserved]
            self.cumulative[k] = [0, *(len(s) for s in chain if s)]
            self.set_index[k] = index
            self.layer_of[k] = layer_of
            self.report.stats.indeterminate_observed += sum(
                1 for v in observed if self.writers[v].outcome is not Outcome.OK
            )

    def _edge(self, u: int, v: int, kind: int) -> None:
        if u == v:
            return
        self.graph[u][v] = self.graph[u].get(v, 0) | kind
        self.nodes.add(u)
        self.nodes.add(v)

    def _data_edges(self) -> None:
        for k, layers in self.layers.items():
            for earlier, later in zip(layers, layers[1:]):
                for a in earlier:
                    for b in later:
                        self._edge(a, b, WW)
            for layer in layers:
                self.nodes.update(layer)
            for read_id, s in self.key_reads.get(k, []):
                self.nodes.add(read_id)
                i = self.set_index[k][s]
                if i >= 1:
                    for w in layers[i - 1]:
                        self._edge(w, read_id, WR)
                if i < len(layers):
                    for w in layers[i]:
                        self._edge(read_id, w, RW)

    def _rmw_edges(self) -> None:
        for rmw_id, n in sorted(self.rmw_counts.items()):
            w = self.writers[rmw_id]
            k = w.read_key
            if k is None or k in self.bad_keys or k not in self.layers:
                continue
            layers = self.layers[k]
            cumulative = self.cumulative[k]
            m = len(cumulative) - 1
            i = bisect.bisect_right(cumulative, n) - 1
            exact = cumulative[i] == n
            self.report.stats.rmw_reads_placed += 1
            if i >= 1:
                for writer in layers[i - 1]:
                    self._edge(writer, rmw_id, WR)
            # An exact count read the observed set S_i, so it missed layer
            # i+1. A count strictly between S_i and S_i+1 read part of layer
            # i+1, so it certainly missed layer i+2. A count beyond every
            # observed set may include unobserved writes.
            j = i + 1 if exact else i + 2
            if (exact or i < m) and j <= m + 1:
                for writer in layers[j - 1]:
                    self._edge(rmw_id, writer, RW)
            if w.write_key == k:
                own = self.layer_of[k].get(rmw_id)
                if own is not None and n >= cumulative[own]:
                    self.report.rmw_self_reads.append(
                        {
                            "rmw": rmw_id,
                            "key": k,
                            "count": n,
                            "own_layer": own,
                            "set_size_with_own_value": cumulative[own],
                        }
                    )

    def _order_edges(self) -> None:
        sessions: dict[str, list[Op]] = defaultdict(list)
        strict: list[Op] = []
        for op_id in self.nodes:
            op = self.by_id[op_id]
            if op.isolation == STRONG_SESSION:
                sessions[op.session].append(op)
            elif op.isolation == STRICT:
                strict.append(op)
        for ops in sessions.values():
            ops.sort(key=lambda o: (o.invoke, o.op_id))
            for a, b in zip(ops, ops[1:]):
                if a.acked:
                    self._edge(a.op_id, b.op_id, PROCESS)
        for a, b in realtime_edges(sorted(strict, key=lambda o: o.op_id)):
            self._edge(a, b, RT)

    def _describe(
        self, cycle: list[int], allowed: int, rw_step: int | None
    ) -> dict[str, Any]:
        steps = []
        for i, u in enumerate(cycle):
            v = cycle[(i + 1) % len(cycle)]
            mask = self.graph[u][v] & (RW if i == rw_step else allowed)
            if rw_step is not None and i != rw_step:
                mask &= ~RW
            label = next(name for bit, name in _LABELS if mask & bit)
            op = self.by_id[u]
            key = None
            if label in ("ww", "wr"):
                key = op.write_key
            elif label == "rw":
                key = self.by_id[v].write_key
            step: dict[str, Any] = {
                "op": u,
                "kind": op.kind.value,
                "outcome": op.outcome.value,
                "isolation": op.isolation,
                "endpoint": op.endpoint,
                "invoke": round(op.invoke, 3),
                "complete": round(op.complete, 3) if op.complete is not None else None,
                "edge": label,
                "key": key,
                "to": v,
            }
            if op.ts is not None:
                step["ts"] = op.ts
            if u in self.reads:
                step["read_sizes"] = {
                    str(k): len(s) for k, s in sorted(self.reads[u].items())
                }
            steps.append(step)
        return {
            "length": len(cycle),
            "steps": steps[:CYCLE_STEPS_SHOWN],
            "endpoints": sorted({self.by_id[u].endpoint for u in cycle}),
        }

    def _cycles(self) -> None:
        stats = self.report.stats
        stats.graph_nodes = len(self.nodes)
        stats.graph_edges = sum(len(vs) for vs in self.graph.values())

        def successors(u: int) -> Iterable[int]:
            return self.graph.get(u, {}).keys()

        for component in strongly_connected_components(sorted(self.nodes), successors):
            if len(component) < 2:
                continue
            members = set(component)
            found = classify_component(self.graph, members)
            assert found is not None, "a strongly connected component has a cycle"
            anomaly, cycle, rw_step = found
            self.report.anomaly_counts[anomaly.name] += 1
            kept = self.report.cycles[anomaly.name]
            if len(kept) < CYCLES_KEPT:
                described = self._describe(cycle, anomaly.allowed, rw_step)
                described["component_size"] = len(members)
                kept.append(described)
        for op_id in self.nodes:
            op = self.by_id[op_id]
            if op.endpoint.startswith("other:"):
                stats.non_active_ops += 1
            if op_id not in self.reads:
                continue
            if op.isolation == STRICT and len(self.reads[op_id]) >= 2:
                stats.multi_key_strict_reads += 1
            stats.paths.update(obs.path for obs in op.observations)

    def _stale_and_lost(self) -> None:
        stats = self.report.stats
        acked: dict[int, list[tuple[float, int]]] = defaultdict(list)
        for w in self.writers.values():
            if w.acked and w.write_key is not None:
                assert w.complete is not None
                acked[w.write_key].append((w.complete, w.op_id))
        for writes in acked.values():
            writes.sort()
        latest: dict[int, tuple[Op, frozenset[int]]] = {}
        for read_id, per_key in sorted(self.reads.items()):
            op = self.by_id[read_id]
            if op.isolation != STRICT:
                continue
            for k, s in per_key.items():
                if k not in latest or latest[k][0].invoke < op.invoke:
                    latest[k] = (op, s)
                writes = acked.get(k, [])
                n = bisect.bisect_left(writes, (op.invoke, -1))
                if n == 0:
                    continue
                stats.strict_reads_after_ack += 1
                gap = op.invoke - writes[n - 1][0]
                if stats.min_ack_gap_s is None or gap < stats.min_ack_gap_s:
                    stats.min_ack_gap_s = gap
                missing = [w for _, w in writes[:n] if w not in s]
                if missing:
                    self.report.stale_reads.append(
                        {
                            "read": read_id,
                            "key": k,
                            "ts": op.ts,
                            "endpoint": op.endpoint,
                            "invoke": op.invoke,
                            "missing": _sample(missing),
                            "missing_endpoints": sorted(
                                {self.writers[w].endpoint for w in missing}
                            ),
                        }
                    )
        for k, (op, s) in sorted(latest.items()):
            lost = [
                w
                for complete, w in acked.get(k, [])
                if complete < op.invoke and w not in s
            ]
            if lost:
                self.report.lost_writes.append(
                    {
                        "key": k,
                        "latest_read": op.op_id,
                        "latest_read_endpoint": op.endpoint,
                        "lost": _sample(lost),
                        "lost_endpoints": sorted(
                            {self.writers[w].endpoint for w in lost}
                        ),
                    }
                )

    def _timestamps(self) -> None:
        r = self.report
        for k, entries in sorted(self.key_reads.items()):
            timed = sorted(
                (self.by_id[read_id].ts, read_id, s)
                for read_id, s in entries
                if self.by_id[read_id].ts is not None
            )
            for (ts_a, a, s_a), (ts_b, b, s_b) in zip(timed, timed[1:]):
                if ts_a == ts_b:
                    if s_a != s_b:
                        r.same_timestamp_mismatches.append(
                            {
                                "key": k,
                                "ts": ts_a,
                                "read": a,
                                "other": b,
                                "only_in_read": _sample(s_a - s_b),
                                "only_in_other": _sample(s_b - s_a),
                            }
                        )
                    continue
                if s_a:
                    r.stats.inclusion_checks_nonempty += 1
                gone = s_a - s_b
                indeterminate = {
                    v for v in gone if self.writers[v].outcome is not Outcome.OK
                }
                details = {
                    "key": k,
                    "earlier": a,
                    "later": b,
                    "earlier_ts": ts_a,
                    "later_ts": ts_b,
                }
                if gone - indeterminate:
                    r.timestamp_regressions.append(
                        {**details, "lost": _sample(gone - indeterminate)}
                    )
                if indeterminate:
                    r.indeterminate_regressions.append(
                        {**details, "lost": _sample(indeterminate)}
                    )


def check(ops: Sequence[Op]) -> Report:
    """Check one closed set of operations: every value a read returns must come
    from an operation in `ops`."""
    return _Analysis(ops).run()


@dataclass
class TimestampOrder:
    regressions: list[dict[str, Any]] = field(default_factory=list)
    checked: int = 0
    across_restart: int = 0
    """Checked reads whose environmentd started after the earlier read completed."""


def realtime_timestamp_order(
    reads: Sequence[Op], only: set[int] | None = None
) -> TimestampOrder:
    """Strict serializable read timestamps never go backwards in real time.

    Considers acknowledged strict reads with a timestamp, across sessions,
    endpoints, and epochs. Only reads in `only` are checked as the later read,
    when given.
    """
    result = TimestampOrder()
    strict: list[tuple[float, int, Op]] = sorted(
        (r.complete, r.op_id, r)
        for r in reads
        if r.isolation == STRICT
        and r.complete is not None
        and r.acked
        and r.ts is not None
    )
    completes = [complete for complete, _, _ in strict]
    # prefix_max[i]: the read with the largest timestamp among the first i+1.
    prefix_max: list[tuple[int, Op]] = []
    for _, _, r in strict:
        ts = r.ts or 0
        if not prefix_max or ts >= prefix_max[-1][0]:
            prefix_max.append((ts, r))
        else:
            prefix_max.append(prefix_max[-1])
    for _, _, r in strict:
        if only is not None and r.op_id not in only:
            continue
        n = bisect.bisect_left(completes, r.invoke)
        if n == 0:
            continue
        result.checked += 1
        top_ts, top = prefix_max[n - 1]
        ts = r.ts or 0
        if top_ts > ts:
            result.regressions.append(
                {
                    "read": r.op_id,
                    "ts": ts,
                    "endpoint": r.endpoint,
                    "earlier_read": top.op_id,
                    "earlier_ts": top_ts,
                    "earlier_endpoint": top.endpoint,
                    "same_session": top.session == r.session,
                }
            )
        latest_complete = completes[n - 1]
        if r.uptime_s is not None and r.uptime_s < r.invoke - latest_complete:
            result.across_restart += 1
    return result


def session_timestamp_order(
    reads: Sequence[Op], only: set[int] | None = None
) -> TimestampOrder:
    """Strong session serializable reads in one session never go backwards."""
    result = TimestampOrder()
    sessions: dict[str, list[Op]] = defaultdict(list)
    for r in reads:
        if r.isolation == STRONG_SESSION and r.acked and r.ts is not None:
            sessions[r.session].append(r)
    for session, ops in sorted(sessions.items()):
        ops.sort(key=lambda o: (o.invoke, o.op_id))
        for a, b in zip(ops, ops[1:]):
            if only is not None and b.op_id not in only:
                continue
            result.checked += 1
            assert a.ts is not None and b.ts is not None
            if a.ts > b.ts:
                result.regressions.append(
                    {
                        "session": session,
                        "earlier": a.op_id,
                        "later": b.op_id,
                        "earlier_ts": a.ts,
                        "later_ts": b.ts,
                    }
                )
    return result
