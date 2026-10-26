# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Bisection orchestration: scratch cluster, tree walk, verdicts, report."""

import contextlib
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pg8000.exceptions import Error as PgError

from materialize.mzbisect import catalog
from materialize.mzbisect.catalog import (
    RELATION_TYPES,
    IndexInfo,
    ObjectInfo,
    ReplicaInfo,
    UnresolvableObject,
)
from materialize.mzbisect.db import ConnParams, Db, error_message, ident, literal
from materialize.mzbisect.probes import (
    ProbeResult,
    Status,
    fingerprint_probe,
    potato_probe,
    scan_probe,
)


@dataclass(frozen=True)
class Options:
    scratch_size: str
    scratch_cluster: str | None
    keep: bool
    timeout_secs: int
    sample: int
    dry_run: bool
    skip: tuple[str, ...]
    # The fingerprint probe is the only place the tool runs queries on the
    # customer's own clusters. Disabling it confines all reads to the scratch
    # cluster, at the cost of not being able to tell a corrupt arrangement
    # from clean persisted data.
    fingerprint: bool
    # Probe only relations that hold state: tables, sources, materialized
    # views, indexed views, and the seed itself. Unindexed views are walked
    # through but not probed. They are stateless, so probing one just
    # recomputes its subtree from the same persisted inputs the durable
    # layer's probes already cover, which gets prohibitively repetitive in
    # deep view stacks.
    durable_only: bool


@dataclass
class Node:
    obj: ObjectInfo
    indexes: list[IndexInfo]
    children: list["Node"]
    # False for repeated occurrences of a DAG node: only the first occurrence
    # carries children and gets probed.
    first_visit: bool
    skipped: str | None = None
    probes: list[ProbeResult] = field(default_factory=list)

    @property
    def verdict(self) -> str:
        if self.skipped is not None:
            return "skipped"
        if any(p.status == Status.CORRUPT for p in self.probes):
            return "CORRUPT"
        if any(p.status == Status.FAILED for p in self.probes):
            return "unknown"
        return "clean"


def build_tree(
    db: Db,
    obj: ObjectInfo,
    seed_id: str,
    skip_ids: set[str],
    opts: Options,
    visited: dict[str, Node],
) -> Node:
    if obj.id in visited:
        canonical = visited[obj.id]
        return Node(
            obj=obj,
            indexes=canonical.indexes,
            children=[],
            first_visit=False,
        )

    node = Node(obj=obj, indexes=[], children=[], first_visit=True)
    visited[obj.id] = node

    # System objects and requested skips prune their whole subtree.
    if obj.is_system:
        node.skipped = "system object"
        return node
    if obj.id in skip_ids:
        node.skipped = "skipped on request"
        return node

    node.indexes = catalog.indexes_on(db, obj)
    if (
        opts.durable_only
        and obj.type == "view"
        and not node.indexes
        and obj.id != seed_id
    ):
        # Not probed, but its subtree still is.
        node.skipped = "unindexed view, durable-only mode"
    for dep in catalog.direct_dependencies(db, obj):
        node.children.append(build_tree(db, dep, seed_id, skip_ids, opts, visited))
    return node


@contextlib.contextmanager
def scratch_cluster(
    db: Db, seed: ObjectInfo, opts: Options
) -> Generator[str, None, None]:
    if opts.scratch_cluster is not None:
        rows = db.query(
            "SELECT name FROM mz_catalog.mz_clusters"
            f" WHERE name = {literal(opts.scratch_cluster)}"
        )
        if not rows:
            raise RuntimeError(f"cluster {opts.scratch_cluster!r} does not exist")
        print(f"reusing scratch cluster {opts.scratch_cluster} (will not drop it)")
        yield opts.scratch_cluster
        return

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    name = f"mzbisect_{seed.id}_{stamp}"
    print(
        f"creating scratch cluster {name} with an unbilled replica"
        f" (SIZE = '{opts.scratch_size}', BILLED AS 'free')"
    )
    # REPLICATION FACTOR 0 so the cluster itself provisions nothing billable.
    # The one replica that does the work is INTERNAL and BILLED AS 'free',
    # the billing exemption finance tracks for support work. Both options
    # require an internal user, hence the mz_system default.
    db.execute(
        f"CREATE CLUSTER {ident(name)}"
        f" (SIZE = {literal(opts.scratch_size)}, REPLICATION FACTOR = 0)"
    )
    db.execute(
        f"CREATE CLUSTER REPLICA {ident(name)}.{ident('_unbilled')}"
        f" (SIZE = {literal(opts.scratch_size)}, INTERNAL, BILLED AS 'free')"
    )
    # Cleanup below is best-effort: if this process dies hard (network drop,
    # teleport certificate expiry), give the operator the statement now.
    print(f"if this run dies before cleanup: DROP CLUSTER {ident(name)} CASCADE;")
    try:
        yield name
    finally:
        if opts.keep:
            print(f"keeping scratch cluster {name}, drop it when done:")
            print(f"    DROP CLUSTER {ident(name)} CASCADE;")
        else:
            print(f"dropping scratch cluster {name}")
            db.execute(f"DROP CLUSTER IF EXISTS {ident(name)} CASCADE")


def _probe_node(
    params: ConnParams,
    control: Db,
    probe_db: Db,
    node: Node,
    scratch: str,
    opts: Options,
    replica_cache: dict[str, list[ReplicaInfo]],
) -> None:
    scan = scan_probe(probe_db, node.obj)
    node.probes.append(scan)
    # If the relation cannot even be read from persist, the remaining probes
    # would fail with the same error, so don't bother.
    if scan.status != Status.OK:
        return

    # The remaining probes are all row-shaped (row(t.*)), which cannot be
    # written against a zero-column relation. The scan probe still covers
    # them: a negative total multiplicity shows up as a negative count.
    if not catalog.has_columns(control, node.obj):
        node.probes.append(
            ProbeResult(
                "potato/fingerprint",
                Status.INFO,
                "relation has no columns, the scan probe covers it",
            )
        )
        return

    node.probes.append(potato_probe(probe_db, node.obj, True, opts.sample))
    node.probes.append(potato_probe(probe_db, node.obj, False, opts.sample))

    if node.indexes and opts.fingerprint:
        home_targets: dict[str, list[ReplicaInfo]] = {}
        for index in node.indexes:
            if index.cluster_id not in replica_cache:
                replica_cache[index.cluster_id] = catalog.cluster_replicas(
                    control, index.cluster_id
                )
            home_targets[index.cluster_name] = replica_cache[index.cluster_id]
        node.probes.extend(
            fingerprint_probe(
                params,
                node.obj,
                home_targets,
                scratch,
                opts.timeout_secs,
            )
        )


def _walk(node: Node, depth: int = 0) -> Generator[tuple[Node, int], None, None]:
    yield node, depth
    for child in node.children:
        yield from _walk(child, depth + 1)


def _print_node_header(node: Node, depth: int) -> None:
    indent = "  " * depth
    suffix = ""
    if not node.first_visit:
        suffix = " (probed above)"
    elif node.skipped is not None:
        suffix = f" [{node.skipped}]"
    indexed = f", {len(node.indexes)} index(es)" if node.indexes else ""
    print(
        f"{indent}{node.obj.display} ({node.obj.id}, {node.obj.type}{indexed}){suffix}"
    )


def _classify(node: Node) -> str:
    """One-line diagnosis for a corrupt node."""
    # The `fingerprint` probes read through the scratch cluster, so a
    # corruption error raised by one of them is evidence about the persisted
    # data, exactly like scan and potato. Only the `arrangement` probes, which
    # compare a replica against that fresh read, implicate an arrangement.
    persist_bad = any(
        p.status == Status.CORRUPT
        and p.probe.startswith(("scan", "potato", "fingerprint"))
        for p in node.probes
    )
    bad_arrangements = [
        p.probe.removeprefix("arrangement ")
        for p in node.probes
        if p.status == Status.CORRUPT and p.probe.startswith("arrangement ")
    ]
    if persist_bad and node.obj.type == "view":
        return (
            "corruption reproduces on a fresh recompute from persist, so it "
            "comes from the view's inputs or deterministically from its own "
            "rendering, not from a stale arrangement"
        )
    if persist_bad:
        return (
            "corruption is in the persisted data itself, rehydration will not "
            "fix it, escalate with the persist/storage team"
        )
    if bad_arrangements:
        replicas = ", ".join(bad_arrangements)
        return (
            f"persisted data is clean but the arrangement diverges on {replicas}, "
            "recreating those replicas (rehydration) should clear it, keep one "
            "for debugging if possible"
        )
    return "corrupt (see probe details)"


def _control_connection(params: ConnParams) -> Db:
    """A connection for catalog reads, DDL, and timestamp selection.

    Pinned to mz_catalog_server, which always exists, so nothing here depends
    on the environment still having a cluster named like the session default.
    """
    db = Db(params)
    db.execute("SET cluster = mz_catalog_server")
    return db


def run_bisect(params: ConnParams, seed_spec: str, opts: Options) -> int:
    try:
        control = _control_connection(params)
    except PgError as e:
        print(
            f"error: cannot connect to {params.host}:{params.port}: {error_message(e)}"
        )
        return 2
    try:
        return _run_bisect(params, control, seed_spec, opts)
    except (PgError, RuntimeError) as e:
        print(f"error: {error_message(e)}")
        return 2
    finally:
        control.close()


def _run_bisect(params: ConnParams, control: Db, seed_spec: str, opts: Options) -> int:
    try:
        seed = catalog.resolve_object(control, seed_spec)
        resolved = catalog.resolve_index_target(control, seed)
        if resolved.id != seed.id:
            print(f"{seed.display} is an index, bisecting {resolved.display}")
        seed = resolved
        skip_ids = {catalog.resolve_object(control, s).id for s in opts.skip}
    except UnresolvableObject as e:
        print(f"error: {e}")
        return 2

    if seed.type not in RELATION_TYPES:
        print(f"error: {seed.display} is a {seed.type}, not a probeable relation")
        return 2

    visited: dict[str, Node] = {}
    root = build_tree(control, seed, seed.id, skip_ids, opts, visited)

    to_probe = [n for n, _ in _walk(root) if n.first_visit and n.skipped is None]
    indexed = sum(1 for n in to_probe if n.indexes)
    print(
        f"dependency closure of {seed.display}: {len(to_probe)} relation(s) "
        f"to probe, {indexed} of them indexed"
    )
    print()

    if opts.dry_run:
        for node, depth in _walk(root):
            _print_node_header(node, depth)
        return 0

    with scratch_cluster(control, seed, opts) as scratch:
        probe_db = Db(params)
        try:
            probe_db.execute(f"SET cluster = {literal(scratch)}")
            probe_db.execute(f"SET statement_timeout = '{opts.timeout_secs}s'")

            replica_cache: dict[str, list[ReplicaInfo]] = {}
            for node, depth in _walk(root):
                _print_node_header(node, depth)
                if not node.first_visit or node.skipped is not None:
                    continue
                _probe_node(
                    params, control, probe_db, node, scratch, opts, replica_cache
                )
                indent = "  " * depth + "    "
                for probe in node.probes:
                    print(
                        f"{indent}{probe.probe}: {probe.status.value}: {probe.detail}"
                    )
        finally:
            probe_db.close()

    return _summarize(visited)


def _summarize(visited: dict[str, Node]) -> int:
    corrupt = [n for n in visited.values() if n.verdict == "CORRUPT"]
    unknown = [n for n in visited.values() if n.verdict == "unknown"]

    print()
    print("summary")
    if not corrupt:
        print("  no corruption detected in the dependency closure")
        if unknown:
            names = ", ".join(n.obj.display for n in unknown)
            print(f"  but some probes failed, so no clean bill of health: {names}")
        return 0

    # Memoized per object because the closure is a DAG, not a tree: a shared
    # input reached by many paths would otherwise be re-walked once per path.
    reaches_corrupt: dict[str, bool] = {}

    def has_corrupt_descendant(node: Node) -> bool:
        cached = reaches_corrupt.get(node.obj.id)
        if cached is not None:
            return cached
        found = False
        for child in node.children:
            canonical = visited[child.obj.id]
            if canonical.verdict == "CORRUPT" or has_corrupt_descendant(canonical):
                found = True
                break
        reaches_corrupt[node.obj.id] = found
        return found

    for node in corrupt:
        print(f"  {node.obj.display} ({node.obj.id}): CORRUPT")
    print()
    introduced = [n for n in corrupt if not has_corrupt_descendant(n)]
    for node in introduced:
        print(f"  corruption is introduced at {node.obj.display} ({node.obj.id}):")
        print(f"    {_classify(node)}")
    if unknown:
        names = ", ".join(n.obj.display for n in unknown)
        print(f"  probes failed on: {names}, treat localization as provisional")
    return 1


def run_scan(params: ConnParams) -> int:
    try:
        control = _control_connection(params)
    except PgError as e:
        print(
            f"error: cannot connect to {params.host}:{params.port}: {error_message(e)}"
        )
        return 2
    try:
        candidates = catalog.compute_error_candidates(control)
    except PgError as e:
        print(f"error: {error_message(e)}")
        return 2
    finally:
        control.close()

    if not candidates:
        print("no persistent user dataflow currently reports a nonzero error count")
        print(
            "note: errors from one-off SELECTs (like a failed oneshot dataflow) do"
            " not show up here, seed `mzbisect run` from the objects that the"
            " failing query read instead"
        )
        return 0

    print("dataflows reporting errors (candidates, not proof of corruption):")
    print()
    targets: dict[str, str] = {}
    for c in candidates:
        object_name = c["object_name"] or "<dropped>"
        cluster = c["cluster_name"] or "<unknown cluster>"
        replica = c["replica_name"] or "<unknown replica>"
        print(
            f"  {object_name} ({c['object_id']}, {c['object_type']}) on"
            f" {cluster}/{replica}: {c['error_count']} error(s)"
        )
        # For an index, bisect the indexed relation instead.
        target = c["indexed_object_id"] or c["object_id"]
        if c["object_name"] is not None:
            targets[target] = object_name
    if targets:
        print()
        print("suggested next steps:")
        for target_id in sorted(targets):
            print(f"  mzbisect run {target_id}")
    return 0
