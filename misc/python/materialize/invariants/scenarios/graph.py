# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Transitive closure of a churning graph, maintained by a recursive dataflow.

Every other scenario reduces or joins its input once. This one iterates: a
WITH MUTUALLY RECURSIVE materialized view maintains the reachability closure
of an edge table that workers insert into and delete from continuously.

Deletions are the point. An insert only ever grows the closure, which an
iterative dataflow can get right by accumulating, while a delete has to
retract every path that went through the removed edge, including paths
discovered several iterations deep.

The invariants need no bookkeeping and hold at every timestamp, so they are
checkable while most op outcomes are unknown: the visible closure must be
transitively closed, must contain every visible edge, must contain no self
loop (edges only ever point from a lower node to a higher one, so no subset
of the operations can produce a cycle), and must equal the same reachability
question recomputed from scratch at the same timestamp.
"""

import random

from materialize.invariants.checkers import (
    GroupCompletenessPeek,
    ProgressPeek,
    ReplicaDivergence,
)
from materialize.invariants.framework import (
    CONVERGE_TIMEOUT,
    SEED_RANGE,
    Action,
    Checker,
    InvariantViolation,
    Outcome,
    Scenario,
    WorkerBundle,
    wait_until,
)
from materialize.invariants.mz import MzClient

# The final check runs on a quiesced system and legitimately scans everything
# the run wrote, which the per-query watchdog is not sized for: that watchdog
# exists to stop a checker hanging during chaos, and cancelling a final-check
# query instead reports a wedge that is really just a big honest query.
FINAL_TIMEOUT = 600

# A DAG whose edges only ever point from a lower node to a higher one, so the
# graph is acyclic no matter which inserts and deletes committed, and the
# closure of whatever edges are currently visible is the thing to check. Small
# enough that a dense closure stays bounded.
GRAPH_NODES = 40

# Reachability recomputed from scratch, for comparison against the maintained
# view. UNION rather than UNION ALL so the iteration converges.
GRAPH_REACH_CTE = (
    "WITH MUTUALLY RECURSIVE reach (src int, dst int) AS ("
    " SELECT src, dst FROM edges"
    " UNION"
    " SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src"
    ")"
)
GRAPH_CLOSURE_SQL = f"{GRAPH_REACH_CTE} SELECT src, dst FROM reach"

# Three properties of the closure of whatever edges are visible right now.
# None of them depend on which operations succeeded, and each fails for a
# different reason: a missing transitive pair means the iteration stopped
# early, a missing edge means the base case was lost, and a self-loop means a
# retraction left a cycle behind in a graph that cannot contain one.
GRAPH_NOT_CLOSED_SQL = (
    "SELECT a.src, a.dst, b.dst FROM closure a"
    " JOIN closure b ON a.dst = b.src"
    " LEFT JOIN closure c ON c.src = a.src AND c.dst = b.dst"
    " WHERE c.src IS NULL LIMIT 5"
)
GRAPH_MISSING_EDGE_SQL = (
    "SELECT e.src, e.dst FROM edges e"
    " LEFT JOIN closure c ON c.src = e.src AND c.dst = e.dst"
    " WHERE c.src IS NULL LIMIT 5"
)
GRAPH_SELF_LOOP_SQL = "SELECT src, dst FROM closure WHERE src = dst LIMIT 5"

# The maintained view against the same question recomputed in one shot, at one
# timestamp. This is the one that catches the incremental iteration diverging
# from the batch answer, which is what retractions through a recursive
# dataflow are most likely to get wrong.
# The recursion is defined once and referenced from both sides: a WITH cannot
# be an operand of a set operation, and this keeps it to one statement, so
# both sides are answered at one timestamp.
GRAPH_DIFFERENTIAL_SQL = (
    f"{GRAPH_REACH_CTE}"
    " (SELECT src, dst FROM closure EXCEPT ALL SELECT src, dst FROM reach)"
    " UNION ALL"
    " (SELECT src, dst FROM reach EXCEPT ALL SELECT src, dst FROM closure)"
)

GRAPH_REACH_COUNT_SQL = f"{GRAPH_REACH_CTE} SELECT count(*) FROM reach"


class GraphChurn(Action):
    """Insert and delete edges of an acyclic graph.

    Edges always point from a lower node to a higher one, so the graph stays
    acyclic whatever subset of the operations applied, which is what lets the
    self-loop check be an invariant rather than a race.
    """

    name = "graph-churn"

    def __init__(self, rng: random.Random, client: MzClient) -> None:
        super().__init__(rng)
        self.client = client

    def run(self) -> Outcome | None:
        if self.rng.random() < 0.4:
            # Delete by predicate rather than by a remembered edge: an
            # UNKNOWN insert may or may not exist, and this is correct either
            # way. Clearing a whole node at once retracts many paths.
            src = self.rng.randrange(GRAPH_NODES - 1)
            return self.client.write(f"DELETE FROM edges WHERE src = {src}")
        if self.rng.random() < 0.2:
            # A chain in one transaction, which creates paths several
            # iterations deep in a single update, so the next delete has
            # something long to retract.
            start = self.rng.randrange(GRAPH_NODES - 4)
            nodes = sorted(
                self.rng.sample(range(start + 1, GRAPH_NODES), self.rng.randint(2, 4))
            )
            chain = [start] + nodes
            values = ", ".join(
                f"({a}, {b})" for a, b in zip(chain, chain[1:], strict=False)
            )
            return self.client.write(f"INSERT INTO edges VALUES {values}")
        src = self.rng.randrange(GRAPH_NODES - 1)
        dst = self.rng.randrange(src + 1, GRAPH_NODES)
        return self.client.write(f"INSERT INTO edges VALUES ({src}, {dst})")

    def close(self) -> None:
        self.client.reset()


class Graph(Scenario):
    name = "graph"
    services: list[str] = []
    legs = [
        "metadata",
        "blob",
        "clusterd-compute",
        "clusterd-compute2",
        "pubsub-compute",
        "pubsub-compute2",
    ]

    def setup(self) -> None:
        client = MzClient(self.ctx, "setup")
        for sql in [
            "CREATE TABLE edges (src int, dst int)",
            # A spanning chain, so the closure is deep from the first
            # timestamp on and the checkers are never vacuous while the
            # workers are still warming up.
            f"INSERT INTO edges SELECT g, g + 1 FROM generate_series(0, {GRAPH_NODES - 2}) g",
            # RETAIN HISTORY covers the timestamp the replica comparison
            # probes, which is deliberately slightly in the past.
            "CREATE MATERIALIZED VIEW closure IN CLUSTER compute"
            f" WITH (RETAIN HISTORY = FOR '600s') AS {GRAPH_CLOSURE_SQL}",
            "CREATE INDEX closure_idx IN CLUSTER compute ON closure (src)",
        ]:
            client.query(sql, timeout=120)
        client.reset()

    def make_worker(self, index: int, rng: random.Random) -> WorkerBundle:
        return WorkerBundle(
            actions=[GraphChurn(rng, MzClient(self.ctx, f"worker-{index}"))],
            weights=[1],
        )

    def checkers(self) -> list[Checker]:
        rngs = [random.Random(self.ctx.rng.randrange(SEED_RANGE)) for _ in range(6)]
        return [
            GroupCompletenessPeek(
                rngs[0], self.ctx, "closure-transitive", GRAPH_NOT_CLOSED_SQL
            ),
            GroupCompletenessPeek(
                rngs[1], self.ctx, "closure-has-edges", GRAPH_MISSING_EDGE_SQL
            ),
            GroupCompletenessPeek(
                rngs[2], self.ctx, "closure-acyclic", GRAPH_SELF_LOOP_SQL
            ),
            # Recomputing the recursion is the expensive one, and it can only
            # run where a dataflow can be installed, not on the storage
            # cluster's default replica.
            GroupCompletenessPeek(
                rngs[3],
                self.ctx,
                "closure-differential",
                GRAPH_DIFFERENTIAL_SQL,
                clusters=["compute"],
            ),
            ReplicaDivergence(rngs[4], self.ctx, ("SELECT count(*) FROM closure",)),
            ProgressPeek(rngs[5], self.ctx, "closure", name="closure-progress"),
        ]

    def converge(self) -> None:
        client = MzClient(self.ctx, "converge")

        def caught_up() -> bool:
            client.query("SET cluster = compute")
            maintained = int(client.query("SELECT count(*) FROM closure")[0][0])
            recomputed = int(client.query(GRAPH_REACH_COUNT_SQL)[0][0])
            return maintained == recomputed

        wait_until(caught_up, CONVERGE_TIMEOUT, "the closure catching up to the edges")
        client.reset()

    def final_check(self) -> None:
        client = MzClient(self.ctx, "final-check")
        client.query("SET cluster = compute")
        # Churn can leave an arbitrarily sparse graph behind, and every oracle
        # here passes trivially on a closure with no paths in it. Restoring
        # the spanning chain pins the end state: with every consecutive edge
        # present, reachability is exactly the pairs (i, j) with i < j, so the
        # closure is both maximally dense and known in advance.
        client.query(
            f"INSERT INTO edges SELECT g, g + 1"
            f" FROM generate_series(0, {GRAPH_NODES - 2}) g"
        )
        expected = GRAPH_NODES * (GRAPH_NODES - 1) // 2
        wait_until(
            lambda: int(client.query("SELECT count(*) FROM closure")[0][0]) == expected,
            CONVERGE_TIMEOUT,
            f"the closure reaching all {expected} pairs of the restored chain",
        )
        for name, sql in (
            ("not transitively closed", GRAPH_NOT_CLOSED_SQL),
            ("missing an edge", GRAPH_MISSING_EDGE_SQL),
            ("has a self loop", GRAPH_SELF_LOOP_SQL),
            ("disagrees with a recomputation", GRAPH_DIFFERENTIAL_SQL),
        ):
            bad = client.query(sql, timeout=FINAL_TIMEOUT)
            if bad:
                raise InvariantViolation(f"closure {name}: {bad[:10]}")
        client.reset()
