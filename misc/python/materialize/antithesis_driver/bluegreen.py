# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Blue/green deployments under load.

A deployment builds generation `g + 1` under the staging names, the schema
and cluster `bg_stage`, swaps them with the live names `bg_live` in one
transaction, and drops what the staging names then hold, generation `g`.
Meanwhile reads and subscriptions go through the live names, so some are
planned against objects that are swapped or dropped while they run, and some
reach a generation that is still hydrating.

`v` is a fixed function of `seed` and every deployed object a fixed function
of its generation, so each row a read returns is judged on its own. A strict
serializable read must also return one generation that was live while it
ran, and every seed whose insert was acknowledged before it began.
"""

from __future__ import annotations

import re
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)

from materialize.antithesis import sql
from materialize.antithesis_driver.client import (
    ABANDON_LINGER_SECONDS,
    ABANDON_SHARE,
    Op,
    Outcome,
    Pool,
    Status,
    Target,
    abandon,
    classify,
    commit,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.draw import Draw

SCHEMA = "bg"
LIVE = "bg_live"
STAGE = "bg_stage"
MIN_ROWS = 150
MAX_ROWS = 300
BUCKETS = 8
HYDRATION_WAIT_SECONDS = 30.0
FINAL_SECONDS = 300.0
"""The least time the deployment after faults stop gets."""
"""Below `client.CANCEL_AFTER_SECONDS`, which counts from the operation's start."""
STRICT = "'strict serializable'"
WRITE_SETTINGS = {"transaction_isolation": STRICT, "cluster": "quickstart"}
READ_SETTINGS = {"transaction_isolation": STRICT, "cluster": LIVE}
CATALOG_SETTINGS = {"transaction_isolation": STRICT, "cluster": "mz_catalog_server"}
# The cursor stays open across FETCHes.
SUBSCRIBE_SETTINGS = {"cluster": LIVE, "idle_in_transaction_session_timeout": "0"}
DROP_STAGE = [
    f"DROP CLUSTER IF EXISTS {STAGE} CASCADE",
    f"DROP SCHEMA IF EXISTS {STAGE} CASCADE",
]
READS = {
    "agg": f"SELECT 'agg', gen, seed, v, d FROM {LIVE}.agg",
    "idx_view": f"SELECT 'idx_view', gen, seed, v, d FROM {LIVE}.idx_view",
    "grp": f"SELECT 'agg', gen, seed, v, d FROM {LIVE}.agg"
    f" UNION ALL SELECT 'grp', gen, bucket, n, s FROM {LIVE}.grp",
}
_NAMES = f"('{LIVE}', '{STAGE}')"
CATALOG_QUERY = (
    "SELECT 'schema', s.name, NULL, NULL FROM mz_schemas s"
    " JOIN mz_databases d ON s.database_id = d.id"
    f" WHERE d.name = 'materialize' AND s.name IN {_NAMES}"
    f" UNION ALL SELECT 'cluster', name, NULL, NULL FROM mz_clusters WHERE name IN {_NAMES}"
    " UNION ALL SELECT 'agg', s.name, c.name, m.comment FROM mz_materialized_views v"
    " JOIN mz_schemas s ON v.schema_id = s.id JOIN mz_databases d ON s.database_id = d.id"
    " LEFT JOIN mz_clusters c ON v.cluster_id = c.id"
    " LEFT JOIN mz_internal.mz_comments m ON m.id = v.id AND m.object_sub_id IS NULL"
    f" WHERE d.name = 'materialize' AND v.name = 'agg' AND s.name IN {_NAMES}"
)
UNHYDRATED_QUERY = (
    "SELECT count(*) FROM mz_objects o JOIN mz_clusters c ON o.cluster_id = c.id"
    f" WHERE c.name = '{STAGE}' AND o.id LIKE 'u%' AND NOT EXISTS (SELECT 1"
    " FROM mz_internal.mz_hydration_statuses h WHERE h.object_id = o.id AND h.hydrated)"
)
# Name resolution reports a missing live name as `unknown schema 'bg_live'`,
# `unknown cluster 'bg_live'` or `unknown catalog item 'bg_live.agg'`. An
# input dropped under a running read or subscription is reported under its
# name at the drop, which after a swap is `bg_stage`: `query could not
# complete because cluster "bg_stage" was dropped`, or `relation
# 'materialize.bg_stage.agg' was dropped` (42704) for a subscription.
_LIVE_MISSING = re.compile(
    r"""^unknown (schema|cluster|catalog item) '(materialize\.)?bg_live['.]"""
    r"""|['"](materialize\.)?bg_live['".].* was dropped$"""
)
_DROPPED = re.compile(r" was dropped$")
_GENERATION = re.compile(r"^generation (\d+)$")


def value(seed: int) -> int:
    return seed * 7919 % 1_000_003


def derived(source: str, gen: int, seed: int, v: int) -> int:
    """`d` of a row of `agg` or `idx_view`, as `deployment_statements` defines
    it. The two differ, so a row read through the wrong object fails too."""
    return seed * gen + v if source == "agg" else v * gen - seed


def deployment_statements(name: str, gen: int, size: str) -> list[str]:
    """Generation `gen` under the schema and the cluster `name`."""
    t = f"{SCHEMA}.t"
    return [
        f"CREATE CLUSTER {name} (SIZE = '{size}', REPLICATION FACTOR = 1)",
        f"CREATE SCHEMA {name}",
        f"CREATE MATERIALIZED VIEW {name}.agg IN CLUSTER {name} AS"
        f" SELECT {gen}::bigint AS gen, seed, v, seed * {gen} + v AS d FROM {t}",
        f"CREATE VIEW {name}.idx_view AS"
        f" SELECT {gen}::bigint AS gen, seed, v, v * {gen} - seed AS d FROM {t}",
        f"CREATE DEFAULT INDEX IN CLUSTER {name} ON {name}.idx_view",
        f"CREATE MATERIALIZED VIEW {name}.grp IN CLUSTER {name} AS"
        f" SELECT {gen}::bigint AS gen, seed % {BUCKETS} AS bucket,"
        f" count(*) AS n, sum(v) AS s FROM {t} GROUP BY seed % {BUCKETS}",
        # Last, so that its presence proves every statement before it applied.
        f"COMMENT ON MATERIALIZED VIEW {name}.agg IS 'generation {gen}'",
    ]


def violations(kind: str, rows: list[tuple[Any, ...]]) -> tuple[set[int], list[str]]:
    """The generations of a read's rows, and what is wrong with them."""
    gens = {int(r[1]) for r in rows}
    errors = [f"generations {sorted(gens)}"] if len(gens) > 1 else []
    seeds: Counter[int] = Counter()
    sums: dict[int, list[int]] = {}
    grp: dict[int, list[int]] = {}
    for source, gen, a, b, c in rows:
        gen, a, b, c = int(gen), int(a), int(b), int(c)
        if source == "grp":
            grp[a] = [b, c]
            continue
        seeds[a] += 1
        if b != value(a) or c != derived(source, gen, a, b):
            errors.append(f"{source} row {(gen, a, b, c)}")
        bucket = sums.setdefault(a % BUCKETS, [0, 0])
        bucket[0] += 1
        bucket[1] += b
    errors += [f"seed {s} appears {n} times" for s, n in seeds.items() if n > 1]
    if kind == "grp" and grp != sums:
        errors.append(f"grp {grp} but agg sums to {sums}")
    return gens, errors


def _first_line(error: BaseException) -> str:
    text = str(error).strip()
    return text.splitlines()[0] if text else type(error).__name__


def _check_live_names(op: str, errors: list[str]) -> None:
    missing = [e for e in errors if _LIVE_MISSING.search(e)]
    always(
        not missing,
        "blue/green: a read through the live names never fails because a live name is missing",
        {"op": op, "errors": missing},
    )


@dataclass
class Seed:
    sent_at: float
    insert: str = "pending"
    """`pending`, `ok` or `unknown`. A rejected insert leaves the model."""
    acked_at: float = 0.0
    delete_sent_at: float | None = None
    """When the first delete was sent. From then on the row may be gone."""
    deleted_at: float | None = None
    deleting: bool = False

    def gone_before(self, t: float) -> bool:
        """No read that begins after `t` may show the row. An insert of
        unknown outcome may still apply, so it is never gone."""
        return (
            self.insert == "ok" and self.deleted_at is not None and self.deleted_at < t
        )


class BlueGreen:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.seeds: dict[int, Seed] = {}
        self._next_seed = 1
        self._reads: dict[int, float] = {}
        """Reads in flight, each with a time no later than its start."""
        self._next_read = 0
        self.live = 1
        """The generation under the live names."""
        self.staging = "free"
        """What the staging names hold: `free`, `new` (generation `live + 1`,
        complete), `old` (an older generation, after a swap) or `dirty`
        (perhaps part of a deployment)."""
        self.unsure = False
        """A swap failed. Its outcome, and so both names, are read from the
        catalog before the next deployment step."""
        self._deploying = False
        self.order_known = True
        """Cleared for good by a swap whose COMMIT got no answer. The server
        can still apply it after the catalog was read, and then the next swap
        puts the older generation back, so the generations no longer follow
        the order the driver sent them in."""
        self._live_since: dict[int, float] = {1: float("-inf")}
        """The earliest time each generation was acknowledged or observed live."""
        self._swap_sent: dict[int, float] = {1: float("-inf")}
        self._swaps: list[tuple[float, float]] = []
        """Recent acknowledged swaps: when COMMIT was sent, when it was acknowledged."""
        self._recent_reads: list[tuple[float, float]] = []
        self._subscriber = Pool("bgsub", Target(ctx.config.host), 1)
        self._subscribed = (float("-inf"), float("-inf"))
        """When the latest subscription began and ended, `inf` until then."""
        self.wait_share = 0.5
        self.delete_share = 0.4

    def configure(self, d: Draw) -> None:
        self.wait_share = d.choice((0.25, 0.5, 0.75))
        self.delete_share = d.choice((0.3, 0.5))

    def setup_statements(self) -> list[str]:
        """Generation 1 under the live names. Clusters outlive a dropped
        schema, so a rerun against one environment drops them first."""
        return [
            f"DROP SCHEMA IF EXISTS {LIVE} CASCADE",
            f"DROP CLUSTER IF EXISTS {LIVE} CASCADE",
            *DROP_STAGE,
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t (seed bigint, v bigint)",
            *deployment_statements(LIVE, 1, self.ctx.config.replica_sizes[0]),
        ]

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def write(self, d: Draw) -> Status:
        count = d.integer(1, 8)
        delete = d.boolean(self.delete_share)
        linger = d.choice(ABANDON_LINGER_SECONDS) if d.boolean(ABANDON_SHARE) else None
        now = time.monotonic()
        with self._lock:
            present = [s for s, x in self.seeds.items() if x.deleted_at is None]
            deletable = sorted(
                s
                for s in present
                if not self.seeds[s].deleting and self.seeds[s].insert != "pending"
            )
            deleting = bool(deletable) and (
                len(present) > MAX_ROWS or (delete and len(present) > MIN_ROWS)
            )
            if deleting:
                seeds = deletable[:count]
                for s in seeds:
                    self.seeds[s].deleting = True
                    if self.seeds[s].delete_sent_at is None:
                        self.seeds[s].delete_sent_at = now
                statement = f"DELETE FROM {SCHEMA}.t WHERE seed IN ({', '.join(map(str, seeds))})"
            else:
                seeds = list(range(self._next_seed, self._next_seed + count))
                self._next_seed += count
                for s in seeds:
                    self.seeds[s] = Seed(now)
                rows = ", ".join(f"({s}, {value(s)})" for s in seeds)
                statement = f"INSERT INTO {SCHEMA}.t VALUES {rows}"

        def execute(conn: psycopg.Connection) -> None:
            if linger is not None:
                abandon(conn, statement.encode(), linger)
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                for s in seeds:
                    x = self.seeds[s]
                    if deleting:
                        # A failed delete is sent again by a later write.
                        x.deleting = False
                        if outcome.ok:
                            x.deleted_at = outcome.end
                    elif outcome.ok:
                        x.insert, x.acked_at = "ok", outcome.end
                    elif outcome.rejected:
                        del self.seeds[s]
                    else:
                        x.insert = "unknown"

        name = "blue/green delete" if deleting else "blue/green insert"
        return self.ctx.sessions.call(
            self._session(d),
            Op(name, execute, complete, WRITE_SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def _visibility(self, start: float, end: float) -> tuple[set[int], set[int]]:
        """The seeds a read over `[start, end]` must show, and those it may show."""
        required, allowed = set(), set()
        for s, x in self.seeds.items():
            if x.sent_at < end and not x.gone_before(start):
                allowed.add(s)
            kept = x.delete_sent_at is None or x.delete_sent_at > end
            if x.insert == "ok" and x.acked_at < start and kept:
                required.add(s)
        return required, allowed

    def _observe(self, gen: int, at: float) -> None:
        self._live_since[gen] = min(self._live_since.get(gen, at), at)

    def _floor(self, start: float) -> int:
        return max(g for g, t in self._live_since.items() if t < start)

    def read(self, d: Draw) -> Status:
        kind = d.choice(tuple(READS))
        return self.ctx.sessions.call(
            self._session(d), self._read_op(kind, []), RULE_WAIT_SECONDS
        )

    def _read_op(self, kind: str, passed: list[str]) -> Op:
        query = READS[kind].encode()
        errors: list[str] = []
        with self._lock:
            token = self._next_read
            self._next_read += 1
            self._reads[token] = time.monotonic()

        def classify_read(error: BaseException) -> sql.Classified:
            errors.append(_first_line(error))
            return classify(error)

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            start, end = outcome.start, outcome.end
            with self._lock:
                required, allowed = self._visibility(start, end)
                floor = self._floor(start)
                sent = {g for g, t in self._swap_sent.items() if t < end}
                across_swap = any(start < c and a < end for c, a in self._swaps)
                self._recent_reads = [*self._recent_reads[-63:], (start, end)]
                # A read that began before a delete was acknowledged may still
                # show the row, so deleted seeds stay until no such read runs.
                del self._reads[token]
                oldest = min(self._reads.values(), default=float("inf"))
                for s in [s for s, x in self.seeds.items() if x.gone_before(oldest)]:
                    del self.seeds[s]
            _check_live_names(f"read {kind}", errors)
            if not outcome.ok:
                return
            assert outcome.value is not None
            gens, wrong = violations(kind, outcome.value)
            visible = {int(r[2]) for r in outcome.value if r[0] != "grp"}
            missing, unexpected = sorted(required - visible), sorted(visible - allowed)
            with self._lock:
                ordered = self.order_known
            stale = [g for g in gens if g < floor] if ordered else []
            unsent = gens - sent if ordered else set()
            details = {"kind": kind, "gens": sorted(gens), "rows": len(outcome.value)}
            always(
                not wrong,
                "blue/green: every row of a read has one generation and is correct for it and its seed",
                {**details, "errors": wrong[:10]},
            )
            always(
                not missing and not unexpected,
                "blue/green: a read shows every seed acknowledged before it began and none deleted before it began",
                {**details, "missing": missing[:20], "unexpected": unexpected[:20]},
            )
            always(
                not stale,
                "blue/green: a read never returns a generation older than one live before it began",
                {**details, "floor": floor},
            )
            always(
                not unsent,
                "blue/green: a read never returns a generation whose swap was sent after it ended",
                {**details, "sent": sorted(sent)},
            )
            sometimes(
                any(g > 1 for g in gens),
                "blue/green: a read was answered by a generation newer than 1",
                details,
            )
            sometimes(
                across_swap,
                "blue/green: a read was in flight across an acknowledged swap",
                details,
            )
            if len(gens) == 1:
                with self._lock:
                    self._observe(gens.pop(), end)
            if not (wrong or missing or unexpected or stale or unsent):
                passed.append(kind)

        return Op(
            f"blue/green read {kind}", execute, complete, READ_SETTINGS, classify_read
        )

    def stage(self, d: Draw) -> Status:
        at = d.integer(0, 8) if d.boolean(ABANDON_SHARE) else None
        linger = d.choice(ABANDON_LINGER_SECONDS)
        return self._deploy(d, lambda: self._stage_op(at, linger))

    def swap(self, d: Draw) -> Status:
        wait = d.boolean(self.wait_share)
        # Never abandoned on purpose: a COMMIT without an answer ends the
        # order checks of the timeline, so only faults lose one.
        return self._deploy(d, lambda: self._swap_op(wait, None))

    def cleanup(self, d: Draw) -> Status:
        cluster_first = d.boolean()
        return self._deploy(d, lambda: self._cleanup_op(cluster_first))

    def _deploy(self, d: Draw, build: Callable[[], Op | None]) -> Status:
        with self._lock:
            unsure = self.unsure
        op = self._resolve_op([]) if unsure else build()
        if op is None:
            return Status.NOT_SENT
        return self.ctx.sessions.call(self._session(d), op, RULE_WAIT_SECONDS)

    def _claim(self, *states: str) -> tuple[int, str] | None:
        """Take the one deployment step allowed in flight, if the staging
        names are in one of `states`. Returns the next generation and the
        staging state."""
        with self._lock:
            if self._deploying or self.unsure or self.staging not in states:
                return None
            self._deploying = True
            return self.live + 1, self.staging

    def _stage_op(self, abandon_at: int | None, linger: float) -> Op | None:
        claim = self._claim("free", "dirty")
        if claim is None:
            return None
        gen, staging = claim
        statements = (DROP_STAGE if staging == "dirty" else []) + (
            deployment_statements(STAGE, gen, self.ctx.config.replica_sizes[0])
        )

        def execute(conn: psycopg.Connection) -> None:
            for i, statement in enumerate(statements):
                if i == abandon_at:
                    abandon(conn, statement.encode(), linger)
                conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self._deploying = False
                # A failed stage may have created some objects, or still may:
                # the next one drops the staging names and starts over.
                self.staging = "new" if outcome.ok else "dirty"

        return Op("blue/green stage", execute, complete, ddl=True)

    def _swap_op(self, wait: bool, linger: float | None) -> Op | None:
        claim = self._claim("new")
        if claim is None:
            return None
        gen = claim[0]
        with self._lock:
            self._swap_sent.setdefault(gen, time.monotonic())
        committing: list[float] = []
        unhydrated: list[int] = []

        def execute(conn: psycopg.Connection) -> None:
            deadline = time.monotonic() + (HYDRATION_WAIT_SECONDS if wait else 0.0)
            while True:
                row = conn.execute(UNHYDRATED_QUERY.encode()).fetchone()
                count = int(row[0]) if row else 0
                if count == 0 or time.monotonic() >= deadline:
                    break
                time.sleep(0.5)
            unhydrated.append(count)
            conn.execute(b"BEGIN")
            conn.execute(f"ALTER SCHEMA {LIVE} SWAP WITH {STAGE}".encode())
            conn.execute(f"ALTER CLUSTER {LIVE} SWAP WITH {STAGE}".encode())
            committing.append(time.monotonic())
            if linger is not None:
                abandon(conn, b"COMMIT", linger)
            commit(conn)

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self._deploying = False
                if outcome.ok:
                    self.live, self.staging = gen, "old"
                    self._observe(gen, outcome.end)
                    self._swaps = [*self._swaps[-31:], (committing[0], outcome.end)]
                else:
                    self.unsure = True
                    if committing and not outcome.rejected:
                        self.order_known = False
            if outcome.ok:
                sometimes(
                    unhydrated[0] > 0,
                    "blue/green: a swap happened before hydration",
                    {"gen": gen, "unhydrated": unhydrated[0]},
                )

        return Op("blue/green swap", execute, complete, CATALOG_SETTINGS, ddl=True)

    def _cleanup_op(self, cluster_first: bool) -> Op | None:
        if self._claim("old") is None:
            return None

        def execute(conn: psycopg.Connection) -> None:
            for statement in DROP_STAGE if cluster_first else DROP_STAGE[::-1]:
                conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            at = outcome.start
            with self._lock:
                self._deploying = False
                # A failed cleanup stays `old` and is sent again: `IF EXISTS`
                # makes a repeat harmless.
                if not outcome.ok:
                    return
                self.staging = "free"
                # Judged here: a read or subscription that the drop fails often
                # ends before the cleanup is acknowledged.
                reads = any(t < at for t in self._reads.values()) or any(
                    s < at < e for s, e in self._recent_reads
                )
                subscribed = self._subscribed[0] < at < self._subscribed[1]
            sometimes(
                reads,
                "blue/green: a read was in flight across an acknowledged cleanup",
                {},
            )
            sometimes(
                subscribed,
                "blue/green: a subscription was in flight across an acknowledged cleanup",
                {},
            )

        return Op("blue/green cleanup", execute, complete, ddl=True)

    def _resolve_op(self, done: list[bool]) -> Op | None:
        with self._lock:
            if self._deploying:
                return None
            self._deploying = True
            expected = {self.live, self.live + 1} if self.unsure else {self.live}
            ordered = self.order_known

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(CATALOG_QUERY.encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                with self._lock:
                    self._deploying = False
                return
            assert outcome.value is not None
            names = {(kind, name) for kind, name, _, _ in outcome.value}
            aggs: dict[str, tuple[str | None, int | None]] = {}
            for kind, name, cluster, comment in outcome.value:
                if kind == "agg":
                    m = _GENERATION.match(comment or "")
                    aggs[name] = (cluster, int(m.group(1)) if m else None)
            live_cluster, live = aggs.get(LIVE, (None, None))
            stage_cluster, staged = aggs.get(STAGE, (None, None))
            always(
                live_cluster == LIVE
                and (live in expected or not ordered)
                and stage_cluster in (None, STAGE),
                "blue/green: the live names hold one whole deployment of an expected generation",
                {"catalog": outcome.value, "expected": sorted(expected)},
            )
            with self._lock:
                self._deploying = False
                self.unsure = False
                if live is not None:
                    self.live = live
                    self._observe(live, outcome.end)
                if staged is not None and stage_cluster == STAGE:
                    self.staging = "new" if staged > self.live else "old"
                elif ("schema", STAGE) in names or ("cluster", STAGE) in names:
                    self.staging = "dirty"
                else:
                    self.staging = "free"
            done.append(True)

        return Op("blue/green resolve", execute, complete, CATALOG_SETTINGS)

    def subscribe(self, d: Draw) -> Status:
        """`SUBSCRIBE` to the live `agg` on a session of its own, long enough
        for a swap and a cleanup to land while it is open."""
        # Below `client.CANCEL_AFTER_SECONDS`, as the hydration wait.
        seconds = d.choice((5.0, 20.0, 45.0))
        limit = d.choice((100, 1000, 10_000))
        opened: list[float] = []
        gens: set[int] = set()
        wrong: list[tuple[int, ...]] = []
        errors: list[str] = []

        def classify_subscribe(error: BaseException) -> sql.Classified:
            errors.append(_first_line(error))
            return classify(error)

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(b"BEGIN")
            opened.append(time.monotonic())
            with self._lock:
                self._subscribed = (opened[0], float("inf"))
            conn.execute(f"DECLARE c CURSOR FOR SUBSCRIBE {LIVE}.agg".encode())
            received = 0
            while time.monotonic() - opened[0] < seconds and received < limit:
                rows = conn.execute(b"FETCH ALL c WITH (timeout = '1s')").fetchall()
                received += len(rows)
                for row in rows:
                    gen, seed, v, dd = (int(x) for x in row[2:])
                    gens.add(gen)
                    if v != value(seed) or dd != derived("agg", gen, seed, v):
                        wrong.append((gen, seed, v, dd))
            conn.execute(b"COMMIT")

        def complete(outcome: Outcome[None]) -> None:
            if not opened:
                return
            with self._lock:
                self._subscribed = (opened[0], outcome.end)
                floor = self._floor(opened[0]) if self.order_known else 0
            always_or_unreachable(
                len(gens) <= 1 and not wrong and all(g >= floor for g in gens),
                "blue/green: a subscription returns rows of one generation, correct for it and not older than one live before it began",
                {"gens": sorted(gens), "floor": floor, "wrong": wrong[:10]},
            )
            _check_live_names("subscribe", errors)
            sometimes(
                any(_DROPPED.search(e) for e in errors),
                "blue/green: a subscription ended with an error because its cluster or object was dropped",
                {"errors": errors},
            )

        op = Op(
            "blue/green subscribe",
            execute,
            complete,
            SUBSCRIBE_SETTINGS,
            classify_subscribe,
        )
        return self._subscriber.call(0, op, RULE_WAIT_SECONDS)

    def eventually(self, deadline: float) -> None:
        """After faults stop, read the catalog, deploy once more, then read
        each object through the live names."""
        # A deployment takes many steps, and earlier checks share `deadline`
        # and may have used it up.
        deadline = max(deadline, time.monotonic() + FINAL_SECONDS)
        resolved: list[bool] = []
        passed: list[str] = []
        target: int | None = None
        session = 0
        while time.monotonic() < deadline and set(passed) != set(READS):
            with self._lock:
                live, staging, unsure = self.live, self.staging, self.unsure
            if resolved and target is None:
                target = live + 1
            if not resolved or unsure:
                op = self._resolve_op(resolved)
            elif staging == "old":
                op = self._cleanup_op(True)
            elif target is not None and live < target:
                op = (
                    self._swap_op(True, None)
                    if staging == "new"
                    else self._stage_op(None, 0.0)
                )
            else:
                op = self._read_op(next(k for k in READS if k not in passed), passed)
            if (
                op is None
                or self.ctx.sessions.call(session, op, 60.0) == Status.NOT_SENT
            ):
                session += 1
            time.sleep(0.5)
        always(
            set(passed) == set(READS),
            "blue/green: after faults stop a deployment completes and a read through the live names passes its checks",
            {"live": self.live, "staging": self.staging, "passed": passed},
        )
