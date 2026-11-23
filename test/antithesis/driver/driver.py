#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import base64
import json
import os
import subprocess
import threading
import time
import traceback
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse

import psycopg
from antithesis.assertions import always, reachable, unreachable  # pyright: ignore
from antithesis.lifecycle import setup_complete  # pyright: ignore
from antithesis.random import AntithesisRandom  # pyright: ignore
from persist_audit import audit

KUBERNETES = "KUBERNETES_SERVICE_HOST" in os.environ
NAMESPACE = "materialize-environment"
ENVIRONMENT = "antithesis"
PG_URL = os.environ.get(
    "PG_URL",
    "postgres://materialize_user:materialize_pass@postgres.materialize.svc.cluster.local:5432/materialize_db",
)
KAFKA = os.environ.get("KAFKA_BROKER", "redpanda.materialize.svc.cluster.local:9092")
SIZES = os.environ.get("REPLICA_SIZES", "antithesis-1;antithesis-2").split(";")
PARAMETERS: dict[str, list[str]] = json.loads(
    base64.b64decode(os.environ.get("SYSTEM_PARAMETERS", "e30="))
)
"""System parameters that CI varies, with the values it varies them over."""

WORKERS = 6
KEYS = 4
PG_ROWS = 8
PG_TOTAL = 2 * PG_ROWS * 100
RECOVERY_SECONDS = 15 * 60
"""How long the system may take to recover once faults stop."""

SUMS = "SELECT k, count(*) AS n, sum(v) AS s FROM ledger GROUP BY k"
ROUND_TRIP = (
    "SELECT (data->>'k')::int, (data->>'n')::bigint, (data->>'s')::bigint FROM sums_rt"
)

rnd = AntithesisRandom()
lock = threading.Lock()
quiesced = threading.Event()
mz_host = os.environ.get("MZ_HOST", "materialized")

next_id = 0
acked: set[int] = set()
sent = [0] * KEYS
counted = [0] * KEYS
generation = 0
rollout: str | None = None
serial = 0
acted: str | None = None
"""The rollout during whose promotion the driver already acted."""
changed = False
"""Whether the driver changed the spec while the last rollout was promoting."""
last_kill = 0.0
kill_gap = float("inf")
"""Seconds between two pod deletions by the driver, drawn per timeline."""


def log(message: str) -> None:
    print(f"driver: {message}", flush=True)


def differ(a: str, b: str) -> str:
    """A scalar subquery: by how many rows two relations differ. Within one
    statement both are read at the same timestamp."""
    a, b = f"(SELECT * FROM {a})", f"(SELECT * FROM {b})"
    return (
        f"(SELECT count(*) FROM (({a} EXCEPT ALL {b}) UNION ALL ({b} EXCEPT ALL {a})))"
    )


class Session:
    """A connection that is replaced after any error, so that no statement
    runs in a transaction or with a setting that a failed one left behind."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.conn: psycopg.Connection | None = None

    def __call__(self, query: str, params: tuple[Any, ...] | None = None) -> list[Any]:
        try:
            if self.conn is None:
                # The keepalives end a statement whose server is partitioned
                # away. Without them it would block its thread forever.
                self.conn = psycopg.connect(
                    autocommit=True,
                    connect_timeout=10,
                    keepalives_idle=5,
                    keepalives_interval=2,
                    keepalives_count=3,
                    tcp_user_timeout=20000,
                    **self.kwargs,
                )
            cur = self.conn.execute(query.encode(), params)
            return cur.fetchall() if cur.description else []
        except Exception:
            conn, self.conn = self.conn, None
            if conn is not None:
                conn.close()
            raise


def mz_session(
    user: str = "materialize", options: str = "-c cluster=c", host: str = ""
) -> Session:
    port = 6877 if user == "mz_system" else 6875
    options += " -c statement_timeout=20s"
    return Session(
        host=host or mz_host,
        port=port,
        user=user,
        dbname="materialize",
        options=options,
    )


class Worker:
    def __init__(self, n: int) -> None:
        self.n = n
        self.mz = mz_session()
        self.pg = Session(conninfo=PG_URL, options="-c statement_timeout=20s")


def kubectl(*args: str, stdin: str | None = None) -> str:
    result = subprocess.run(
        ["kubectl", *args],
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    return result.stdout


TARGET = ["materialize", ENVIRONMENT, "-n", NAMESPACE]


def resource() -> dict[str, Any]:
    return json.loads(kubectl("get", *TARGET, "-o", "json"))


def patch(**spec: Any) -> None:
    kubectl("patch", *TARGET, "--type=merge", "-p", json.dumps({"spec": spec}))
    log(f"patched {spec}")


def phase(cr: dict[str, Any]) -> str:
    """What orchestratord reports for the current spec: a rollout phase
    (`Applying`, `ReadyToPromote`, `Promoting`), `Applied`, `WaitingForApproval`
    after a spec change without a rollout request, or nothing while it has not
    looked at the spec yet."""
    for c in (cr.get("status") or {}).get("conditions") or []:
        if (
            c["type"] == "UpToDate"
            and c.get("observedGeneration") == cr["metadata"]["generation"]
        ):
            return c["reason"]
    return ""


def up_to_date(cr: dict[str, Any]) -> bool:
    done = (cr.get("status") or {}).get("lastCompletedRolloutRequest")
    return phase(cr) == "Applied" and done == cr["spec"].get("requestRollout")


def extra_env() -> list[dict[str, str]]:
    """environmentd's extra environment. Soft assertions panic, and Antithesis
    reports the crash. `ROLLOUT` makes every rollout a change of the spec that
    the generation running it can be asked for."""
    env = {"MZ_SOFT_ASSERTIONS": "1", "ROLLOUT": str(serial)}
    return [{"name": name, "value": value} for name, value in env.items()]


def request_rollout(strategy: str = "") -> None:
    global rollout, serial, changed
    rollout = str(uuid.UUID(int=rnd.getrandbits(128)))
    serial, changed = serial + 1, False
    strategy = strategy or rnd.choice(["WaitUntilReady", "ManuallyPromote"])
    patch(
        requestRollout=rollout,
        rolloutStrategy=strategy,
        environmentdExtraEnv=extra_env(),
    )


def delete_pod(app: str, note: str = "", gap: float | None = None) -> None:
    global last_kill
    with lock:
        since = time.monotonic() - last_kill
        if quiesced.is_set() or since < (kill_gap if gap is None else gap):
            return
        last_kill = time.monotonic()
    namespace = "materialize" if app == "orchestratord" else NAMESPACE
    selector = {
        "environmentd": "materialize.cloud/app=environmentd",
        "clusterd": "cluster.environmentd.materialize.cloud/cluster-id",
        "orchestratord": "app.kubernetes.io/name=materialize-operator",
    }[app]
    pods = kubectl("get", "pods", "-n", namespace, "-l", selector, "-o", "name").split()
    if pods:
        force = ["--grace-period=0", "--force"] if rnd.random() < 0.5 else []
        pod = rnd.choice(pods)
        kubectl("delete", "-n", namespace, "--wait=false", *force, pod)
        log(f"deleted {pod} {force} {note}")


# Rules. Each takes the worker whose sessions it uses.


def write(w: Worker) -> None:
    """One transaction that inserts the same rows into two tables."""
    global next_id
    count = rnd.randint(1, 3)
    with lock:
        ids = range(next_id, next_id + count)
        next_id += count
    rows = ", ".join(f"({i}, {rnd.randrange(KEYS)}, {rnd.randrange(100)})" for i in ids)
    try:
        w.mz("BEGIN")
        w.mz(f"INSERT INTO ledger VALUES {rows}")
        w.mz(f"INSERT INTO ledger2 VALUES {rows}")
        w.mz("COMMIT")
    except Exception:
        reachable("a write failed or its outcome stayed unknown", {})
        raise
    with lock:
        acked.update(ids)


def read(w: Worker) -> None:
    with lock:
        before = set(acked)
    ids = [row[0] for row in w.mz("SELECT id FROM ledger")]
    with lock:
        limit = next_id
    seen = set(ids)
    d = {"rows": len(ids), "distinct": len(seen), "limit": limit}
    valid = len(seen) == len(ids) and all(0 <= i < limit for i in seen)
    always(valid, "a read shows only rows the workload wrote, each once", d)
    missing = sorted(before - seen)
    d = {"missing": missing[:20], "count": len(missing)}
    always(not missing, "a read shows every write acknowledged before it began", d)


def views(w: Worker) -> None:
    join = "(SELECT k, n, 2 * s, v FROM sums JOIN counter USING (k))"
    query = f"SELECT {differ('ledger', 'ledger2')}, {differ('sums', f'({SUMS})')}, {differ('joined', join)}"
    # Also on one replica alone, and at serializable isolation, where a read
    # may be stale but shows one timestamp as well.
    option = rnd.choice(
        (
            None,
            None,
            "cluster_replica=r1",
            "cluster_replica=r2",
            "transaction_isolation=serializable",
        )
    )
    mz = mz_session(options=f"-c cluster=c -c {option}") if option else w.mz
    atomic, view, join = mz(query)[0]
    if option and mz.conn:
        mz.conn.close()
    d = {"atomic": atomic, "view": view, "join": join}
    always(atomic == 0, "the two tables of a transaction hold the same rows", d)
    always(view == 0, "a materialized view equals its query at every timestamp", d)
    always(join == 0, "an indexed join equals the materialized view it follows from", d)


def increment(w: Worker) -> None:
    k = rnd.randrange(KEYS)
    with lock:
        sent[k] += 1
    w.mz("UPDATE counter SET v = v + 1 WHERE k = %s", (k,))
    with lock:
        counted[k] += 1


def read_counter(w: Worker) -> None:
    with lock:
        low = list(counted)
    values = dict(w.mz("SELECT k, v FROM counter"))
    with lock:
        high = list(sent)
    d = {"acknowledged": low, "values": values, "sent": high}
    bounded = all(low[k] <= values.get(k, -1) <= high[k] for k in range(KEYS))
    always(bounded, "a counter stays within the increments acknowledged and sent", d)


def pg_write(w: Worker) -> None:
    """Moves an amount between two upstream tables. One statement, because a
    transaction that a lost connection leaves open upstream would block every
    later snapshot of the source."""
    amount, a, b = rnd.randint(1, 10), rnd.randrange(PG_ROWS), rnd.randrange(PG_ROWS)
    w.pg(
        f"WITH x AS (UPDATE upstream.a SET v = v - {amount} WHERE id = {a})"
        f" UPDATE upstream.b SET v = v + {amount} WHERE id = {b}"
    )


def pg_read(w: Worker) -> None:
    total = w.mz("SELECT (SELECT sum(v) FROM pg_a) + (SELECT sum(v) FROM pg_b)")[0][0]
    d = {"total": total, "expected": PG_TOTAL}
    always(total == PG_TOTAL, "a Postgres source shows whole upstream transactions", d)


def pg_export(w: Worker) -> None:
    """Adds a second table for an upstream table to the running source."""
    name = f"pg_a{w.n}"
    w.mz(f"DROP TABLE IF EXISTS {name}")
    w.mz(f"CREATE TABLE {name} FROM SOURCE pg_src (REFERENCE upstream.a)")
    d = {"differing": w.mz(f"SELECT {differ(name, 'pg_a')}")[0][0]}
    always(d["differing"] == 0, "a table added to a Postgres source equals its twin", d)


def kafka_read(w: Worker) -> None:
    """The ledger only grows, so a state that the sink wrote never exceeds the
    view's later state. The source assigns its own timestamps to what the sink
    wrote, so the view is read in a second statement, and after a write: the
    view can be ahead of the timestamps that reads get until a later write is
    acknowledged."""
    back = w.mz(ROUND_TRIP)
    write(w)
    now = {k: (n, s) for k, n, s in w.mz("SELECT * FROM sums")}
    zero = (0, 0)
    ahead = [r for r in back if any(a > b for a, b in zip(r[1:], now.get(r[0], zero)))]
    d = {"ahead": ahead, "view": now}
    always(not ahead, "a Kafka sink read back never runs ahead of its view", d)


def ddl(w: Worker) -> None:
    """One step in the life of a view, a table or the cluster, so that kills
    land between any two of them."""
    view, table = f"scratch{w.n}", f"scratch_t{rnd.randrange(2)}"
    statements = [
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view} IN CLUSTER c AS {SUMS}",
        f"CREATE DEFAULT INDEX IF NOT EXISTS IN CLUSTER c ON {view}",
        f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE",
        f"CREATE TABLE IF NOT EXISTS {table} (id bigint)",
        f"INSERT INTO {table} SELECT max(id) FROM ledger",
        f"CREATE DEFAULT INDEX IF NOT EXISTS IN CLUSTER s ON {table}",
        f"DROP TABLE IF EXISTS {table} CASCADE",
        f"ALTER CLUSTER c SET (REPLICATION FACTOR {rnd.randint(1, 2)})",
        f"ALTER CLUSTER c SET (SIZE '{rnd.choice(SIZES)}')",
    ]
    w.mz(rnd.choice(statements))


def ddl_read(w: Worker) -> None:
    d = {"differing": w.mz(f"SELECT {differ(f'scratch{w.n}', 'sums')}")[0][0]}
    always(d["differing"] == 0, "a view created under faults equals its query", d)


def kill(w: Worker) -> None:
    delete_pod(rnd.choice(["environmentd", "clusterd", "clusterd", "orchestratord"]))


def kill_after_ddl(w: Worker) -> None:
    """Deletes environmentd a moment after a DDL statement is sent. Its catalog
    change commits first and work in other durable stores follows, and the
    environment has to boot from whatever a death in between leaves."""
    delay, rule = rnd.choice([0, 0.02, 0.05, 0.1, 0.2, 0.5, 1]), rnd.choice(
        [ddl, pg_export]
    )
    note = f"{delay} s after {rule.__name__} began"
    threading.Timer(delay, delete_pod, ["environmentd", note]).start()
    rule(w)


def write_to_candidate(w: Worker) -> None:
    """Sends a write straight to the pod of a generation that orchestratord has
    not told to promote. It runs read-only and must not acknowledge the write."""

    def observe() -> tuple[Any, str, dict[str, Any]]:
        """The active generation, the phase, and every environmentd pod's
        generation, address and restart counts."""
        cr = resource()
        selector = "materialize.cloud/app=environmentd"
        listed = kubectl("get", "pods", "-n", NAMESPACE, "-l", selector, "-o", "json")
        pods = {}
        for pod in json.loads(listed)["items"]:
            status = pod["status"]
            restarts = [
                c["restartCount"] for c in status.get("containerStatuses") or []
            ]
            generation = int(pod["metadata"]["name"].split("-")[-2])
            pods[pod["metadata"]["uid"]] = (generation, status.get("podIP"), restarts)
        return (cr.get("status") or {}).get("activeGeneration"), phase(cr), pods

    active, before, pods = observe()
    waiting = [uid for uid, pod in pods.items() if pod[1] and pod[0] > active]
    if before not in ("Applying", "ReadyToPromote") or not waiting:
        return
    uid = rnd.choice(waiting)
    try:
        mz_session(options="", host=pods[uid][1])("INSERT INTO probe VALUES (1)")
        acked = True
    except (psycopg.Error, OSError):
        acked = False
    after = observe()
    # Judged only if the generation was not promoted meanwhile, and the address
    # still belonged to the same process.
    if after[:2] == (active, before) and after[2].get(uid) == pods[uid]:
        d = {"pod": pods[uid], "active": active, "phase": before}
        always(not acked, "a generation that was not promoted acknowledges no write", d)


def roll(w: Worker) -> None:
    """Follows the Materialize resource through 0dt rollouts: requests them,
    promotes the manual ones, deletes pods in their phases, and changes the
    spec while one is promoting."""
    global generation, serial, acted, changed
    # Once a second: rollouts get time between them, and a promotion, which
    # takes a few seconds, is seen.
    time.sleep(1)
    if quiesced.is_set():
        return
    cr = resource()
    status, spec, now = cr.get("status") or {}, cr["spec"], phase(cr)
    active = status.get("activeGeneration", generation)
    d = {"active": active, "previous": generation}
    always(active >= generation, "the active generation never decreases", d)
    generation = active
    if up_to_date(cr):
        name = f"mz{status['resourceId']}-environmentd-{active}"
        listed = kubectl("get", "statefulset", name, "-n", NAMESPACE, "-o", "json")
        containers = json.loads(listed)["spec"]["template"]["spec"]["containers"]
        runs = [
            e for c in containers for e in c.get("env") or [] if e["name"] == "ROLLOUT"
        ]
        wants = [e for e in spec["environmentdExtraEnv"] if e["name"] == "ROLLOUT"]
        d = {"runs": runs, "wants": wants, "generation": active}
        if changed:
            always(
                runs == wants,
                "an up-to-date resource runs a spec changed while promoting",
                d,
            )
        else:
            always(runs == wants, "an up-to-date resource runs its spec", d)
        if rollout == spec.get("requestRollout"):
            reachable("a rollout was applied while faults were active", {})
        if rnd.random() < 0.1:
            request_rollout()
    elif now == "WaitingForApproval":
        request_rollout()
    elif now == "ReadyToPromote" and spec.get("rolloutStrategy") == "ManuallyPromote":
        if rnd.random() < 0.3:
            patch(forcePromote=spec["requestRollout"])
    elif now == "Promoting" and acted != spec["requestRollout"]:
        acted = spec["requestRollout"]
        act = rnd.choice(["", "", "change", "kill"])
        if act == "change":
            # Without a rollout request, so that this change must not be
            # reported as applied by the rollout that is promoting (CLO-339).
            serial, changed = serial + 1, True
            patch(environmentdExtraEnv=extra_env())
        elif act == "kill":
            app = rnd.choice(["environmentd", "orchestratord"])
            delete_pod(app, "while promoting", 0)


Rule = Callable[[Worker], None]
DATA: dict[Rule, int] = {
    write: 8,
    read: 3,
    views: 3,
    increment: 3,
    read_counter: 2,
    pg_write: 4,
    pg_read: 2,
    kafka_read: 2,
}
CHURN: dict[Rule, int] = {ddl: 2, ddl_read: 1, pg_export: 1}


def work(n: int, weights: dict[Rule, int]) -> None:
    w = Worker(n)
    while not quiesced.is_set():
        rule = rnd.choices(list(weights), list(weights.values()))[0]
        try:
            rule(w)
        except (psycopg.Error, OSError, subprocess.SubprocessError):
            # Expected under faults. The pause keeps the workers from spinning
            # while nothing answers.
            time.sleep(1)
        except Exception as e:
            d = {
                "rule": rule.__name__,
                "error": repr(e),
                "trace": traceback.format_exc(),
            }
            unreachable("the driver raises no unexpected exception", d)
        time.sleep(rnd.random() / 10)


def retry(what: str, step: Callable[[], Any]) -> Any:
    """Setup runs before faults start, so a failure is one of starting up and
    is retried without a deadline."""
    last = None
    while True:
        try:
            return step()
        except Exception as e:
            if "already exists" in str(e):
                return None
            if repr(e) != last:
                last = repr(e)
                log(f"waiting for {what}: {last.splitlines()[0]}")
            time.sleep(2)


def create_environment() -> str:
    """Creates the Materialize resource, which the cluster accepts only once
    orchestratord has registered its definition. Returns the SQL host."""
    cr = {
        "apiVersion": "materialize.cloud/v1alpha1",
        "kind": "Materialize",
        "metadata": {"name": ENVIRONMENT, "namespace": NAMESPACE},
        "spec": {
            "environmentdImageRef": os.environ["ENVIRONMENTD_IMAGE"],
            "backendSecretName": "materialize-backend",
            "authenticatorKind": "None",
            "environmentdExtraEnv": extra_env(),
        },
    }
    retry("the resource", lambda: kubectl("apply", "-f", "-", stdin=json.dumps(cr)))

    def host() -> str:
        cr = resource()
        assert up_to_date(cr), "the first generation is not applied"
        return (
            f"mz{cr['status']['resourceId']}-environmentd.{NAMESPACE}.svc.cluster.local"
        )

    return retry("the first generation", host)


def setup() -> None:
    pg = urlparse(PG_URL)
    upstream = [
        "CREATE SCHEMA upstream",
        "CREATE TABLE upstream.a (id int PRIMARY KEY, v int NOT NULL)",
        "CREATE TABLE upstream.b (id int PRIMARY KEY, v int NOT NULL)",
        "ALTER TABLE upstream.a REPLICA IDENTITY FULL",
        "ALTER TABLE upstream.b REPLICA IDENTITY FULL",
        f"INSERT INTO upstream.a SELECT generate_series(1, {PG_ROWS}) - 1, 100",
        f"INSERT INTO upstream.b SELECT generate_series(1, {PG_ROWS}) - 1, 100",
        "CREATE PUBLICATION antithesis FOR TABLE upstream.a, upstream.b",
    ]
    system = [
        # All replicas share the one node.
        "ALTER SYSTEM SET cluster_soften_replication_anti_affinity = true",
        # The defaults of the next three (5 min, 10 min, 5 min) outlast most
        # timelines, so that no rollout would complete within one.
        "ALTER SYSTEM SET with_0dt_deployment_ddl_check_interval = '30s'",
        "ALTER SYSTEM SET with_0dt_caught_up_check_stability_period = '30s'",
        "ALTER SYSTEM SET controller_past_generation_replica_cleanup_retry_interval = '30s'",
        "GRANT CREATECLUSTER ON SYSTEM TO materialize",
    ]
    user = [
        f"CREATE CLUSTER c (SIZE '{SIZES[0]}', REPLICATION FACTOR 1)",
        f"CREATE CLUSTER s (SIZE '{SIZES[0]}', REPLICATION FACTOR 1)",
        "CREATE TABLE ledger (id bigint, k int, v int)",
        "CREATE TABLE ledger2 (id bigint, k int, v int)",
        "CREATE TABLE counter (k int, v bigint)",
        "CREATE TABLE probe (x int)",
        # The first read-only generation can still be reading this index when
        # the active generation drops the table with CASCADE (PER-163).
        "CREATE TABLE scratch_t0 (id bigint)",
        "INSERT INTO scratch_t0 VALUES (0)",
        "CREATE DEFAULT INDEX IN CLUSTER s ON scratch_t0",
        f"INSERT INTO counter SELECT generate_series(1, {KEYS}) - 1, 0",
        f"CREATE MATERIALIZED VIEW sums IN CLUSTER c AS {SUMS}",
        # The counters change in place, so they retract through this join.
        "CREATE VIEW joined AS SELECT l.k, count(*) AS n, sum(l.v + m.v) AS s, c.v"
        " FROM ledger l JOIN ledger2 m USING (id) JOIN counter c ON c.k = l.k GROUP BY l.k, c.v",
        "CREATE DEFAULT INDEX IN CLUSTER c ON joined",
        f"CREATE SECRET pg_password AS '{pg.password}'",
        f"CREATE CONNECTION pg TO POSTGRES (HOST '{pg.hostname}', PORT {pg.port or 5432},"
        f" USER '{pg.username}', PASSWORD SECRET pg_password, DATABASE '{pg.path[1:]}')",
        "CREATE SOURCE pg_src IN CLUSTER s FROM POSTGRES CONNECTION pg (PUBLICATION 'antithesis')",
        "CREATE TABLE pg_a FROM SOURCE pg_src (REFERENCE upstream.a)",
        "CREATE TABLE pg_b FROM SOURCE pg_src (REFERENCE upstream.b)",
        f"CREATE CONNECTION kafka TO KAFKA (BROKER '{KAFKA}', SECURITY PROTOCOL PLAINTEXT)",
        "CREATE SINK sums_sink IN CLUSTER s FROM sums INTO KAFKA CONNECTION kafka"
        " (TOPIC 'sums') KEY (k) NOT ENFORCED FORMAT JSON ENVELOPE UPSERT",
        "CREATE SOURCE kafka_src IN CLUSTER s FROM KAFKA CONNECTION kafka (TOPIC 'sums')",
        'CREATE TABLE sums_rt FROM SOURCE kafka_src (REFERENCE "sums")'
        " KEY FORMAT JSON VALUE FORMAT JSON ENVELOPE UPSERT",
    ]
    w = Worker(0)
    for session, statements in (
        (w.pg, upstream),
        # Under orchestratord the role `materialize` exists once it logged in.
        (mz_session(options=""), ["SELECT 1"]),
        (mz_session("mz_system", ""), system),
        (mz_session(options=""), user),
    ):
        for statement in statements:
            retry(statement, lambda: session(statement))
    # Every timeline starts from here, so it starts with everything readable.
    for rule in (write, read, views, increment, read_counter, pg_read, kafka_read):
        retry(rule.__name__, lambda: rule(w))


def persist_backends() -> tuple[str, str]:
    if KUBERNETES:
        urls = kubectl(
            "get",
            "secret",
            "materialize-backend",
            "-n",
            NAMESPACE,
            "-o",
            'jsonpath={.data.metadata_backend_url}{" "}{.data.persist_backend_url}',
        ).split()
        metadata, blob = (base64.b64decode(url).decode() for url in urls)
        parts = urlparse(metadata)
        query = parse_qsl(parts.query) + [("options", "--search_path=consensus")]
        consensus = parts._replace(query=urlencode(query)).geturl()
    else:
        consensus, blob = os.environ["CONSENSUS_URI"], os.environ["BLOB_URI"]
    return consensus, blob


def recover(workers: list[threading.Thread], backends: tuple[str, str]) -> None:
    """Stops the faults and asserts that everything recovers."""
    quiesced.set()
    log("stopping faults")
    if "ANTITHESIS_STOP_FAULTS" in os.environ:
        subprocess.run([os.environ["ANTITHESIS_STOP_FAULTS"], "86400"], check=False)
    deadline = time.monotonic() + 60
    for worker in workers:
        worker.join(max(0, deadline - time.monotonic()))
    deadline = time.monotonic() + RECOVERY_SECONDS

    def tables() -> None:
        w = Worker(WORKERS)
        for rule in (write, read, views, increment, read_counter, pg_read, kafka_read):
            rule(w)

    def pg_source() -> Any:
        w = Worker(WORKERS)
        for table in "ab":
            upstream = w.pg(f"SELECT id, v FROM upstream.{table}")
            if sorted(upstream) != sorted(w.mz(f"SELECT id, v FROM pg_{table}")):
                return f"pg_{table} differs from the upstream"

    def kafka() -> Any:
        return mz_session()(f"SELECT {differ(f'({ROUND_TRIP})', 'sums')}")[0][0]

    def replicas() -> Any:
        return mz_session()(
            "SELECT r.name, s.status FROM mz_cluster_replicas r"
            " JOIN mz_internal.mz_cluster_replica_statuses s ON s.replica_id = r.id"
            " WHERE s.status <> 'online'"
        )

    def applied() -> Any:
        if not KUBERNETES:
            return None
        cr = resource()
        if phase(cr) == "ReadyToPromote":
            patch(forcePromote=cr["spec"]["requestRollout"])
        if phase(cr) == "WaitingForApproval":
            request_rollout()
        return not up_to_date(cr) and f"not up to date: {phase(cr)}"

    def persist() -> Any:
        return audit(
            mz_session("mz_system", "-c cluster=mz_catalog_server"),
            *backends,
            rnd,
            lambda: False,
            deadline,
        )

    def recovers(check: Callable[[], Any]) -> None:
        # Until the check neither raises nor returns a problem.
        while True:
            try:
                problem = check()
            except Exception as e:
                problem = repr(e)
            if not problem or time.monotonic() > deadline:
                problems[check.__name__] = {"problem": str(problem)} if problem else {}
                return
            time.sleep(2)

    # Side by side: a timeline seldom lives for minutes once faults stop.
    problems: dict[str, dict[str, str]] = {}
    checks = [tables, pg_source, kafka, replicas, persist, applied]
    threads = [threading.Thread(target=recovers, args=(check,)) for check in checks]
    for thread in threads:
        thread.start()
    # A rollout in progress takes minutes, so the others are asserted first.
    for thread in threads[:-1]:
        thread.join()
    t, p, k, r, shards = (problems[check.__name__] for check in checks[:-1])
    always(not t, "every check passes again after faults stop", t)
    always(not p, "the Postgres source catches up after faults stop", p)
    always(not k, "the Kafka sink read back equals its view after faults stop", k)
    always(not r, "every replica is online after faults stop", r)
    always(not shards, "persist shards can be audited after faults stop", shards)
    threads[-1].join()
    a = problems["applied"]
    always(not a, "the resource is up to date after faults stop", a)
    log(f"after faults stopped: {problems}")
    reachable("the checks after faults stop ran to completion", {})


def main() -> None:
    global mz_host, kill_gap
    if KUBERNETES:
        mz_host = create_environment()
    setup()
    if KUBERNETES:
        # A rollout takes a minute to get here, longer than most timelines
        # last, so every timeline starts with a generation ready to promote.
        request_rollout("ManuallyPromote")
        while phase(resource()) != "ReadyToPromote":
            time.sleep(2)
    backends = persist_backends()
    setup_complete({"host": mz_host})
    # The pod's readiness probe. Antithesis also starts the test once every
    # resource is ready, which must not come before the setup is complete.
    Path("/tmp/ready").touch()
    log("setup complete")
    # Every timeline branches from the wait for this file, which the test
    # command `first_start` creates, so what was drawn before is the same in
    # all of them.
    while KUBERNETES and not Path("/workload-active").exists():
        time.sleep(0.1)

    # A timeline without DDL or without pod deletions lets the rest go further:
    # a rollout completes only after about two minutes in which no DDL commits
    # and nothing deletes environmentd.
    churn = rnd.choice([0, 1, 1, 2])
    kill_gap = rnd.choice([float("inf"), float("inf"), 20, 60])
    weights = DATA | {rule: weight * churn for rule, weight in CHURN.items()}
    if KUBERNETES:
        weights |= {write_to_candidate: 1, kill: 2, kill_after_ddl: churn}
    # Some timelines run with a few system parameters at other values. This has
    # no check of its own: every other check holds under each value.
    system = mz_session("mz_system", "")
    for name in rnd.sample(
        sorted(PARAMETERS), rnd.choice([0, 0, min(3, len(PARAMETERS))])
    ):
        statement = f"ALTER SYSTEM SET {name} = '{rnd.choice(PARAMETERS[name])}'"
        try:
            system(statement)
            log(statement)
        except (psycopg.Error, OSError) as e:
            log(f"{statement}: {e!r}")
    skip = os.environ.get("SKIP_RULES", "").split(",")
    weights = {rule: w for rule, w in weights.items() if rule.__name__ not in skip}
    log(f"churn {churn}, seconds between pod deletions {kill_gap}")
    # One thread follows the rollouts, so that the generations are compared in
    # the order read.
    rules = [weights] * WORKERS + ([{roll: 1}] if KUBERNETES else [])

    def audit_shards(w: Worker) -> None:
        audit(
            mz_session("mz_system", "-c cluster=mz_catalog_server"),
            *backends,
            rnd,
            quiesced.is_set,
        )

    rules.append({audit_shards: 1})
    workers = [
        threading.Thread(target=work, args=(n, r), daemon=True)
        for n, r in enumerate(rules)
    ]
    for worker in workers:
        worker.start()

    # Under Antithesis the moment at which faults stop is drawn, so that a run
    # recovers from many different states. A local run stops after DURATION.
    started = time.monotonic()
    duration = float(os.environ.get("DURATION", "inf"))
    while time.monotonic() - started < duration:
        time.sleep(1)
        if KUBERNETES and time.monotonic() - started > 60 and rnd.random() < 1 / 60:
            break
    recover(workers, backends)
    # Outside Antithesis the SDK only records assertions, in this file.
    output = Path(os.environ.get("ANTITHESIS_SDK_LOCAL_OUTPUT", os.devnull)).read_text()
    asserts = [
        json.loads(line).get("antithesis_assert") for line in output.splitlines()
    ]
    failed = [a for a in asserts if a and not a["condition"]]
    for a in failed:
        log(f"FAILED: {a['message']}: {a['details']}")
    # Not a clean exit of the interpreter: a worker may still be blocked.
    os._exit(1 if failed else 0)


if __name__ == "__main__":
    main()
