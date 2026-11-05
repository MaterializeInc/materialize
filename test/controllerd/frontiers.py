#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

"""Check native frontier publication through same-deployment adapter churn.

Run with bin/pyactivate against an existing local controller, for example:
  bin/pyactivate test/controllerd/frontiers.py --controller http://127.0.0.1:6882

Requires catalog read protection already enabled, a usable compute cluster, and
Linux pidfds plus permission to inspect/signal the managed adapter processes.
The controller API currently has no restart operation. Only the newly created
adapter is signaled. No configuration is changed outside this script's sessions.
Timeouts allow development builds. Polling phases use --timeout deadlines, with
up to one in-flight sample beyond the deadline. --operation-timeout bounds SQL
statements server-side and HTTP requests. A sample issues three SQL statements.
"""

import argparse
import ipaddress
import json
import os
import signal
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import psutil
import psycopg
from psycopg import sql


def request(controller, path, timeout, method="GET", payload=None):
    req = urllib.request.Request(
        controller + path,
        method=method,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    started = time.monotonic()
    with urllib.request.urlopen(req, timeout=timeout) as response:
        result = json.load(response)
    assert time.monotonic() - started < timeout, f"HTTP {method} {path} unresponsive"
    return result


def connect(adapter, timeout, cluster):
    host, port = adapter["endpoints"]["sql-external"][0].rsplit(":", 1)
    conn = psycopg.connect(
        host=host.strip("[]"),
        port=int(port),
        user="materialize",
        dbname="materialize",
        connect_timeout=timeout,
        autocommit=True,
        options=f"-c statement_timeout={timeout * 1000}",
    )
    try:
        conn.execute(sql.SQL("SET cluster = {}").format(sql.Identifier(cluster)))
        conn.execute("SET transaction_isolation = 'strict serializable'")
    except BaseException:
        conn.close()
        raise
    return conn


def at_least(old, new):
    """Unknown is bottom. Known NULL is the empty antichain, above every time."""
    old_known, old_time = old
    new_known, new_time = new
    if not old_known:
        return True
    if not new_known:
        return False
    if new_time is None:
        return True
    return old_time is not None and new_time >= old_time


def identities(conn, schema):
    return conn.execute(
        """
        SELECT o.name, g.global_id, c.data->'value'->>'shard'
        FROM mz_catalog.mz_objects o
        JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
        JOIN mz_catalog.mz_databases d ON d.id = s.database_id
        JOIN mz_internal.mz_object_global_ids g ON g.id = o.id
        LEFT JOIN mz_internal.mz_catalog_shared c
          ON c.data->>'kind' = 'StorageCollectionMetadata'
         AND mz_internal.parse_catalog_id(c.data->'key'->'id') = g.global_id
        WHERE d.name = current_database() AND s.name = %s
        ORDER BY o.name, g.global_id
        """,
        (schema,),
    ).fetchall()


class Observer:
    """One history per persistent SQL session, never shared across reconnects."""

    def __init__(self, conn, label, schema, objects):
        self.conn = conn
        self.label = label
        self.schema = schema
        self.objects = objects
        self.shards = {gid: shard for _, gid, shard in objects}
        self.expected = {
            (kind, gid, shard)
            for _, gid, shard in objects
            for kind in ("raw", "current", "public")
            if shard is not None or kind == "public"
        }
        self.frontiers = {}
        self.rows = {"input": set(), "output": set()}
        self.identity_seen = False
        self.samples = 0

    def sample(self):
        objects = identities(self.conn, self.schema)
        if self.identity_seen:
            assert objects == self.objects, (
                self.label,
                "object/shard identity changed",
                objects,
            )
        elif objects != self.objects:
            # A new session may not yet have replayed the test DDL.
            return False
        self.identity_seen = True
        records = self.conn.execute(
            """
            SELECT 'raw', object_id, data_shard, read_known,
                   read_frontier::text, write_known, write_frontier::text
            FROM mz_internal.mz_storage_frontier_observations
            WHERE object_id = ANY(%s)
            UNION ALL
            SELECT 'current', object_id, NULL, read_known,
                   read_frontier::text, write_known, write_frontier::text
            FROM mz_internal.mz_current_storage_frontiers
            WHERE object_id = ANY(%s)
            UNION ALL
            SELECT 'public', object_id, NULL, true,
                   read_frontier::text, true, write_frontier::text
            FROM mz_internal.mz_frontiers
            WHERE object_id = ANY(%s)
            """,
            (list(self.shards),) * 3,
        ).fetchall()
        current = {}
        for kind, gid, shard, rk, read, wk, write in records:
            if kind == "raw":
                assert shard == self.shards[gid], (
                    self.label,
                    "unexpected shard",
                    gid,
                    shard,
                )
            key = (kind, gid, self.shards[gid])
            assert key in self.expected, (self.label, "unexpected frontier", key)
            assert key not in current, (self.label, "duplicate frontier", key)
            fields = (
                (rk, None if read is None else int(read)),
                (wk, None if write is None else int(write)),
            )
            for known, value in fields:
                assert known or value is None, (
                    self.label,
                    "unknown with a timestamp",
                    key,
                )
            if key in self.frontiers:
                assert all(
                    at_least(a, b) for a, b in zip(self.frontiers[key], fields)
                ), (self.label, "frontier regression", key, self.frontiers[key], fields)
            current[key] = fields
        assert self.frontiers.keys() <= current.keys(), (
            self.label,
            "lost established frontier rows",
            self.frontiers.keys() - current.keys(),
        )
        self.frontiers = current
        # Exact row sets catch loss and duplicates, rather than allowing new
        # inserts to conceal loss behind an increasing count.
        records = self.conn.execute(
            sql.SQL(
                "SELECT 'input', n FROM {} UNION ALL SELECT 'output', n FROM {}"
            ).format(
                sql.Identifier(self.schema, "input"),
                sql.Identifier(self.schema, "output"),
            )
        ).fetchall()
        rows = {"input": set(), "output": set()}
        for name, n in records:
            assert n not in rows[name], (self.label, "duplicate data row", name, n)
            rows[name].add(n)
        for name in rows:
            assert self.rows[name] <= rows[name], (self.label, "lost data rows", name)
        self.rows = rows
        self.samples += 1
        return self.expected == current.keys() and all(
            known and value is not None
            for fields in current.values()
            for known, value in fields
        )

    def checkpoint(self):
        return (
            dict(self.frontiers),
            {name: set(rows) for name, rows in self.rows.items()},
        )

    def caught_up(self, checkpoint):
        frontiers, rows = checkpoint
        return all(
            key in self.frontiers
            and all(at_least(a, b) for a, b in zip(fields, self.frontiers[key]))
            for key, fields in frontiers.items()
        ) and all(values <= self.rows[name] for name, values in rows.items())


def adapter_process(adapter, inventory):
    """Find one local adapter by command identity AND its assigned SQL socket."""
    assert adapter["generation"] == inventory["generation"]
    assert (
        adapter["service"] == f"adapter-{adapter['id']}-gen-{inventory['generation']}"
    )
    required = {
        f"--adapter-id={adapter['id']}",
        f"--deploy-generation={inventory['generation']}",
        f"--environment-id={inventory['environment_id']}",
        "--orchestrator=process",
        "--process=0",
    }
    port = int(adapter["endpoints"]["sql-external"][0].rsplit(":", 1)[1])
    matches = []
    for process in psutil.process_iter():
        try:
            command = process.cmdline()
            if not required <= set(command):
                continue
            assert (
                Path(process.exe()).name == "adapterd"
            ), "refusing to signal a non-adapter executable"
            listeners = [
                arg.split("=", 1)[1]
                for arg in command
                if arg.startswith("--listeners-config-json=")
            ]
            assert len(listeners) == 1, "cannot verify managed listener configuration"

            address = json.loads(listeners[0])["sql"]["external"]["addr"]
            assert (
                int(address.rsplit(":", 1)[1]) == port
            ), "command does not match returned SQL endpoint"
            assert any(
                c.status == psutil.CONN_LISTEN and c.laddr.port == port
                for c in process.net_connections(kind="tcp")
            ), "process does not own returned SQL listener"
            matches.append(process)
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
        except psutil.AccessDenied:
            # Unrelated users' processes are not candidates. A hidden target
            # results in no match, never a weaker signaling fallback.
            continue
    assert (
        len(matches) == 1
    ), f"cannot safely identify {adapter['id']}: {len(matches)} matching processes"
    return matches[0]


def alive(process):
    try:
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--controller", required=True)
    parser.add_argument("--adapter", default="a1", help="existing adapter name (or ID)")
    parser.add_argument("--cluster", default="quickstart")
    parser.add_argument("--timeout", type=int, default=180, help="seconds per phase")
    parser.add_argument(
        "--operation-timeout",
        type=int,
        default=30,
        help="seconds per SQL/HTTP operation",
    )
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError("run without -O: this test requires assertions")
    assert args.timeout > args.operation_timeout > 0
    assert hasattr(os, "pidfd_open") and hasattr(
        signal, "pidfd_send_signal"
    ), "safe restart requires Linux pidfds"
    controller = args.controller.rstrip("/")
    host = urllib.parse.urlsplit(controller).hostname
    assert (
        host == "localhost" or ipaddress.ip_address(host).is_loopback
    ), "TERM restart requires a local controller URL"

    def api(path, **kwargs):
        return request(controller, path, args.operation_timeout, **kwargs)

    inventory = api("/api/inventory")
    adapters = api("/api/adapters")
    matches = [a for a in adapters if a["name"] == args.adapter]
    if not matches:
        matches = [a for a in adapters if a["id"] == args.adapter]
    assert len(matches) == 1, "select one existing survivor adapter"
    survivor = matches[0]
    assert (survivor.get("readiness") or {}).get("ready"), "survivor is not ready"
    assert survivor["generation"] == inventory["generation"]
    schema = "frontiers_" + uuid.uuid4().hex
    peer_name = schema + "_peer"
    print(json.dumps({"schema": schema, "peer_name": peer_name}), flush=True)
    peer_id = None
    peer_conn = None
    worker = None
    stop = threading.Event()
    lock = threading.Lock()
    state = {"error": None, "n": 0, "checkpoint": None, "samples": 0}
    checks = []
    owner = connect(survivor, args.operation_timeout, args.cluster)
    created_schema = False
    peer_attempted = False

    def status():
        with lock:
            result = dict(state)
        if result["error"] is not None:
            raise RuntimeError("survivor writer/observer failed") from result["error"]
        return result

    def wait(label, condition):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            status()
            adapters = api("/api/adapters")
            value = condition(adapters)
            if value:
                checks.append(label)
                print(
                    json.dumps({"check": label, "survivor_writes": status()["n"]}),
                    flush=True,
                )
                return value
            stop.wait(0.5)
        raise TimeoutError(f"{label}: no completion within {args.timeout}s")

    def progressed(before):
        now = status()
        if now["checkpoint"] is None or now["n"] < before["n"] + 3:
            return False
        old = before["checkpoint"][0]
        new = now["checkpoint"][0]
        return all(
            key in new
            and new[key][1][0]
            and new[key][1][1] is not None
            and new[key][1][1] > fields[1][1]
            for key, fields in old.items()
        )

    def ready(adapters):
        return next(
            (
                a
                for a in adapters
                if a["id"] == peer_id and (a.get("readiness") or {}).get("ready")
            ),
            None,
        )

    try:
        assert owner.execute(
            "SELECT catalog_read_protection_enabled FROM mz_internal.mz_catalog_deployment_authority"
        ).fetchone() == (
            True,
        ), "requires native catalog read protection already enabled"
        created_schema = True
        owner.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        table = sql.Identifier(schema, "input")
        owner.execute(sql.SQL("CREATE TABLE {} (n INT)").format(table))
        owner.execute(
            sql.SQL("CREATE INDEX {} ON {} (n)").format(
                sql.Identifier("input_idx"), table
            )
        )
        owner.execute(
            sql.SQL("CREATE MATERIALIZED VIEW {} AS SELECT n FROM {}").format(
                sql.Identifier(schema, "output"), table
            )
        )

        def resolve_objects(_):
            objects = identities(owner, schema)
            if {name for name, _, _ in objects} != {"input", "input_idx", "output"}:
                return None
            # Fresh objects have one global ID each. No ALTER or replacement is
            # part of this test, so any later identity change is unexpected.
            assert (
                len(objects) == 3 and len({gid for _, gid, _ in objects}) == 3
            ), objects
            return (
                objects
                if all(
                    (shard is not None) == (name != "input_idx")
                    for name, _, shard in objects
                )
                else None
            )

        objects = wait("resolve actual global IDs and storage shards", resolve_objects)
        observer = Observer(owner, "survivor", schema, objects)

        def publish():
            try:
                n = 0
                while not stop.is_set():
                    n += 1
                    owner.execute(
                        sql.SQL("INSERT INTO {} VALUES (%s)").format(table), (n,)
                    )
                    complete = observer.sample()
                    expected_rows = set(range(1, n + 1))
                    assert all(
                        rows == expected_rows for rows in observer.rows.values()
                    ), (
                        "acknowledged writes missing from table or materialized view",
                        n,
                    )
                    with lock:
                        if state["checkpoint"] is not None:
                            assert (
                                complete
                            ), "live object frontiers became unknown or empty"
                        state.update(
                            n=n,
                            samples=observer.samples,
                            checkpoint=observer.checkpoint() if complete else None,
                        )
                    stop.wait(1)
            except BaseException as error:
                with lock:
                    state["error"] = error

        worker = threading.Thread(target=publish, daemon=True)
        worker.start()
        wait(
            "establish finite raw/current/public frontiers",
            lambda _: status()["checkpoint"] is not None,
        )
        before = status()
        peer_attempted = True
        created = api(
            "/api/adapters",
            method="POST",
            payload={"name": peer_name, "size": survivor["size"]},
        )
        assert created["name"] == peer_name and created["id"] != survivor["id"], created
        peer_id = created["id"]
        peer = wait(
            "survivor SQL/frontier progress during peer join",
            lambda rows: ready(rows) if progressed(before) else None,
        )
        assert peer["generation"] == inventory["generation"]
        peer_conn = connect(peer, args.operation_timeout, args.cluster)
        peer_observer = Observer(peer_conn, "peer-before-restart", schema, objects)
        target = status()["checkpoint"]
        wait(
            "joining peer catches up to survivor",
            lambda _: peer_observer.sample()
            and peer_observer.samples >= 3
            and peer_observer.caught_up(target),
        )
        prior_peer = peer_observer.checkpoint()
        peer_conn.close()
        peer_conn = None
        before = status()
        process = adapter_process(peer, inventory)
        birth = process.create_time()
        # Bind the signal to this process, not a PID that could have been reused
        # between validation and delivery. Revalidate after acquiring the pidfd.
        fd = os.pidfd_open(process.pid)
        try:
            verified = adapter_process(peer, inventory)
            assert (
                verified.pid == process.pid
                and verified.create_time() == birth
                and process.is_running()
            ), "adapter process changed before TERM"
            signal.pidfd_send_signal(fd, signal.SIGTERM)
        finally:
            os.close(fd)

        def restarted(rows):
            row = ready(rows)
            if (
                not row
                or row["readiness"]["changed_at"] == peer["readiness"]["changed_at"]
            ):
                return None
            if alive(process):
                return None
            return row if progressed(before) else None

        peer = wait("survivor SQL/frontier progress during peer restart", restarted)
        replacement = adapter_process(peer, inventory)
        assert (replacement.pid, replacement.create_time()) != (process.pid, birth)
        peer_conn = connect(peer, args.operation_timeout, args.cluster)
        peer_observer = Observer(peer_conn, "peer-after-restart", schema, objects)
        target = status()["checkpoint"]
        wait(
            "restarted peer catches up to survivor and pre-restart observations",
            lambda _: peer_observer.sample()
            and peer_observer.samples >= 3
            and peer_observer.caught_up(target)
            and peer_observer.caught_up(prior_peer),
        )
        peer_conn.close()
        peer_conn = None
        before = status()
        api(f"/api/adapters/{peer_id}", method="DELETE")
        wait(
            "survivor SQL/frontier progress after peer removal",
            lambda rows: all(a["id"] != peer_id for a in rows)
            and not alive(replacement)
            and progressed(before),
        )
        peer_id = None
        peer_attempted = False
        print(
            json.dumps(
                {
                    "checks": checks,
                    "objects": objects,
                    "survivor_samples": status()["samples"],
                    "restart": "identity-checked SIGTERM",
                }
            ),
            flush=True,
        )
    finally:
        stop.set()
        cleanup_errors = []
        if worker is not None:
            worker.join(timeout=4 * args.operation_timeout + 10)
            if worker.is_alive():
                cleanup_errors.append("survivor worker did not stop")
        if peer_conn is not None:
            peer_conn.close()
        owner.close()
        # POST can commit even if its response times out. Recover only the
        # unique name generated here, never a pre-existing adapter.
        if peer_attempted:
            try:
                found = False
                for adapter in api("/api/adapters"):
                    if adapter["name"] == peer_name:
                        found = True
                        assert adapter["id"] != survivor["id"]
                        api(f"/api/adapters/{adapter['id']}", method="DELETE")
                if peer_id is None and not found:
                    cleanup_errors.append(
                        f"POST outcome unknown: inspect adapter {peer_name}"
                    )
            except Exception as error:
                cleanup_errors.append(f"adapter {peer_name}: {error}")
        if created_schema:
            try:
                with connect(survivor, args.operation_timeout, args.cluster) as cleanup:
                    for kind, name in (
                        ("MATERIALIZED VIEW", "output"),
                        ("INDEX", "input_idx"),
                        ("TABLE", "input"),
                    ):
                        cleanup.execute(
                            sql.SQL("DROP {} IF EXISTS {}").format(
                                sql.SQL(kind), sql.Identifier(schema, name)
                            )
                        )
                    cleanup.execute(
                        sql.SQL("DROP SCHEMA IF EXISTS {}").format(
                            sql.Identifier(schema)
                        )
                    )
            except Exception as error:
                cleanup_errors.append(f"schema {schema}: {error}")
        if cleanup_errors:
            print(json.dumps({"cleanup_errors": cleanup_errors}), file=sys.stderr)
            if sys.exc_info()[0] is None:
                raise RuntimeError("cleanup failed")
        if sys.exc_info()[0] is None:
            status()


if __name__ == "__main__":
    main()
