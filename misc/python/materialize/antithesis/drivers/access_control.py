# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Owners, privileges and cluster settings follow the acknowledged DDL history.

`driver_main` (`parallel_driver_access_control`) owns a small set of objects
nobody else touches: schema `materialize.acl` with tables `t1`, `t2` and view
`v1`, cluster `acl_c` (replication factor 0, so it costs no memory), owner
roles `acl_o1` and `acl_o2`, and grantee roles `acl_g1` and `acl_g2`. Each
invocation observes their owners, the privileges granted to the grantee
roles, and the cluster size, compares them with `AclModel`, then runs a few
`ALTER ... OWNER TO`, `GRANT`, `REVOKE` and `ALTER CLUSTER ... SET (SIZE)`
statements and observes again.

Owners move only among the owner roles and grants go only to the grantee
roles, so the owner's implicit privileges, which `ALTER OWNER` rewrites,
never land on a modelled key. Grants are compared without their grantor,
which `ALTER OWNER` also rewrites.

None of these statements changes the set of user items or replicas, so they
do not restart a read-only generation's catch-up: a candidate that booted
before them must pick them up from the durable catalog when it reboots as
leader. That makes the comparison after a promotion the check of
user-state-equal-across-generation-handoff for this state.

One invocation at a time holds an exclusive file lock across both
observations and every statement, so the model sees every change. Others exit
at once.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable,
    reachable,
    sometimes,
)

from materialize.antithesis import sql, state
from materialize.antithesis.drivers import rollouts
from materialize.antithesis.environment import Environment
from materialize.antithesis.isolation import AclModel, Key
from materialize.antithesis.rng import rng

STATE_DB = "access_control"
LOCK_FILE = "access_control.lock"
SCHEMA = "materialize.acl"
CLUSTER = "acl_c"
OWNERS = ("acl_o1", "acl_o2")
GRANTEES = ("acl_g1", "acl_g2")
SIZES = ("antithesis-1", "antithesis-2")
# Object name to (GRANT keyword, qualified name, privileges modelled).
OBJECTS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "t1": ("TABLE", f"{SCHEMA}.t1", ("SELECT", "INSERT")),
    "t2": ("TABLE", f"{SCHEMA}.t2", ("SELECT", "INSERT")),
    "v1": ("TABLE", f"{SCHEMA}.v1", ("SELECT",)),
    "acl": ("SCHEMA", SCHEMA, ("USAGE", "CREATE")),
    CLUSTER: ("CLUSTER", CLUSTER, ("USAGE", "CREATE")),
}
# `ALTER <keyword> <name> OWNER TO`.
OWNER_KEYWORD = {"t1": "TABLE", "t2": "TABLE", "v1": "VIEW", "acl": "SCHEMA"}
OWNER_KEYWORD[CLUSTER] = "CLUSTER"
# How long a statement with an unknown outcome may still take effect. Its
# session's statement timeout is `STATEMENT_TIMEOUT_MS`, and an environmentd
# that restarted cannot apply it at all. Not yet calibrated.
SETTLE_S = 600.0
STATEMENT_TIMEOUT_MS = 30_000
STEPS_MENU = (1, 2, 4)

OWNERS_SQL = """
SELECT o.name, r.name
FROM mz_catalog.mz_objects o
JOIN mz_catalog.mz_schemas s ON o.schema_id = s.id
JOIN mz_catalog.mz_databases d ON s.database_id = d.id
JOIN mz_catalog.mz_roles r ON o.owner_id = r.id
WHERE d.name = 'materialize' AND s.name = 'acl' AND o.type IN ('table', 'view')
UNION ALL
SELECT s.name, r.name
FROM mz_catalog.mz_schemas s
JOIN mz_catalog.mz_databases d ON s.database_id = d.id
JOIN mz_catalog.mz_roles r ON s.owner_id = r.id
WHERE d.name = 'materialize' AND s.name = 'acl'
UNION ALL
SELECT c.name, r.name
FROM mz_catalog.mz_clusters c
JOIN mz_catalog.mz_roles r ON c.owner_id = r.id
WHERE c.name = 'acl_c'
"""
GRANTS_SQL = """
SELECT name, grantee, privilege_type
FROM mz_internal.mz_show_all_privileges
WHERE grantee IN ('acl_g1', 'acl_g2')
  AND ((database = 'materialize' AND schema = 'acl')
    OR (object_type = 'schema' AND database = 'materialize' AND name = 'acl')
    OR (object_type = 'cluster' AND name = 'acl_c'))
"""
CLUSTER_SQL = "SELECT size FROM mz_catalog.mz_clusters WHERE name = 'acl_c'"


def log(message: str) -> None:
    print(f"access_control[{os.getpid()}]: {message}", flush=True)


def owner_key(name: str) -> str:
    return f"owner:{name}"


def grant_key(name: str, grantee: str, privilege: str) -> str:
    return f"grant:{name}:{grantee}:{privilege}"


SIZE_KEY = f"size:{CLUSTER}"


@contextmanager
def exclusive(path: str) -> Iterator[bool]:
    """Whether this process holds the lock; released when the process exits."""
    with open(path, "a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.execute(
        "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    db.commit()
    return db


def kv_get(db: sqlite3.Connection, key: str) -> Any:
    row = db.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def kv_set(db: sqlite3.Connection, key: str, value: Any) -> None:
    with db:
        db.execute(
            "INSERT INTO kv (key, value) VALUES (?, ?)"
            " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, json.dumps(value)),
        )


class Driver:
    def __init__(self) -> None:
        self.env = Environment()
        self.host = self.env.sql_host()
        self.kube = rollouts.Kube(self.env)
        self.db = open_state()
        stored = kv_get(self.db, "model") or {}
        self.model = AclModel(
            {k: Key.from_json(v) for k, v in stored.items()}, SETTLE_S
        )

    def save(self) -> None:
        kv_set(self.db, "model", {k: v.to_json() for k, v in self.model.keys.items()})

    def connect(self) -> psycopg.Connection:
        return sql.connect(
            self.host, internal=True, statement_timeout_ms=STATEMENT_TIMEOUT_MS
        )

    def setup(self) -> bool:
        """Create whatever is missing. Each statement is idempotent or skipped."""
        statements = [
            *(f"CREATE ROLE {r}" for r in (*OWNERS, *GRANTEES)),
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t1 (a int)",
            f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t2 (a int)",
            f"CREATE VIEW IF NOT EXISTS {SCHEMA}.v1 AS SELECT a FROM {SCHEMA}.t1",
            f"CREATE CLUSTER {CLUSTER} (SIZE '{SIZES[0]}', REPLICATION FACTOR 0)",
        ]
        if kv_get(self.db, "setup_done"):
            return True
        try:
            with self.connect() as conn:
                for statement in statements:
                    try:
                        conn.execute(statement.encode())
                    except psycopg.Error as e:
                        c = sql.classify(e)
                        if c.outcome != sql.Outcome.REJECTED:
                            log(f"setup: {statement}: {c.template}")
                            return False
        except (psycopg.Error, OSError) as e:
            log(f"setup: cannot connect: {e}")
            return False
        kv_set(self.db, "setup_done", True)
        return True

    def observe(self) -> dict[str, Any] | None:
        try:
            with self.connect() as conn:
                owners = dict(conn.execute(OWNERS_SQL.encode()).fetchall())
                grants = {
                    (str(n), str(g), str(p))
                    for n, g, p in conn.execute(GRANTS_SQL.encode()).fetchall()
                }
                size = conn.execute(CLUSTER_SQL.encode()).fetchone()
        except (psycopg.Error, OSError) as e:
            log(f"observation skipped: {sql.classify(e).template}")
            return None
        if set(owners) != set(OBJECTS) or size is None:
            log(f"observation incomplete: owners {owners}, size {size}")
            return None
        observed: dict[str, Any] = {owner_key(n): r for n, r in owners.items()}
        for name, (_, _, privileges) in OBJECTS.items():
            for grantee in GRANTEES:
                for privilege in privileges:
                    observed[grant_key(name, grantee, privilege)] = (
                        name,
                        grantee,
                        privilege,
                    ) in grants
        observed[SIZE_KEY] = size[0]
        return observed

    def check(self, origin: str) -> bool:
        snap = self.kube.try_snapshot()
        observed = self.observe()
        if observed is None:
            return False
        adopted = not all(k in self.model.keys for k in observed)
        mismatches = self.model.observe(observed, time.monotonic())
        self.save()
        families: dict[str, list[dict[str, Any]]] = {
            "owner": [],
            "grant": [],
            "size": [],
        }
        for m in mismatches:
            families[m["key"].split(":", 1)[0]].append(m)
        active = snap.active if snap else None
        details = {"origin": origin, "active_generation": active}
        always_or_unreachable(
            not families["owner"],
            "access control: object owners match the acknowledged DDL history",
            {**details, "mismatches": families["owner"]},
        )
        always_or_unreachable(
            not families["grant"],
            "access control: privilege grants match the acknowledged DDL history",
            {**details, "mismatches": families["grant"]},
        )
        always_or_unreachable(
            not families["size"],
            "access control: cluster size matches the acknowledged DDL history",
            {**details, "mismatches": families["size"]},
        )
        if not adopted:
            reachable("access control: compared observed state with the model", details)
            previous = kv_get(self.db, "last_check_active")
            sometimes(
                previous is not None and active is not None and active > previous,
                "access control: a check compared state acknowledged under an earlier active generation",
                {**details, "previous_active_generation": previous},
            )
        if active is not None:
            kv_set(self.db, "last_check_active", active)
        return True

    def step(self) -> None:
        kind = rng.choice(("owner", "grant", "revoke", "size"))
        name = rng.choice(list(OBJECTS))
        keyword, qualified, privileges = OBJECTS[name]
        if kind == "owner":
            role = rng.choice(OWNERS)
            key, value = owner_key(name), role
            statement = f"ALTER {OWNER_KEYWORD[name]} {qualified} OWNER TO {role}"
        elif kind == "size":
            size = rng.choice(SIZES)
            key, value = SIZE_KEY, size
            statement = f"ALTER CLUSTER {CLUSTER} SET (SIZE '{size}')"
        else:
            grantee = rng.choice(GRANTEES)
            privilege = rng.choice(privileges)
            key, value = grant_key(name, grantee, privilege), kind == "grant"
            statement = (
                f"GRANT {privilege} ON {keyword} {qualified} TO {grantee}"
                if kind == "grant"
                else f"REVOKE {privilege} ON {keyword} {qualified} FROM {grantee}"
            )
        snap = self.kube.try_snapshot()
        sent_at = time.monotonic()
        self.model.sent(key, value, sent_at)
        self.save()
        try:
            with self.connect() as conn:
                conn.execute(statement.encode())
        except (psycopg.Error, OSError) as e:
            c = sql.classify(e)
            log(f"{statement}: {c.outcome.value} {c.sqlstate} {c.template}")
            if c.outcome == sql.Outcome.REJECTED:
                self.model.rejected(key, value, sent_at)
            self.save()
            return
        self.model.acknowledged(key, value, sent_at)
        self.save()
        sometimes(
            snap is not None and snap.reason in ("Applying", "ReadyToPromote"),
            "access control: DDL acknowledged while a read-only generation was live",
            {"statement": statement, "cr": snap.summary() if snap else None},
        )

    def run(self) -> None:
        if not self.setup():
            return
        if not self.check("before"):
            return
        for _ in range(rng.choice(STEPS_MENU)):
            self.step()
        self.check("after")


def driver_main() -> int:
    env = Environment()
    lock = str(env.endpoints.state_dir / LOCK_FILE)
    env.endpoints.state_dir.mkdir(parents=True, exist_ok=True)
    with exclusive(lock) as held:
        if not held:
            log("another invocation holds the lock")
            return 0
        try:
            Driver().run()
        except rollouts.TRANSIENT_ERRORS as e:
            log(f"invocation ended early: {e}")
    return 0
