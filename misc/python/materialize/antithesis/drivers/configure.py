# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Per-timeline system parameter configuration (`first_configure`).

Each timeline rolls one configuration and writes it into the `system-params`
ConfigMap, which environmentd re-reads every second through
`--config-sync-file-path`. The roll has two layers:

* A profile: `production` (code defaults), `test` (the defaults mzcompose
  gives every CI run, from `get_minimal_system_parameters` plus the default of
  every `get_variable_system_parameters` entry), or `test-random` (the same,
  with each variable parameter drawn from its CI value list).
* Headline choices drawn independently of the profile: the two-implementation
  flags from flag-history-independent-correctness,
  `persist_reader_lease_duration` (production order by default, a short-lease
  arm at `SHORT_LEASE_P`), and the 0dt deployment windows.
* Timeline arms that are workload settings, not parameters (`roll_arms`).

Precedence, lowest first: the `render.OVERRIDABLE_BASE_PARAMETERS` defaults,
profile, headline, `HARNESS_REQUIRED`, then the rest of
`render.BASE_SYSTEM_PARAMETERS`, which always wins because the harness depends
on it. `guard_panic` then adjusts the merged map.

Contracts for other drivers:

* Cluster `SHARED_CLUSTER` (`antithesis_shared`) exists after this command,
  managed, size `antithesis-1`, replication factor 1 or 2. Drivers may create
  objects in it but must not drop or resize it.
* State database `config` (see `state.open_db`):
  - `profile(key, value)`: the rolled profile name, every headline choice, and
    every arm (`lease_arm`, `pg_terminal_upstream_ops`). Read with
    `profile_value`.
  - `system_params(name, value)`: every parameter this command wrote.
  - `flag_history(seq, at, name, value, source)`: one row per value a flip-set
    flag has taken, `source` is `first` or `mid-run`. Oracles tag verdicts
    with this to tell "broke after a flip" from "broke under one value". The
    reader lease is recorded too, with source
    `first-short-lease-reversed-order` in the short-lease arm.
* `roll_flag` flips one `FLIP_FLAGS` entry mid-run. Only the lifecycle driver
  calls it today.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
import time
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)
from kubernetes import client  # type: ignore
from kubernetes.client.rest import ApiException  # type: ignore

from materialize.antithesis import sql, state
from materialize.antithesis.environment import Environment
from materialize.antithesis.render import (
    BASE_SYSTEM_PARAMETERS,
    OVERRIDABLE_BASE_PARAMETERS,
)
from materialize.antithesis.rng import rng

# `materialize.mzcompose` pulls in `semver` and `colored`. If the workload
# image lacks them the `test` profiles are unavailable and their `sometimes`
# arms stay unfired, which the triage report shows.
try:
    from materialize.mz_version import MzVersion
    from materialize.mzcompose import (
        get_minimal_system_parameters,
        get_variable_system_parameters,
    )

    MZCOMPOSE_IMPORT_ERROR: Exception | None = None
except ImportError as e:  # pragma: no cover - depends on the image
    MZCOMPOSE_IMPORT_ERROR = e

CONFIGMAP = "system-params"
CONFIGMAP_KEY = "system-params.json"
SHARED_CLUSTER = "antithesis_shared"
STATE_DB = "config"

# Kubelet propagates a ConfigMap volume update within its sync period plus
# its cache TTL (about 1 to 2 min with defaults), then environmentd's sync
# loop picks it up within a second. Not yet measured under Antithesis; 10 min
# leaves room for a slow kubelet on one simulated core.
SYNC_TIMEOUT_S = 600
SQL_TIMEOUT_S = 600
CONFIGMAP_CAS_ATTEMPTS = 20

# Flags G1 needs regardless of profile. `production` keeps them at code
# defaults otherwise.
HARNESS_REQUIRED: dict[str, str] = {
    "enable_alter_table_add_column": "true",
    "enable_replacement_materialized_views": "true",
    "enable_logical_compaction_window": "true",
}

# Keys the mzcompose profiles set that conflict with the harness. The CR fixes
# the authenticator to None, and every driver connects as `materialize`, which
# holds no privileges on objects other roles create once RBAC is checked.
HARNESS_EXCLUDED = {"enable_password_auth", "enable_rbac_checks"}

# Flags that choose between two implementations of one contract
# (flag-history-independent-correctness). Mid-run rolls flip these.
FLIP_FLAGS = (
    "enable_upsert_v2",
    "enable_upsert_chunked_stash",
    "enable_frontend_peek_sequencing",
    "enable_frontend_subscribes",
    "enable_cluster_reconfiguration_lag_gate",
)

# Every value exceeds the hard-coded 300 s remap `deferred_expire`, so the
# production order (reader lease longer than deferred_expire) holds
# (hard-coded-windows-are-exceeded). 900 s is the code default, 301 s sits just
# past the boundary. Readers heartbeat at lease / 4.
LEASE_MENU = ("900s", "600s", "360s", "301s")
# NOTE: the short-lease arm reverses the production order: a reader lease
# shorter than `deferred_expire` can expire under a remap subscribe that is
# still within its window. It exists because pauses and stalls on Antithesis
# rarely reach 301 s, which leaves every lease-expiry path unexplored. Findings
# from this arm may be harness artifacts; `profile.lease_arm` and the
# `flag_history` source tag mark them.
SHORT_LEASE_MENU = ("30s", "60s")
SHORT_LEASE_P = 0.2
LEASE_ARM_KEY = "lease_arm"
LEASE_ARM_PRODUCTION = "production-order"
LEASE_ARM_SHORT = "short-reversed-order"
LEASE_PARAM = "persist_reader_lease_duration"
# Per-timeline arm read by `postgres_sources`: with "off", the upstream driver
# never TRUNCATEs or DROPs a table, so source oracles run without the known
# terminal-op restart bugs in play.
PG_TERMINAL_OPS_KEY = "pg_terminal_upstream_ops"
PG_TERMINAL_OPS_OFF_P = 0.25
# 8760h is the code default (one year, never cut over un-hydrated); 1800s is
# the mzcompose value; the short values let a slow catch-up hit the timeout.
MAX_WAIT_MENU = ("8760h", "1800s", "300s", "60s")
# With `enable_0dt_deployment_panic_after_timeout` on, a short max wait turns
# every slow 0dt catch-up into a designed panic loop, which is a harness
# artifact. Below this floor the panic flag is forced off.
PANIC_MIN_MAX_WAIT_S = 1800
PANIC_FLAG = "enable_0dt_deployment_panic_after_timeout"
MAX_WAIT = "with_0dt_deployment_max_wait"
CODE_DEFAULT_MAX_WAIT = "8760h"
# 300s is the code default.
DDL_CHECK_MENU = ("300s", "30s", "5s", "1s")

_UNIT_US = {
    "us": 1,
    "ms": 1_000,
    "s": 1_000_000,
    "min": 60_000_000,
    "h": 3_600_000_000,
    "d": 86_400_000_000,
}


def log(message: str) -> None:
    print(f"configure: {message}", flush=True)


def normalize(value: Any) -> str:
    """Canonical form of a parameter value, so `SHOW` output compares equal
    to what was written (`30 min` to `1800s`, `on` to `true`)."""
    s = str(value).strip().strip("'\"").lower()
    if s in ("on", "true", "t"):
        return "true"
    if s in ("off", "false", "f"):
        return "false"
    m = re.fullmatch(r"(\d+)\s*(us|ms|s|min|h|d)?", s)
    if m:
        return f"{int(m[1]) * _UNIT_US[m[2] or 'ms']}us"
    return s


def duration_seconds(value: str) -> float:
    return int(normalize(value).removesuffix("us")) / 1_000_000


def open_state() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.execute("CREATE TABLE IF NOT EXISTS profile (key TEXT PRIMARY KEY, value TEXT)")
    db.execute(
        "CREATE TABLE IF NOT EXISTS system_params (name TEXT PRIMARY KEY, value TEXT)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS flag_history ("
        " seq INTEGER PRIMARY KEY AUTOINCREMENT, at REAL, name TEXT, value TEXT,"
        " source TEXT)"
    )
    db.commit()
    return db


def guard_panic(params: dict[str, Any]) -> dict[str, Any]:
    """`params` with the 0dt panic flag forced off if the max wait is below
    `PANIC_MIN_MAX_WAIT_S`."""
    panic = normalize(params.get(PANIC_FLAG, "false")) == "true"
    wait = duration_seconds(str(params.get(MAX_WAIT, CODE_DEFAULT_MAX_WAIT)))
    if panic and wait < PANIC_MIN_MAX_WAIT_S:
        return {**params, PANIC_FLAG: "false"}
    return params


def update_system_params(namespace: str, updates: dict[str, str]) -> dict[str, Any]:
    """Merge `updates` into the ConfigMap in the precedence the module
    docstring gives.

    Uses the read `resourceVersion` for optimistic concurrency, so concurrent
    callers never lose each other's keys. Returns the written map.
    """
    defaults = {
        k: v
        for k, v in BASE_SYSTEM_PARAMETERS.items()
        if k in OVERRIDABLE_BASE_PARAMETERS
    }
    fixed = {
        k: v
        for k, v in BASE_SYSTEM_PARAMETERS.items()
        if k not in OVERRIDABLE_BASE_PARAMETERS
    }
    api = client.CoreV1Api()
    for _ in range(CONFIGMAP_CAS_ATTEMPTS):
        cm = api.read_namespaced_config_map(CONFIGMAP, namespace)
        data = cm.data or {}
        try:
            current = json.loads(data.get(CONFIGMAP_KEY) or "{}")
        except json.JSONDecodeError:
            current = {}
        merged = guard_panic({**defaults, **current, **updates, **fixed})
        cm.data = {**data, CONFIGMAP_KEY: json.dumps(merged, indent=2, sort_keys=True)}
        try:
            api.replace_namespaced_config_map(CONFIGMAP, namespace, cm)
            return merged
        except ApiException as e:
            if e.status != 409:
                raise
    raise RuntimeError("system-params ConfigMap update kept conflicting")


def show(conn: psycopg.Connection, name: str) -> str:
    row = conn.execute(f"SHOW {name}".encode()).fetchone()
    return str(row[0]) if row else ""


def show_or_none(conn: psycopg.Connection, name: str) -> str | None:
    """`SHOW name`, or None if this version does not know the parameter."""
    try:
        return show(conn, name)
    except psycopg.Error as e:
        if sql.classify(e).outcome is sql.Outcome.INDETERMINATE:
            raise
        return None


def mz_version(conn: psycopg.Connection) -> str:
    row = conn.execute("SELECT mz_version()").fetchone()
    return str(row[0]).split()[0] if row else ""


def profile_params(profile: str, version: str) -> dict[str, str]:
    if profile == "production":
        return {}
    v = MzVersion.parse_mz(version)
    params = dict(get_minimal_system_parameters(v))
    for p in get_variable_system_parameters(v, False, "postgres-metadata"):
        params[p.key] = p.default if profile == "test" else rng.choice(p.values)
    return {k: v for k, v in params.items() if k not in HARNESS_EXCLUDED}


def roll_headline() -> dict[str, str]:
    short = rng.random() < SHORT_LEASE_P
    return {
        "enable_upsert_v2": rng.choice(["true", "false"]),
        "enable_frontend_peek_sequencing": rng.choice(["true", "false"]),
        LEASE_PARAM: rng.choice(SHORT_LEASE_MENU if short else LEASE_MENU),
        MAX_WAIT: rng.choice(MAX_WAIT_MENU),
        "with_0dt_deployment_ddl_check_interval": rng.choice(DDL_CHECK_MENU),
    }


def roll_arms(headline: dict[str, str]) -> dict[str, str]:
    """Timeline arms that are not system parameters, stored in `profile`."""
    short = headline[LEASE_PARAM] in SHORT_LEASE_MENU
    return {
        LEASE_ARM_KEY: LEASE_ARM_SHORT if short else LEASE_ARM_PRODUCTION,
        PG_TERMINAL_OPS_KEY: "off" if rng.random() < PG_TERMINAL_OPS_OFF_P else "on",
    }


def profile_value(key: str) -> str | None:
    """A value `first_configure` stored in `profile`, or None before it ran."""
    db = open_state()
    try:
        row = db.execute("SELECT value FROM profile WHERE key = ?", (key,)).fetchone()
        return str(row[0]) if row else None
    finally:
        db.close()


def report_arms(profile: str, headline: dict[str, str], arms: dict[str, str]) -> None:
    """One `sometimes` per menu arm, so an arm no timeline drew is visible."""
    sometimes(
        profile == "production", "configure drew the production-defaults profile", {}
    )
    sometimes(profile == "test", "configure drew the test-defaults profile", {})
    sometimes(
        profile == "test-random", "configure drew the randomized test profile", {}
    )

    upsert_v2 = normalize(headline["enable_upsert_v2"]) == "true"
    sometimes(upsert_v2, "configure enabled upsert v2", {})
    sometimes(not upsert_v2, "configure disabled upsert v2", {})

    peek = normalize(headline["enable_frontend_peek_sequencing"]) == "true"
    sometimes(peek, "configure enabled frontend peek sequencing", {})
    sometimes(not peek, "configure disabled frontend peek sequencing", {})

    lease = headline["persist_reader_lease_duration"]
    sometimes(lease == "900s", "configure kept the production reader lease", {})
    sometimes(lease == "600s", "configure drew a 600s reader lease", {})
    sometimes(lease == "360s", "configure drew a 360s reader lease", {})
    sometimes(
        lease == "301s", "configure drew a reader lease just past deferred_expire", {}
    )
    sometimes(
        arms[LEASE_ARM_KEY] == LEASE_ARM_SHORT,
        "configure drew a short reader lease that reverses the deferred_expire ordering",
        {"lease": lease},
    )

    terminal_off = arms[PG_TERMINAL_OPS_KEY] == "off"
    sometimes(terminal_off, "configure drew no terminal upstream ops this timeline", {})
    sometimes(
        not terminal_off, "configure allowed terminal upstream ops this timeline", {}
    )

    wait = headline["with_0dt_deployment_max_wait"]
    sometimes(wait == "8760h", "configure kept the production 0dt max wait", {})
    sometimes(wait == "1800s", "configure drew the mzcompose 0dt max wait", {})
    sometimes(wait == "300s", "configure drew a 300s 0dt max wait", {})
    sometimes(wait == "60s", "configure drew a 60s 0dt max wait", {})

    ddl = headline["with_0dt_deployment_ddl_check_interval"]
    sometimes(ddl == "300s", "configure kept the production 0dt DDL check interval", {})
    sometimes(ddl == "30s", "configure drew a 30s 0dt DDL check interval", {})
    sometimes(ddl == "5s", "configure drew a 5s 0dt DDL check interval", {})
    sometimes(ddl == "1s", "configure drew a 1s 0dt DDL check interval", {})


def wait_until_reflected(host: str, expected: dict[str, str]) -> dict[str, str]:
    """Poll `SHOW` as mz_system until every expected value is in effect.

    Returns the parameters still mismatching at the deadline (empty on success).
    """
    deadline = time.monotonic() + SYNC_TIMEOUT_S
    mismatched: dict[str, str] = dict(expected)
    while True:
        try:
            with sql.connection(host, internal=True) as conn:
                mismatched = {}
                for name, value in expected.items():
                    seen = show(conn, name)
                    if normalize(seen) != normalize(value):
                        mismatched[name] = f"want {value}, have {seen}"
            if not mismatched:
                return {}
        except (psycopg.Error, OSError) as e:
            log(f"waiting for parameters: {e}")
        if time.monotonic() >= deadline:
            return mismatched
        time.sleep(2)


def ensure_shared_cluster(host: str, replication_factor: int) -> None:
    deadline = time.monotonic() + SQL_TIMEOUT_S
    while True:
        try:
            with sql.connection(host) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM mz_clusters WHERE name = %s", (SHARED_CLUSTER,)
                ).fetchone()
                if exists is None:
                    conn.execute(
                        f"CREATE CLUSTER {SHARED_CLUSTER} (SIZE 'antithesis-1', "
                        f"REPLICATION FACTOR {replication_factor})".encode()
                    )
                    log(f"created {SHARED_CLUSTER} with RF {replication_factor}")
                return
        except (psycopg.Error, OSError) as e:
            c = sql.classify(e)
            if c.outcome is sql.Outcome.VIOLATION or time.monotonic() >= deadline:
                raise
            log(f"retrying shared cluster creation: {c.template}")
            time.sleep(2)


def record_flag(db: sqlite3.Connection, name: str, value: str, source: str) -> None:
    db.execute(
        "INSERT INTO flag_history (at, name, value, source) VALUES (?, ?, ?, ?)",
        (time.time(), name, normalize(value), source),
    )
    db.commit()


def current_flag_value(db: sqlite3.Connection, name: str) -> str | None:
    row = db.execute(
        "SELECT value FROM flag_history WHERE name = ? ORDER BY seq DESC LIMIT 1",
        (name,),
    ).fetchone()
    return row[0] if row else None


def roll_flag(env: Environment, db: sqlite3.Connection | None = None) -> str:
    """Flip one `FLIP_FLAGS` entry in the ConfigMap and record it.

    Does not wait for environmentd to apply the value: it runs under faults,
    and the next `flag_history` reader sees the intended value with its time.
    Returns the flipped flag name.
    """
    db = db or open_state()
    name = rng.choice(FLIP_FLAGS)
    previous = current_flag_value(db, name)
    if previous is None:
        new = rng.choice(["true", "false"])
    else:
        new = "false" if previous == "true" else "true"
    update_system_params(env.endpoints.namespace, {name: new})
    record_flag(db, name, new, "mid-run")
    sometimes(
        name == "enable_upsert_v2",
        "mid-run roll flipped enable_upsert_v2",
        {"value": new},
    )
    sometimes(
        name == "enable_upsert_chunked_stash",
        "mid-run roll flipped enable_upsert_chunked_stash",
        {"value": new},
    )
    sometimes(
        name == "enable_frontend_peek_sequencing",
        "mid-run roll flipped enable_frontend_peek_sequencing",
        {"value": new},
    )
    sometimes(
        name == "enable_frontend_subscribes",
        "mid-run roll flipped enable_frontend_subscribes",
        {"value": new},
    )
    sometimes(
        name == "enable_cluster_reconfiguration_lag_gate",
        "mid-run roll flipped enable_cluster_reconfiguration_lag_gate",
        {"value": new},
    )
    return name


def main() -> int:
    env = Environment()
    host = env.sql_host()
    db = open_state()

    with sql.connect_with_retry(host, SQL_TIMEOUT_S, internal=True) as conn:
        version = mz_version(conn)
        defaults = {name: show_or_none(conn, name) for name in FLIP_FLAGS}

    profiles = ["production"]
    if MZCOMPOSE_IMPORT_ERROR is None:
        profiles += ["test", "test-random"]
    else:
        log(f"test profiles unavailable: {MZCOMPOSE_IMPORT_ERROR}")
    profile = rng.choice(profiles)
    base = profile_params(profile, version)
    headline = roll_headline()
    arms = roll_arms(headline)
    desired = {**base, **headline, **HARNESS_REQUIRED}
    log(f"profile {profile} on {version}, headline {headline}, arms {arms}")

    written = update_system_params(env.endpoints.namespace, desired)

    with db:
        db.execute("DELETE FROM profile")
        db.execute("DELETE FROM system_params")
        db.execute("INSERT INTO profile VALUES ('profile', ?)", (profile,))
        db.execute("INSERT INTO profile VALUES ('version', ?)", (version,))
        db.executemany("INSERT INTO profile VALUES (?, ?)", list(headline.items()))
        db.executemany("INSERT INTO profile VALUES (?, ?)", list(arms.items()))
        db.executemany(
            "INSERT INTO system_params VALUES (?, ?)",
            [(k, str(v)) for k, v in written.items()],
        )
    for name in FLIP_FLAGS:
        value = written.get(name, defaults.get(name))
        if value is not None:
            record_flag(db, name, str(value), "first")
    lease_source = "first"
    if arms[LEASE_ARM_KEY] == LEASE_ARM_SHORT:
        lease_source = "first-short-lease-reversed-order"
    record_flag(db, LEASE_PARAM, str(written[LEASE_PARAM]), lease_source)

    report_arms(profile, headline, arms)

    verify = {
        name: str(written[name])
        for name in (*headline, *HARNESS_REQUIRED, PANIC_FLAG)
        if name in written
    }
    mismatched = wait_until_reflected(host, verify)
    always(
        not mismatched,
        "system parameters written to the ConfigMap take effect in environmentd",
        {"profile": profile, "mismatched": mismatched, "timeout_s": SYNC_TIMEOUT_S},
    )

    ensure_shared_cluster(host, rng.choice([1, 2]))
    reachable(
        "configure finished with the shared cluster in place", {"profile": profile}
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
