#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Local pager-hydration repro.

Adapted from run.py to run against a local `bin/environmentd`, for fast
iteration on the slow-hydration / serial-fetch investigation without looping
through CI or a staging region.

Differences from run.py, forced by running locally:

* No LaunchDarkly. Scoped per-replica system parameters are written only by the
  config sync loop from the delivery service (see
  doc/developer/design/20260609_scoped_feature_flags.md), so the cloud approach of two
  replicas differing only by a scoped `enable_column_paged_batcher` override is
  not reproducible locally. Instead the pager on/off axis is a GLOBAL
  `ALTER SYSTEM SET`, applied sequentially: set the flag, add one replica, let
  it hydrate, measure, drop. Only one flag value is ever live at a time.
* Connections use psql. Application DDL/queries go to the external SQL port
  (default 6875, user `materialize`). `ALTER SYSTEM` goes to the internal SQL
  port (default 6877, user `mz_system`), because that is the superuser surface
  locally.
* Cluster sizes are local `scale=1,workers=N` strings (see
  src/catalog/src/config.rs), small by default so a laptop can run them.
* Scale factors default to 1 and 3.

The pager on/off comparison is measured rehydrate-style: the workload (base-table
indexes, indexed views, materialized views for five heavy TPCH queries) is
created once per scale factor and left live, and only the single replica churns
under each flag value. The source uses a TICK INTERVAL so its write frontier
never goes final, otherwise mz_hydration_statuses reports a freshly added
replica hydrated before it has loaded anything.
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# --- Configuration --------------------------------------------------------

DEFAULT_SIZE = "scale=1,workers=8"  # small: 8 timely workers, whole process.
DEFAULT_SOURCE_SIZE = "scale=1,workers=2"
DEFAULT_SCALE_FACTORS = [1, 3]
DEFAULT_TRIALS = 3
DEFAULT_TICK = "1s"
QUERIES = ["Q03", "Q05", "Q09", "Q18", "Q21"]
# (label, enable_column_paged_batcher). pager_on = paged batcher + spill;
# pager_off = legacy columnation batcher (no pager).
PAGER_CONDITIONS = [("pager_on", True), ("pager_off", False)]
SLT = "test/sqllogictest/tpch_create_materialized_view.slt"

BASE_INDEXES = [
    ("pk_nation", "nation", "n_nationkey"),
    ("fk_nation_region", "nation", "n_regionkey"),
    ("pk_region", "region", "r_regionkey"),
    ("pk_part", "part", "p_partkey"),
    ("pk_supplier", "supplier", "s_suppkey"),
    ("fk_supplier_nation", "supplier", "s_nationkey"),
    ("pk_partsupp", "partsupp", "ps_partkey, ps_suppkey"),
    ("fk_partsupp_part", "partsupp", "ps_partkey"),
    ("fk_partsupp_supp", "partsupp", "ps_suppkey"),
    ("pk_customer", "customer", "c_custkey"),
    ("fk_customer_nation", "customer", "c_nationkey"),
    ("pk_orders", "orders", "o_orderkey"),
    ("fk_orders_cust", "orders", "o_custkey"),
    ("pk_lineitem", "lineitem", "l_orderkey, l_linenumber"),
    ("fk_lineitem_order", "lineitem", "l_orderkey"),
    ("fk_lineitem_part", "lineitem", "l_partkey"),
    ("fk_lineitem_supp", "lineitem", "l_suppkey"),
    ("fk_lineitem_partsupp", "lineitem", "l_partkey, l_suppkey"),
]

POLL_INTERVAL = 3.0
INGEST_POLL_INTERVAL = 15.0

# Pager knobs set alongside the batcher flag so pager_on exercises the full
# spill path (matches the environment-wide staging config).
PAGER_ON_FLAGS = {
    "enable_column_paged_batcher": "true",
    "enable_column_paged_batcher_spill": "true",
    "column_paged_batcher_lz4": "true",
}
PAGER_OFF_FLAGS = {
    "enable_column_paged_batcher": "false",
}


class Psql:
    """Runs `psql -tAq -c` against one (host, port, user)."""

    def __init__(self, host, port, user, database):
        self.base = [
            "psql", "-tAqX", "-h", host, "-p", str(port),
            "-U", user, "-d", database, "-c",
        ]

    def __call__(self, stmt, timeout=900, search_path=None):
        env = os.environ.copy()
        # No password locally (trust/dev). Keep libpq from prompting.
        env.setdefault("PGPASSWORD", "")
        if search_path is not None:
            # search_path at connection startup lets one DDL use unqualified
            # names. A `SET ...; CREATE ...` prelude would form a multi-statement
            # transaction, which DDL cannot enter.
            env["PGOPTIONS"] = f"-c search_path={search_path}"
        last = None
        for _attempt in range(3):
            try:
                r = subprocess.run(
                    self.base + [stmt], capture_output=True, text=True,
                    timeout=timeout, env=env,
                )
            except subprocess.TimeoutExpired:
                last = f"timeout after {timeout}s"
                time.sleep(5)
                continue
            if r.returncode == 0:
                return [ln for ln in r.stdout.splitlines() if ln.strip()]
            last = r.stderr or r.stdout
            time.sleep(5)
        raise RuntimeError(f"SQL failed after retries: {stmt}\n{last}")

    def one(self, stmt, timeout=900, search_path=None):
        rows = self(stmt, timeout=timeout, search_path=search_path)
        return rows[0] if rows else None


# --- Names ----------------------------------------------------------------

def schema(sf):
    return f"sf{sf}"


def src_cluster(sf):
    return f"ldgen_sf{sf}"


def test_cluster(sf):
    return f"test_sf{sf}"


def extract_bodies():
    """Return {Qxx: SELECT body} for the target queries from the canonical slt."""
    text = Path(SLT).read_text()
    bodies = {}
    for q in QUERIES:
        m = re.search(rf"CREATE MATERIALIZED VIEW {q} AS\n(.*?);\n", text, re.S)
        if not m:
            raise RuntimeError(f"missing {q} in {SLT}")
        bodies[q] = m.group(1).strip()
    return bodies


# --- Pager flag (global, sequential) --------------------------------------

def set_pager(system, on):
    """Set the global pager flags for the next replica's hydration.

    Replica-local dyncfgs are pushed to running replicas live, so this must be
    set before adding the replica whose hydration we intend to measure, and only
    one value can be live across the environment at a time.
    """
    flags = PAGER_ON_FLAGS if on else PAGER_OFF_FLAGS
    for name, value in flags.items():
        system(f"ALTER SYSTEM SET {name} = {value};")
    # Reset the flags not present in the OFF set back to default so a prior ON
    # run does not leak spill/lz4 into an OFF run.
    if not on:
        for name in ("enable_column_paged_batcher_spill", "column_paged_batcher_lz4"):
            system(f"ALTER SYSTEM RESET {name};")


# --- Source ---------------------------------------------------------------

def source_status(mz, sf):
    return mz.one(
        f"SELECT status FROM mz_internal.mz_source_statuses s "
        f"JOIN mz_sources so ON s.id = so.id "
        f"JOIN mz_schemas sc ON so.schema_id = sc.id "
        f"WHERE sc.name = '{schema(sf)}' AND so.name = 'ldgen';"
    )


def ensure_source(mz, sf, source_size, tick):
    """Create the ticking TPCH source for `sf`, or reuse a running one."""
    sch, cl = schema(sf), src_cluster(sf)
    st = source_status(mz, sf)
    if st == "running":
        print(f"  [sf{sf}] source running, reusing")
        return
    if st is None:
        print(f"  [sf{sf}] creating source cluster + TPCH SF{sf} (tick {tick})")
        if not mz.one(f"SELECT 1 FROM mz_schemas WHERE name = '{sch}';"):
            mz(f"CREATE SCHEMA {sch};")
        if not mz.one(f"SELECT 1 FROM mz_clusters WHERE name = '{cl}';"):
            mz(f"CREATE CLUSTER {cl} SIZE '{source_size}';")
        mz(
            f"CREATE SOURCE {sch}.ldgen IN CLUSTER {cl} "
            f"FROM LOAD GENERATOR TPCH (SCALE FACTOR {sf}, TICK INTERVAL '{tick}') "
            f"FOR ALL TABLES;"
        )
    else:
        print(f"  [sf{sf}] source present but status={st}; waiting")
    # A load-generator source reports `running` only after its snapshot is
    # committed. Do not probe with count(*): that scan can exceed the timeout.
    while source_status(mz, sf) != "running":
        print(f"  [sf{sf}] ingest status={source_status(mz, sf)}")
        time.sleep(INGEST_POLL_INTERVAL)


# --- Test cluster + workload ----------------------------------------------

def ensure_test_cluster(mz, sf):
    cl = test_cluster(sf)
    if not mz.one(f"SELECT 1 FROM mz_clusters WHERE name = '{cl}';"):
        mz(f"CREATE CLUSTER {cl} REPLICAS ();")
        print(f"  [sf{sf}] created unmanaged cluster {cl}")


def add_replica(mz, sf, name, size):
    mz(f"CREATE CLUSTER REPLICA {test_cluster(sf)}.{name} SIZE '{size}';")


def drop_replica(mz, sf, name):
    mz(f"DROP CLUSTER REPLICA IF EXISTS {test_cluster(sf)}.{name};")


def replica_id(mz, sf, name):
    return mz.one(
        f"SELECT r.id FROM mz_cluster_replicas r "
        f"JOIN mz_clusters c ON r.cluster_id = c.id "
        f"WHERE c.name = '{test_cluster(sf)}' AND r.name = '{name}';"
    )


def workload_exists(mz, sf):
    return bool(mz.one(
        f"SELECT 1 FROM mz_materialized_views mv "
        f"JOIN mz_schemas sc ON mv.schema_id = sc.id "
        f"WHERE sc.name = '{schema(sf)}' AND mv.name = 'mq03';"
    ))


def create_workload(mz, sf, bodies):
    """Base-table indexes, indexed views, and MVs for the five queries."""
    sch, cl = schema(sf), test_cluster(sf)
    for name, table, cols in BASE_INDEXES:
        mz(f"CREATE INDEX {name} IN CLUSTER {cl} ON {table} ({cols});", search_path=sch)
    for q in QUERIES:
        v = f"v{q.lower()}"
        mz(f"CREATE VIEW {sch}.{v} AS {bodies[q]};", search_path=sch)
        mz(f"CREATE DEFAULT INDEX IN CLUSTER {cl} ON {sch}.{v};", search_path=sch)
        mz(
            f"CREATE MATERIALIZED VIEW {sch}.m{q.lower()} IN CLUSTER {cl} "
            f"AS {bodies[q]};",
            search_path=sch,
        )


def object_map(mz, sf):
    """Return {object_id: (label, kind)} for all indexes and MVs on the cluster."""
    cl, sch = test_cluster(sf), schema(sf)
    out = {}
    for row in mz(
        f"SELECT i.id, i.name FROM mz_indexes i "
        f"JOIN mz_clusters c ON i.cluster_id = c.id "
        f"JOIN mz_relations r ON i.on_id = r.id "
        f"JOIN mz_schemas s ON r.schema_id = s.id "
        f"WHERE c.name = '{cl}' AND s.name = '{sch}';"
    ):
        oid, name = row.split("|")
        out[oid] = (name, "index")
    for row in mz(
        f"SELECT mv.id, mv.name FROM mz_materialized_views mv "
        f"JOIN mz_schemas s ON mv.schema_id = s.id WHERE s.name = '{sch}';"
    ):
        oid, name = row.split("|")
        out[oid] = (name, "mv")
    return out


# --- Measurement ----------------------------------------------------------

def measure(mz, objs, rid, flavor, timeout):
    """Time hydration of `objs` on a single replica id.

    Returns rows of (object_label, kind, flavor, wallclock_s, time_ns_or_empty).
    Wall-clock completion is when mz_hydration_statuses first reports each object
    hydrated on the replica. time_ns is read afterwards for index objects
    (mz_compute_hydration_times; NULL for MVs, CLU-175).
    """
    objs_in = ",".join(f"'{o}'" for o in objs)
    n_expected = len(objs)

    t0 = time.monotonic()
    done = {}
    deadline = t0 + timeout
    while time.monotonic() < deadline:
        rows = mz(
            f"SELECT object_id FROM mz_internal.mz_hydration_statuses "
            f"WHERE object_id IN ({objs_in}) AND replica_id = '{rid}' AND hydrated;"
        )
        now = time.monotonic()
        for oid in rows:
            if oid not in done:
                done[oid] = round(now - t0, 2)
        print(f"    [{flavor}] hydrated {len(done)}/{n_expected}", flush=True)
        if len(done) >= n_expected:
            break
        time.sleep(POLL_INTERVAL)
    else:
        print(f"    [{flavor}] WARNING: timed out, {n_expected - len(done)} missing")

    time_ns = {}
    for row in mz(
        f"SELECT object_id, time_ns "
        f"FROM mz_internal.mz_compute_hydration_times "
        f"WHERE object_id IN ({objs_in}) AND replica_id = '{rid}' "
        f"AND time_ns IS NOT NULL;"
    ):
        oid, tns = row.split("|")
        time_ns[oid] = tns

    rows = []
    for oid, secs in done.items():
        label, kind = objs[oid]
        rows.append((label, kind, flavor, secs, time_ns.get(oid, "")))
    return rows


def run_cell(app, system, sf, flavor, on, trial, size, timeout):
    """Set the pager flag, add a fresh replica, time its hydration, drop it."""
    print(f"  [sf{sf}] {flavor} trial {trial}")
    name = f"{flavor}_t{trial}"
    drop_replica(app, sf, name)
    set_pager(system, on)
    add_replica(app, sf, name, size)
    rid = replica_id(app, sf, name)
    objs = object_map(app, sf)
    rows = measure(app, objs, rid, flavor, timeout)
    drop_replica(app, sf, name)
    out = [[sf, flavor, label, kind, trial, secs, tns]
           for (label, kind, _flavor, secs, tns) in sorted(rows)]
    print(f"    recorded {len(out)} rows "
          f"({sum(1 for r in out if r[3]=='index')} index, "
          f"{sum(1 for r in out if r[3]=='mv')} mv)")
    return out


# --- Main -----------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--app-port", type=int, default=6875)
    ap.add_argument("--app-user", default="materialize")
    ap.add_argument("--system-port", type=int, default=6877,
                    help="internal SQL port for ALTER SYSTEM (mz_system)")
    ap.add_argument("--system-user", default="mz_system")
    ap.add_argument("--database", default="materialize")
    ap.add_argument("--scale-factors", default=",".join(map(str, DEFAULT_SCALE_FACTORS)))
    ap.add_argument("--trials", type=int, default=DEFAULT_TRIALS)
    ap.add_argument("--size", default=DEFAULT_SIZE)
    ap.add_argument("--source-size", default=DEFAULT_SOURCE_SIZE)
    ap.add_argument("--tick", default=DEFAULT_TICK)
    ap.add_argument("--outdir", default="pager-hydration-results-local")
    ap.add_argument("--hydrate-timeout", type=int, default=1800)
    ap.add_argument("--keep", action="store_true", help="skip test-cluster teardown")
    ap.add_argument("--purge", action="store_true", help="also drop sources")
    args = ap.parse_args()

    app = Psql(args.host, args.app_port, args.app_user, args.database)
    system = Psql(args.host, args.system_port, args.system_user, args.database)

    # Fail fast with a clear message if environmentd is not up.
    try:
        app.one("SELECT 1;", timeout=15)
        system.one("SELECT 1;", timeout=15)
    except Exception as e:
        print(f"ERROR: cannot reach local environmentd "
              f"(app {args.host}:{args.app_port}, system {args.host}:{args.system_port}).\n"
              f"Start it with `bin/environmentd` (CockroachDB must be running).\n{e}")
        sys.exit(1)

    scale_factors = [int(x) for x in args.scale_factors.split(",")]
    bodies = extract_bodies()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    results_path = outdir / "results.csv"
    done_cells = set()
    if results_path.exists():
        with results_path.open() as f:
            for row in csv.DictReader(f):
                done_cells.add((int(row["sf"]), row["flavor"], int(row["trial"])))
        if done_cells:
            print(f"resuming: {len(done_cells)} cells already done")

    rf = results_path.open("a", newline="")
    rw = csv.writer(rf)
    if rf.tell() == 0:
        rw.writerow(["sf", "flavor", "object", "kind", "trial",
                     "hydration_seconds", "time_ns"])
    rf.flush()

    for sf in scale_factors:
        print(f"=== scale factor {sf} ===")
        ensure_source(app, sf, args.source_size, args.tick)
        ensure_test_cluster(app, sf)
        if not workload_exists(app, sf):
            create_workload(app, sf, bodies)
            print(f"  [sf{sf}] workload created")
        for flavor, on in PAGER_CONDITIONS:
            for trial in range(1, args.trials + 1):
                if (sf, flavor, trial) in done_cells:
                    print(f"  [sf{sf}] {flavor} trial {trial} done, skipping")
                    continue
                rows = run_cell(app, system, sf, flavor, on, trial,
                                args.size, args.hydrate_timeout)
                rw.writerows(rows)
                rf.flush()
        if not args.keep:
            drop_workload_and_cluster(app, sf)
            print(f"  [sf{sf}] test cluster torn down (source kept)")
        if args.purge:
            app(f"DROP CLUSTER IF EXISTS {src_cluster(sf)} CASCADE;")
            app(f"DROP SCHEMA IF EXISTS {schema(sf)} CASCADE;")
            print(f"  [sf{sf}] source purged")

    # Leave the environment with the pager flag at its default.
    system("ALTER SYSTEM RESET enable_column_paged_batcher;")
    system("ALTER SYSTEM RESET enable_column_paged_batcher_spill;")
    system("ALTER SYSTEM RESET column_paged_batcher_lz4;")
    rf.close()
    print(f"\nresults -> {results_path}")
    print("DONE")


def drop_workload_and_cluster(mz, sf):
    sch = schema(sf)
    for q in QUERIES:
        mz(f"DROP MATERIALIZED VIEW IF EXISTS {sch}.m{q.lower()};")
        mz(f"DROP VIEW IF EXISTS {sch}.v{q.lower()} CASCADE;")
    for name, _, _ in BASE_INDEXES:
        mz(f"DROP INDEX IF EXISTS {sch}.{name};")
    mz(f"DROP CLUSTER IF EXISTS {test_cluster(sf)} CASCADE;")


if __name__ == "__main__":
    main()
