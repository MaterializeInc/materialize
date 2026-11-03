#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Pager hydration experiment.

Measures the impact of the column-paged batcher's spill-to-disk mechanism (the
"pager") on hydration time and memory, as a function of TPCH scale factor,
against a staging Materialize region.

Design: doc/developer/design/20260716_pager_hydration_experiment.md

Two replicas per test cluster differ only by name; the name-to-flag mapping is
applied out of band via per-replica scoped system parameters:

    pager_on : inherits environment-wide (paged batcher + spill + lz4 on)
    pager_off: scoped override enable_column_paged_batcher=false (legacy batcher)

Per scale factor the workload is, for five heavy TPCH queries (Q03/05/09/18/21),
both an indexed view and a materialized view, plus the base-table pk/fk indexes
the joins use. Indexes report a precise internal hydration time
(mz_compute_hydration_times.time_ns); materialized views do not (CLU-175), so
they are timed by wall-clock via mz_hydration_statuses.

The source uses a TICK INTERVAL so its write frontier never goes final. Without
that, a bounded source's frontier is final and mz_hydration_statuses reports a
freshly added replica as hydrated before it has loaded anything, which makes
re-hydration timing meaningless. The tick inserts and retracts, so total data
size stays roughly constant.
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

# M.1-8xlarge: 62 vCPU, 470 GiB memory, 2820 GiB disk, single process
# (whole machine). Same compute as 3200cc but 4x the disk, so pager_on has spill
# headroom at high scale factors and disk capacity is not a confound.
DEFAULT_SIZE = "M.1-8xlarge"
DEFAULT_SOURCE_SIZE = "200cc"  # 4 vCPU: headroom for the single ingest thread.
DEFAULT_SCALE_FACTORS = [1, 10, 30, 100]
DEFAULT_TRIALS = 3
DEFAULT_TICK = "1s"
QUERIES = ["Q03", "Q05", "Q09", "Q18", "Q21"]
REPLICAS = ["pager_on", "pager_off"]
SCENARIOS = ["initial", "rehydrate"]
SLT = "test/sqllogictest/tpch_create_materialized_view.slt"

# Base-table pk/fk indexes (name, table, key columns). Mirrors the canonical
# TPCH index set for the tables the five queries join.
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


class Mz:
    """Wrapper around `mz ... sql -- -tAq -c` for one region/profile."""

    def __init__(self, mz_bin, config, region, profile):
        self.base = [
            mz_bin, "--config", config, "--region", region,
            "--profile", profile, "sql", "--", "-tAq", "-c",
        ]

    def __call__(self, stmt, timeout=900, search_path=None):
        env = os.environ.copy()
        if search_path is not None:
            # Set search_path at connection startup so a single DDL statement
            # can use unqualified table names. A `SET ...; CREATE ...` prelude
            # would form a multi-statement transaction, which DDL cannot enter.
            env["PGOPTIONS"] = f"-c search_path={search_path}"
        # Retry transient CLI/coordinator blips, which show up under heavy load
        # at high scale factors as a non-zero exit with little or no stderr. A
        # persistent error still surfaces after the final attempt.
        last = None
        for attempt in range(3):
            try:
                r = subprocess.run(
                    self.base + [stmt], capture_output=True, text=True,
                    timeout=timeout, env=env,
                )
            except subprocess.TimeoutExpired as e:
                last = f"timeout after {timeout}s"
                time.sleep(5)
                continue
            if r.returncode == 0:
                return [
                    line for line in r.stdout.splitlines()
                    if line.strip() and not line.startswith("Time:")
                ]
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


# --- Source ---------------------------------------------------------------

def source_status(mz, sf):
    return mz.one(
        f"SELECT status FROM mz_internal.mz_source_statuses s "
        f"JOIN mz_sources so ON s.id = so.id "
        f"JOIN mz_schemas sc ON so.schema_id = sc.id "
        f"WHERE sc.name = '{schema(sf)}' AND so.name = 'ldgen';"
    )


def ensure_source(mz, sf, source_size, tick):
    """Create the ticking TPCH source for `sf`, or reuse a running one.

    The single-threaded generator must never be restarted, so a running source
    is left untouched. Each scale factor lives in its own schema so per-table
    subsources do not collide across factors.
    """
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
    # committed. Do not probe with count(*): that scan exceeds the SQL timeout
    # at high scale factors.
    while source_status(mz, sf) != "running":
        print(f"  [sf{sf}] ingest status={source_status(mz, sf)}")
        time.sleep(INGEST_POLL_INTERVAL)


# --- Test cluster + workload ---------------------------------------------

def ensure_test_cluster(mz, sf):
    cl = test_cluster(sf)
    if not mz.one(f"SELECT 1 FROM mz_clusters WHERE name = '{cl}';"):
        mz(f"CREATE CLUSTER {cl} REPLICAS ();")
        print(f"  [sf{sf}] created unmanaged cluster {cl}")


def add_replicas(mz, sf, size):
    for name in REPLICAS:
        mz(f"CREATE CLUSTER REPLICA {test_cluster(sf)}.{name} SIZE '{size}';")


def drop_replicas(mz, sf):
    for name in REPLICAS:
        mz(f"DROP CLUSTER REPLICA IF EXISTS {test_cluster(sf)}.{name};")


def replica_ids(mz, sf):
    rows = mz(
        f"SELECT r.name, r.id FROM mz_cluster_replicas r "
        f"JOIN mz_clusters c ON r.cluster_id = c.id "
        f"WHERE c.name = '{test_cluster(sf)}';"
    )
    return {row.split("|")[0]: row.split("|")[1] for row in rows}


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


def drop_workload(mz, sf):
    sch = schema(sf)
    for q in QUERIES:
        mz(f"DROP MATERIALIZED VIEW IF EXISTS {sch}.m{q.lower()};")
        mz(f"DROP VIEW IF EXISTS {sch}.v{q.lower()} CASCADE;")
    for name, _, _ in BASE_INDEXES:
        mz(f"DROP INDEX IF EXISTS {sch}.{name};")


def object_map(mz, sf):
    """Return {object_id: (label, kind)} for all indexes and MVs on the cluster.

    kind is 'index' (time_ns available) or 'mv' (wall-clock only).
    """
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


def check_flags(mz, sf):
    return mz(
        f"SELECT r.name, p.name, p.value "
        f"FROM mz_internal.mz_replica_system_parameters p "
        f"JOIN mz_cluster_replicas r ON p.replica_id = r.id "
        f"JOIN mz_clusters c ON r.cluster_id = c.id "
        f"WHERE c.name = '{test_cluster(sf)}' AND p.name LIKE '%column_paged_batcher%' "
        f"ORDER BY r.name, p.name;"
    )


# --- Measurement ----------------------------------------------------------

def measure(mz, sf, objs, target_ids, timeout):
    """Time hydration of `objs` on the given replica ids.

    Returns (rows, windows). rows is a list of
    (object_label, kind, flavor, wallclock_s, time_ns_or_empty). windows maps
    flavor -> (started_at_db, ended_at_db).

    Wall-clock completion is when mz_hydration_statuses first reports each
    (object, replica) hydrated. This is honest here because the ticking source
    keeps the write frontier live. time_ns is read afterwards for index objects.
    """
    ids_in = ",".join(f"'{i}'" for i in target_ids)
    objs_in = ",".join(f"'{o}'" for o in objs)
    n_expected = len(objs) * len(target_ids)

    t0_db = mz.one("SELECT now();")
    t0 = time.monotonic()
    done = {}  # (object_id, replica_id) -> wallclock seconds
    deadline = t0 + timeout
    while time.monotonic() < deadline:
        rows = mz(
            f"SELECT object_id, replica_id FROM mz_internal.mz_hydration_statuses "
            f"WHERE object_id IN ({objs_in}) AND replica_id IN ({ids_in}) AND hydrated;"
        )
        now = time.monotonic()
        for row in rows:
            oid, rid = row.split("|")
            if (oid, rid) not in done:
                done[(oid, rid)] = round(now - t0, 2)
        print(f"    hydrated {len(done)}/{n_expected}", flush=True)
        if len(done) >= n_expected:
            break
        time.sleep(POLL_INTERVAL)
    else:
        print(f"    WARNING: timed out, {n_expected - len(done)} pairs missing")

    t1_db = mz.one("SELECT now();")

    # Precise internal time for index exports (NULL for MVs, CLU-175).
    time_ns = {}
    for row in mz(
        f"SELECT object_id, replica_id, time_ns "
        f"FROM mz_internal.mz_compute_hydration_times "
        f"WHERE object_id IN ({objs_in}) AND replica_id IN ({ids_in}) "
        f"AND time_ns IS NOT NULL;"
    ):
        oid, rid, tns = row.split("|")
        time_ns[(oid, rid)] = tns

    rows = []
    for (oid, rid), secs in done.items():
        label, kind = objs[oid]
        rows.append((label, kind, target_ids[rid], secs, time_ns.get((oid, rid), "")))
    windows = {flavor: (t0_db, t1_db) for flavor in target_ids.values()}
    return rows, windows


def run_scenario(mz, sf, scenario, trial, size, bodies, timeout, verify_flags=False):
    print(f"  [sf{sf}] {scenario} trial {trial}")
    if scenario == "initial":
        drop_workload(mz, sf)
        drop_replicas(mz, sf)
        add_replicas(mz, sf, size)
        if verify_flags:
            flags = check_flags(mz, sf)
            print(f"  pager overrides: {flags if flags else '(none; inherit env-wide)'}")
        ids = {rid: name for name, rid in replica_ids(mz, sf).items()}
        create_workload(mz, sf, bodies)
    else:  # rehydrate: workload stays live, replicas churn.
        if not workload_exists(mz, sf):
            add_replicas(mz, sf, size)
            create_workload(mz, sf, bodies)
            measure(mz, sf, object_map(mz, sf),
                    {rid: n for n, rid in replica_ids(mz, sf).items()}, timeout)
        drop_replicas(mz, sf)
        add_replicas(mz, sf, size)
        ids = {rid: name for name, rid in replica_ids(mz, sf).items()}

    objs = object_map(mz, sf)
    rows, windows = measure(mz, sf, objs, ids, timeout)
    result_rows, window_rows = [], []
    for (label, kind, flavor, secs, tns) in sorted(rows):
        result_rows.append([sf, scenario, flavor, label, kind, trial, secs, tns])
    for flavor, (start, end) in windows.items():
        rid = next(r for r, f in ids.items() if f == flavor)
        window_rows.append([sf, scenario, flavor, trial, rid, start, end])
    print(f"    recorded {len(result_rows)} rows "
          f"({sum(1 for r in result_rows if r[4]=='index')} index, "
          f"{sum(1 for r in result_rows if r[4]=='mv')} mv)")
    return result_rows, window_rows


# --- Main -----------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mz-bin", default="bin/mz")
    ap.add_argument("--config", default=os.environ.get("MZ_CONFIG", ""))
    ap.add_argument("--region", default="aws/us-east-1")
    ap.add_argument("--profile", default="staging")
    ap.add_argument("--scale-factors", default=",".join(map(str, DEFAULT_SCALE_FACTORS)))
    ap.add_argument("--trials", type=int, default=DEFAULT_TRIALS)
    ap.add_argument("--size", default=DEFAULT_SIZE)
    ap.add_argument("--source-size", default=DEFAULT_SOURCE_SIZE)
    ap.add_argument("--tick", default=DEFAULT_TICK)
    ap.add_argument("--outdir", default="pager-hydration-results")
    ap.add_argument("--hydrate-timeout", type=int, default=5400)
    ap.add_argument("--keep", action="store_true", help="skip test-cluster teardown")
    ap.add_argument("--purge", action="store_true", help="also drop sources")
    args = ap.parse_args()

    if not args.config:
        print("ERROR: pass --config or set MZ_CONFIG (writable mz.toml path)")
        sys.exit(1)

    mz = Mz(args.mz_bin, args.config, args.region, args.profile)
    scale_factors = [int(x) for x in args.scale_factors.split(",")]
    bodies = extract_bodies()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    env_id = mz.one("SELECT mz_environment_id();") or "unknown"
    namespace = "environment-" + env_id.removeprefix(args.region.replace("/", "-") + "-")
    print(f"environment {env_id} (namespace {namespace})")

    results_path = outdir / "results.csv"
    windows_path = outdir / "windows.csv"
    done_cells = set()
    if results_path.exists():
        with results_path.open() as f:
            for row in csv.DictReader(f):
                done_cells.add((int(row["sf"]), row["scenario"], int(row["trial"])))
        if done_cells:
            print(f"resuming: {len(done_cells)} cells already done")

    rf = results_path.open("a", newline="")
    wf = windows_path.open("a", newline="")
    rw, ww = csv.writer(rf), csv.writer(wf)
    if rf.tell() == 0:
        rw.writerow(["sf", "scenario", "flavor", "object", "kind", "trial",
                     "hydration_seconds", "time_ns"])
    if wf.tell() == 0:
        ww.writerow(["sf", "scenario", "flavor", "trial", "replica_id",
                     "started_at", "ended_at"])
    rf.flush()
    wf.flush()

    for sf in scale_factors:
        print(f"=== scale factor {sf} ===")
        ensure_source(mz, sf, args.source_size, args.tick)
        ensure_test_cluster(mz, sf)
        first = True
        for scenario in SCENARIOS:
            for trial in range(1, args.trials + 1):
                if (sf, scenario, trial) in done_cells:
                    print(f"  [sf{sf}] {scenario} trial {trial} done, skipping")
                    continue
                result_rows, window_rows = run_scenario(
                    mz, sf, scenario, trial, args.size, bodies,
                    args.hydrate_timeout, verify_flags=first,
                )
                rw.writerows(result_rows)
                ww.writerows(window_rows)
                rf.flush()
                wf.flush()
                first = False
        if not args.keep:
            drop_replicas(mz, sf)
            drop_workload(mz, sf)
            mz(f"DROP CLUSTER IF EXISTS {test_cluster(sf)} CASCADE;")
            print(f"  [sf{sf}] test cluster torn down (source kept)")
        if args.purge:
            mz(f"DROP CLUSTER IF EXISTS {src_cluster(sf)} CASCADE;")
            mz(f"DROP SCHEMA IF EXISTS {schema(sf)} CASCADE;")
            print(f"  [sf{sf}] source purged")
    rf.close()
    wf.close()
    print(f"\nresults -> {results_path}\nwindows -> {windows_path}")
    print(f"grafana namespace: {namespace}")
    print("DONE")


if __name__ == "__main__":
    main()
