#!/usr/bin/env python3
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Out-of-core hydration experiment for indexes and materialized views.

Measures hydration time per arm against a staging Materialize region, for three
phases:

    idx        : TPCH base-table indexes and indexed views, RF 0 -> 1.
    mv_first   : TPCH materialized views writing their first snapshot into
                 empty shards (created at RF 0, then RF 0 -> 1).
    mv_second  : the same materialized views rehydrating against shards that
                 already hold data, RF 1 -> 0 -> 1.

Every arm owns two managed clusters, `idx_<arm>` and `mv_<arm>`, with one
replica each. The arm's flags are applied out of band as LaunchDarkly rules on
the replica context's `cluster_name`, so a recreated replica inherits its arm.
All arms run concurrently. With `--parallel-trials`, all trials of a phase run
concurrently too, trial n on clusters `idx_<arm>_t<n>` and `mv_<arm>_t<n>`.

Materialized views get their own cluster and their own objects per arm. With
several replicas writing one sink, the first replica to write wins the race and
the others hold a correction buffer against already persisted data, which
confounds both time and memory of a first write.

Memory (peak RSS and swap) is not measured here. `windows.csv` records the
replica id and wall-clock window of every cell. Read the peaks from
`mz_internal.mz_cluster_replica_metrics_history` for those windows:
`memory_bytes` is RSS and `heap_bytes - memory_bytes` is swap, sampled once a
minute. Staging Prometheus had too few cAdvisor samples per window to use.

The source uses a TICK INTERVAL so its write frontier never goes final.
Without that, a bounded source's frontier is final and a freshly added replica
reports hydrated before it has loaded anything.
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_SIZE = "M.1-3xlarge"
DEFAULT_SOURCE_SIZE = "200cc"  # 4 vCPU: headroom for the single ingest thread.
DEFAULT_SCALE_FACTOR = 150
DEFAULT_TICK = "1s"
QUERIES = ["Q03", "Q05", "Q09", "Q18", "Q21"]
ARMS = ["columnation", "chunked", "pool_raw", "pool_lz4", "pool_lz4_1"]
PHASES = ["idx", "mv_first", "mv_second"]
SLT = "test/sqllogictest/tpch_create_materialized_view.slt"
# Flags whose effective per-replica value is recorded with every cell.
FLAG_PATTERNS = [
    "%column_paged_batcher%",
    "enable_columnar_merge_batcher",
    "column_chunk_compress_min_depth",
    "enable_compute_correction_v2%",
    "compute_hydration_concurrency",
]

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

POLL_INTERVAL = 5.0
INGEST_POLL_INTERVAL = 15.0
# After RF 1 -> 0 the old replica's process must be gone before RF 0 -> 1, else
# the new replica can share a node with a terminating pod.
RF_ZERO_SETTLE = 60.0


class Mz:
    """Wrapper around `mz ... sql -- -tAq -c` for one region/profile."""

    def __init__(self, mz_bin, config, region, profile):
        self.base = [
            mz_bin, "--config", config, "--region", region,
            "--profile", profile, "sql", "--", "-tAq", "-c",
        ]

    def __call__(self, stmt, timeout=900, search_path=None):
        env = os.environ.copy()
        # Catalog queries otherwise route through mz_catalog_server, which hangs
        # for minutes after a deploy. The ldgen sources' frontiers can trail, so
        # strict serializable reads may block.
        opts = [
            "-c auto_route_catalog_queries=false",
            "-c transaction_isolation=serializable",
        ]
        if search_path is not None:
            # Set search_path at connection startup so a single DDL statement
            # can use unqualified table names. A `SET ...; CREATE ...` prelude
            # would form a multi-statement transaction, which DDL cannot enter.
            opts.append(f"-c search_path={search_path}")
        env["PGOPTIONS"] = " ".join(opts)
        # Retry transient CLI/coordinator blips, which show up under heavy load
        # as a non-zero exit with little or no stderr, and as intermittent auth
        # failures ("Error fetching JWKS") when the admin endpoint briefly
        # refuses the token. Back off progressively so a blip that lasts tens of
        # seconds does not abort a multi-hour run. A persistent error still
        # surfaces after the final attempt.
        last = None
        attempts = 10
        for attempt in range(attempts):
            try:
                r = subprocess.run(
                    self.base + [stmt], capture_output=True, text=True,
                    timeout=timeout, env=env,
                )
            except subprocess.TimeoutExpired:
                last = f"timeout after {timeout}s"
                time.sleep(min(5 * (attempt + 1), 30))
                continue
            if r.returncode == 0:
                return [
                    line for line in r.stdout.splitlines()
                    if line.strip() and not line.startswith("Time:")
                ]
            last = r.stderr or r.stdout
            time.sleep(min(5 * (attempt + 1), 30))
        raise RuntimeError(f"SQL failed after retries: {stmt}\n{last}")

    def one(self, stmt, timeout=900, search_path=None):
        rows = self(stmt, timeout=timeout, search_path=search_path)
        return rows[0] if rows else None


# --- Names ----------------------------------------------------------------

def schema(sf):
    return f"sf{sf}"


def src_cluster(sf):
    return f"ldgen_sf{sf}"


def tag(arm, slot):
    """Name suffix of an arm's clusters and objects in `slot`.

    Sequential trials all run in slot 1, which keeps the bare arm name. With
    `--parallel-trials`, trial n runs in slot n on its own clusters, and the
    LaunchDarkly rules must list those cluster names as well.
    """
    return arm if slot == 1 else f"{arm}_t{slot}"


def cluster(kind, arm, slot=1):
    return f"{kind}_{tag(arm, slot)}"


def quote_list(values):
    return ",".join(f"'{v}'" for v in values)


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
    is left untouched.
    """
    sch, cl = schema(sf), src_cluster(sf)
    st = source_status(mz, sf)
    if st == "running":
        print(f"source sf{sf} running, reusing")
        return
    if st is None:
        print(f"creating source cluster + TPCH SF{sf} (tick {tick})")
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
        print(f"source sf{sf} present but status={st}; waiting")
    # `running` fires once the snapshot is committed, but after a pause the
    # generator replays its backlog for a long time afterwards. Check frontier
    # lag by hand before starting a measured run. Do not probe with count(*):
    # that scan exceeds the SQL timeout at high scale factors.
    while source_status(mz, sf) != "running":
        print(f"source sf{sf} ingest status={source_status(mz, sf)}")
        time.sleep(INGEST_POLL_INTERVAL)


# --- Clusters + workload --------------------------------------------------

def ensure_cluster(mz, name, size):
    """Create managed cluster `name` at RF 0, or resize an existing one."""
    if mz.one(f"SELECT 1 FROM mz_clusters WHERE name = '{name}';"):
        mz(f"ALTER CLUSTER {name} SET (SIZE '{size}');")
    else:
        mz(f"CREATE CLUSTER {name} (SIZE '{size}', REPLICATION FACTOR 0);")
        print(f"  created cluster {name}")


def set_rf(mz, names, rf):
    """Set the replication factor of every cluster in `names`.

    Returns {cluster: monotonic time the ALTER returned}, the per-cluster start
    of a measurement window.
    """
    started = {}
    for name in names:
        mz(f"ALTER CLUSTER {name} SET (REPLICATION FACTOR {rf});")
        started[name] = time.monotonic()
    return started


def replica_ids(mz, names):
    """Return {cluster: replica id} for the single replica of each cluster."""
    rows = mz(
        f"SELECT c.name, r.id FROM mz_cluster_replicas r "
        f"JOIN mz_clusters c ON r.cluster_id = c.id "
        f"WHERE c.name IN ({quote_list(names)});"
    )
    out = {}
    for row in rows:
        name, rid = row.split("|")
        if name in out:
            raise RuntimeError(f"cluster {name} has more than one replica")
        out[name] = rid
    missing = set(names) - set(out)
    if missing:
        raise RuntimeError(f"clusters without a replica: {sorted(missing)}")
    return out


def ensure_views(mz, sf, bodies):
    """Create the five query views once; every arm indexes the same views."""
    sch = schema(sf)
    for q in QUERIES:
        v = f"v{q.lower()}"
        if not mz.one(
            f"SELECT 1 FROM mz_views v JOIN mz_schemas s ON v.schema_id = s.id "
            f"WHERE s.name = '{sch}' AND v.name = '{v}';"
        ):
            # The index and MV processes reach this check together once the
            # source is running, so both can see the view missing.
            mz(f"CREATE VIEW IF NOT EXISTS {sch}.{v} AS {bodies[q]};", search_path=sch)


def create_idx_workload(mz, sf, arm, slot):
    sch, cl, sfx = schema(sf), cluster("idx", arm, slot), tag(arm, slot)
    for name, table, cols in BASE_INDEXES:
        mz(
            f"CREATE INDEX IF NOT EXISTS {name}_{sfx} IN CLUSTER {cl} "
            f"ON {table} ({cols});",
            search_path=sch,
        )
    for q in QUERIES:
        v = f"v{q.lower()}"
        mz(f"CREATE DEFAULT INDEX IF NOT EXISTS {v}_{sfx} IN CLUSTER {cl} ON {sch}.{v};",
           search_path=sch)


def create_mv_workload(mz, sf, arm, slot, bodies):
    sch, cl, sfx = schema(sf), cluster("mv", arm, slot), tag(arm, slot)
    for q in QUERIES:
        mz(
            f"CREATE MATERIALIZED VIEW {sch}.m{q.lower()}_{sfx} IN CLUSTER {cl} "
            f"AS {bodies[q]};",
            search_path=sch,
        )


def drop_mv_workload(mz, sf, arm, slot):
    sch, sfx = schema(sf), tag(arm, slot)
    for q in QUERIES:
        mz(f"DROP MATERIALIZED VIEW IF EXISTS {sch}.m{q.lower()}_{sfx};")


def objects(mz, name):
    """Return {object_id: (label, kind)} for the user indexes and MVs on `name`."""
    out = {}
    # Every cluster also carries system introspection indexes (`s` ids).
    for row in mz(
        f"SELECT i.id, i.name FROM mz_indexes i "
        f"JOIN mz_clusters c ON i.cluster_id = c.id "
        f"WHERE c.name = '{name}' AND i.id LIKE 'u%';"
    ):
        oid, label = row.split("|")
        out[oid] = (label, "index")
    for row in mz(
        f"SELECT mv.id, mv.name FROM mz_materialized_views mv "
        f"JOIN mz_clusters c ON mv.cluster_id = c.id WHERE c.name = '{name}';"
    ):
        oid, label = row.split("|")
        out[oid] = (label, "mv")
    return out


def write_frontiers(mz, oids):
    """Return {object_id: write frontier as int, or None when empty}."""
    if not oids:
        return {}
    out = {}
    for row in mz(
        f"SELECT object_id, write_frontier::text FROM mz_internal.mz_frontiers "
        f"WHERE object_id IN ({quote_list(oids)});"
    ):
        oid, wf = row.split("|")
        out[oid] = int(wf) if wf else None
    return out


def effective_flags(mz, rids):
    likes = " OR ".join(f"p.name LIKE '{p}'" for p in FLAG_PATTERNS)
    out = {}
    for row in mz(
        f"SELECT p.replica_id, p.name, p.value "
        f"FROM mz_internal.mz_replica_system_parameters p "
        f"WHERE p.replica_id IN ({quote_list(rids)}) AND ({likes}) "
        f"ORDER BY p.name;"
    ):
        rid, name, value = row.split("|")
        out.setdefault(rid, []).append(f"{name}={value}")
    return {rid: ";".join(v) for rid, v in out.items()}


def offline_events(mz, rid, start_db, end_db):
    """Return 'reason@time' for every offline event of `rid` in the window."""
    rows = mz(
        f"SELECT coalesce(reason, 'unknown'), occurred_at::text "
        f"FROM mz_internal.mz_cluster_replica_status_history "
        f"WHERE replica_id = '{rid}' AND status = 'offline' "
        f"AND occurred_at BETWEEN '{start_db}' AND '{end_db}';"
    )
    return ";".join(r.replace("|", "@") for r in rows)


# --- Measurement ----------------------------------------------------------

def measure(mz, started, objs_by_cluster, rids, baseline, timeout):
    """Wait until every object on every cluster hydrated.

    For an index, done means `mz_hydration_statuses` reports it hydrated. For a
    materialized view, done additionally needs the shard's write frontier to
    advance past `baseline`, its value read before RF 0 -> 1: the sink only
    writes once its correction buffer holds the full snapshot, and during RF 0
    no replica advances the frontier.

    Returns {(cluster, object_id): (hydrated_s, written_s or None)}, seconds
    from the cluster's own start in `started`.
    """
    mv_oids = [
        oid for objs in objs_by_cluster.values()
        for oid, (_, kind) in objs.items() if kind == "mv"
    ]
    rid_to_cluster = {rid: name for name, rid in rids.items()}
    all_oids = [oid for objs in objs_by_cluster.values() for oid in objs]
    hydrated, written = {}, {}
    n_hydrated = len(all_oids)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = mz(
            f"SELECT object_id, replica_id FROM mz_internal.mz_hydration_statuses "
            f"WHERE object_id IN ({quote_list(all_oids)}) "
            f"AND replica_id IN ({quote_list(rids.values())}) AND hydrated;"
        )
        now = time.monotonic()
        for row in rows:
            oid, rid = row.split("|")
            name = rid_to_cluster[rid]
            if oid in objs_by_cluster[name] and (name, oid) not in hydrated:
                hydrated[(name, oid)] = round(now - started[name], 2)
        pending_mv = [oid for oid in mv_oids if oid not in written]
        for oid, wf in write_frontiers(mz, pending_mv).items():
            base = baseline.get(oid)
            if wf is None or (base is not None and wf > base):
                name = next(n for n, objs in objs_by_cluster.items() if oid in objs)
                written[oid] = round(now - started[name], 2)
        print(
            f"    hydrated {len(hydrated)}/{n_hydrated}, "
            f"mv written {len(written)}/{len(mv_oids)}",
            flush=True,
        )
        if len(hydrated) >= n_hydrated and len(written) >= len(mv_oids):
            break
        time.sleep(POLL_INTERVAL)
    else:
        print(f"    WARNING: timed out after {timeout}s")

    out = {}
    for name, objs in objs_by_cluster.items():
        for oid in objs:
            out[(name, oid)] = (hydrated.get((name, oid)), written.get(oid))
    return out


def time_ns(mz, oids, rids):
    """Internal hydration time per (object, replica). NULL for some MVs (CLU-175)."""
    out = {}
    for row in mz(
        f"SELECT object_id, replica_id, time_ns "
        f"FROM mz_internal.mz_compute_hydration_times "
        f"WHERE object_id IN ({quote_list(oids)}) "
        f"AND replica_id IN ({quote_list(rids)}) AND time_ns IS NOT NULL;"
    ):
        oid, rid, tns = row.split("|")
        out[(oid, rid)] = tns
    return out


def run_cell(mz, sf, phase, trials, arms, size, bodies, timeout):
    """Run `phase` for every (trial, slot) in `trials` across all arms concurrently.

    Returns (result_rows, window_rows).
    """
    kind = "idx" if phase == "idx" else "mv"
    instances = {
        cluster(kind, arm, slot): (arm, trial, slot)
        for trial, slot in trials for arm in arms
    }
    names = list(instances)
    print(f"  {phase} trials {[t for t, _ in trials]}: {', '.join(names)}")
    for name in names:
        ensure_cluster(mz, name, size)

    if phase == "idx":
        set_rf(mz, names, 0)
        for arm, _, slot in instances.values():
            create_idx_workload(mz, sf, arm, slot)
        time.sleep(RF_ZERO_SETTLE)
    elif phase == "mv_first":
        set_rf(mz, names, 0)
        for arm, _, slot in instances.values():
            drop_mv_workload(mz, sf, arm, slot)
            create_mv_workload(mz, sf, arm, slot, bodies)
        time.sleep(RF_ZERO_SETTLE)
    else:  # mv_second: needs MVs that already wrote, then RF 1 -> 0 -> 1.
        for name in names:
            if not objects(mz, name):
                raise RuntimeError(f"{name} has no MVs; run mv_first first")
        set_rf(mz, names, 0)
        time.sleep(RF_ZERO_SETTLE)

    objs_by_cluster = {name: objects(mz, name) for name in names}
    uptime0 = mz.one("SELECT mz_uptime()::text;")
    baseline = write_frontiers(mz, [
        oid for objs in objs_by_cluster.values()
        for oid, (_, okind) in objs.items() if okind == "mv"
    ])
    t0_db = mz.one("SELECT now()::text;")
    started = set_rf(mz, names, 1)
    rids = replica_ids(mz, names)
    done = measure(mz, started, objs_by_cluster, rids, baseline, timeout)
    # Read after the run: a fresh replica receives its overrides asynchronously.
    flags = effective_flags(mz, rids.values())
    t1_db = mz.one("SELECT now()::text;")
    uptime1 = mz.one("SELECT mz_uptime()::text;")
    tns = time_ns(mz, [o for objs in objs_by_cluster.values() for o in objs],
                  rids.values())

    result_rows, window_rows = [], []
    for (name, oid), (hyd_s, wr_s) in sorted(done.items()):
        label, okind = objs_by_cluster[name][oid]
        arm, trial, _ = instances[name]
        result_rows.append([
            sf, phase, arm, trial, label, okind,
            "" if hyd_s is None else hyd_s,
            "" if wr_s is None else wr_s,
            tns.get((oid, rids[name]), ""),
        ])
    for name in names:
        arm, trial, _ = instances[name]
        window_rows.append([
            sf, phase, arm, trial, name, rids[name], size, t0_db, t1_db,
            uptime0, uptime1, offline_events(mz, rids[name], t0_db, t1_db),
            flags.get(rids[name], ""),
        ])
    print(f"    recorded {len(result_rows)} rows")
    return result_rows, window_rows


# --- Main -----------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mz-bin", default="bin/mz")
    ap.add_argument("--config", default=os.environ.get("MZ_CONFIG", ""))
    ap.add_argument("--region", default="aws/us-east-1")
    ap.add_argument("--profile", default="staging")
    ap.add_argument("--scale-factor", type=int, default=DEFAULT_SCALE_FACTOR)
    ap.add_argument("--arms", default=",".join(ARMS))
    ap.add_argument("--phases", default=",".join(PHASES))
    ap.add_argument("--trials", default="idx=2,mv_first=2,mv_second=3",
                    help="per-phase trial counts")
    ap.add_argument("--size", default=DEFAULT_SIZE)
    ap.add_argument("--source-size", default=DEFAULT_SOURCE_SIZE)
    ap.add_argument("--tick", default=DEFAULT_TICK)
    ap.add_argument("--outdir", default="ooc-hydration-results")
    ap.add_argument("--hydrate-timeout", type=int, default=3 * 3600)
    ap.add_argument("--parallel-trials", action="store_true",
                    help="run all trials of a phase at once, each on its own "
                         "clusters (mv_second reuses mv_first's trials, so "
                         "give mv_first at least as many)")
    ap.add_argument("--teardown", action="store_true",
                    help="drop the arm clusters and their objects at the end")
    args = ap.parse_args()

    if not args.config:
        print("ERROR: pass --config or set MZ_CONFIG (writable mz.toml path)")
        sys.exit(1)

    mz = Mz(args.mz_bin, args.config, args.region, args.profile)
    sf = args.scale_factor
    arms = args.arms.split(",")
    unknown = set(arms) - set(ARMS)
    if unknown:
        print(f"ERROR: unknown arms {sorted(unknown)}")
        sys.exit(1)
    phases = args.phases.split(",")
    trials = {k: int(v) for k, v in (kv.split("=") for kv in args.trials.split(","))}
    bodies = extract_bodies()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    env_id = mz.one("SELECT mz_environment_id();") or "unknown"
    print(f"environment {env_id}")

    results_path = outdir / "results.csv"
    windows_path = outdir / "windows.csv"
    done_cells = set()
    if windows_path.exists():
        with windows_path.open() as f:
            for row in csv.DictReader(f):
                done_cells.add((row["phase"], int(row["trial"]), row["size"]))
        if done_cells:
            print(f"resuming: {len(done_cells)} cells already done")

    rf = results_path.open("a", newline="")
    wf = windows_path.open("a", newline="")
    rw, ww = csv.writer(rf), csv.writer(wf)
    if rf.tell() == 0:
        rw.writerow(["sf", "phase", "arm", "trial", "object", "kind",
                     "hydrated_s", "written_s", "time_ns"])
    if wf.tell() == 0:
        ww.writerow(["sf", "phase", "arm", "trial", "cluster", "replica_id",
                     "size", "started_at", "ended_at", "uptime_start",
                     "uptime_end", "offline_events", "flags"])
    rf.flush()
    wf.flush()

    ensure_source(mz, sf, args.source_size, args.tick)
    ensure_views(mz, sf, bodies)
    for phase in phases:
        pending = []
        for trial in range(1, trials.get(phase, 1) + 1):
            if (phase, trial, args.size) in done_cells:
                print(f"  {phase} trial {trial} done, skipping")
            else:
                pending.append(trial)
        if args.parallel_trials:
            batches = [[(t, t) for t in pending]]
        else:
            batches = [[(t, 1)] for t in pending]
        for batch in batches:
            if not batch:
                continue
            result_rows, window_rows = run_cell(
                mz, sf, phase, batch, arms, args.size, bodies,
                args.hydrate_timeout,
            )
            rw.writerows(result_rows)
            ww.writerows(window_rows)
            rf.flush()
            wf.flush()

    # Leave clusters at RF 0 so idle arms do not hold nodes between runs. Only
    # touch the clusters of this run's phases: a concurrent run of the other
    # workload is still measuring on its clusters.
    kinds = {"idx" if phase == "idx" else "mv" for phase in phases}
    max_slot = max(trials.get(phase, 1) for phase in phases) if args.parallel_trials else 1
    for kind in sorted(kinds):
        names = [cluster(kind, arm, slot)
                 for slot in range(1, max_slot + 1) for arm in arms]
        existing = [n for n in names
                    if mz.one(f"SELECT 1 FROM mz_clusters WHERE name = '{n}';")]
        set_rf(mz, existing, 0)
        if args.teardown:
            for name in existing:
                mz(f"DROP CLUSTER IF EXISTS {name} CASCADE;")
    rf.close()
    wf.close()
    print(f"\nresults -> {results_path}\nwindows -> {windows_path}")
    print("DONE")


if __name__ == "__main__":
    main()
