# Catalog frontend spike

Local spike, not for merge. Branch `catalog-frontend-spike` off main (ab4e4edba0).

## What changed

1. **Builtin views to builtin MVs.** The 47 builtin views that had an index on
   `mz_catalog_server` are now builtin materialized views maintained there
   (`IN CLUSTER mz_catalog_server AS ...`). No other views changed. All 47
   planned as MVs without changes to their SQL.
2. **`mz_catalog_server_frontend` builtin cluster.** All 82 builtin indexes
   moved to it. It only reads from persist, so a fresh replica hydrates
   without recomputing any catalog view. `mz_catalog_server` now only runs
   the MVs.
3. **Configurable auto-routing.** System var
   `catalog_server_auto_route_cluster` (default
   `mz_catalog_server_frontend`) names the cluster that catalog-only queries
   route to. Falls back to `mz_catalog_server` if the named cluster is
   missing. The user-object restriction applies to `mz_catalog_server`, the
   frontend, and whatever the var names.
4. **Load-balanced clusters.** System var `load_balanced_clusters` (default
   `mz_catalog_server_frontend`). On those compute instances the controller
   sends each untargeted peek to one replica, round-robin among replicas
   whose write frontier is already past the read time. Transient dataflows
   (slow-path peeks, subscribes, COPY TO) are also placed on one replica.
   Maintained dataflows still run on every replica. A fast-path peek whose
   replica disappears is re-sent to another replica.

## Measurements (local, 4.4k objects, 45k columns)

| | dataflows | hydration | arrangements |
|---|---|---|---|
| main (v26.50 container), `mz_catalog_server` | 166 | 5.6 s | 51.2 MB |
| spike, `mz_catalog_server` (MV maintenance) | 131 | 6.0 s | 47.2 MB |
| spike, `mz_catalog_server_frontend` (serving) | 115 | 2.0 s | 12.5 MB |

Dataflow counts include 33 per-cluster introspection indexes on every
cluster. The spike adds dataflows overall (246 across both clusters vs 166):
each converted view is now an MV dataflow plus an index dataflow, where it
used to be one index dataflow that computed the view. What moves is where the
view computation runs, not how much of it there is.

The local catalog is too small to reproduce the 7-minute production
hydration. The point is the shape: the serving cluster no longer
recomputes catalog views, so its hydration cost tracks index size, not view
complexity. Production's heaviest views (recent activity log, wallclock lag
history, console utilization) need a staging or production-scale run to get
real numbers.

Scale sweep (fresh environments, user objects = 2:1 tables with 20 columns
and views, on top of ~1.4k system objects). Replica rehydration, two runs
each, memory is total arrangement size on the cycled replica:

| user objects | spike frontend | spike catalog_server | main catalog_server |
|---|---|---|---|
| 400 | 2.0 / 2.0 s, 3.5 MB | 3.1 s, 10.9 MB | 3.1 / 3.0 s, 13.7 MB |
| 800 | 4.0 / 3.9 s, 5.0 MB | 2.7 / 2.8 s, 14.9 MB | 5.0 / 5.0 s, 18.7 MB |
| 1600 | 3.3 / 2.1 s, 7.5 MB | 4.9 / 5.5 s, 23.5 MB | 5.1 / 4.3 s, 29.3 MB |
| 3200 | 3.4 / 2.1 s, 12.5 MB | 5.9 / 6.0 s, 40.2 MB | 4.7 / 5.6 s, 50.5 MB |
| 6400 | 3.9 / 2.1 s, 23.0 MB | 9.9 / 11.0 s, 74.7 MB | 7.9 / 7.8 s, 93.7 MB |

Two runs were dropped as measurement bugs, since fixed: 400 spike
catalog_server run 2 stopped before all statuses reported, and 800 spike
catalog_server run 1 read memory before introspection caught up.
"main" is the v26.50 Docker image, so cross-environment timings are
approximate. Comparisons within the spike are like for like.

Frontend hydration vs data volume. Production's slow catalog server is not
driven by object count (the analytics environment has ~3k objects) but by
history-style relations (activity log, wallclock lag history, console
utilization), so this measures an index on an MV of activity-log-shaped rows.
Time is from replica creation until the first query on the index returns
(three runs). An index on an empty MV gives the fixed overhead (0.25 s).

| rows | arrangement | 1 worker | 2 workers |
|---|---|---|---|
| 1M | 64 MB | 0.87 / 0.88 / 0.88 s | 0.67 / 0.66 / 0.60 s |
| 4M | 248 MB | 3.08 / 3.16 / 2.98 s | 2.17 / 1.95 / 1.63 s |
| 16M | 980 MB | 11.89 / 11.87 / 11.59 s | 6.97 / 6.64 / 7.13 s |

Linear in data: about 0.72 s per million rows (85 MB/s of arrangement) on
one worker, about 0.42 s per million (145 MB/s) on two. Local persist is a
file blob on local disk. S3 throughput needs measuring on staging.

NOTE: `mz_compute_hydration_times` reported 0.05 ms for the 1M-row index
above, and ~0.36 s for every frontend index, so per-dataflow times from it
are not usable for indexes that hydrate from persist. Looks like a bug, not
investigated.

Load balancing, 3 frontend replicas:

- 300 fast-path peeks: exactly 100 per replica.
- 60 slow-path peeks: 20 per replica, each dataflow built only on its replica.
- Dropping a replica (replication factor 3 to 2) during 3000 peeks: 0 errors.
- `kill -9` of a replica process during 3000 peeks: 0 errors.

Catalog freshness after DDL: `CREATE TABLE` then a `mz_show_columns` read
saw the new table every time. The median read latency was 59 ms, against
45 ms on main (the container on main runs under Docker, so this is noise
level).

## Bugs found along the way

- Targeted peeks were broadcast to all replicas
  (`Instance::target_replica` skipped `Peek`). That was only safe while
  targeted transient indexes were forbidden. The coordinator slow path
  installed the peek's dataflow on every replica even when the session pinned
  `cluster_replica`. Both are fixed here: targeted peeks go only to their
  target, and the slow-path dataflow is installed on the target replica.
  On main this means `SET cluster_replica` slow-path queries run their
  dataflow on every replica. That is wasted work, and maybe worth its own PR.
- `mz_indexes` hardcoded `mz_catalog_server` as every builtin index's cluster
  (`CROSS JOIN catalog_server_cluster`). Now derived from each index's
  `IN CLUSTER`.

## Not done / open questions

- **Upgrades.** No `MigrationStep::replacement` entries for the 47
  conversions, so an existing environment will not migrate. Only fresh
  environments were tested.
- **Bootstrap sizing.** The frontend reuses the catalog server's bootstrap
  size config (TODO in `BuiltinBootstrapClusterConfigMap::get_config`). It
  needs its own flag and cloud config.
- **Persist write cost.** The 47 MVs now write to persist continuously,
  including temporal ones that retract on a schedule (recent activity log,
  wallclock lag history, console utilization). Measure write amplification
  and blob cost at production scale.
- **Load-balancing scope.** Only fast-path peeks on maintained indexes fail
  over. Slow-path peeks and subscribes on a failed replica still error, as
  before. Replica choice is round-robin over caught-up replicas, with no
  load awareness. The flag is a name list in a system var, not a cluster
  property in the catalog.
- **Restricting `mz_catalog_server`.** Users can still `SET cluster =
  mz_catalog_server` and query. Revoking `USAGE` from `PUBLIC` there is now
  possible because auto-routing no longer lands on it, but it isn't done.
- **Tests.** No SLT or testdrive runs. Expect churn in `SHOW` and
  `mz_indexes`/`mz_materialized_views` expectations, catalog docs, and
  anything that assumes builtin indexes live on `mz_catalog_server`.
- `caught_up.rs` peeks `mz_cluster_replica_history` on `mz_catalog_server`.
  That is now an unindexed MV there, so the read goes to persist. It works,
  but should probably target the frontend.
