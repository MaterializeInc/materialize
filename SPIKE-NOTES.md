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

The local catalog is too small to reproduce the 7-minute production
hydration. The point is the shape: the serving cluster no longer
recomputes catalog views, so its hydration cost tracks index size, not view
complexity. Production's heaviest views (recent activity log, wallclock lag
history, console utilization) need a staging or production-scale run to get
real numbers.

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
