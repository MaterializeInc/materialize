# Introduction

Reproduce data for the cluster spec sheet effort.

# Usage

`bin/mzcompose --find cluster-spec-sheet run default`

This will run all scenarios currently defined for the cluster spec sheet.

Pass `--cleanup` to disable the region after the test.

# Running


## Running via Buildkite

The workload runs as part of the release qualification pipeline in Buildkite.

## Running manually in Cloud

To run the cloud canary test manually, you can specify either `--target=cloud-production` (which is hardcoded to aws/us-east-1) or `--target=cloud-staging` (which is hardcoded to aws/eu-west-1). For production, you need to set the environment variables `NIGHTLY_MZ_USERNAME` and `MZ_CLI_APP_PASSWORD`. For staging, you need to set the environment variables `NIGHTLY_CANARY_USERNAME` and `NIGHTLY_CANARY_APP_PASSWORD`.

The username is an email address, the app password is a password generated in the cloud console (something like `mzp_...`).

Once the environment variables have been set, you can run:

```
cd test/cluster-spec-sheet
./mzcompose run default
```

## Running in Docker

To run the test in local Docker containers, set the `--target` parameter to `docker`.
In this case, the environment variables are not required.

```
bin/mzcompose --find cluster-spec-sheet run default --target=docker
```

## Scenarios

There are four kinds of scenarios:
- cluster scaling: These measure run times and arrangement sizes.
- envd qps scalability: These measure QPS while varying envd's CPU allocation.
- envd objects scalability: These measure adapter/envd latency (DDL, simple peeks) as the number of catalog objects grows.
- cluster object limits: These find the maximum number of idle indexes / materialized views a cluster can hold while remaining fresh.

The envd qps scalability and cluster object limits scenarios can't be run in Production: the former because changing envd's CPU cores using `mz` is not allowed there, the latter because we don't want spill-over effects to real customer environments. Both need to be run with `--target=cloud-staging` (or `--target=docker`).

You can invoke only one kind of scenarios by using the group name from `SCENARIO_GROUPS`. For example:
```
bin/mzcompose --find cluster-spec-sheet run default envd_qps_scalability  --target=cloud-staging
```
or
```
bin/mzcompose --find cluster-spec-sheet run default cluster
```
or
```
bin/mzcompose --find cluster-spec-sheet run default envd_objects_scalability
```
or
```
bin/mzcompose --find cluster-spec-sheet run default cluster_object_limits --target=cloud-staging
```

You can also specify a specific scenario by name.

For testing just the scaffolding of the cluster spec sheet itself, you can make the run much faster by using the various scaling options, e.g.:
```
--scale-tpch=0.01 --scale-tpch-queries=0.01 --scale-auction=1 --max-scale=4 --envd-objects-scalability-sizes=1,10,100 --cluster-object-limits-max=500
```

### envd objects scalability scenarios

The `envd_objects_scalability_tables` and `envd_objects_scalability_mvs`
scenarios fix the measurement cluster size and vary the number of pre-existing
catalog objects, measuring `CREATE TABLE` and `SELECT` latency at each size
point. By default they walk the full size list (`1, 10, 100, 1000, 3000, 5000,
10000, 20000, 30000`); the MV scenario spreads MVs across pad clusters
at 10000 materialized views per cluster (so 30000 MVs spans 3
single-replica clusters). Override the size list with `--envd-objects-scalability-sizes`.
These runs are long; expect hours for the full size range, especially the MV
scenario.

### cluster object limits scenarios

The `cluster_object_limits_indexes` and `cluster_object_limits_mvs` scenarios
find, per cluster size, how many idle materializations (indexes or
materialized views) one cluster can keep fresh. Each materialization is
derived from a one-row base table that is never updated, so the only work the
cluster has to do is keep advancing each materialization's `write_frontier`
in step with the table's frontier. Once the cluster can't keep up, freshness
collapses; we record the largest N at which freshness was still good.

Procedure (per cluster size in `REPLICA_SCALES`, capped by `--max-scale`):
1. (Re)create the test cluster `c` at that size, plus a one-row base table.
2. Walk an N-list, adding objects incrementally and querying
   `mz_internal.mz_materialization_lag` for max `local_lag` across the test
   objects on `c`.
3. Declare healthy if all N objects are reporting and max lag is below
   `CLUSTER_OBJECT_LIMITS_LAG_THRESHOLD_MS` (default 2 s). Stop walking N on
   the first unhealthy point (the unhealthy point is still recorded so the
   cliff is visible).
4. Bisect the (last_healthy, first_unhealthy) interval
   `--cluster-object-limits-bisect-steps` times (default 4), adding or
   dropping objects in place each step. Each bisection probe halves the
   uncertainty around the cliff. Set to 0 to disable.

The default N-list ramps geometrically up to 1000, then steps in +1000
increments up to `--cluster-object-limits-max` (default 30000). Override
explicitly with `--cluster-object-limits-sizes=...`. These runs are long;
expect tens of minutes per cluster size on small replicas, longer on bigger
ones.

Results go to `*.cluster_object_limits.csv` (one row per (cluster_size, N)
sample, with an extra `healthy` column). The analysis produces two plots per
scenario: max-healthy-N per cluster size, and freshness lag vs N with one
series per cluster size.
