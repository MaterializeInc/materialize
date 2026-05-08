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

There are three kinds of scenarios:
- cluster scaling: These measure run times and arrangement sizes.
- envd scaling: These measure QPS while varying envd's CPU allocation.
- envd scalability: These measure adapter/envd latency (DDL, simple peeks) as the number of catalog objects grows.

Currently, the envd scaling scenarios can't be run in Production, because changing envd's CPU cores using `mz` is not allowed there. Therefore, these scenarios need to be run with `--target=cloud-staging`.

You can invoke only one kind of scenarios by using the group name from `SCENARIO_GROUPS`. For example:
```
bin/mzcompose --find cluster-spec-sheet run default environmentd  --target=cloud-staging
```
or
```
bin/mzcompose --find cluster-spec-sheet run default cluster
```
or
```
bin/mzcompose --find cluster-spec-sheet run default envd_scalability
```

You can also specify a specific scenario by name.

For testing just the scaffolding of the cluster spec sheet itself, you can make the run much faster by using the various scaling options, e.g.:
```
--scale-tpch=0.01 --scale-tpch-queries=0.01 --scale-auction=1 --max-scale=4 --envd-scalability-sizes=1,10,100
```

### envd scalability scenarios

The `envd_scalability_tables` and `envd_scalability_mvs` scenarios fix the
measurement cluster size and vary the number of pre-existing catalog objects,
measuring `CREATE TABLE` and `SELECT` latency at each size point. By default
they walk the full size list (`1, 10, 100, 1000, 3000, 5000, 10000, 20000,
30000, 50000, 100000`); the MV scenario shards across pad clusters at 10000
materialized views per cluster (so 100000 MVs spans 10 single-replica
clusters). Override the size list with `--envd-scalability-sizes`. These runs
are long; expect hours for the full size range, especially the MV scenario.
