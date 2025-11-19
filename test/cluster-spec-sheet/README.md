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

There are two kinds of scenarios:
- cluster scaling: These measure run times and arrangement sizes.
- envd scaling: These measure QPS.
