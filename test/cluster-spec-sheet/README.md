# Introduction

Reproduce data for the cluster spec sheet effort.

# Usage

`bin/mzcompose --find cluster-spec-sheet run default`

This will run all scenarios currently defined for the cluster spec sheet.

The test expects a default cluster.

Pass `--cleanup` to disable the region after the test.

# Running

The workload runs as part of the release qualification pipeline.

# Running manually

To run the cloud canary test manually, a set of environment variables need to be made available locally:

```
export NIGHTLY_MZ_USERNAME=...
export MZ_CLI_APP_PASSWORD=mzp_...
export ENVIRONMENT=...
export REGION=...
```

The username is an email address, the app password is a password generated in the cloud console.
The environment is either `production` or `staging`, and the region is one of the supported regions, e.g. `aws/us-east-1`.

Once the environment variables have been set, you can run:

```
cd test/cluster-spec-sheet
./mzcompose run default
```
