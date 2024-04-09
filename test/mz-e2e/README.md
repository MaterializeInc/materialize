# Introduction

Mz test runs multiple commands to test the `mz` CLI.

# Usage

The mz-e2e is made to run alone and during nightly.

# Running

## Running via Buildkite

The easiest way to run the mz-e2e test is via Nightly, using the mz-e2e Buildkite step.

# Running manually

To run the mz-e2e test manually, a set of environment variables need to be made available locally:

```
export MZ_CLI_APP_PASSWORD=...
export NIGHTLY_MZ_USERNAME=...
export NIGHTLY_CANARY_CONFLUENT_CLOUD_API_SECRET=...
export NIGHTLY_CANARY_CONFLUENT_CLOUD_API_KEY=...
```

- `NIGHTLY_CANARY_CONFLUENT_CLOUD_API_SECRET` is stored as a Pulumi secret in the i2 repository.

- `NIGHTLY_CANARY_CONFLUENT_CLOUD_API_KEY` is available from the i2/buildkite.py file
in the i2 repository.

- `MZ_CLI_APP_PASSWORD` is stored as Pulumi secrets in the i2 repository.

- `NIGHTLY_MZ_USERNAME` is not defined in the i2 repository. A default is used "infra+bot@materialize.com".

Please ask in #cloud if you require additional access rights. You can also use an App Password from
your own cloud account, if you do not mind your region being destroyed by the test.

Once the environment variables have been set, you can run:

```
cd test/mz-e2e
./mzcompose run default
```

# Debugging

If the test fails to start a Mz region, it will fail in the following-non descriptive way:

```
[2023-05-26T16:23:26Z] C> waiting for dbname=materialize host=...
...
5 5 4 4 3 3 2 2 1 1 0 0Shutting down region aws/us-east-1 ...
```

Before the test has timed out and shut down the region, you can obtain diagnostic information using kubectl:

```
cd /path/to/cloud/repository
bin/mzadmin aws login
kubectl --context mzcloud-staging-us-east-1-0 -n environment-$ENVIRONMENT_ID-0 get all
kubectl --context mzcloud-staging-us-east-1-0 -n environment-$ENVIRONMENT_ID-0 logs pod/environmentd-0
```

The `ENVIRONMENT_ID` can be obtained from Frontegg. Go to Backoffice -> Accounts and search for "Materialize mz CLI CI"
