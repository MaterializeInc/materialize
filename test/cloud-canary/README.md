# Introduction

The cloud-canary test starts a region on the Materialize cloud before running a set of simple smoke tests.

# Usage

The cloud-canary is used either stand-alone, where a single Mz environment is being created, or
as part of the cloud load test were numerous environments are being created in sequence.

# Running

## Running via Buildkite

The easiest way to run the cloud canary test is via Nightly, using the cloud-canary Buildkite step.

# Running manually

To run the cloud canary test manually, a set of environment variables need to be made available locally:

```
export NIGHTLY_CANARY_APP_PASSWORD=...
export NIGHTLY_CANARY_CONFLUENT_CLOUD_API_SECRET=...
export NIGHTLY_CANARY_CONFLUENT_CLOUD_API_KEY=...
export BUILDKITE_COMMIT=...
```

- `NIGHTLY_CANARY_APP_PASSWORD` and `NIGHTLY_CANARY_CONFLUENT_CLOUD_API_SECRET` are stored
as Pulumi secrets in the i2 repository.

- `NIGHTLY_CANARY_CONFLUENT_CLOUD_API_KEY` is available from the i2/buildkite.py file
in the i2 repository.

- `BUILDKITE_COMMIT` must be the full Git SHA whose containers are already available on DockerHub.

Please ask in #cloud if you require additional access rights. You can also use an App Password from
your own cloud account, if you do not mind your region being destroyed by the test.

Once the environment variables have been set, you can run:

```
cd test/cloud-canary
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

The `ENVIRONMENT_ID` can be obtained from Frontegg. Go to Backoffice -> Accounts and search for "Nightly Canary"
