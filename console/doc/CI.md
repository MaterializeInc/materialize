# CI / CD pipelines

Our CI / CD uses Github Actions.

## PR CI

The PR workflow currently has 3 jobs.

### lint-and-test

Runs eslint, typescript and vitest.

### sql-tests

Runs a separate vitest suite specifically for tests that execute SQL against a running
Materialize instance. To do this, we clone the materialize database repo and run
mzcompose. The version we run is determined by first checking the `MATERIALIZE_REF` file.
If it contains the string `__LATEST__`, we fetch the 10 most recent tags from the Github
API, then check each of the tags against the tags published on Docker Hub until we find
one that is valid. This keeps us on the latest release version of Materialize, without
any manual effort. If `MATERIALIZE_REF` is any other value, we use that as the ref to
checkout. This allows us to pin a version temporarily in case a new release has an
unexpected incompatible change.

### e2e-tests

Runs our Playwright tests using the cloud infrastructure. Similar to the sql-tests, we
have a `CLOUD_REF` file that tells us which ref to checkout. In general, this should
always be `__LATEST__` unless we have to pin a version temporarily to unblock our CI. We use
scripts from the cloud repo to run the cloud services in Kind, and start a development
server in the background, then execute the Playwright suite.

## Merge queue CD

When you queue a PR to merge, it kicks off a separate workflow, which also runs the exact
same lint-and-test and sql-tests builds. The e2e tests also run, but they are con figured
differently. Instead of cloning the cloud repo and running service in the action, we
create a custom Vercel preview deployment that uses our production configuration and
secrets. Once this build is ready, we execute the Playwright suite against that live
deployment, which uses production cloud services. This gives us high confidence that the
new Console build will work correctly with the currently running cloud services.
