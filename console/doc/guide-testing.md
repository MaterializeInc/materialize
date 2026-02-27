# Testing Guide

## Suite

For our unit/integration tests, we use Vitest as our test runner, [Testing Library](https://testing-library.com/) to assert against the browser's DOM, and [Mock Service Worker](https://mswjs.io/) to mock network calls. Note that all `console.log` output is redirected to
`log/test.log`. When running locally, the output is colorized, so you will want to use
`less -R log/test.log` or install [grc](https://github.com/garabik/grc) and use it with
tail: `grc tail -f log/test.log` to view the logs. To enable inline `console.log` output, set the environment variable `PRINT_TEST_CONSOLE_LOGS=true yarn test...`.

For our E2E tests, we use Playwright.

### Running unit/integration tests

To run our suite, run `yarn test`.

### Running SQL tests

SQL tests require the [console
mzcompose](https://github.com/MaterializeInc/materialize/pull/26600) workflow and Docker to be
running.

```shell
cd ../test/console
./mzcompose run default
cd -
yarn test:sql
```

Once the workflow is running, you generally shouldn't have to restart it.

When you are done, you can stop the docker containers:

```shell
cd ../test/console
./mzcompose down
```

SQL tests rely on a shared materialize instance, and therefore must be run in serial.
They also rely on
[testdrive](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/testdrive.md)
to reset the state of the database between tests. Specifically, testdrive will reset the
database state on startup unless you pass the `--no-reset` flag, so each test should
start with a testdrive call, even if you don't need to run anything in testdrive.

#### Dnsmasq

Because the cloud load balancers have dynamic domain names, we need a way to map a
wildcard domain to localhost, so requests can be handled by services running in docker.
These instructions are written for macos, though the actual Dnsmasq config should work on
Linux as well.

```shell
brew install dnsmasq

```

Edit `$(brew --prefix)/etc/dnsmasq.conf` and add this line:

```
conf-dir=/opt/homebrew/etc/dnsmasq.d/,*.conf
```

Next, update your nameservers so that the loopback address is the first option. This is
important, since many tools will only check the first nameserver. Dnsmasq will forward
requests it doesn't know how to handle to other nameservers automatically. In the example
below, I am using the loopback address and Cloudflare DNS, but you could also include
your ISP DNS IP. You can list the existing values with e.g.
`networksetup -getdnsservers "Wi-Fi"`.

```shell
networksetup -listallnetworkservices
# Update the DNS nameservers for your network interfaces, e.g.
networksetup -setdnsservers "Wi-Fi" 127.0.0.1 1.1.1.1
networksetup -setdnsservers "USB 10/100/1000 LAN" 127.0.0.1 1.1.1.1
```

Now copy the kind config file into your dnsmasq config directory.

```shell
cp misc/dnsmasq/mzcloud-kind.conf /opt/homebrew/etc/dnsmasq.d/
```

Now run `sudo brew services start dnsmasq`. Dnsmasq will not work without sudo.

Validate everything is working:

```shell
$ nslookup foo.lb.testing.materialize.cloud
Server:         127.0.0.1
Address:        127.0.0.1#53

Name:   foo.lb.testing.materialize.cloud
Address: 127.0.0.1
```

### Running E2E tests

To start, follow the [Cloud Setup](../README.md#cloud-setup) to set up your local stack.

Before attempting to run these tests, you should make sure you've pulled the latest
cloud changes and docker images (or build the docker images locally. One wrinkle here is
if the most recent code changes have not yet pushed docker images, you will either have
to check out a previous commit, or build updated images. The docker images are tagged
with the commit SHA, so it's easy to check for the latest SHA in the
[Github Pacakges repo](https://github.com/MaterializeInc/cloud/pkgs/container/cloud).

First, in the `console/` directory, install all playwright dependencies if you haven't already.

```shell
yarn playwright install --with-deps
```

The playwright end-to-end tests require a cloud stack to run, generally using Kind is
simplest locally. First, run the console dev server pointing to your local stack:

```shell
DEFAULT_STACK=local yarn start
```

Then, in another terminal, use the cloud scripts to bring up the local stack:

```shell
cd ../../cloud
# Activate the python venv, required for some cloud scripts
source venv/bin/activate
# Export necessary variables for staging Frontegg
export FRONTEGG_URL=https://admin.staging.cloud.materialize.com
export FRONTEGG_JWK_STRING="$(bin/pulumi stack --stack materialize/staging output frontegg_jwk | perl -pe 's/\n/\\n/g')"
export FRONTEGG_ADMIN_ROLE=MaterializePlatformAdmin
# Create and configure a k8s cluster
bin/kind-delete && bin/kind-create
```

In yet another terminal:

```shell
cd ../../cloud
# Export the test user password
export E2E_TEST_PASSWORD=$(bin/pulumi stack output --stack materialize/mzcloud/staging --show-secrets console_e2e_test_password)
cd ../materialize/console
yarn test:e2e
```

You only have to do this once per shell where you want to run tests, you can
now run the tests any time in that shell session.

Tests save traces to the folder `test-results`, and you can use the following
playwright command to view these traces after the fact (also very useful for CI
failures).

```shell
yarn playwright show-trace test-results/platform-use-region-local-kind/trace-1.zip
```

## Testing Philosophy

We follow the same testing philosophy as the rest of the company. In particular:

> Unit/integration tests are great for complicated pure functions.

Also, rather than writing complex mocks:

> a battery of system tests is a much better use of time.

Anything that can be a pure function we should extract and unit test. When testing
interactions and user experience flows, call them integration tests, we act and assert
against the DOM using Testing Library and mock out network calls using Mock Service
Worker. When we want don't want to mock network calls, we create end to end tests using
Playwright.

Our SQL queries are tested using mzcompose and testdrive, which allow us to run queries
against a running Materialize instance along with supporting services running in Docker.
These tests are fairly slow, so it's expected that a single test may run different
permutations of query to test different states and parameters. In general, we should
test that each query builder parameter works as expected, but it's not necessary to
exhaustively test each possibly permutation.

For more information about Materialize's testing philosophy, see [Developer guide:
testing](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/guide-testing.md).

We don't enforce 100% code coverage but allow the developer to decide what tests are
valuable to write. However, when bugs are fixed, it's good practice to create a test for
the bug.

## Integration Test Structure

### 1. Mock out network calls using Mock Service Worker

When dealing with the Materialize API, we've created helper functions such as `buildSqlQueryHandlerV2` and `mapKyselyToTabular`. This might look like:

```ts
server.use(
  buildSqlQueryHandlerV2({
    queryKey: secretQueryKeys.list(),
    results: mapKyselyToTabular({
      rows: [],
      columns: useSecretsListColumns,
    }),
  }),
);
```

If you have trouble getting your query to match, you can enable our custom
debug logging for our handlers.

```bash
DEBUG=console:msw:sql yarn test
# or the lower level request logging, most useful for non-sql APIs
DEBUG=console:msw yarn test
# you can also use wildcards, e.g. enable all console debug logs
DEBUG=console:* yarn test

```

### 2. Render the React component

For integration tests, we usually want to start the test high enough up the React component tree such that we're able to test our desired UX flows. This will most likely be where the routes of the component are defined and rendered (i.e. `SourceRoutes.tsx`).

We use a helper method called `renderComponent` which is a wrapped over React Testing Library's `render`. It provides the necessary React Providers and initial states for components to render correctly.

### 3. Act stage

Now that everything's set up, we treat each test from the perspective of a user. We use utility methods from Testing Library to simulate DOM events such as `click`. When using Testing Library's utilities, we follow their [priority list](https://testing-library.com/docs/queries/about#priority).

### 4. Assert

After "acting", we assert against the DOM using Testing Library. This could be clicking a button then asserting that text pops up. A test can have multiple asserts and it's up to the developer how they want to split up their tests as long as each represent a UX flow.

#### Asserting the presence of animated elements

If an element is animated into the viewport, a simple `expect(element).toBeVisible()` may fail because the element hasn't yet rendered at the time of the test. This can be worked around by wrapping the assertion in a `waitFor`.

```js
await waitFor(async () =>
  expect(await screen.findByText("Usage & Billing")).toBeVisible(),
);
```

This will periodically check for the element to be visible. If it takes longer than a few seconds the assertion will fail.

## Snapshot Testing

We don't rely too much on snapshot testing since it leads to frequent failing tests that are "fixed" without much thought. However, we heavily utilize snapshot testing to
check if a Kysely generated query creates correct SQL text. The process is generally:

1. Create a compiled query via Kysely and get `parameters` and `sql`
2. Run `expect({ sql, parameters }).toMatchSnapshot();` to generate the snapshot
3. Copy + paste the output from the snapshot into [https://mz.sqlfum.pt/](https://mz.sqlfum.pt/) to read the SQL text or paste it into a Materialize SQL client to test if it's correct

If you want to easily get combined SQL text, you can run this ad-hoc script:

```js
function interpolateSqlQuery(sql, parameters) {
  return sql.replace(/\$(\d+)/g, (match, capture) => {
    const index = parseInt(capture) - 1;
    return parameters[index] !== undefined
      ? typeof parameters[index] === "string"
        ? `'${parameters[index]}'`
        : `'${JSON.stringify(parameters[index])}}'`
      : match;
  });
}
```
