# Developer guide: testing

*Last updated August 21, 2019.*

This guide details our testing philosophy, as well as how to run the test
suites.

## Overview

There are broadly three test suites:

  1. The Rust **unit/integration test suite**, which is run by `cargo test`.
     These live alongside the code they test in a `mod tests { ... }` block, or
     in the `tests/` directory within a crate.

  2. The data-driven **system test suite** in the top-level [test/](/test)
     directory. These consist of text files in various DSLs (sqllogictest and
     testdrive, at the moment) that essentially specify SQL commands to run and
     their expected output.

  3. The long-running **performance and stability test suite**. This test suite
     has yet to be automated. At the moment it consists of engineers manually
     running the demo in [demo/chbench/](/demo/chbench).

The unit/integration and system test suites are run on every PR and can easily
be run locally. The goal is for these test suites to be quite fast, ideally
under 10m, while still catching 95% of bugs.

The long-running tests will simulate actual production runs of Materialize on
gigabytes to terabytes of data over several hours to days, and will therefore
report their results asynchronously (perhaps nightly or weekly).

Details about each of the test suites follow.

## Unit/integration tests

The unit/integration test suite uses the standard test framework that ships with
Cargo. You can run the full test suite like so:

```shell
$ cargo test
```

Some of the packages have tests that depend on ZooKeeper, Kafka, and the
Confluent Schema Registry running locally on the default ports. See the
[Developer guide](develop.md) for full details on setting this up, but
the following will do the trick if you have the Confluent Platform 5.3+ CLI
installed and configured:

```shell
$ confluent local start schema-registry
```

`cargo test` supports many options for precisely controlling what tests get
run, like `--package CRATE`, `--lib`, `--doc`. See its documentation for the
full details.

Two arguments bear special mention.

The first is the `--nocapture` argument, which tells Cargo not to hide the
output of successful test runs:

```shell
$ cargo test -- --nocapture
```

Notice how the `--nocapture` argument appears after a `--` argument. This is not
a typo. This is how you pass an argument to the test binary itself, rather than
to Cargo. This syntax might be a bit confusing at first, but it's used
pervasively throughout Cargo, and eventually you get used to it. (It's also the
POSIX-blessed syntax for indicating that you want option parsing to stop, so you
might be familiar with it from other command-line tools.)

Without `--nocapture`, `println!()` and `dbg!()` output from tests can go
missing, making debugging a very frustrating experience.

The second argument worth special mention is the filter argument, which only
runs the tests that match the specified pattern. For example, to only run tests
with `avro` in their name:

```shell
$ cargo test -- avro
```

As mentioned above, the Rust unit/integration tests follow the standard
convention, and live in a `mod tests { ... }` block alongside the code
they test, or in a `tests/` subdirectory of the crate they test, respectively.

### Clippy, formatting, and linting

CI also performs the lawful evil task of ensuring "good code style" with the following tools:

Tool | Use | Run locally with
-----|-----|-------------------
[Clippy] | Rust semantic nits | `./bin/check`
[rustfmt] | Rust code formatter | `cargo fmt`
Linter | General formatting nits | `./bin/lint`

[Clippy]: https://github.com/rust-lang/rust-clippy
[rustfmt]: https://github.com/rust-lang/rustfmt

## System tests

There are presently two system test frameworks. These are tests against with
Materialize as a whole or near-whole, and often test its interaction with other
systems, like Kafka.

### sqllogictest

The first system test framework is [sqllogictest], which was developed by
the authors of SQLite to cross-check queries across various database vendors.
Here's a representative snippet from a SQLite file:

```
query I rowsort
SELECT - col2 + col1 + col2 FROM tab2
----
17
31
59
```

This snippet will verify that the query `SELECT - col2 + col1 + col2 FROM tab2`
outputs a table with one integer column (that's the `query I` bit) with the
specified results without verifying order (that's the `rowsort` bit). The full
syntax is specified in the [sqllogictest docs][sqllogictest].

sqllogictest ships with a *huge* corpus of test data—something like 6+ million
queries. This takes hours to run in full, so in CI we only run a small subset
on each PR, but can schedule the full run on demand. For many features, you may
not need to write many tests yourself, if you can find an existing sqllogictest
file that has coverage of that feature! (Grep is your friend.)

Note that we certainly don't pass every upstream sqllogictest test at the
moment. You can see the latest status at https://mtrlz.dev/civiz.

To run a sqllogictest against Materialize, you'll need to have PostgreSQL
running on the default port, 5432, and have created a database named after
your user. On macOS:

```shell
$ brew install postgresql
$ brew services start postgresql
$ createdb $(id -u)  # Yes, this is a shell command, not a SQL command.
```

You might reasonably wonder why PostgreSQL is necessary for running
sqllogictests against Materialize. The answer is that we offload the hard work
of mutating queries, like `INSERT INTO ...` and `UPDATE`, to PostgreSQL. We
slurp up the changelog, much like we would if we were running against a
Kafka–PostgreSQL [CDC] solution in production, and then run the `SELECT` queries
against Materialize and verify they match the results specified by the
sqllogictest file.

Once PostgreSQL is running, you can run a sqllogictest file like so:

```shell
$ cargo run --bin sqllogictest --release -- test/sqllogictest/TESTFILE.slt
```

For larger test files, it is imperative that you compile in release mode, i.e.,
by passing the `--release` flag as above. The extra compile time will quickly be
made up for by a much faster execution.

To add logging for tests, append `-vv`, e.g.:

```shell
$ cargo run --bin sqllogictest --release -- test/TESTFILE.slt -vv
```

The offical SQLite test files are in
[test/sqllogictest/sqlite](/test/sqllogictest/sqlite), and some additional test
files from CockroachDB are in
[test/sqllogictest/cockroach](/test/sqllogictest/cockroach). Some additional
Materialize-specific sqllogictest files live in
[test/sqllogictest](/test/sqllogictest) with a filename suffix of `.slt`—feel
free to add more!

### testdrive

Testdrive is a Materialize invention that feels a lot like sqllogictest, except
it supports pluggable commands for interacting with external systems, like
Kafka. Here's a representative snippet of a testdrive file:

```
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=42
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 1}}
{"before": null, "after": {"a": 3, "b": 1}}
{"before": null, "after": {"a": 1, "b": 2}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=43
{"before": null, "after": null}

> SELECT * FROM data
a b
---
1 1
2 1
3 1
1 2
```

The first two commands, the `$ kafka-ingest ...` commands, ingest some data into
a topic named `data`. The next command, `> SELECT * FROM data` operates like a
sqllogictest file. It runs the specified SQL command against Materialize and
verifies the result.

Commands in testdrive are introduced with a `$`, `>`, or `!` character. These
were designed to be reminiscent of how your shell would look if you were running
these commands manually. `$` commands take the name of a testdrive function
(like `kafka-ingest` or `set`), followed by any number of `key=value` pairs. `>`
and `!` introduce a SQL command that is expected to succeed or fail,
respectively.

Note that testdrive actually interacts with Materialize over the network, via
the PostgreSQL wire protocol, so it tests more of Materialize than our
sqllogictest driver does. (It would be possible to run sqllogictest over the
network too, but this is just not how things are configured at the moment.)
Therefore it's often useful to write some additional testdrive tests to make
sure things get serialized on the wire properly. For example, when adding a new
data type, like, say, `DATE`, the upstream CockroachDB sqllogictests will
provide most of the coverage, but it's worth adding some (*very*) basic
testdrive tests (e.g., `> SELECT DATE '1999-01-01'`) to ensure that our pgwire
implementation is properly serializing dates.

To run a testdrive script, you'll need two terminal windows open. In the
first terminal, run Materialize:

```shell
$ cargo run --bin materialized --release
```

In the second terminal, run testdrive:

```shell
$ cargo run --bin testdrive --release -- test/testdrive/TESTFILE.td
```

Testdrive scripts live in [test/](/test) with a `.td` suffix. Again, please add
more!

## Long-running tests

These are still a work in progress. The beginning of the orchestration has
begun, though; see the Docker Compose demo in [demo/chbench](/demo/chbench) if
you're curious.

## What kind of tests should I write?

tl;dr add additional system tests, like sqllogictests and testdrive tests.

Unit/integration tests are great for complicated pure functions. A good example
is Avro decoding. Bytes go in, and `avro::Value`s come out. This is a very easy
unit test to write, and one that provides a lot of value. The Avro specification
is stable, so the behaviour of the function should never change, modulo bugs.

But unit/integration tests can be detrimental when testing stateful functions,
especially those in the middle of the stack. Trying to unit test some of the
functions in the [dataflow](/src/dataflow) package would be an exercise in
frustration. You'd need to mock out a dozen different objects and introduce
several traits, and the logic under test is changing frequently, so the test
will hamper development as often as it catches regressions.

Nikhil's philosophy is that writing a battery of system tests is a much better
use of time. Testdrive and sqllogictest have discovered numerous bugs with the
fiddly bits of timestamp assignment in the dataflow package, even though that's
not what they're designed for. It's true that it's much harder to ascertain what
exactly went wrong–some of these failures presented as hangs in CI—but I wager
that you still net save time by not writing overly complicated and brittle unit
tests.

As a module stabilizes and its abstraction boundaries harden, unit
testing becomes more attractive. But be wary of introducing abstraction just
to make unit tests easier to right.

And, as a general rule of thumb, you don't want to add a new long-running test.
Experience suggests that these are quite finicky (e.g., because a VM fails to
boot), never mind that they're quite slow, so they're best limited to a small
set of tests that exercise core business metrics. A long-running test should, in
general, be scheduled as a separate work item, so that it can receive the
nurturing required to be stable enough to be useful.

[CDC]: https://en.wikipedia.org/wiki/Change_data_capture
