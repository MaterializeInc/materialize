# Developer guide

This guide details what you'll need to contribute to Materialize.

Materialize is written in [Rust] and should compile on any recent stable
version.

As far as external components go, Materialize has hard dependencies on [Apache
ZooKeeper] and [Apache Kafka]. It also has an optional dependency on the
[Confluent Schema Registry]. Conveniently, the [Confluent Platform] bundles these
three components into a single download, and the [Confluent CLI] allows easy
management of the services for local development.

[Rust]: https://www.rust-lang.org
[Apache ZooKeeper]: https://zookeeper.apache.org
[Apache Kafka]: https://kafka.apache.org
[Confluent Schema Registry]: https://www.confluent.io/confluent-schema-registry/
[Confluent Platform]: https://www.confluent.io/product/confluent-platform/
[Confluent CLI]: https://docs.confluent.io/current/cli/installing.html#scripted-installation

## Installing

### Rust

Install Rust via [rustup]:

```shell
curl https://sh.rustup.rs -sSf | sh
```

Rustup will automatically select the correct toolchain version specified in
[materialize/rust-toolchain](/rust-toolchain).

[rustup]: https://www.rust-lang.org/tools/install


### Confluent Platform

On macOS, the easiest installation method is to use [Homebrew]:

```shell
brew install confluent-platform
```

On Debian-based Linux variants, it's a tad more involved:

```shell
curl http://packages.confluent.io/deb/5.2/archive.key | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/5.2 stable main"
sudo apt update
sudo apt install openjdk-8-jre-headless confluent-community-2.12
```

On other Linux variants, you'll need to make your own way through [Confluent's
installation instructions][confluent-install]. Note that, at the time of
writing, Java 8 is a strict requirement. Later versions of Java are not
supported.

On Linux, you might want to consider using [nix]. It is a purely functional
package manager, which is appealing because bits and pieces of state usually
are to blame when package management goes wrong. Plus, getting started is easy:

```shell
cd materialize
nix-shell
```

This will start a new shell with all the necessary dependencies available and
pinned to the correct version. Reach out to @jamii for more information.

[Homebrew]: https://brew.sh
[confluent-install]: https://docs.confluent.io/current/installation/installing_cp/index.html
[nix]: https://nixos.org/nix/

### Confluent CLI

As of Sep 23, 2019 you can run:

```shell
curl -L https://cnfl.io/cli | sh -s -- -b /usr/local/bin
```

However, if this ever stops working, check out these great docs on the
[Confluent CLI].

## Building Materialize

Materialize is fully integrated with Cargo, so building it is dead simple:

```shell
git clone git@github.com:MaterializeInc/materialize.git
cd materialize
cargo run --bin materialized
```

Because the MaterializeInc organization requires two-factor authentication
(2FA), you'll need to clone via SSH as indicated above, or [configure a personal
access token for use with HTTPS][github-https].

[MaterializeInc/sqlparser]: https://github.com/MaterializeInc/sqlparser.git
[github-https]: https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line

## Prepping Confluent

Like we mentioned above, you need to have a few Confluent services running to
get Materialize to work. To prep what you need (for the [demo], at least), run
the following:

```shell
confluent local start kafka     # Also starts zookeeper
confluent local start schema-registry
```

You can also use the included `confluent` CLI command to start and stop
individual services. For example:

```shell
confluent local status        # View what services are currently running.
confluent local start kafka   # Start Kafka and any services it depends upon.
confluent local log kafka     # View Kafka log file.
```

Beware that the CLI is fairly buggy, especially around service management.
Putting your computer to sleep often causes the service status to get out of
sync. In other words, trust the output of `confluent local log` and `ps ... |
grep` over the output of `confluent local status`. Still, it's reliable enough
to be more convenient than managing each service manually.

[demo](demo.md)

## Symbiosis mode

For the convenience of developers, Materialize has a semi-secret "symbiosis"
mode that turns Materialize into a full HTAP system, rather than an OLAP system
that must sit atop a OLTP system via a CDC pipeline. In other words, where
you would normally need to plug MySQL into Debezium into Kafka into Materialize,
and run all the Confluent services that that entails, you can instead run:

    $ materialized --symbiosis postgres://localhost:5432

When symbiosis mode is active, all DDL statements and all writes will be routed
to the specified PostgreSQL server. `CREATE TABLE`, for example, will create
both a table in PostgreSQL and a source in Materialize that mirrors that table.
`INSERT`, `UPDATE`, and `DELETE` statements that target that table will be
reflected in Materialize for the next `SELECT` statement.

Symbiosis mode is not suitable for production use, as its implementation is
very inefficient. It is, however, excellent for manually taking Materialize
for a spin without the hassle of setting up various Kafka topics and Avro
schemas. It also powers our sqllogictest runner.

See the [symbiosis crate documentation](https://mtrlz.dev/api/symbiosis) for
more details.

## Testing

Materialize's testing philosophy is sufficiently complex that it warrants its
own document. See [Developer guide: testing](develop-testing.md).

## Git workflow

### Submitting changes

We require that every change is first opened as a GitHub pull request and
subjected to a CI run. GitHub will prevent you from pushing directly to master
or merging a PR that does not have a green CI run.

Our CI provider is Buildkite (https://buildkite.com). It's like Travis CI or
Circle CI, if you're familiar with either of those, except that it lets you
bring your own infrastructure. Details about the setup are in
[ci/README.md](/ci/README.md), but the day-to-day interaction with Buildkite
should be straightforward.

If you want more confidence that your PR will succeed in CI, you can run
`bin/pre-push` before pushing your changes. You can configure Git to do this
automatically by following the instructions in
[misc/githooks/pre-push](/misc/githooks/pre-push). The `pre-push` checks don't
run the full battery of tests, but a small subset that experience shows are
the most frustrating when they fail in CI, like linters.

While the team is small, we leave it up to you to decide whether your PR needs a
review. Your first several PRs at Materialize should go through review no matter
what, but once you learn the ropes you should feel free to land small,
uncontroversial changes without review. It's not always possible to perfectly
predict controversiality ahead of time, of course, but reverts are cheap and
easy, so err on the side of merging for now.

### Git details

Nikhil highly recommends that you configure `git pull` to use rebase instead
of merge:

```shell
git config pull.rebase true
git config rebase.autoStash true
```

This keeps the Git history tidy, since it avoids creating merge commits when you
`git pull` with unpushed changes. The `rebase.autoStash` option makes this
workflow particularly ergonomic by stashing any uncommitted changes you have
when you run `git pull`, then unstashing them after the rebase is complete.

## Other repositories

Several components of Materialize are maintained in separate Git repositories.
Where possible, we prefer to keep things in the main repository (a "monorepo"
approach), but when forking existing packages, maintaining a separate repository
makes it easier to integrate changes from upstream.

Some notable repositories include:

  * **[mtrlz-setup]**, containing automatic development environment setup
    scripts;
  * **[rust-rdkafka]**, a fork of the Rust Kafka library;
  * **[sqlparser]**, a heavily-customized fork of a general SQL parsing package.

As mentioned before, because the MaterializeInc organization requires two-factor
authentication (2FA), to clone these repositories you'll need to use either SSH
or [configure a personal access token for use with HTTPS][github-https].

[mtrlz-setup]: https://github.com/MaterializeInc/mtrlz-setup
[rust-rdkafka]: https://github.com/MaterializeInc/rust-rdkafka
[sqlparser]: https://github.com/MaterializeInc/sqlparser
