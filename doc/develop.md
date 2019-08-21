# Developer guide

This guide details what you'll need to contribute to Materialize.

Materialize is written in [Rust] and should compile on any recent stable version.

As far as external components go, Materialize has hard dependencies on [Apache
ZooKeeper] and [Apache Kafka]. It also has an optional dependency on the [Confluent
Schema Registry]. Conveniently, [Confluent Platform] bundles these three components
into a single download. Not so conveniently, it bundles many other components as
well. Strangely, their webpages do not mention ZooKeeper at all, even though version
5.3.0 most definitely does include the corresponding binaries.

[Rust]: https://www.rust-lang.org
[Apache ZooKeeper]: https://zookeeper.apache.org
[Apache Kafka]: https://kafka.apache.org
[Confluent Schema Registry]: https://www.confluent.io/confluent-schema-registry/
[Confluent Platform]: https://www.confluent.io/product/confluent-platform/

## Installing

### Rust

Install Rust via [rustup]:

```shell
curl https://sh.rustup.rs -sSf | sh
```

Rustup will automatically select the correct toolchain version specified in [materialize/rust-toolchain](/rust-toolchain).

[rustup]: https://www.rust-lang.org/tools/install


### Confluent Platform

On macOS, [Homebrew] makes package installation easy breezy:

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

This will start a new shell with all the necessary dependencies available and pinned to the correct version.
Reach out to @jamii for more information.

[Homebrew]: https://brew.sh
[confluent-install]: https://docs.confluent.io/current/installation/installing_cp/index.html
[nix]: https://nixos.org/nix/

## Building

Materialize is fully integrated with Cargo, so building it is dead simple:

```shell
git clone git@github.com:MaterializeInc/materialize.git
cd materialize
cargo run --bin materialized
```

Note that we currently depend on a private GitHub repository,
[MaterializeInc/sqlparser], and Cargo doesn't handle this particularly well. In
particular, you'll need to have an SSH agent running with credentials that
allow access to the MaterializeInc GitHub organization.

If you don't already have an SSH agent running, this will get you unblocked:

```shell
eval $(ssh-agent)  # Start SSH agent and export SSH_AUTH_SOCK variable.
ssh-add            # Add default key (~/.ssh/id_rsa) to SSH agent.
```

You'll likely want to do something more clever so that an SSH agent is
automatically started upon login, but we leave it to you to sort that out.

[MaterializeInc/sqlparser]: https://github.com/MaterializeInc/sqlparser.git

You can use the included `confluent` CLI command to start and stop individual services. For example:

```shell
confluent status        # View what services are currently running.
confluent start kafka   # Start Kafka and any services it depends upon.
confluent log kafka     # View Kafka log file.
```

Beware that the CLI is fairly buggy, especially around service management.
Putting your computer to sleep often causes the service status to get out of
sync. In other words, trust the output of `confluent log` and `ps ... | grep`
over the output of `confluent status`. Still, it's reliable enough to be more
convenient than managing each service manually.

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

