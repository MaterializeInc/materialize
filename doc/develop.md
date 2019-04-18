# Developer guide

This guide details what you'll need to contribute to Materialize.

Materialize is written in [Rust] and should compile on any recent stable version.

As far as external components go, Materialize has a hard dependency on [Apache
ZooKeeper] and [Apache Kafka], and an optional dependency on the [Confluent
Schema Registry].

[Rust]: https://www.rust-lang.org
[Apache ZooKeeper]: https://zookeeper.apache.org
[Apache Kafka]: https://kafka.apache.org
[Confluent Schema Registry]: https://www.confluent.io/confluent-schema-registry/

## Installing dependencies with Nix

The easiest way to install all the necessary dependencies is to use [Nix]:

```shell
cd materialize
nix-shell
```

This will start a new shell with all the necessary dependencies available and pinned to the correct version.

(If you encounter any problems with Nix, @jamii is responsible for fixing them)

[Nix]: https://nixos.org/nix/

## Installing dependencies manually

### Rust

Install Rust via [rustup]:

```shell
curl https://sh.rustup.rs -sSf | sh
```

Rustup will automatically select the correct toolchain version specified in [materialize/rust-toolchain](/rust-toolchain).

[rustup]: https://www.rust-lang.org/tools/install

### Confluent platform

The "Confluent Platform" bundles ZooKeeper, Kafka, and the Confluent Schema Registry
(along with a few other components that we don't currently care about).

On macOS, the easiest installation method is to use Homebrew:

```shell
brew install confluent-oss
```

On Debian-based Linux variants:

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

[confluent-install]: https://docs.confluent.io/current/installation/installing_cp/index.html

## Building

Materialize is fully integrated with Cargo, so building it is dead simple:

```shell
git clone git@github.com:MaterializeInc/materialize.git
cd materialize
cargo run
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

## Git workflow

### Submitting changes

While the team is small, there is virtually no process around submitting
changes to master. Just commit and push!

Of course, if you feel like a change is particularly in need of review, feel
free to create a branch and open a PR instead of landing the change immediately.
If you're new to Rust, we recommend you do this for your first few PRs, so the
more experienced folks can teach you about idioms.

As an experiment, Nikhil has been using Phabricator's post-commit audit feature
to ensure that every commit gets at least one other pair of eyes on it without
introducing pre-push review latency. It's not yet clear whether this is
worthwhile, especially given how heavyweight the Phabricator suite is. You can
view the list of unaudited commits here:
https://materialize.phacility.com/diffusion/commit/.

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

### Continuous integration

Even though we commit directly to master, CI (https://buildkite.com) runs on
every commit. It's expected (and encouraged) to occasionally break the build! If
we're not occasionally breaking the build, we're not moving fast enough. Just
try to notice when you've pushed a failing commit, and fix it as soon as you
can. You can configure Buildkite to email you whenever a build fails, or you can
wait for someone else to notice and bug you.

If the idea of pushing untested code to master scares you, you can simulate a
CI run locally by running `bin/ci-test` before pushing.

Caveat emptor: this workflow is intentionally designed not to scale. We'll need
to continually evolve this process as the team grows and the product matures.
