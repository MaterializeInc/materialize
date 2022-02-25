# Developer guide

This guide details what you'll need to contribute to Materialize.

Materialize is written in [Rust] and should compile on any recent stable
version.

Materialize can be connected to many different types of event sources:

* Local files with line-by-line textual events, where structured data can be
  extracted via regular expressions or CSV parsing.
* Custom Kafka topics, where events are encoded with Protobuf or Avro, with
  support for additional encoding formats coming soon.
* Kafka topics managed by a CDC tool like Debezium, where events adhere to a
  particular "envelope" format that distinguishes updates from insertions and
  deletions.
* Streaming HTTP sources? Apache Pulsar sources? With a bit of elbow grease,
  support for any message bus can be added to Materialize!

Note that local file sources are intended only for ad-hoc experimentation and
analysis. Production use cases are expected to use Kafka sources, which have a
better availability and durability story.

## Installing

### C components

Materialize depends on several components that are written in C and C++, so
you'll need a working C and C++ toolchain. You'll also need to install the
[CMake] build system.

On macOS, if you install [Homebrew], you'll be guided through the process of
installing Apple's developer tools, which includes a C compiler. Then it's a
cinch to install CMake:

```
brew install cmake
```

On Debian-based Linux variants, it's even easier:

```shell
sudo apt update
sudo apt install build-essential cmake
```

On other platforms, you'll have to figure out how to get these tools yourself.

### Rust

Install Rust via [rustup]:

```shell
curl https://sh.rustup.rs -sSf | sh
```

We recommend that you do _not_ install Rust via your system's package manager.
We closely track the most recent version of Rust. The version of Rust in your
package manager is likely too old to build Materialize.

### Editors and IDEs

In principle, any text editor can be used to edit Rust code. There is nothing special about Materialize's codebase: any general Rust development setup should work fine.

By default, we recomend that developers without a strong preference of editor use
Visual Studio Code with the Rust-Analyzer plugin. This is the most mainstream
setup for developing Materialize, and the one for which you are the most likely
to be able to get help if something goes wrong. It's important to note that you
**should not** install the "Rust" plugin, as it is known to
conflict with Rust-Analyzer; the latter has far more advanced code navigation
features and is the de-facto standard for developing Rust.

Visual Studio Code also works well for editing Python; to work on the Python code
in the Materialize repository, install the official Python extension from Microsoft
and add the following to your `settings.json`.

``` json
{
  "python.linting.mypyEnabled": true,
  "python.analysis.extraPaths": [
      "misc/python"
  ],
  "python.defaultInterpreterPath": "misc/python/venv/bin/python"
}
```

If you prefer to use another editor, such as Vim or Emacs, we recommend that
you install an LSP plugin with Rust-Analyzer. How to do so is beyond the scope
of this document; if you have any issues, ask in one of the engineering channels
on Slack.

Besides Rust-Analyzer, the only other known tool with good code navigation features
is CLion along with its Rust plugin. This is a good choice for developers who prefer
the JetBrains ecosystem, but we no longer recommend it by default, since
Rust-Analyzer has long since caught up to it in maturity. If you are a
Materialize employee, ask Nikhil Benesch on Slack for access to our corporate
JetBrains license. If you're not yet sure you want to use CLion, you can
use the 30-day free trial.

### Debugging

The standard debuggers for Rust code are `rust-lldb` on macOS, and `rust-gdb` on GNU/Linux.
(It is also possible to run `rust-lldb` on GNU/Linux if necessary for whatever reason).
These are wrappers around `lldb` and `gdb`, respectively, that endow them with slightly
improved capabilities for pretty-printing Rust data structures. Visual Studio Code
users may want to try the [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb)
plugin.

Unfortunately, you will soon find that these programs work less well than the equivalent
tools for some other mainstream programming languages. In particular, inspecting
complex data structures is often tedious and difficult. For this reason, most developers routinely use
`println!` statements for debugging, in addition to (or instead of) these standard debuggers.

### Confluent Platform

The [Confluent Platform] bundles [Apache ZooKeeper] and [Apache Kafka] with
several non-free Confluent tools, like the [Confluent Schema Registry] and
[Control Center]. For local development, the [Confluent CLI] allows easy
management of these services.

**Confluent Platform is not be required for changes that don't need
Kafka integration**. If your changes don't affect integration with external systems
and can be fully exercised by SQL logic tests, we recommend not installing
the Confluent Platform, as it is a rather heavy dependency. Most Materialize employees,
or other major contributors, will probably need to run the full test suite and
should therefore install the Confluent Platform.

#### All platforms

First, install the CLI. As of January 2021 you can run this command on
macOS and Linux:

```shell
curl -L --http1.1 https://cnfl.io/cli | sh -s -- -b /usr/local/bin
```

If this no longer works, follow the instructions in the [Confluent CLI]
documentation. Then please update this guide with the new instructions!

#### macOS

You will need JDK 8 or 11. The easiest way to install this is via Homebrew:

```shell
brew install --cask homebrew/cask-versions/temurin8
```

Then, download and extract the Confluent Platform tarball:

```shell
INSTALL_DIR=$HOME/confluent  # You can choose somewhere else if you like.
mkdir $INSTALL_DIR
curl http://packages.confluent.io/archive/7.0/confluent-7.0.1.tar.gz | tar -xC $INSTALL_DIR --strip-components=1
echo export CONFLUENT_HOME=(cd $INSTALL_DIR && pwd) >> ~/.bashrc
source ~/.bashrc
confluent local services start
```

If you have multiple JDKs installed and your current JAVA_HOME points to an incompatible version,
you can explicitly run confluent with JDK 8 or 11:

```
JAVA_HOME=$(/usr/libexec/java_home -v 1.8) confluent local services start
```

#### Linux

On Debian-based Linux variants, you can use APT to install Java and the
Confluent Platform:

```shell
curl http://packages.confluent.io/deb/6.0/archive.key | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/6.0 stable main"
sudo apt update
sudo apt install openjdk-8-jre-headless confluent-community-2.13
echo export CONFLUENT_HOME=/ >> ~/.bashrc
source ~/.bashrc
confluent local services start
```

On other Linux variants, you'll need to make your own way through [Confluent's
installation instructions][confluent-install]. Note that, at the time of
writing, only Java 8 and 11 are supported.

## Building Materialize

Materialize is fully integrated with Cargo, so building it is dead simple:

```shell
git clone git@github.com:MaterializeInc/materialize.git
cd materialize
cargo run --release -- --workers 2
```

Because the MaterializeInc organization requires two-factor authentication
(2FA), you'll need to clone via SSH as indicated above, or [configure a personal
access token for use with HTTPS][github-https].

## Prepping Confluent

As mentioned above, **Confluent Platform is only required need to test Kafka
sources and sinks against a *local* Kafka installation.** If possible, we
recommend that you don't run the Confluent Platform if you don't need it, as it
is very memory hungry.

If you do need the Confluent Platform running locally, execute the following
commands:

```shell
confluent local services kafka start     # Also starts Zookeeper.
confluent local services schema-registry start
```

You can also use the included `confluent` CLI command to start and stop
individual services. For example:

```shell
confluent local services status        # View what services are currently running.
confluent local services kafka start   # Start Kafka and any services it depends upon.
confluent local services kafka log     # View Kafka log file.
```

Beware that the CLI is fairly buggy, especially around service management.
Putting your computer to sleep often causes the service status to get out of
sync. In other words, trust the output of `confluent local services <service>
log` and `ps ... | grep` over the output of `confluent local services status`.
Still, it's reliable enough to be more convenient than managing each service
manually.

When the confluent local services are running, they can be examined via a web
UI which defaults to `localhost:9021`.

## Web UI

Materialize embeds a web UI, which it serves from port 6875. If you're running
Materialize locally, you can view the web UI at http://localhost:6875.

Developing the web UI can be painful, as by default the HTML, CSS, and JS source
code for the UI gets baked into the binary, and so making a change requires a
full rebuild of the binary.

To speed up the development cycle, you can enable the `dev-web` feature like so:

```shell
cd src/materialized
cargo run --features=dev-web -- --dev -w1
```

In this mode, every request for a static file will reload the file from disk.
Changes to standalone CSS and JS files will be reflected immediately upon
reload, without requiring a recompile!

Note that `dev-web` can only hot-reload the the files in
`src/materialized/src/static`. The HTML templates in
`src/materialized/src/templates` use a compile-time templating library called
[`askama`], and so changes to those templates necessarily require a recompile.

For details about adding a new JavaScript/CSS dependency, see the comment in
[`src/materialized/build/npm.rs`](/src/materialized/build/npm.rs).

## Testing

Materialize's testing philosophy is sufficiently complex that it warrants its
own document. See [Developer guide: testing](guide-testing.md).

## Style

CI performs the lawful evil task of ensuring "good code style" with the
following tools:

Tool                  | Use                     | Run locally with
----------------------|-------------------------|-------------------
[Clippy]              | Rust semantic nits      | `./bin/check`
[rustfmt]             | Rust code formatter     | `cargo fmt`
Linter                | General formatting nits | `./bin/lint`
[Unused dependencies] | Check for unused deps   | `./bin/unused-deps`

See also the [style guide](style.md) for some additional soft
recommendations.

### Unused dependencies

Use `.bin/unused-deps`. This is also run in ci; use this
[guide][https://github.com/est31/cargo-udeps#ignoring-some-of-the-dependencies]
for information about how to ignore false-positives. Common examples are
dependencies only used on certain platforms.

## Submitting and reviewing changes

See [Developer guide: submitting and reviewing changes](guide-changes.md).

## Publishing code changes

### crates.io

Before publishing internal Rust crates to `crates.io`(https://crates.io/), be sure to
indicate that `MaterializeInc` is the reponsible maintainer by running the following
command:
```shell
cargo owner --rm <your personal login>
cargo owner --add github:MaterializeInc:crate-owners
```

### PyPI

Use the [`materializeinc`](https://pypi.org/user/materializeinc/) PyPI user to upload
and update Materialize's Python packages on PyPI. Login information can be found in the
shared 1Password account.

## Other repositories

Where possible, we prefer to keep things in the main repository (a "monorepo"
approach). There are a few notable exceptions:

  * **[rust-sasl]**, Cyrus SASL bindings for Rust
  * **[rust-krb5-src]**, Rust build system integration for libkrb5, MIT's
    Kerberos implementation.
  * **[dbt-materialize]**, data build tool (dbt) adapter for Materialize

Don't add to this list without good reason! Separate repositories are
acceptable for:

  * Rapid iteration on new Materialize plugins or integrations, where the CI
    time or code quality requirements in the main repository would be
    burdensome. When the code is more stable, the repository should be
    integrated into the main Materialize repository.

  * Stable foundational components where community contribution is desirable.
    For example, rust-sasl is a very small package, and asking contributors
    to clone the entire Materialize repository would be a large barrier to
    entry. Changes to Materialize very rarely require changes in rust-sasl, so
    maintaining the two separately does not introduce much overhead.

## Our Crates

We break up our rust code into crates mostly to improve compilation time, but
also sometimes because our code is intended to be open sourced or to be re-used
between different binaries.

You can view a relationship diagram of our crates by running the following
command:

```shell
bin/crate-diagram
```

It is possible to view transitive dependencies of a select subset of roots by specifying the `--roots` flag with a comma separated list of crates:

```shell
bin/crate-diagram --roots sql,dataflow
```

## Developer tools

The Materialize repo contains many useful scripts that can be included in your environment
to improve your development experience.

### Automatic style checks

To ensure each code change passes all style nits before pushing to GitHub, symlink `pre-push`
into your local git hooks:

```sh
ln -s ./misc/githooks/pre-push .git/hooks/pre-push
```

### Shell completion

Some Materialize scripts have shell completion, and the latest versions of the completions files
are checked in to `misc/completions`. The contents of this directory can be sourced into your shell,
and will stay updated as any changes are made.

To add the completions to bash, add the following to your `~/.bashrc`:

```shell
source /path/to/materialize/misc/completions/bash/*
```

For zsh, add the follow to your `~/.zshrc`:

```shell
source /path/to/materialize/misc/completions/zsh/*
```

### Editor add-ons

A few editor-specific add-ons and configurations have been authored to improve the editing of
Materialize-specific code. Check `misc/editor` for add-ons that may be relevant for your editor
of choice.

[Apache Kafka]: https://kafka.apache.org
[Apache ZooKeeper]: https://zookeeper.apache.org
[askama]: https://github.com/djc/askama
[Clippy]: https://github.com/rust-lang/rust-clippy
[Unused dependencies]: https://github.com/est31/cargo-udeps
[CMake]: https://cmake.org
[Confluent CLI]: https://docs.confluent.io/current/cli/installing.html#scripted-installation
[Confluent Platform]: https://www.confluent.io/product/confluent-platform/
[Confluent Schema Registry]: https://www.confluent.io/confluent-schema-registry/
[confluent-install]: https://docs.confluent.io/current/installation/installing_cp/index.html
[Control Center]: https://www.confluent.io/confluent-control-center/
[dbt-materialize]: https://github.com/materializeInc/dbt-materialize
[github-https]: https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line
[Homebrew]: https://brew.sh
[rust-krb5-src]: https://github.com/MaterializeInc/rust-krb5-src
[rust-sasl]: https://github.com/MaterializeInc/rust-sasl
[Rust]: https://www.rust-lang.org
[rustfmt]: https://github.com/rust-lang/rustfmt
[rustup]: https://www.rust-lang.org/tools/install
[sqlparser]: https://github.com/MaterializeInc/sqlparser
