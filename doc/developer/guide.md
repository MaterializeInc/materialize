# Developer guide

This guide details what you'll need to contribute to Materialize.

Materialize consists of several services written in [Rust] that are orchestrated
by [Kubernetes]. Supporting build and test tools are written in a combination of
Rust, [Python], and [Bash]. Tests often use [Docker Compose] rather than
Kubernetes to orchestrate interactions with other systems, like [Apache Kafka].

## Install build and test dependencies

### C components

Materialize depends on several components that are written in C and C++, so
you'll need a working C and C++ toolchain. You'll also need to install:
* The [CMake] build system
* libclang
* PostgreSQL
* lld (on Linux, or set a custom `RUSTFLAGS`)

On macOS, if you install [Homebrew], you'll be guided through the process of
installing Apple's developer tools, which includes a C compiler and libclang.
Then it's a cinch to install CMake and PostgreSQL.

```
brew install cmake postgresql
```

On Debian-based Linux variants, it's even easier:

```shell
sudo apt update
sudo apt install build-essential cmake postgresql-client libclang-dev lld
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

For details on how we upgrade Rust see [here](/doc/developer/upgrade-rust.md).

### Docker

Materialize's tests mostly require Docker and Docker Compose to be installed. On macOS it is part of Docker Desktop:

```shell
brew install docker
```

On Debian-based Linux both Docker and the Docker Compose plugin have to be installed:

```shell
sudo apt update
sudo apt install docker docker-compose-plugin
```

### Bazel

Materialize can also optionally be built with [Bazel](https://bazel.build/). To
learn more about Bazel and how it's setup at Materialize, checkout our
[Bazel documentation](/doc/developer/bazel.md).

### Metadata store

Running Materialize locally requires a running Postgres / CockroachDB server.

On macOS, when using Homebrew, CockroachDB can be installed and started via:

```shell
brew install materializeinc/cockroach/cockroach
brew services start cockroach
```

(We recommend use of our [forked Homebrew tap][forked-cockroach-tap] because it
runs CockroachDB using an in-memory store, which avoids slow filesystem
operations on macOS.)

On Linux, we recommend using Docker:

```shell
docker run --name=cockroach -d -p 127.0.0.1:26257:26257 -p 127.0.0.1:26258:8080 cockroachdb/cockroach:v23.1.11 start-single-node --insecure
```

If you can successfully connect to CockroachDB with either
`psql postgres://root@localhost:26257` or `cockroach sql --insecure`, you're
all set.

### Eatmydata

If you are just testing Materialize locally and don't care about data loss you can run environmentd with `eatmydata environmentd`, which will disable fsync calls.

Similarly postgres as the metadata store can be instructed to eat your data using `echo LD_PRELOAD=libeatmydata.so > /etc/postgresql/17/main/environment`, and then restarting it.

On my Linux system without `eatmydata` for both Materialize and Postgres, running with `bin/environmentd --reset --optimized --no-default-features --postgres=postgres://deen@%2Fvar%2Frun%2Fpostgresql`:
```
DROP TABLE
Time: 105.810 ms
CREATE TABLE
Time: 163.484 ms
```

After enabling `eatmydata` in Materialize and Postgres:
```
DROP TABLE
Time: 7.951 ms
CREATE TABLE
Time: 10.459 ms
```

Or in mzcompose:
```bash
docker run --env MZ_EAT_MY_DATA=1 -p 127.0.0.1:6875:6875 materialize/materialized:latest
```

Before:
```
DROP TABLE
Time: 133.021 ms
CREATE TABLE
Time: 111.492 ms
```
After:
```
DROP TABLE
Time: 6.504 ms
CREATE TABLE
Time: 8.773 ms
```

### Python

Materialize's build and test infrastructure is largely written in [Python];
running our integration tests, in particular, requires a local Python
environment. Most of this should be taken care of by the `bin/pyactivate`
script, which constructs a local virtual environment and keeps necessary
dependencies up to date.

We support, as a minimum version, the default Python provided in the [most
recent Ubuntu LTS release](https://wiki.ubuntu.com/Releases). As of October 2023
this is Python 3.10, provided in Ubuntu "Jammy Jellyfish". Earlier versions may
work but are not supported. Our recommended installation methods are:

- macOS: [Homebrew](https://brew.sh)
- Linux: System package manager if possible, or [community package repositories](https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa) if necessary
- Windows: [Microsoft App Store](https://apps.microsoft.com/detail/python-3-11/9NRWMJP3717K?hl=en-US&gl=US)

If none of the above work well for you, these are a few other methods that have
worked for us in the past, but are not formally supported:

- [pyenv](https://github.com/pyenv/pyenv)
- [asdf](https://asdf-vm.com/)
- [Pixi](https://github.com/prefix-dev/pixi)
- [Conda](https://docs.conda.io/en/latest/)

### Confluent Platform

The [Confluent Platform] bundles [Apache ZooKeeper] and [Apache Kafka] with
several non-free Confluent tools, like the [Confluent Schema Registry] and
[Control Center]. For local development, the [Confluent CLI] allows easy
management of these services.

**Confluent Platform is not required for changes that don't need Kafka
integration**. If your changes don't affect integration with external systems
and can be fully exercised by SQL logic tests, we recommend not installing the
Confluent Platform, as it is a rather heavy dependency. Most Materialize
employees, or other major contributors, will probably need to run the full test
suite and should therefore install the Confluent Platform.


#### All platforms

First, install the CLI. As of early July 2022 you can run this command on
macOS and Linux:

```shell
curl -sL --http1.1 https://cnfl.io/cli | sudo sh -s -- -b /usr/local/bin latest
```

If this no longer works, follow the instructions in the [Confluent CLI]
documentation. Then please update this guide with the new instructions!

#### macOS

You will need JDK 8 or 11. The easiest way to install this is via Homebrew:

```shell
brew install --cask homebrew/cask-versions/temurin11
```

Then, download and extract the Confluent Platform tarball (when using bash, replace `~/.zshrc` with `~/.bashrc`):

```shell
INSTALL_DIR=$HOME/confluent  # You can choose somewhere else if you like.
mkdir $INSTALL_DIR
curl http://packages.confluent.io/archive/7.0/confluent-7.0.1.tar.gz | tar -xzC $INSTALL_DIR --strip-components=1
echo export CONFLUENT_HOME=$(cd $INSTALL_DIR && pwd) >> ~/.zshrc
source ~/.zshrc
confluent local services start
```
When using bash, note that you need to create a `.bash_profile` that sources `.bashrc` to ensure
the above works with the Terminal app.

If you have multiple JDKs installed and your current JAVA_HOME points to an incompatible version,
you can explicitly run confluent with JDK 8 or 11:

```
JAVA_HOME=$(/usr/libexec/java_home -v 1.11) confluent local services start
```

#### Linux

On Debian-based Linux variants, you can use APT to install Java and the
Confluent Platform:

```shell
curl http://packages.confluent.io/deb/6.0/archive.key | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/6.0 stable main"
sudo apt update
sudo apt install openjdk-11-jre-headless confluent-community-2.13
echo export CONFLUENT_HOME=/ >> ~/.bashrc
source ~/.bashrc
confluent local services start
```

On other Linux variants, you'll need to make your own way through [Confluent's
installation instructions][confluent-install]. Note that, at the time of
writing, only Java 8 and 11 are supported.

Alternatively, it is possible to get an all-in-one tarball from
[here](https://packages.confluent.io/archive/). Then untar this to a
location, set `$CONFLUENT_HOME` to this location and add `$CONFLUENT_HOME/bin`
to your $PATH. I found this to be the most convenient way to get confluent
and it also works in a distro neutral way (if you are using, Arch Linux for example).

### Nix

Optionally, you can use [nix][Nix] to install all required dependencies on both Linux and macOS,
using the provided [`shell.nix`](../../misc/nix). [Install nix][nix-manual] and use `nix-shell` to enter an
environment that is isolated from the main OS.

```bash
nix-shell misc/nix/shell.nix
[nix-shell]$ rustup install stable # If not installed already
```

Materialize can then be built inside this shell. Note that CockroachDB is not included in the above configuration
and needs to be installed separately, as described above. Also, IDEs will not be able to access the installed
dependencies unless they are started from within the `nix-shell` environment:

```bash
# Linux
[nix-shell]$ code .
# macOS
[nix-shell]$ open -na "RustRover"
[nix-shell]$ open -na "Visual Studio Code"
```

Note that on macOS, the `mzcompose` tests fail to run from within `nix-shell`,
as our config does not yet set up [cross-compilation support](/misc/python/materialize/xcompile.py)
for `x86-64` needed to run `mzcompose`.

## Building Materialize

First, clone this repository:

```shell
git clone git@github.com:MaterializeInc/materialize.git
```

Because the MaterializeInc organization requires two-factor authentication
(2FA), you'll need to clone via SSH as indicated above, or [configure a personal
access token for use with HTTPS][github-https].

You also have to clone the associated submodules, at least `misc/fivetran-sdk` is required to build Materialize, while `test/sqllogictest/sqlite` is only required to run SQL Logic Tests:

```shell
git submodule update --init --recursive
```

Then you can build Materialize. Because Materialize is a collection of several
Rust services that need to be built together, each service can be built
individually via Cargo, but we recommend using the `bin/environmentd` script to
drive the process:

```shell
cd materialize
bin/environmentd [--release] [--optimized] [<environmentd arg>...]
```

### WebAssembly / WASM

Some crates are compiled to WebAssembly and published to npm. This is
accomplished through `wasm-pack`. Install it by running:

```shell
cargo install wasm-pack
```

WASM builds can then be initiated through

```shell
./bin/wasm-build <path/to/crate>
```

WASM crates reside in `misc/wasm/` Cargo workspace, and should be kept out of
the main Cargo workspace to avoid cache invalidation issues.

## Running Confluent Platform

As mentioned above, **Confluent Platform is only required need to test Kafka
sources and sinks against a *local* Kafka installation.** If possible, we
recommend that you don't run the Confluent Platform if you don't need it, as it
is very memory hungry.

If you do need the Confluent Platform running locally, execute the following
commands:

```shell
confluent local services schema-registry start  # Also starts ZooKeeper and Kafka.
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
UI which defaults to http://localhost:9021.

It might happen that the start script says that it failed to start
zookeeper/kafka/schema-registry, but it actually starts them successfully, it
just can't detect them for some reason. In this case, you can just run
`confluent local services schema-registry start` 3 times, and then everything
is up.

## Running and connecting to local Materialize

Once things are built and CockroachDB is running, you can start Materialize:

```shell
bin/environmentd --reset -- --all-features --unsafe-mode
```

This should bootstrap a fresh Materialize instance. Once you see the logline
"environmentd v<version> listening...", you can connect to the database via:

```shell
psql -U materialize -h localhost -p 6875 materialize
```

This uses the external SQL port. If you wish to connect using a system account,
you can use the internal port with the `mz_system` user:

```shell
psql -U mz_system -h localhost -p 6877 materialize
```

In order to run Materialize with large clusters and more memory available, you have to set a license key. Set `export MZ_LICENSE_KEY=$HOME/license-key` and write the `materialize dev license key` from 1Password into that file. In order to have `mzcompose` based tests pick up the license key, set `export MZ_CI_LICENSE_KEY=$(cat $MZ_LICENSE_KEY)`.

## Console UI

Console can point at your local environmentd. To use this feature, pass the
internal console flag:

```shell
bin/environmentd -- --internal-console-redirect-url="https://local.console.materialize.com"
```

Then visit http://localhost:6878/internal-console/. This is a great way to
dogfood the console, feedback is valuable.

Note there is no frontegg login in this mode, so all frontegg features are
disabled.

## Web UI

Materialize embeds a web UI, which it serves from port 6876. If you're running
Materialize locally, you can view the web UI at http://localhost:6876.

Developing the web UI can be painful, as by default the HTML, CSS, and JS source
code for the UI gets baked into the binary, and so making a change requires a
full rebuild of the binary.

To speed up the development cycle, you can enable the `dev-web` feature like so:

```shell
cd src/environmentd
bin/environmentd --features=dev-web
```

In this mode, every request for a static file will reload the file from disk.
Changes to standalone CSS and JS files will be reflected immediately upon
reload, without requiring a recompile!

Note that `dev-web` can only hot-reload the files in
`src/environmentd/src/static`. The HTML templates in
`src/environmentd/src/templates` use a compile-time templating library called
[askama], and so changes to those templates necessarily require a recompile.

For details about adding a new JavaScript/CSS dependency, see the comment in
[`src/environmentd/build/npm.rs`](/src/environmentd/build/npm.rs).

## Testing

Materialize's testing philosophy is sufficiently complex that it warrants its
own document. See [Developer guide: testing](guide-testing.md).

## Style

We use the following tools to perform automatic code style checks:

| Tool          | Use                                | Run locally with    |
|---------------|------------------------------------|---------------------|
| [Clippy]      | Rust semantic nits                 | `cargo clippy`      |
| [rustfmt]     | Rust code formatter                | `cargo fmt`         |
| Linter        | General formatting nits            | `bin/lint`          |
| [cargo-udeps] | Check for unused Rust dependencies | `bin/unused-deps`   |

See the [style guide](style.md) for additional recommendations on code style.

### Required Tools
Linting requires the following tools and Cargo packages to be installed:
* buf ([installation guide](https://buf.build/docs/installation))
* shellcheck ([installation guide](https://hackage.haskell.org/package/ShellCheck#installing))
* cargo-about (`cargo install cargo-about`)
* cargo-hakari (`cargo install cargo-hakari`)
* cargo-deplint (`cargo install cargo-deplint`)
* cargo-deny (`cargo install cargo-deny`)

## Submitting and reviewing changes

See [Developer guide: submitting and reviewing changes](guide-changes.md).

## Code organization

### Repository structure

This repository has the following basic structure:

  * **`bin`** contains scripts for contributor use.
  * **`ci`** contains configuration and scripts for [CI](/ci/README.md).
  * **`doc/developer`** contains documentation for Materialize contributors,
    including this document.
  * **`doc/user`** contains the user-facing documentation, which is published to
    <https://materialize.com/docs>.
  * **`misc`** contains a variety of supporting tools and projects. Some
    highlights:
    * **`misc/dbt-materialize`** contains the Materialize [dbt] adapter.
    * **`misc/python`** contains Python developer tools, like
      [mzbuild](mzbuild.md).
    * **`misc/nix`** contains an experimental [Nix] configuration for
      developing Materialize.
    * **`misc/wasm`** contains the Rust crates that are published to NPM as WebAssembly.
    * **`misc/www`** contains the source code for <https://dev.materialize.com>.
  * **`src`** contains the primary Rust crates that comprise Materialize.
  * **`test`** contains test suites, which are described in
    [Developer guide: testing](guide-testing.md).

### Rust crate structure

We break our Rust code into crates primarily to promote organization of code by
team, thereby introducing ownership and autonomy. As such, many crates are owned
by a specific team (which does not preclude the existence of shared, cross-team
crates).

Although the primary unit of code organization at the inter-team level is the
crate, modules within a crate are also useful for code organization, especially
because they are the level at which `pub` visibility operates.

We make a best-effort attempt to document the ownership of the Rust code in this
repository using GitHub's [CODEOWNERS](/.github/CODEOWNERS) file.

You can create and view a relationship diagram of our crates by running the following
command (this will require [graphviz]):

```shell
bin/crate-diagram
```

It is possible to view transitive dependencies of a select subset of roots by
specifying the `--roots` flag with a comma separated list of crates:

```shell
bin/crate-diagram --roots mz-sql,mz-dataflow
```

#### `workspace-hack`

The [`workspace-hack`](../../src/workspace-hack/) crate speeds up rebuilds by
ensuring that all crates use the same features of all transitive dependencies in
the graph. This prevents Cargo from recompiling huge chunks of the dependency
graph when you move between crates in the workspace. For details, see the
[hakari documentation].

If you add or remove dependencies on crates, you will likely need to regenerate
the `workspace-hack` crate. You can do this by running:

```
cargo install --locked cargo-hakari
cargo hakari generate
cargo hakari manage-deps
```

CI will enforce that the `workspace-hack` crate is kept up to date.

## Other repositories

Where possible, we prefer to keep things in the main repository (a "monorepo"
approach). There are a few exceptions:

  * **[demos]**, which showcases several use cases for Materialize
  * **[rust-dec]**, libdecnumber bindings for Rust
  * **[materialize-dbt-utils]**, data build tool (dbt) utilities for Materialize
  * [Several custom Pulumi providers](https://github.com/MaterializeInc?q=pulumi)

Don't add to this list without good reason! Separate repositories are
acceptable for:

  * Rapid iteration on new Materialize plugins or integrations, where the CI
    time or code quality requirements in the main repository would be
    burdensome. When the code is more stable, the repository should be
    integrated into the main Materialize repository.

  * External requirements that require a separate repository. For example,
    Pulumi providers are conventionally developed each in their own repository.
    Similarly, materialize-dbt-utils can only appear on [dbt hub] if it is
    developed in a standalone repository.

  * Stable foundational components where community contribution is desirable.
    For example, rust-dec is a very small package, and asking contributors
    to clone the entire Materialize repository would be a large barrier to
    entry. Changes to Materialize very rarely require changes in rust-dec, so
    maintaining the two separately does not introduce much overhead.

## Developer tools

### Editors and IDEs

In principle, any text editor can be used to edit Rust code.

#### Visual Studio Code

By default, we recommend that developers without a strong preference of an editor use
[Visual Studio Code] with the [rust-analyzer] plugin.
This is the most mainstream
setup for developing Materialize, and the one for which you are the most likely
to be able to get help if something goes wrong.

Visual Studio Code also works well for editing Python; to work on the Python code
in the Materialize repository, install the [official Python extension][vscode-python] from Microsoft
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

If you are using Rust-Analyzer, you should configure it to conform to our
[style guide](style.md) by setting the following options:

* `imports.granularity.group` = `module`
* `imports.prefix` = `crate`

#### RustRover

[RustRover] is another option for an IDE with good code navigation features.
This is a good choice for developers who prefer the JetBrains ecosystem. This
folder provides some [example run configurations](/misc/editor/rustrover)
to help get started with running and debugging Materialize in RustRover.

If you are a Materialize employee, ask in the #jetbrains channel on Slack for
access to a corporate JetBrains license. If you're not yet sure you want to use
RustRover, you can use the 30-day free trial.

### Editor add-ons

A few editor-specific add-ons and configurations have been authored to improve the editing of
Materialize-specific code. Check `misc/editor` for add-ons that may be relevant for your editor
of choice.

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

### Automatic style checks

To ensure each code change passes all style nits before pushing to GitHub,
symlink `pre-push` into your local git hooks:

```sh
ln -s ../../misc/githooks/pre-push .git/hooks/pre-push
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

[Apache Kafka]: https://kafka.apache.org
[Apache ZooKeeper]: https://zookeeper.apache.org
[askama]: https://github.com/djc/askama
[Bash]: https://www.gnu.org/software/bash/
[cargo-udeps]: https://github.com/est31/cargo-udeps
[Clippy]: https://github.com/rust-lang/rust-clippy
[CMake]: https://cmake.org
[Confluent CLI]: https://docs.confluent.io/current/cli/installing.html#scripted-installation
[Confluent Platform]: https://www.confluent.io/product/confluent-platform/
[Confluent Schema Registry]: https://www.confluent.io/confluent-schema-registry/
[confluent-install]: https://docs.confluent.io/current/installation/installing_cp/index.html
[Control Center]: https://www.confluent.io/confluent-control-center/
[dbt]: https://getdbt.com
[dbt hub]: https://hub.getdbt.com
[dbt-materialize]: https://github.com/materializeInc/dbt-materialize
[demos]: https://github.com/MaterializeInc/demos
[Docker Compose]: https://docs.docker.com/compose/
[github-https]: https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line
[graphviz]: https://graphviz.org/
[hakari documentation]: https://docs.rs/cargo-hakari/latest/cargo_hakari/about/index.html
[Homebrew]: https://brew.sh
[forked-cockroach-tap]: https://github.com/materializeInc/homebrew-cockroach
[Kubernetes]: https://kubernetes.io
[materialize-dbt-utils]: https://github.com/MaterializeInc/materialize-dbt-utils
[Nix]: https://nix.dev/tutorials/first-steps/ad-hoc-shell-environments
[nix-manual]: https://nix.dev/manual/nix/2.18/installation/installing-binary
[Python]: https://www.python.org
[rust-dec]: https://github.com/MaterializeInc/rust-dec
[Rust]: https://www.rust-lang.org
[rust-analyzer]: https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer
[rustfmt]: https://github.com/rust-lang/rustfmt
[rustup]: https://www.rust-lang.org/tools/install
[sqlparser]: https://github.com/MaterializeInc/sqlparser
[Python]: https://www.python.org
[vscode-python]: https://marketplace.visualstudio.com/items?itemName=ms-python.python
[Visual Studio Code]: https://code.visualstudio.com
[RustRover]: https://www.jetbrains.com/rust/
