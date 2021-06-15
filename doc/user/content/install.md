---
title: "Install"
description: "Get started with Materialize"
menu: "main"
weight: 2
---

{{< promo >}}

[Want to connect with Materialize? Join our growing community on Slack! →](https://materialize.com/s/chat)

{{< /promo >}}

You can access Materialize through the `materialized` binary, which you can
install on macOS and Linux, or [build](#build-from-source) on most OSes (e.g. FreeBSD). These
instructions install the latest release of Materialize, **{{< version >}}**. The latest (unstable)
developer builds are available at https://mtrlz.dev/. For prior releases,
see the [Versions page](/versions).

**Have any questions?** [Contact us](https://materialize.com/contact/)

## Docker

We provide the `materialize/materialized` image in Docker Hub. If you already have
[Docker][docker-start] installed, you can run materialized with your tools in the usual
way. For example:

```shell
docker run -p 6875:6875 materialize/materialized:{{< version >}} --workers 1
```

[docker-start]: https://www.docker.com/get-started

## macOS installation

### Homebrew

Assuming you've installed [Homebrew](https://brew.sh/):

```shell
brew install MaterializeInc/materialize/materialized
```

### curl

```shell
curl -L https://downloads.mtrlz.dev/materialized-{{< version >}}-x86_64-apple-darwin.tar.gz \
    | tar -xzC /usr/local --strip-components=1
```

## Linux installation

### apt (Ubuntu, Debian, or variants)

Run the following commands as root.

```shell
# Add the signing key for the Materialize apt repository
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 379CE192D401AB61
# Add and update the repository
sh -c 'echo "deb http://packages.materialize.io/apt/ /" > /etc/apt/sources.list.d/materialize.list'
apt update
# Install materialized
apt install materialized
```

### curl
```shell
curl -L https://downloads.mtrlz.dev/materialized-{{< version >}}-x86_64-unknown-linux-gnu.tar.gz \
    | tar -xzC /usr/local --strip-components=1
```

## Build from source

Materialize is written primarily in Rust, but incorporates several components
written in C. To build Materialize, you will need to acquire the following
tools:

  * A recent version of Git

  * A C compiler that supports C11

  * CMake v3.2+

  * Rust v{{< rust-version >}}+

Your system's package manager, like Homebrew on macOS or APT on Debian, likely
contains sufficiently recent versions of Git, a C compiler, and CMake. However,
we recommend installing Rust via [rustup]. rustup configures your system so that
running `cargo build` in the Materialize repository will automatically download
and use the correct version of Rust.

{{< warning >}}
Materialize requires a very recent version of Rust. The version of Rust
available in your package manager is likely too old.
{{< /warning >}}

Once you've installed the prerequisites, to build your own `materialized`
binary, you can clone the [`MaterializeInc/materialize` repo from
GitHub][mz-repo], and build it using `cargo build`. Be sure to check out the tag
for the correct release.

```shell
git clone https://github.com/MaterializeInc/materialize.git
cd materialize
git checkout {{< version >}}
cargo build --release
```

## Run the binary

You can start the `materialized` process by simply running the binary, e.g.

```nofmt
./materialized -w 1
```

`-w 1` specifies that the process will use 1 worker. You can also find more detail
about our [command line flags](/cli/#command-line-flags).

By default `materialized` uses:

Detail | Info
----------|------
**Database** | `materialize`
**Port** | `6875`

For more information, see [CLI Connections](/connect/cli/).

{{< cta href="/get-started" >}}
Next, let's get started with Materialize →
{{</ cta >}}

[Rustup]: https://rustup.rs
[mz-repo]: https://github.com/MaterializeInc/materialize
