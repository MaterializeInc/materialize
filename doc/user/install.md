---
title: "Install"
description: "Get started with Materialize"
menu: "main"
weight: 2
---

You can access Materialize through the `materialized` binary, which you can
install on macOS and Linux. These instructions install the latest release of
Materialize, **{{< version >}}**. For prior releases, see the [Versions
page](../versions).

**Note**: We have unofficial support for other operating systems, e.g. FreeBSD, if you [build from source](#build-from-source).

## macOS installation

### Homebrew

Assuming you've installed [Homebrew](https://brew.sh/):

```shell
brew install MaterializeInc/materialize/materialized
```

### curl

```shell
curl -L https://downloads.mtrlz.dev/materialized-{{< version >}}-x86_64-apple-darwin.tar.gz | tar -xzC /usr/local --strip-components=1
```

## Linux installation

### apt (Ubuntu, Debian, or variants)

```shell
# Add the signing key for the Materialize apt repository
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 379CE192D401AB61
# Add and update the repository
apt-add-repository 'deb http://packages.materialize.io/apt/ /'
apt update
# Install materialized
apt install materialized
```

### curl
```shell
curl -L https://downloads.mtrlz.dev/materialized-{{< version >}}-x86_64-unknown-linux-gnu.tar.gz | tar -xzC /usr/local --strip-components=1
```

## Build from source

Materialize is written in Rust and requires a recent Rust toolchain to build
from source. Follow [Rust's getting started
guide](https://www.rust-lang.org/learn/get-started) if you don't already have
Rust installed.

Then, to build your own `materialized` binary, you can clone the
[`MaterializeInc/materialize` repo from GitHub](https://github.com/MaterializeInc/materialize),
and build it using `cargo build`. Be sure to check out the tag for the correct
release.

```shell
git clone https://github.com/MaterializeInc/materialize.git
cd materialize
git checkout {{< version >}}
cargo build
```

## Run the binary

You can start the `materialized` process by simply running the binary, e.g.

```nofmt
./materialized
```

By default `materialized` uses:

Detail | Info
----------|------
**Database** | `materialize`
**Port** | `6875`

For more information, see [CLI Connections](../connect/cli/).

## Up next

With `materialized` installed, let's [get started](../get-started).
