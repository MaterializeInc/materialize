---
title: "Install"
description: "Get started with Materialize"
menu: "main"
weight: 2
---

You can access Materialize through the `materialized` binary, which you can install on macOS and Linux.

**Note**: We have unofficial support for other operating systems, e.g. BSD, if you [build from source](#build-from-source).

## macOS installation

### Homebrew

Assuming you've installed [Homebrew](https://brew.sh/):

```shell
brew install MaterializeInc/materialize/materialized
```

### curl

For `v0.1.0`, this method only works on macOS Catalina (10.15).

```shell
curl -L https://downloads.mtrlz.dev/materialized-latest-x86_64-apple-darwin.tar.gz | tar -xzC /usr/local --strip-components=1
```

## Linux installation

### apt

```shell
echo "deb [trusted=yes] https://packages.materialize.io/apt/ /" > /etc/apt/sources.list.d/materialize.list
apt update
apt install materialized
```

### curl
```shell
curl -L https://downloads.mtrlz.dev/materialized-latest-x86_64-unknown-linux-gnu.tar.gz | tar -xzC /usr/local --strip-components=1
```

## Build from source

Materialize is written in Rust, and relies on `cargo` to build binaries.

To build your own `materialized` binary, you can clone the [`MaterializeInc/materialize` repo from GitHub](https://github.com/materializeinc/materialize), and build it using `cargo build`.

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
