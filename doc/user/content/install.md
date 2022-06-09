---
title: "Install Materialize"
description: "Install the Materialize development environment"
menu:
  main:
    parent: quickstarts
weight: 1
aliases:
 - /install
---

{{< cta target="_blank" full_width="true" href="https://materialize.com/s/chat" >}}
Want to connect with Materialize? Join our growing community on Slack! →
{{< /cta >}}

You can install the Materialize development environment on macOS and Linux, or
[build](#build-from-source) on most OSes (e.g. FreeBSD).

{{< warning >}}
Support for the ARM CPU architecture is in beta. You may encounter performance
and stability issues. Running Materialize on ARM in production is not yet
recommended.
{{< /warning >}}

## Docker

We provide the `materialize/materialized` image in Docker Hub. If you already have
[Docker][docker-start] installed, you can run `materialized` with your tools in the usual
way. For example:

```shell
docker run -p 6875:6875 materialize/materialized:{{< version >}}
```

### Using Docker volumes

To persist Materialize metadata, you can create a Docker volume and mount it to the [`/mzdata` directory](/cli/#data-directory) in the container:

```shell
# Create a volume
docker volume create --name mzdata
# Create a container with the volume mounted
docker run -v mzdata:/mzdata -p 6875:6875 materialize/materialized:{{< version >}}
```

## Build from source

Materialize is written primarily in Rust, but incorporates several components
written in C. To build Materialize, you will need the following tools:

  * A recent version of Git

  * A C compiler that supports C11

  * A C++ compiler that supports C++11

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
cargo build --release --bin storaged --bin computed --bin materialized
```

### Run the binary

You can start the `materialized` process by running the binary, e.g.

```nofmt
./target/release/materialized
```

By default `materialized` uses:

Detail | Info
----------|------
**Database** | `materialize`
**Port** | `6875`

## CLI Connections

To connect to a running instance, you can use any Materialize-compatible CLI,
like `psql`. For an overview of compatible SQL clients and their current level of support, check out [Tools and Integrations](/integrations/#sql-clients).

{{< tabs >}}
{{< tab "Docker">}}

For Docker environments, we provide the [`materialize/cli` image](https://hub.docker.com/r/materialize/cli), which bundles `psql` and can be used to spin up a minimal `docker-compose` setup:

```yaml
services:
  materialized:
    image: materialize/materialized:{{< version >}}
    ports:
      - "6875:6875"
  cli:
    image: materialize/cli:{{< version >}}
```

{{< /tab >}}

{{< tab "macOS">}}

To install the `psql` client:

```shell
brew install postgresql
```

Once `psql` is installed, connect using:

```shell
psql -U materialize -h localhost -p 6875 materialize
```

{{< /tab >}}

{{< tab "Linux">}}

To install the `psql` client:

```shell
apt install postgresql-client
```

Once `psql` is installed, connect using:

```shell
psql -U materialize -h localhost -p 6875 materialize
```

{{< /tab >}}

{{< /tabs >}}

<p>

{{< cta href="/get-started" >}}
Next, let's get started with Materialize! →
{{</ cta >}}

[docker-start]: https://www.docker.com/get-started
[Rustup]: https://rustup.rs
[mz-repo]: https://github.com/MaterializeInc/materialize
