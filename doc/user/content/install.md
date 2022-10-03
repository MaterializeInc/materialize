---
title: "Install Materialize"
description: "Install the Materialize binary"
menu:
  main:
    parent: quickstarts
weight: 1
aliases:
 - /install
---

Materialize was first built as a single binary that runs on a single node: `materialized`. To support mission-critical deployments at any scale, we are now evolving the binary into a [cloud-native platform](https://materialize.com/blog/next-generation/) with built-in horizontal scaling, active replication and decoupled storage.

You can [sign up for early access](https://materialize.com/materialize-cloud-access/) to learn more about the new architecture, and in the meantime use these instructions to install the latest long-term support (LTS) release of Materialize, **{{< version >}}**. Once Materialize is generally available in the cloud (Fall 2022 🚀), Docker will be supported for local development and testing.

{{< warning >}}
Support for the ARM CPU architecture is in beta. You may encounter performance
and stability issues. Running Materialize on ARM in production is not yet
recommended.
{{< /warning >}}

## Docker

We provide the `materialize/materialized` image in Docker Hub. If you already have
[Docker][docker-start] installed, you can run materialized with your tools in the usual
way. For example:

```shell
docker run -p 6875:6875 materialize/materialized:{{< version >}} --workers 1
```

### Using Docker volumes

To persist Materialize metadata, you can create a Docker volume and mount it to the [`/mzdata` directory](/cli/#data-directory) in the container:

```shell
# Create a volume
docker volume create --name mzdata
# Create a container with the volume mounted
docker run -v mzdata:/mzdata -p 6875:6875 materialize/materialized:{{< version >}}
```

## macOS installation

### Homebrew

Assuming you've installed [Homebrew](https://brew.sh/):

```shell
brew install MaterializeInc/materialize/materialized
```

### curl

```shell
curl -L https://binaries.materialize.com/materialized-{{< version >}}-$(uname -m)-apple-darwin.tar.gz \
    | sudo tar -xzC /usr/local --strip-components=1
```

## Linux installation

### apt (Ubuntu, Debian, or variants)

{{< note >}}
These instructions changed between versions 0.8.0 and 0.8.1. If you ran them
previously, you may need to do so again to continue receiving updates.
{{</ note >}}


```shell
# Add the signing key for the Materialize apt repository
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 79DEC5E1B7AE7694
# Add and update the repository
sudo sh -c 'echo "deb http://apt.materialize.com/ generic main" > /etc/apt/sources.list.d/materialize.list'
sudo apt update
# Install materialized
sudo apt install materialized
```

### apt DEB822 (Ubuntu jammy/22.04+, Debian bookworm/testing+)

For Debian-based distributions with apt version `2.3.10` or later, we offer a DEB822-compliant sources file to install Materialize:

```shell
# Add and update the repository
curl https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
sudo apt update
# Install materialized
sudo apt install materialized
```

### curl

```shell
curl -L https://binaries.materialize.com/materialized-{{< version >}}-$(uname -m)-unknown-linux-gnu.tar.gz \
    | sudo tar -xzC /usr/local --strip-components=1
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
cargo build --release --bin materialized
```

## Run the binary

You can start the `materialized` process by running the binary, e.g.

```nofmt
./materialized --workers 1
```

`--workers 1` specifies that the process will use 1 worker. You can also find more detail
about our [command line flags](/cli/#command-line-flags).

By default `materialized` uses:

Detail | Info
----------|------
**Database** | `materialize`
**Port** | `6875`

### `systemd` service

If you've installed Materialize via [`apt`](#apt-ubuntu-debian-or-variants), you can start it as a service by running:

```shell
systemctl start materialized.service
```

To enable the service to start up at boot, run:

```shell
systemctl enable materialized.service
```

## CLI Connections

To connect to a running instance, you can use any [Materialize-compatible CLI](/integrations/psql/),
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
