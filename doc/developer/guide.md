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

[Rust]: https://www.rust-lang.org

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

The [Confluent Platform] bundles [Apache ZooKeeper] and [Apache Kafka] with
several non-free Confluent tools, like the [Confluent Schema Registry] and
[Control Center]. For local development, the [Confluent CLI] allows easy
management of these services

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

As of Feb 24, 2020 you can run:

```shell
curl -L --http1.1 https://cnfl.io/cli | sh -s -- -b /usr/local/bin
```

However, if this ever stops working, check out these great docs on the
[Confluent CLI].


[Apache ZooKeeper]: https://zookeeper.apache.org
[Apache Kafka]: https://kafka.apache.org
[Confluent Schema Registry]: https://www.confluent.io/confluent-schema-registry/
[Confluent Platform]: https://www.confluent.io/product/confluent-platform/
[Control Center]: https://www.confluent.io/confluent-control-center/
[Confluent CLI]: https://docs.confluent.io/current/cli/installing.html#scripted-installation

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

**Note:** As of August 2020, we're laying the groundwork to phase out symbiosis
mode with [tables](https://materialize.io/docs/sql/create-table). But we're
stuck with symbiosis mode until tables support `UPDATE` and `DELETE`—at the time
of writing they only support [`INSERT`](https://materialize.io/docs/sql/insert).

## Testing

Materialize's testing philosophy is sufficiently complex that it warrants its
own document. See [Developer guide: testing](guide-testing.md).

## Style

CI performs the lawful evil task of ensuring "good code style" with the
following tools:

Tool      | Use                     | Run locally with
----------|-------------------------|-------------------
[Clippy]  | Rust semantic nits      | `./bin/check`
[rustfmt] | Rust code formatter     | `cargo fmt`
Linter    | General formatting nits | `./bin/lint`

See also the [style guide](style.md) for some additional soft
recommendations.

[Clippy]: https://github.com/rust-lang/rust-clippy
[rustfmt]: https://github.com/rust-lang/rustfmt

## Code review

## Other repositories

Several components of Materialize are maintained in separate Git repositories.
Where possible, we prefer to keep things in the main repository (a "monorepo"
approach), but when forking existing packages, maintaining a separate repository
makes it easier to integrate changes from upstream.

Some notable repositories include:

  * **[mtrlz-setup]**, containing automatic development environment setup
    scripts;
  * **[rust-sasl]**, Cyrus SASL bindings for Rust
  * **[rust-krb5-src]**, Rust build system integration for libkrb5, MIT's
    Kerberos implementation.

As mentioned before, because the MaterializeInc organization requires two-factor
authentication (2FA), to clone these repositories you'll need to use either SSH
or [configure a personal access token for use with HTTPS][github-https].

[mtrlz-setup]: https://github.com/MaterializeInc/mtrlz-setup
[rust-sasl]: https://github.com/MaterializeInc/rust-sasl
[rust-krb5-src]: https://github.com/MaterializeInc/rust-krb5-src
[sqlparser]: https://github.com/MaterializeInc/sqlparser
