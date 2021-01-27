---
title: "CLI"
menu: "main"
weight: 20
description: "Find out how to start the materialized binary with different configurations"
---

## Command line flags

The `materialized` binary supports the following command line flags:

Flag | Default | Modifies
-----|---------|----------
[`--address-file`](#horizontally-scaled-clusters) | N/A |  Text file containing addresses of all coordinating Materialize nodes
[`--cache-max-pending-records`](#source-cache) | 1000000 | Maximum number of input records buffered before flushing immediately to disk.
[`--data-directory`](#data-directory) | `./mzdata` | Where data is persisted
[`--differential-idle-merge-effort`](#dataflow-tuning) | N/A | *Advanced.* Amount of compaction to perform when idle.
`--help` | N/A | NOP&mdash;prints binary's list of command line flags
[`--disable-telemetry`](#telemetry) | N/A | Disables telemetry reporting.
[`--experimental`](#experimental-mode) | Disabled | *Dangerous.* Enable experimental features.
[`--listen-addr`](#listen-address) | `0.0.0.0:6875` | Materialize node's host and port
[`--logical-compaction-window`](#compaction-window) | 60s | The amount of historical detail to retain in arrangements
[`--process`](#horizontally-scaled-clusters) | 0 | This node's ID when coordinating with other Materialize nodes
[`--processes`](#horizontally-scaled-clusters) | 1 | Number of coordinating Materialize nodes
[`--timely-progress-mode`](#dataflow-tuning) | demand | *Advanced.* Timely progress tracking mode.
[`--tls-cert`](#tls-encryption) | N/A | Path to TLS certificate file
[`--tls-key`](#tls-encryption) | N/A | Path to TLS private key file
[`--workers`](#worker-threads) | NCPUs / 2 | Dataflow worker threads
[`-w`](#worker-threads) | REQ |  Dataflow worker threads
`-v` | N/A | Print version and exit
`-vv` | N/A | Print version and additional build information, and exit

If a command line flag takes an argument, you can alternatively set that flag
via an environment variable named after the flag. If both the environment
variable and command line flag are specified, the command line flag takes
precedence.

The process for converting a flag name to an environment variable name is as
follows:

  1. Convert all characters to uppercase
  2. Replace all hyphens with underscores
  3. add an `MZ_` prefix.

For example, the `--data-directory` command line flag corresponds to the
`MZ_DATA_DIRECTORY` environment variable.

Note that command line flags that do not take arguments, like `--experimental`
and `--disable-telemetry`, do not yet have corresponding environment variables.

### Data directory

Upon startup `materialized` creates a directory where it persists metadata. By
default, this directory is called `mzdata` and is situated in the current
working directory of the materialized process. Currently, only metadata is
persisted in `mzdata`. You can specify a different directory using the
`--data-directory` flag. Upon start, `materialized` checks for an existing data
directory, and will reinstall source and view definitions from it if one is
found.

### Worker threads

A `materialized` instance runs a specified number of timely dataflow worker
threads. Worker threads can only be specified at startup by setting the
`--workers` flag, and cannot be changed without shutting down `materialized`
and restarting. If `--workers` is not set, `materialized` will default to using
half of the machine's physical cores as the thread count.  In the future,
dynamically changing the number of worker threads will be possible over
distributed clusters, see
[#2449](https://github.com/MaterializeInc/materialize/issues/2449).

{{< version-changed v0.4.0 >}}
Rename the `--threads` flag to `--workers`.
{{</ version-changed >}}

{{< version-changed v0.5.1 >}}
When unspecified, default to using half of the machine's physical cores.
{{</ version-changed >}}

#### How many worker threads should you run?

Adding worker threads allows Materialize to handle more throughput. Reducing
worker threads consumes fewer resources, and reduces tail latencies.

In general, you should use the fewest number of worker threads that can handle
your peak throughputs. This is also the most resource efficient.

You should never run Materialize in a configuration greater than `n-1` workers,
where `n` is the number of _physical_ cores. Note that major cloud providers
list the number of hyperthreaded cores (or _virtual_ CPUs). Divide this number
by two to get the number of physical cores available. The reasoning is simple:
Timely Dataflow is very computationally efficient and typically uses all
available computational resources. Under high throughput, you should see each
worker pinning a core at 100% CPU, with no headroom for hyperthreading. One
additional core is required for metadata management and coordination. Timely
workers that have to fight for physical resources will only block each other.

Example: an `r5d.4xlarge` instance has 16 VCPUs, or 8 physical cores. The
recommended worker setting on this VM is `7`.

### Horizontally scaled clusters

{{< warning >}}
Note that multi-node Materialize clusters are **not** supported by Materialize,
and are not permitted in production under the free usage BSL license without a
separate commercial agreement with Materialize.
{{< /warning >}}

`--processes` controls the total number of nodes in a horizontally-scaled
Materialize cluster. The IP addresses of each node should be specified in a
file, one per line, which is specified by the `--address-file` flag.

When each node is started, it must additionally be told which `--process` it is,
from `0` to `processes - 1`.

You should not attempt running a horizontally-scaled Materialize cluster until
you have maxed-out vertical-scaling. Multi-node clusters are not particulary
efficient. An `x1.32xlarge` instance on AWS has 128 VCPUs, and will be superior
in every way (reliability, cost, ease-of-use) to a multi-node Materialize
cluster with the same total number of VCPUs. It is our performance goal that
Materialize under that configuration be able to handle every conceivable
streaming workload that you may wish to throw at it.

### Listen address

By default, `materialized` binds to `0.0.0.0:6875`. This means that Materialize
will accept any incoming SQL connection to port 6875 from anywhere. It is the
responsibility of the network firewall to limit incoming connections. If you
wish to configure `materialized` to only listen to, e.g. localhost connections,
you can set `--listen-addr` to `localhost:6875`. You can also use this to change
the port that Materialize listens on from the default `6875`.

### Compaction window

The `--logical-compaction-window` option specifies the duration of time for
which Materialize is required to maintain full historical detail in its
[arrangements](/overview/api-components#indexes). Note that compaction happens
lazily, so Materialize may retain more historical detail than requested, but it
will never retain less.

The value of the option is a duration string like `10ms` (10 milliseconds) or
`1min 30s` (1 minute, 30 seconds).  The special value `off` indicates disables
logical compaction.

The logical compaction window ends at the current time and extends backwards in
time for the configured duration. The default window is 60 seconds.

See the [Deployment section](/ops/deployment#compaction) for guidance on tuning
the compaction window.

### TLS encryption

Materialize can use Transport Layer Security (TLS) to encrypt traffic between
SQL and HTTP clients and the `materialized` server.

#### Configuration

To enable TLS, you will need to supply two files, one containing a TLS
certificate and one containing the corresponding private key. Point
`materialized` at these files using the `--tls-cert` and `--tls-key` options,
respectively:

```shell
$ materialized -w1 --tls-cert=server.crt --tls-key=server.key
```

When TLS is enabled, Materialize serves both unencrypted and encrypted traffic
over the same TCP port, as specified by [`--listen-addr`](#listen-address). The
web UI will be served over HTTPS in addition to HTTP. Incoming SQL connections
can negotiate TLS encryption at the client's option; consult your SQL client's
documentation for details.

It is not currently possible to configure Materialize to reject unencrypted
connections.

Materialize statically links against a vendored copy of [OpenSSL]. It does *not*
use any SSL library that may be provided by your system. To see the version of
OpenSSL used by a particular `materialized` binary, inquire with the `-vv` flag:

```shell
$ materialize -vv
```
```nofmt
materialized v0.2.3-dev (c62c988e8167875b92122719eee5709cf81cdac4)
OpenSSL 1.1.1g  21 Apr 2020
librdkafka v1.4.2
```

Materialize configures OpenSSL according to Mozilla's [Modern
compatibility][moz-modern] level, which requires TLS v1.3 and modern cipher
suites. Using weaker cipher suites or older TLS protocol versions is not
supported.

[moz-modern]: https://wiki.mozilla.org/Security/Server_Side_TLS#Modern_compatibility

#### Generating TLS certificates

You can generate a self-signed certificate for development use with the
`openssl` command-line tool:

```shell
$ openssl req -new -x509 -days 365 -nodes -text \
    -out server.crt -keyout server.key -subj "/CN=<SERVER-HOSTNAME>"
```

Production deployments typically should not use self-signed certificates.
Acquire a certificate from a proper certificate authority (CA) instead.

[OpenSSL]: https://www.openssl.org

### Experimental mode

{{< version-added v0.4.0 />}}

{{< warning >}}

If you want to use experimental mode, you should **really** read the section below!

{{< /warning >}}

Materialize offers access to experimental features through the `--experimental`
flag. Unlike most features in Materialize, experimental features' syntax and/or
semantics can shift at any time, and **there is no guarantee that future
versions of Materialize will be interoperable with the experimental features**.

Using experimental mode means that **you are likely to lose access to all of
your sources and views within Materialize** and will have to recreate them and
re-ingest all of your data.

Because of this volatility:
- You can only start new nodes in experimental mode.
- Nodes started in experimental mode must always be started in experimental
  mode.

We recommend only using experimental mode to explore Materialize, i.e.
absolutely never in production. If your explorations yield interesting results
or things you'd like to see changed, let us know on [GitHub][gh-feature].

#### Disabling experimental mode

You cannot disable experimental mode for a node. You can, however, extract your
view and source definitions ([`SHOW CREATE VIEW`][scv], [`SHOW CREATE SOURCE`][scs],
etc.), and then create a new node with those items.

### Source cache

The `--cache-max-pending-records` specifies the number of input messages
Materialize buffers in memory before flushing them all to disk when using
[cached sources][cache]. The default value is 1000000 messages. Note
that Materialize will also flush buffered records every 10 minutes as well. See
the [Deployment section][cache] for more guidance on how to tune this
parameter.

### Telemetry

Unless disabled with `--disable-telemetry`, upon startup `materialized`
reports its current version and cluster id to a central server operated by
materialize.com. If a newer version is available a warning will be logged.

### Dataflow tuning

{{< warning >}}
The dataflow tuning parameters are not stable. Backwards-incompatible changes
to the dataflow tuning parameters may be made at any time.
{{< /warning >}}

There are several command-line options that tune various parameters for
Materialize's underlying dataflow engine:

  * `--differential-idle-merge-effort` controls how aggressively Materialize
    will perform compaction when idle.
  * `--timely-progress-mode` sets Timely Dataflow's progress tracking mode.

Using these parameters correctly requires substantial knowledge about how
the underlying Timely and Differential Dataflow engines work. Typically you
should only set these parameters in consultation with Materialize engineers.

[gh-feature]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
[parse-duration-syntax]: https://docs.rs/parse_duration/2.1.0/parse_duration/#syntax
[scv]: /sql/show-create-view
[scs]: /sql/show-create-source
[cache]: /ops/deployment/#source-caching
