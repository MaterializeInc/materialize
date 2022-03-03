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
[`-D`](#data-directory) / [`--data-directory`](#data-directory) | `./mzdata` | Where data is persisted<br><br>**Known issue.** The short form of this option was inadvertently removed in v0.7.0. It will be restored in v0.7.1.
[`--differential-idle-merge-effort`](#dataflow-tuning) | N/A | *Advanced.* Amount of compaction to perform when idle.
`--help` | N/A | NOP&mdash;prints binary's list of command line flags
[`--disable-telemetry`](#telemetry) | N/A | Disables telemetry reporting.
[`--experimental`](#experimental-mode) | Disabled | *Dangerous.* Enable experimental features.
[`--introspection-frequency`](#introspection-sources) | 1s | The frequency at which to update [introspection sources](#introspection-sources).
[`--metrics-scraping-interval`](#prometheus-metrics) | 30s | The update interval for the `mz_metrics` table, see [prometheus metrics](#prometheus-metrics).
[`--listen-addr`](#listen-address) | `0.0.0.0:6875` | The host and port on which to listen for HTTP and SQL connections
[`-l`](#compaction-window) / [`--logical-compaction-window`](#compaction-window) | 1ms | The amount of historical detail to retain in arrangements
[`--log-file`](#log-file) | [`mzdata`](#data-directory)`/materialized.log` | Where to emit log messages
[`--log-filter`](#log-filter) | `info` | Which log messages to emit
[`--timely-progress-mode`](#dataflow-tuning) | demand | *Advanced.* Timely progress tracking mode.
[`--tls-ca`](#tls-encryption) | N/A | Path to TLS certificate authority (CA) {{< version-added v0.7.1 />}}
[`--tls-cert`](#tls-encryption) | N/A | Path to TLS certificate file
[`--tls-mode`](#tls-encryption) | N/A | How stringently to demand TLS authentication and encryption {{< version-added v0.7.1 />}}
[`--tls-key`](#tls-encryption) | N/A | Path to TLS private key file
[`-w`](#worker-threads) / [`--workers`](#worker-threads) | NCPUs / 2 | Dataflow worker threads
`-v` / `--version` | N/A | Print version and exit
`-vv` | N/A | Print version and additional build information, and exit
[`--disable-user-indexes`](#disable-user-indexes) | Disabled | Start without creating any dataflows for user indexes.

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

If the same command line flag is specified multiple times, the last
specification takes precedence.

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
[arrangements][api-indexes]. Note that compaction happens
lazily, so Materialize may retain more historical detail than requested, but it
will never retain less.

The value of the option is any valid SQL [interval](/sql/types/interval)
string, like `10ms` (10 milliseconds) or `1min 30s` (1 minute, 30
seconds). The special value `off` disables logical compaction and
corresponds to an unboundedly large duration.

The logical compaction window ends at the current time and extends backwards in
time for the configured duration. The default window is 1 millisecond.

See the [Deployment section](/ops/memory#compaction) for guidance on tuning
the compaction window.

### Logging

#### Log file

The `--log-file` option specifies the path to a file in which Materialize will
write its [log messages](/ops/monitoring#logging). The value `stderr` is treated
specially and specifies the standard error stream.

If the option is unspecified, Materialize writes log messages to the
`materialized.log` file in the [data directory](#data-directory) and
additionally forwards any log messages at the `WARN` or `ERROR` levels to the
standard error stream. Forwarding does not occur if you explicitly specify a log
file.

#### Log filter

{{< version-added v0.7.2 />}}

The `--log-filter` option specifies which [log
messages](/ops/monitoring#logging) Materialize will emit. Its value is a
comma-separated list of filter directives. Each filter directive has the
following format:

```
[module::path=]level
```

A filter directive registers interest in log messages from the specified module
that are at least as severe as the specified level. If a directive omits the
module, then it implicitly applies to all modules. When directives conflict, the
last directive wins. Materialize will only emit log messages that match at least
one filter directive.

Specifying module paths in filter directives requires familiarity with
Materialize's codebase and is intended for advanced users.

The valid levels for a log message are documented in the [logging
section](/ops/monitoring/#levels) of the monitoring documentation and are not
case sensitive. The special level `off` may be used in a directive to suppress
all log messages, even those at the `error` level.

As an example, the following filter specifies the `TRACE` level for the `pgwire`
module, which handles SQL network connections, and the `INFO` level for all
other modules.

```
pgwire=trace,info
```

### Introspection sources

{{< version-changed v0.7.1 >}}
In prior versions of Materialize, this option was undocumented but available
under the name `--logging-granularity`.
{{< /version-changed >}}

Materialize maintains several built-in sources and views in
[`mz_catalog`](/sql/system-catalog) that describe the internal state of the
dataflow execution layer, like `mz_scheduling_elapsed`.

The `--introspection-frequency` option determines the frequency at which the
base sources are updated. The default frequency is `1s`. To disable
introspection entirely, use the special value `off`.

Higher frequencies provide more up-to-date introspection but increase load on
the system. Lower frequencies increase staleness in exchange for decreased load.
The default frequency is a good choice for most deployments.

### Prometheus metrics

{{< version-changed v0.9.1 >}}
In prior versions of Materialize, the metrics scraping interval was linked to
the introspection interval.
{{< /version-changed >}}

The `--metrics-scraping-interval` option determines the interval at which the
prometheus metrics are collected to update the `mz_metrics` table. The default
interval is `30s`. To disable prometheus metrics collection entirely, use the
special value `off`.

Lower intervals provide more up-to-date metrics but increase load on
the system. Higher intervals increase staleness in exchange for decreased load.
The default interval is a good choice for most deployments.

{{< version-changed v0.7.3 >}}
Materialize imports its own [Prometheus metrics](/ops/monitoring#prometheus)
into the systems tables `mz_metrics` (counters and gauge readings),
`mz_metric_histograms` (histogram distributions) and `mz_metrics_meta` (type
information and help for each metric). These readings are imported once per
`--metrics-scraping-interval` period, and are retained for the duration given with
`--retain-prometheus-metrics` (defaulting to 5 minutes). Higher retention
periods lead to greater memory usage.
{{< /version-changed >}}

### TLS encryption

Materialize can use Transport Layer Security (TLS) to:

 * Encrypt traffic between SQL and HTTP clients and the `materialized` server
 * Authenticate SQL and HTTP clients

{{< version-added v0.7.1 >}}
The `--tls-mode` and `--tls-ca` options.
{{< /version-changed >}}

#### Configuration

Whether Materialize requires TLS encryption or authentication is determined by
the value of the `--tls-mode` option:

Value         | Description
--------------|------------
`disable`     | Disables TLS.<br><br>Materialize will reject HTTPS connections and SQL connections that negotiate TLS. This is the default mode if `--tls-cert` is not specified.
`require`     | Requires TLS encryption.<br><br>Materialize will reject HTTP connections and SQL connections that do not negotiate TLS.
`verify-ca`   | Like `require`, but additionally requires that clients present a certificate.<br><br>Materialize verifies that the client certificate is issued by the certificate authority (CA) specified by the `--tls-ca` option.
`verify-full` | Like `verify-ca`, but the Common Name (CN) field of the client certificate additionally determines the user who is connecting.<br><br>For HTTPS connections, this user is taken directly from the CN field. For SQL connections, the name of the user in the connection parameters must match the name specified in the CN field.<br><br>This is the default mode if `--tls-cert` is specified.

In all TLS modes but `disable`, you will need to supply two files, one
containing a TLS certificate and one containing the corresponding private key.
Point `materialized` at these files using the `--tls-cert` and `--tls-key`
options, respectively.

If the TLS mode is `verify-ca` or `verify-full`, you will additionally need to
supply the path to a TLS certificate authority (CA) via the `--tls-ca` flag.
Client certificates will be verified using this CA.

The following example demonstrates how to configure a server in `verify-full`
mode:

```shell
$ materialized -w1 --tls-cert=server.crt --tls-key=server.key --tls-ca=root.crt
```

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

Materialize configures OpenSSL according to Mozilla's [Intermediate
compatibility][moz-intermediate] level, which requires TLS v1.2+ and recent
cipher suites. Using weaker cipher suites or older TLS protocol versions is not
supported.

[moz-intermediate]: https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29

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
- You can only initialize new servers in experimental mode.
- Servers started in experimental mode must always be started in experimental
  mode.

We recommend only using experimental mode to explore Materialize, i.e.
absolutely never in production. If your explorations yield interesting results
or things you'd like to see changed, let us know on [GitHub][gh-feature].

#### Disabling experimental mode

You cannot disable experimental mode for a server. You can, however, extract your
view and source definitions ([`SHOW CREATE VIEW`][scv], [`SHOW CREATE SOURCE`][scs],
etc.), and then create a new server with those items.

### Telemetry

Materialize periodically communicates with `telemetry.materialize.com` to report
usage data and check for new versions. You can opt out of this communication
with the `--disable-telemetry` flag.

We record the following data:

* Public IP of the host running Materialize
* Cluster ID, a unique ID which is persistent across Materialize restarts
* Session ID, a unique ID which is reset on each Materialize restart
* Materialize version
* Number of worker threads
* Uptime
* Count of sinks, sources, and views by type

We use this data to guide our product roadmap. Unless you are using [Materialize
Cloud](/cloud/), we do not and cannot correlate this
data to your identity.

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

### Disable user indexes

{{< version-added v0.9.2 />}}

{{< warning >}}
This feature is primarily meant for advanced administrators of Materialize.
{{< /warning >}}

If you cannot boot a Materialize server because it runs out of memory, you can use
the `--disable-user-indexes` to prevent Materialize from creating any
[indexes][api-indexes] on user-created objects. For
example, if you add a view that contains a cross join that causes your server to
immediately run out of memory on boot, you can use `--disable-user-indexes` to
boot the server and then drop the offending view.

{{% troubleshooting/disable-user-indexes %}}

## Special environment variables

Materialize respects several environment variables that have conventional
meanings in Unix systems.

### HTTP proxies

The `http_proxy`, `https_proxy`, `all_proxy`, and `no_proxy` environment
variables specify a proxy to use for outgoing HTTP and HTTPS traffic. There is
no precise specification of how these variables behave, but Materialize's
behavior generally matches the behavior of other HTTP clients.

For precise details of Materialize's behavior, consult the documentation of
the [`mz_http_proxy`](https://docs.rs/mz_http_proxy) crate.

[api-indexes]: /overview/api-components#indexes
[gh-feature]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
[scv]: /sql/show-create-view
[scs]: /sql/show-create-source
[sys-cat]: /sql/system-catalog
