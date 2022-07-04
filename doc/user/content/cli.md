---
title: "materialized service configuration reference"
weight: 20
description: "Find out how to start the materialized binary with different configurations"
menu:
  main:
    parent: reference
    weight: 200
    name: "`materialized` binary"
---

## Command line flags

The `materialized` binary supports the following command line flags:

Flag | Default | Modifies
-----|---------|----------
[`-D`](#data-directory) / [`--data-directory`](#data-directory) | `./mzdata` | Where data is persisted.
`--help` | N/A | NOP&mdash;prints binary's list of command line flags.
[`--listen-addr`](#listen-address) | `0.0.0.0:6875` | The host and port on which to listen for HTTP and SQL connections.
[`--log-filter`](#log-filter) | `info` | Which log messages to emit.
[`--tls-ca`](#tls-encryption) | N/A | Path to TLS certificate authority (CA).
[`--tls-cert`](#tls-encryption) | N/A | Path to TLS certificate file.
[`--tls-mode`](#tls-encryption) | N/A | How stringently to demand TLS authentication and encryption.
[`--tls-key`](#tls-encryption) | N/A | Path to TLS private key file.
[`-w`](#worker-threads) / [`--workers`](#worker-threads) | NCPUs / 2 | Dataflow worker threads.
`-v` / `--version` | N/A | Print version and exit.
`-vv` | N/A | Print version and additional build information, and exit.

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

By default, `materialized` binds to `127.0.0.1:6875`. This means that
Materialize will accept any incoming SQL connection to port 6875 from only the
local machine. If you wish to configure `materialized` to accept connections
from anywhere, you can set `--listen-addr` to `0.0.0.0:6875`. You can
also use this to change the port that Materialize listens on from the default
`6875`.

The `materialized` [Docker image](/install/#docker) instead uses a listen
address of `0.0.0.0:6875` by default, in accordance with Docker conventions.

### Log filter

The `--log-filter` option specifies which log messages Materialize will emit.
Its value is a comma-separated list of filter directives. Each filter directive
has the following format:

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

The valid levels for a log message are, in order of decreasing severity,
`error`, `warn`, `info`, `debug`, and `trace`. The special level `off` may be
used in a directive to suppress all log messages, even those at the `error`
level.

As an example, the following filter specifies the `trace` level for the `pgwire`
module, which handles SQL network connections, and the `info` level for all
other modules.

```
pgwire=trace,info
```

### Introspection sources

Materialize maintains several built-in sources and views in
[`mz_catalog`](/sql/system-catalog) that describe the internal state of the
dataflow execution layer, like `mz_scheduling_elapsed`.

The `--introspection-frequency` option determines the frequency at which the
base sources are updated. The default frequency is `1s`. To disable
introspection entirely, use the special value `off`.

Higher frequencies provide more up-to-date introspection but increase load on
the system. Lower frequencies increase staleness in exchange for decreased load.
The default frequency is a good choice for most deployments.

### TLS encryption

Materialize can use Transport Layer Security (TLS) to:

 * Encrypt traffic between SQL and HTTP clients and the `materialized` server
 * Authenticate SQL and HTTP clients

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
$ materialized --workers 1 --tls-cert=server.crt --tls-key=server.key --tls-ca=root.crt
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

#### Generating TLS certificates

You can generate a self-signed certificate for development use with the
`openssl` command-line tool:

```shell
$ openssl req -new -x509 -days 365 -nodes -text \
    -out server.crt -keyout server.key -subj "/CN=<SERVER-HOSTNAME>"
```

Production deployments typically should not use self-signed certificates.
Acquire a certificate from a proper certificate authority (CA) instead.

[api-indexes]: /overview/key-concepts/#indexes
[gh-feature]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
[moz-intermediate]: https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28recommended.29
[OpenSSL]: https://www.openssl.org
[scv]: /sql/show-create-view
[scs]: /sql/show-create-source
[sys-cat]: /sql/system-catalog
